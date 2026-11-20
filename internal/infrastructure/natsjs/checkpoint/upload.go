package checkpoint

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"hash"
	"io"
	"slices"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/DmytroLysenko1/faultline/internal/application/processing"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs"
	"github.com/DmytroLysenko1/faultline/pkg/retry"
)

const (
	chunkSize      = 2 << 20
	burstChunks    = 2
	chunkAttempt   = 5 * time.Second
	chunkFirstWait = 100 * time.Millisecond
	chunkMaxWait   = time.Second
	stopReserve    = time.Second
	chunkIDFormat  = "%s.%d"
)

var (
	errForeignAck   = errors.New("checkpoint chunk acknowledged by another stream or out of order")
	errNoTimeLeft   = errors.New("too little time left before the stop deadline to send a chunk")
	errBucketFull   = errors.New("checkpoint image does not fit the object bucket")
	errBucketUnread = errors.New("checkpoint object bucket could not be read")
)

type object struct {
	name      string
	nuid      string
	meta      processing.CheckpointMeta
	size      int64
	chunks    uint32
	digest    string
	lastSeq   uint64
	chunkSize uint32
	took      time.Duration
	verified  time.Duration
}

func newObject(written *spool, chunk uint32) *object {
	return &object{
		name:      fmt.Sprintf("%d%s%d", written.meta.Epoch, nameSeparator, written.meta.Position.Sequence),
		nuid:      rand.Text(),
		meta:      written.meta,
		chunkSize: chunk,
	}
}

func (o *object) ref() *objectRef {
	return &objectRef{
		Name:     o.name,
		NUID:     o.nuid,
		Digest:   o.digest,
		Size:     unsigned(o.size),
		Position: o.meta.Position.Sequence,
	}
}

func (o *object) acknowledged(ack *jetstream.PubAck, stream string) error {
	if ack.Stream != stream || ack.Sequence <= o.lastSeq {
		return fmt.Errorf("%w: chunk %d of %s by %q at %d after %d", errForeignAck, o.chunks, o.name, ack.Stream,
			ack.Sequence, o.lastSeq)
	}
	o.lastSeq = ack.Sequence
	o.chunks++
	return nil
}

func (u *uploader) cycle(root, upload context.Context, next *spool) error {
	if !u.store.lease.Valid() {
		return skipped(skipLeaseSuspended, errLeaseLost)
	}
	if err := u.fits(upload, next.size); err != nil {
		return err
	}
	budgeted, cancel := context.WithTimeout(upload, u.uploadBudget(next.size))
	defer cancel()
	written, err := u.send(root, budgeted, next)
	if err != nil {
		return err
	}
	if err := u.check(budgeted, upload, written); err != nil {
		return u.dropped(root, written, err)
	}
	return u.commit(root, written)
}

func (u *uploader) fits(upload context.Context, size int64) error {
	used, err := u.bucketBytes(upload)
	if err != nil {
		return u.classified(failed(failBucketRead, fmt.Errorf("%w: %w", errBucketUnread, err)))
	}
	u.store.stats.bucketBytes.Store(used)
	if used+unsigned(size) > unsigned(u.store.options.MaxBytes) {
		return skipped(skipBucketFull, fmt.Errorf("%w: %d bytes, the bucket holds %d of %d", errBucketFull, size, used,
			u.store.options.MaxBytes))
	}
	return nil
}

func (u *uploader) send(root, ctx context.Context, next *spool) (*object, error) {
	started := time.Now()
	written := newObject(next, u.store.chunkBytes)
	err := u.sendChunks(ctx, next, written)
	if err == nil {
		err = u.sendMeta(ctx, written)
	}
	if err != nil {
		return nil, u.dropped(root, written, err)
	}
	written.took = time.Since(started)
	return written, nil
}

func (u *uploader) uploadBudget(size int64) time.Duration {
	return transfer(size, u.store.options.UploadRate) + uploadSlack
}

func (u *uploader) sendChunks(ctx context.Context, next *spool, written *object) error {
	file, err := u.store.openSpool(next.name)
	if err != nil {
		return err
	}
	sending := &chunkSender{
		up:     u,
		object: written,
		digest: sha256.New(),
		pace:   newPace(u.store.options.UploadRate, burstChunks*int64(u.store.chunkBytes), time.Now()),
	}
	return errors.Join(sending.from(ctx, file), file.Close())
}

type chunkSender struct {
	up     *uploader
	object *object
	digest hash.Hash
	pace   *pace
}

func (c *chunkSender) from(ctx context.Context, spooled io.Reader) error {
	buffer := make([]byte, c.up.store.chunkBytes)
	for more := true; more; {
		data, last, err := readChunk(spooled, buffer)
		if err != nil {
			return err
		}
		if err := c.chunk(ctx, data); err != nil {
			return err
		}
		more = !last
	}
	c.object.digest = jetstream.GetObjectDigestValue(c.digest)
	return nil
}

func readChunk(spooled io.Reader, buffer []byte) ([]byte, bool, error) {
	read, err := io.ReadFull(spooled, buffer)
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return buffer[:read], true, nil
	}
	return buffer[:read], false, err
}

func (c *chunkSender) chunk(ctx context.Context, data []byte) error {
	if len(data) == 0 {
		return nil
	}
	if !c.up.store.lease.Valid() {
		return skipped(skipLeaseSuspended, errLeaseLost)
	}
	if err := retry.Sleep(ctx, c.pace.reserve(time.Now(), int64(len(data)))); err != nil {
		return err
	}
	c.digest.Write(data)
	c.object.size += int64(len(data))
	ack, err := c.up.publish(ctx, &nats.Msg{Subject: c.up.store.chunkSubject(c.object.nuid), Data: data},
		jetstream.WithMsgID(fmt.Sprintf(chunkIDFormat, c.object.nuid, c.object.chunks)))
	if err != nil {
		return err
	}
	return c.object.acknowledged(ack, c.up.store.buckets.object)
}

func (u *uploader) publish(
	ctx context.Context,
	msg *nats.Msg, options ...jetstream.PublishOpt,
) (*jetstream.PubAck, error) {
	options = append(options, jetstream.WithExpectStream(u.store.buckets.object), jetstream.WithRetryAttempts(0))
	var ack *jetstream.PubAck
	err := retry.Do(ctx, chunkPolicy(), func(attempt context.Context) error {
		sent, cancel, err := u.attemptContext(attempt)
		if err != nil {
			return err
		}
		defer cancel()
		ack, err = u.store.buckets.data.PublishMsg(sent, msg, options...)
		return err
	})
	return ack, err
}

func (u *uploader) attemptContext(attempt context.Context) (context.Context, context.CancelFunc, error) {
	deadline, _ := attempt.Deadline()
	if stopAt, stopping := u.stopDeadline(); stopping {
		deadline = earlier(deadline, stopAt.Add(-stopReserve))
	}
	if !time.Now().Before(deadline) {
		return nil, nil, errNoTimeLeft
	}
	// An attempt on its way to the server is not canceled with its caller; only its own deadline ends it.
	sent, cancel := context.WithDeadline(context.WithoutCancel(attempt), deadline)
	return sent, cancel, nil
}

func earlier(a, b time.Time) time.Time {
	if b.Before(a) {
		return b
	}
	return a
}

func chunkPolicy() *retry.Policy {
	return &retry.Policy{
		Attempt:   chunkAttempt,
		FirstWait: chunkFirstWait,
		MaxWait:   chunkMaxWait,
		Retryable: chunkRetryable,
	}
}

func chunkRetryable(err error) bool {
	return slices.Contains([]natsjs.Reason{
		natsjs.ReasonNoResponders,
		natsjs.ReasonDisconnected,
		natsjs.ReasonTimeout,
		natsjs.ReasonInProcess,
	}, natsjs.Classify(err).Reason)
}
