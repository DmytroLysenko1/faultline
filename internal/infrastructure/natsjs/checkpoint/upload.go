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
	at        time.Time
	final     bool
	metaSent  bool
}

func newObject(written *spool, chunk uint32) *object {
	return &object{
		name:      fmt.Sprintf("%d%s%d", written.meta.Epoch, nameSeparator, written.meta.Position.Sequence),
		nuid:      rand.Text(),
		meta:      written.meta,
		chunkSize: chunk,
		final:     written.final,
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

func (u *uploader) cycle(upload, control context.Context, next *spool) error {
	u.size = next.size
	u.progress = progress{}
	if err := u.ready(upload, control, next.size); err != nil {
		return err
	}
	budgeted, cancel := context.WithTimeout(upload, u.uploadBudget(next.size))
	defer cancel()
	written, err := u.send(control, budgeted, next)
	if err != nil {
		return err
	}
	if err := u.check(budgeted, upload, written); err != nil {
		return u.dropped(control, written, err)
	}
	return u.commit(control, written)
}

func (u *uploader) ready(upload, control context.Context, size int64) error {
	if !u.store.lease.Valid() {
		return skipped(skipLeaseSuspended, errLeaseLost)
	}
	if err := u.resolve(control); err != nil {
		return u.classified(err)
	}
	return u.fits(upload, size)
}

func (u *uploader) fits(ctx context.Context, size int64) error {
	used, err := u.bucketBytes(ctx)
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

func (u *uploader) send(control, budgeted context.Context, next *spool) (*object, error) {
	started := time.Now()
	written := newObject(next, u.store.chunkBytes)
	written.at = u.store.now()
	u.progress.started = started
	err := u.sendChunks(budgeted, next, written)
	if err == nil {
		err = u.sendMeta(budgeted, written)
	}
	if err != nil {
		return nil, u.dropped(control, written, err)
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
	left := c.up.size - c.object.size
	if err := retry.Sleep(ctx, c.pace.reserve(time.Now(), int64(len(data)))); err != nil {
		return stopMarked(ctx, err)
	}
	c.digest.Write(data)
	c.object.size += int64(len(data))
	ack, err := c.up.publish(ctx, left, &nats.Msg{Subject: c.up.store.chunkSubject(c.object.nuid), Data: data},
		jetstream.WithMsgID(fmt.Sprintf(chunkIDFormat, c.object.nuid, c.object.chunks)))
	if err != nil {
		return err
	}
	return c.landed(ack)
}

func (c *chunkSender) landed(ack *jetstream.PubAck) error {
	if err := c.object.acknowledged(ack, c.up.store.buckets.object); err != nil {
		return err
	}
	c.up.progress.sent = c.object.size
	c.up.progress.acked++
	return nil
}

func (u *uploader) publish(
	ctx context.Context,
	left int64,
	msg *nats.Msg, options ...jetstream.PublishOpt,
) (*jetstream.PubAck, error) {
	options = append(options, jetstream.WithExpectStream(u.store.buckets.object), jetstream.WithRetryAttempts(0))
	var ack *jetstream.PubAck
	err := retry.Do(ctx, chunkPolicy(), func(attempt context.Context) error {
		if err := u.boundary(phaseUpload, left); err != nil {
			return err
		}
		deadline, _ := attempt.Deadline()
		return u.uploadWithin(ctx, time.Until(deadline), func(sent context.Context) error {
			var err error
			ack, err = u.store.buckets.data.PublishMsg(sent, msg, options...)
			return err
		})
	})
	return ack, stopMarked(ctx, err)
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
	return !stopCaused(err) && slices.Contains([]natsjs.Reason{
		natsjs.ReasonNoResponders,
		natsjs.ReasonDisconnected,
		natsjs.ReasonTimeout,
		natsjs.ReasonInProcess,
	}, natsjs.Classify(err).Reason)
}
