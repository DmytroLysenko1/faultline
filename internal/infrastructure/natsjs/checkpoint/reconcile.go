package checkpoint

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/DmytroLysenko1/faultline/internal/domain/topology"
	framed "github.com/DmytroLysenko1/faultline/pkg/checkpoint"
)

const verifySlack = 5 * time.Second

var (
	errChunkCount  = errors.New("checkpoint object does not hold the chunks that were sent")
	errVerifyEnded = errors.New("checkpoint verify ended before its read")
)

func (u *uploader) check(budgeted, upload context.Context, written *object) error {
	if err := u.reconcile(budgeted, written); err != nil {
		return err
	}
	return u.verify(upload, written)
}

func (u *uploader) reconcile(budgeted context.Context, written *object) error {
	subject := u.store.chunkSubject(written.nuid)
	held, err := u.subjectCount(budgeted, subject)
	if err != nil {
		return failed(failChunkCount, fmt.Errorf("count the chunks of %s: %w", written.name, err))
	}
	if held != uint64(written.chunks) {
		return failed(failChunkCount, fmt.Errorf("%w: %s holds %d of %d", errChunkCount, written.name, held,
			written.chunks))
	}
	return nil
}

func (u *uploader) verify(upload context.Context, written *object) error {
	started := time.Now()
	ctx, cancel := context.WithTimeout(upload, transfer(written.size, u.store.options.verifyRate())+verifySlack)
	defer cancel()
	reader := u.store.readerWith(ctx, written.info(u.store.options.objectBucket()), missingIsCorrupt)
	err := framed.Walk(u.paced(ctx, reader), topology.CheckpointSections()...)
	if err = errors.Join(err, reader.Close()); err != nil {
		return failed(failVerify, fmt.Errorf("read back %s: %w", written.name, err))
	}
	written.verified = time.Since(started)
	return nil
}

func missingIsCorrupt() error {
	return nil
}

func (u *uploader) paced(ctx context.Context, from io.Reader) io.Reader {
	if u.store.options.VerifyRate == 0 {
		return from
	}
	return &pacedReader{
		done: ctx.Done(),
		from: from,
		pace: newPace(u.store.options.VerifyRate, burstChunks*int64(u.store.chunkBytes), time.Now()),
	}
}

type pacedReader struct {
	done <-chan struct{}
	from io.Reader
	pace *pace
}

func (p *pacedReader) Read(out []byte) (int, error) {
	read, err := p.from.Read(out)
	if read == 0 {
		return read, err
	}
	return read, errors.Join(err, p.wait(p.pace.reserve(time.Now(), int64(read))))
}

func (p *pacedReader) wait(gap time.Duration) error {
	timer := time.NewTimer(gap)
	defer timer.Stop()
	select {
	case <-p.done:
		return errVerifyEnded
	case <-timer.C:
		return nil
	}
}

func (u *uploader) subjectCount(ctx context.Context, subject string) (uint64, error) {
	var held uint64
	err := retrying(ctx, func(attempt context.Context) error {
		info, err := u.store.buckets.bucket.Info(attempt, jetstream.WithSubjectFilter(subject))
		if err == nil {
			held = info.State.Subjects[subject]
		}
		return err
	})
	return held, err
}

func (u *uploader) bucketBytes(upload context.Context) (uint64, error) {
	ctx, cancel := context.WithTimeout(upload, sweepCallBudget)
	defer cancel()
	var used uint64
	err := retrying(ctx, func(attempt context.Context) error {
		info, err := u.store.buckets.bucket.Info(attempt)
		if err == nil {
			used = info.State.Bytes
		}
		return err
	})
	return used, err
}
