package lease

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs"
)

const (
	keyHistory     = 1
	bucketAttempt  = time.Second
	bucketRetryGap = time.Second
)

type bucket struct {
	kv jetstream.KeyValue
}

func (b bucket) Create(ctx context.Context, key string, value []byte) (uint64, error) {
	return b.kv.Create(ctx, key, value)
}

func (b bucket) Update(ctx context.Context, key string, value []byte, revision uint64) (uint64, error) {
	return b.kv.Update(ctx, key, value, revision)
}

func (b bucket) Get(ctx context.Context, key string) (entry, error) {
	found, err := b.kv.Get(ctx, key)
	if err != nil {
		return entry{}, err
	}
	return entry{
		value:    found.Value(),
		revision: found.Revision(),
	}, nil
}

func (b bucket) Delete(ctx context.Context, key string, revision uint64) error {
	return b.kv.Delete(ctx, key, jetstream.LastRevision(revision))
}

func openBucket(
	ctx context.Context,
	js jetstream.JetStream,
	options *Options,
) (jetstream.KeyValue, time.Duration, error) {
	kv, err := createOrOpen(ctx, js, options)
	if err != nil {
		return nil, 0, err
	}
	maxAge, err := grownMaxAge(ctx, js, kv, options)
	if err != nil {
		return nil, 0, err
	}
	return kv, maxAge, nil
}

func grownMaxAge(
	ctx context.Context,
	js jetstream.JetStream,
	kv jetstream.KeyValue,
	options *Options,
) (time.Duration, error) {
	status, err := checkedStatus(ctx, kv, options)
	if err != nil {
		return 0, err
	}
	if status.TTL() >= options.TTL {
		return status.TTL(), nil
	}
	if err := grow(ctx, js, new(status.Config()), options.TTL); err != nil {
		return 0, err
	}
	return grownTo(ctx, kv, options)
}

func grownTo(ctx context.Context, kv jetstream.KeyValue, options *Options) (time.Duration, error) {
	status, err := checkedStatus(ctx, kv, options)
	if err != nil {
		return 0, err
	}
	if status.TTL() < options.TTL {
		return 0, fmt.Errorf("%w: max age %s after growing it to %s",
			ErrBucketDrift, status.TTL(), options.TTL,
		)
	}
	return status.TTL(), nil
}

func createOrOpen(
	ctx context.Context,
	js jetstream.JetStream,
	options *Options,
) (jetstream.KeyValue, error) {
	var kv jetstream.KeyValue
	err := retrying(ctx, func(attempt context.Context) error {
		var err error
		kv, err = js.CreateKeyValue(attempt, jetstream.KeyValueConfig{
			Bucket:   Bucket,
			TTL:      options.TTL,
			History:  keyHistory,
			Storage:  jetstream.FileStorage,
			Replicas: options.Topology.Replicas,
		})
		if errors.Is(err, jetstream.ErrBucketExists) {
			kv, err = js.KeyValue(attempt, Bucket)
		}
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("open lease bucket: %w", err)
	}
	return kv, nil
}

func checkedStatus(
	ctx context.Context,
	kv jetstream.KeyValue,
	options *Options,
) (jetstream.KeyValueStatus, error) {
	var status jetstream.KeyValueStatus
	err := retrying(ctx, func(attempt context.Context) error {
		var err error
		status, err = kv.Status(attempt)
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("read lease bucket: %w", err)
	}
	config := status.Config()
	if config.Storage != jetstream.FileStorage || config.Replicas != options.Topology.Replicas {
		return nil, fmt.Errorf("%w: storage %s and %d replicas, want file and %d",
			ErrBucketDrift, config.Storage, config.Replicas, options.Topology.Replicas,
		)
	}
	return status, nil
}

func grow(
	ctx context.Context,
	js jetstream.JetStream,
	config *jetstream.KeyValueConfig,
	ttl time.Duration,
) error {
	config.TTL = ttl
	err := retrying(ctx, func(attempt context.Context) error {
		_, err := js.UpdateKeyValue(attempt, *config)
		return err
	})
	if err != nil {
		return fmt.Errorf("grow lease bucket max age to %s: %w", ttl, err)
	}
	return nil
}

func retrying(ctx context.Context, call func(attempt context.Context) error) error {
	for {
		attempt, cancel := context.WithTimeout(ctx, bucketAttempt)
		err := call(attempt)
		cancel()
		if !natsjs.Temporary(err) {
			return err
		}
		if waitErr := waitUntil(ctx, bucketRetryGap); waitErr != nil {
			return errors.Join(err, waitErr)
		}
	}
}
