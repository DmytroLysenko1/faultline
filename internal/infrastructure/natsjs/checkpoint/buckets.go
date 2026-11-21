package checkpoint

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs"
	"github.com/DmytroLysenko1/faultline/pkg/retry"
)

const (
	pointerHistory    = 1
	unlimited         = -1
	kvStreamPrefix    = "KV_"
	kvSubjectPrefix   = "$KV."
	objStreamPrefix   = "OBJ_"
	objSubjectPrefix  = "$O."
	chunkSubjectPart  = ".C."
	metaSubjectPart   = ".M."
	bucketAttempt     = 2 * time.Second
	bucketFirstWait   = 100 * time.Millisecond
	bucketMaxWait     = time.Second
	subjectWildcardGT = ">"
	subjectFilterAll  = ".>"
)

type buckets struct {
	pointers jetstream.KeyValue
	objects  jetstream.ObjectStore
	read     jetstream.Stream
	bucket   jetstream.Stream
	control  jetstream.JetStream
	data     jetstream.JetStream
	stream   string
	prefix   string
	object   string
	window   time.Duration
}

func openBuckets(
	ctx context.Context,
	control, data jetstream.JetStream,
	options *Options,
	window time.Duration,
) (buckets, error) {
	pointers := bucketWant{config: pointerConfig(options.Topology.Replicas)}
	if err := ensureStream(ctx, control, &pointers); err != nil {
		return buckets{}, fmt.Errorf("pointer bucket: %w", err)
	}
	objects := bucketWant{config: objectConfig(options, window), holdsObjects: true}
	if err := ensureStream(ctx, control, &objects); err != nil {
		return buckets{}, fmt.Errorf("object bucket %s: %w", options.objectBucket(), err)
	}
	bound, err := bind(ctx, control, data, options)
	if err != nil {
		return buckets{}, err
	}
	bound.window = window
	return bound, nil
}

func bind(ctx context.Context, control, data jetstream.JetStream, options *Options) (buckets, error) {
	var bound buckets
	err := retrying(ctx, func(attempt context.Context) error {
		var err error
		bound, err = bindOnce(attempt, control, data, options)
		return err
	})
	if err != nil {
		return buckets{}, fmt.Errorf("bind checkpoint buckets: %w", err)
	}
	return bound, nil
}

func bindOnce(ctx context.Context, control, data jetstream.JetStream, options *Options) (buckets, error) {
	kv, err := control.KeyValue(ctx, PointerBucket)
	if err != nil {
		return buckets{}, err
	}
	objects, err := data.ObjectStore(ctx, options.objectBucket())
	if err != nil {
		return buckets{}, err
	}
	read, written, err := bucketStreams(ctx, control, data, options.objectBucket())
	if err != nil {
		return buckets{}, err
	}
	return buckets{
		pointers: kv,
		objects:  objects,
		read:     read,
		bucket:   written,
		control:  control,
		data:     data,
		stream:   options.Topology.Stream(options.Partition),
		prefix:   objSubjectPrefix + options.objectBucket(),
		object:   objStreamPrefix + options.objectBucket(),
	}, nil
}

func bucketStreams(ctx context.Context, control, data jetstream.JetStream, bucket string) (
	jetstream.Stream, jetstream.Stream, error,
) {
	read, err := data.Stream(ctx, objStreamPrefix+bucket)
	if err != nil {
		return nil, nil, err
	}
	written, err := control.Stream(ctx, objStreamPrefix+bucket)
	if err != nil {
		return nil, nil, err
	}
	return read, written, nil
}

func (b *buckets) chunkAt(ctx context.Context, subject string, sequence uint64) (*jetstream.RawStreamMsg, error) {
	return b.read.GetMsg(ctx, sequence, jetstream.WithGetMsgSubject(subject))
}

func (b *buckets) subjects(ctx context.Context) (map[string]uint64, error) {
	info, err := b.bucket.Info(ctx, jetstream.WithSubjectFilter(b.prefix+subjectFilterAll))
	if err != nil {
		return nil, err
	}
	return info.State.Subjects, nil
}

func (b *buckets) describe(ctx context.Context) (*jetstream.StreamInfo, error) {
	stream, err := b.control.Stream(ctx, b.stream)
	if err != nil {
		return nil, err
	}
	return stream.CachedInfo(), nil
}

func pointerConfig(replicas int) jetstream.StreamConfig {
	return jetstream.StreamConfig{
		Name:              kvStreamPrefix + PointerBucket,
		Subjects:          []string{kvSubjectPrefix + PointerBucket + "." + subjectWildcardGT},
		MaxMsgsPerSubject: pointerHistory,
		MaxBytes:          unlimited,
		MaxMsgSize:        unlimited,
		MaxMsgs:           unlimited,
		MaxConsumers:      unlimited,
		Storage:           jetstream.FileStorage,
		Replicas:          replicas,
		AllowRollup:       true,
		DenyDelete:        true,
		Duplicates:        dedupWindow,
		Discard:           jetstream.DiscardNew,
		AllowDirect:       false,
	}
}

func objectConfig(options *Options, window time.Duration) jetstream.StreamConfig {
	bucket := objSubjectPrefix + options.objectBucket()
	return jetstream.StreamConfig{
		Name: objStreamPrefix + options.objectBucket(),
		Subjects: []string{
			bucket + chunkSubjectPart + subjectWildcardGT,
			bucket + metaSubjectPart + subjectWildcardGT,
		},
		MaxBytes:          options.MaxBytes,
		MaxMsgsPerSubject: unlimited,
		Storage:           jetstream.FileStorage,
		Replicas:          options.Topology.Replicas,
		Discard:           jetstream.DiscardNew,
		AllowRollup:       true,
		AllowDirect:       false,
		Duplicates:        window,
	}
}

type bucketWant struct {
	config       jetstream.StreamConfig
	holdsObjects bool
}

func ensureStream(ctx context.Context, js jetstream.JetStream, want *bucketWant) error {
	err := retrying(ctx, func(attempt context.Context) error {
		_, err := js.CreateStream(attempt, want.config)
		return err
	})
	if !errors.Is(err, jetstream.ErrStreamNameAlreadyInUse) {
		return err
	}
	return reconcile(ctx, js, want)
}

func reconcile(ctx context.Context, js jetstream.JetStream, want *bucketWant) error {
	var have jetstream.StreamConfig
	err := retrying(ctx, func(attempt context.Context) error {
		stream, err := js.Stream(attempt, want.config.Name)
		if err == nil {
			have = stream.CachedInfo().Config
		}
		return err
	})
	if err != nil {
		return err
	}
	if err := want.drift(&have); err != nil {
		return err
	}
	return grow(ctx, js, &have, want.config.MaxBytes)
}

func (w *bucketWant) drift(have *jetstream.StreamConfig) error {
	if err := checkDrift(have, &w.config); err != nil {
		return err
	}
	if !w.holdsObjects {
		return nil
	}
	return checkObjectDrift(have, &w.config)
}

func checkDrift(have, want *jetstream.StreamConfig) error {
	if have.Storage != want.Storage || have.Replicas != want.Replicas || have.AllowDirect != want.AllowDirect {
		return fmt.Errorf("%w: %s has storage %s, %d replicas, direct %t; want %s, %d, %t", ErrBucketDrift, want.Name,
			have.Storage, have.Replicas, have.AllowDirect, want.Storage, want.Replicas, want.AllowDirect)
	}
	return nil
}

func checkObjectDrift(have, want *jetstream.StreamConfig) error {
	if have.Discard != want.Discard ||
		have.MaxAge != want.MaxAge ||
		have.MaxMsgsPerSubject != want.MaxMsgsPerSubject {
		return fmt.Errorf("%w: %s has discard %s, max age %s, %d per subject; want %s, %s, %d", ErrBucketDrift,
			want.Name, have.Discard, have.MaxAge, have.MaxMsgsPerSubject, want.Discard, want.MaxAge,
			want.MaxMsgsPerSubject)
	}
	return checkDuplicates(have, want.Duplicates)
}

func checkDuplicates(have *jetstream.StreamConfig, window time.Duration) error {
	if have.Duplicates < window {
		return fmt.Errorf("%w: %s keeps duplicates for %s, under the %s window", ErrBucketDrift, have.Name,
			have.Duplicates, window)
	}
	return nil
}

func grow(ctx context.Context, js jetstream.JetStream, have *jetstream.StreamConfig, maxBytes int64) error {
	if maxBytes == unlimited || have.MaxBytes == unlimited || have.MaxBytes >= maxBytes {
		return nil
	}
	have.MaxBytes = maxBytes
	return retrying(ctx, func(attempt context.Context) error {
		_, err := js.UpdateStream(attempt, *have)
		return err
	})
}

func retrying(ctx context.Context, call func(attempt context.Context) error) error {
	return retry.Do(ctx, &retry.Policy{
		Attempt:   bucketAttempt,
		FirstWait: bucketFirstWait,
		MaxWait:   bucketMaxWait,
		Retryable: natsjs.Temporary,
	}, call)
}
