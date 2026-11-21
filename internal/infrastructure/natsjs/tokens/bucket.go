package tokens

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs"
	"github.com/DmytroLysenko1/faultline/pkg/retry"
)

const (
	minReplicas    = 1
	maxReplicas    = 5
	attemptTimeout = time.Second
	firstRetryWait = 100 * time.Millisecond
	lastRetryWait  = time.Second
)

var (
	ErrInvalidOptions = errors.New("invalid token bucket options")
	ErrNeedsRecreate  = errors.New("token bucket drift cannot be fixed in place")
)

func CreateBucket(ctx context.Context, conn *natsjs.Conn, replicas int) error {
	if err := validReplicas(replicas); err != nil {
		return err
	}
	ctx, cancel := bounded(ctx)
	defer cancel()
	js, err := jetstream.New(conn.Control.NATS)
	if err != nil {
		return fmt.Errorf("jetstream on the control connection: %w", err)
	}
	return create(ctx, js, replicas)
}

func ReconcileBucket(ctx context.Context, conn *natsjs.Conn, replicas int) error {
	if err := validReplicas(replicas); err != nil {
		return err
	}
	ctx, cancel := bounded(ctx)
	defer cancel()
	js, err := jetstream.New(conn.Control.NATS)
	if err != nil {
		return fmt.Errorf("jetstream on the control connection: %w", err)
	}
	actual, err := readConfig(ctx, js)
	if errors.Is(err, ErrBucketMissing) {
		return create(ctx, js, replicas)
	}
	if err != nil {
		return err
	}
	return reconcile(ctx, js, actual, replicas)
}

func create(ctx context.Context, js jetstream.JetStream, replicas int) error {
	err := retrying(ctx, func(attempt context.Context) error {
		_, err := js.CreateStream(attempt, desiredConfig(replicas))
		return err
	})
	if natsjs.HasCode(err, jetstream.JSErrCodeStreamNameInUse) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("create stream %s: %w", streamName, err)
	}
	return nil
}

func reconcile(ctx context.Context, js jetstream.JetStream, actual *jetstream.StreamConfig, replicas int) error {
	update := repaired(actual, replicas)
	if before := verdictOf(actual); before.irreversible() {
		return fmt.Errorf("%s: %w", strings.Join(before.notes(), ","), errors.Join(ErrNeedsRecreate, ErrIrreversible))
	}
	if reflect.DeepEqual(&update, actual) {
		return nil
	}
	err := retrying(ctx, func(attempt context.Context) error {
		_, err := js.UpdateStream(attempt, update)
		return err
	})
	if err != nil {
		return fmt.Errorf("update stream %s: %w", streamName, errors.Join(ErrNeedsRecreate, err))
	}
	return readBack(ctx, js)
}

func readBack(ctx context.Context, js jetstream.JetStream) error {
	reread, err := readConfig(ctx, js)
	if err != nil {
		return err
	}
	if after := verdictOf(reread); after.drifted() || after.irreversible() {
		return fmt.Errorf("after update %s: %w", strings.Join(after.notes(), ","), ErrNeedsRecreate)
	}
	return nil
}

func repaired(actual *jetstream.StreamConfig, replicas int) jetstream.StreamConfig {
	update := *actual
	want := desiredConfig(max(actual.Replicas, replicas))
	target := reflect.ValueOf(&update).Elem()
	source := reflect.ValueOf(&want).Elem()
	for _, field := range configFields {
		if field.repairable() && field.differs(&want, actual) {
			target.FieldByName(field.name).Set(source.FieldByName(field.name))
		}
	}
	update.Replicas = want.Replicas
	return update
}

func (f configField) repairable() bool {
	return f.class == classBlocksReading || f.class == classStrict
}

func readConfig(ctx context.Context, js jetstream.JetStream) (*jetstream.StreamConfig, error) {
	var config *jetstream.StreamConfig
	err := retrying(ctx, func(attempt context.Context) error {
		stream, err := js.Stream(attempt, streamName)
		if err != nil {
			return err
		}
		config = &stream.CachedInfo().Config
		return nil
	})
	if natsjs.HasCode(err, jetstream.JSErrCodeStreamNotFound) {
		return nil, fmt.Errorf("stream %s: %w", streamName, ErrBucketMissing)
	}
	if err != nil {
		return nil, fmt.Errorf("read stream %s: %w", streamName, err)
	}
	return config, nil
}

func retrying(ctx context.Context, call func(attempt context.Context) error) error {
	return retry.Do(ctx, &retry.Policy{
		Attempt:   attemptTimeout,
		FirstWait: firstRetryWait,
		MaxWait:   lastRetryWait,
		Retryable: natsjs.Temporary,
	}, call)
}

func validReplicas(replicas int) error {
	if replicas < minReplicas || replicas > maxReplicas {
		return fmt.Errorf("replicas %d outside %d..%d: %w", replicas, minReplicas, maxReplicas, ErrInvalidOptions)
	}
	return nil
}
