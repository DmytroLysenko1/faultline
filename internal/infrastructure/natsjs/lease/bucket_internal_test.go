package lease

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/natsjstest"
)

const (
	embeddedTTL      = 6 * time.Second
	embeddedStep     = 200 * time.Millisecond
	embeddedLost     = time.Second
	embeddedElection = 400 * time.Millisecond
	shrunkMaxAge     = 3 * time.Second
	embeddedBound    = 10 * time.Second
	singleNode       = 1
	threeNodes       = 3
	firstKey         = keyPrefix + "0"
	heldValue        = "held"
	movedOnValue     = "moved-on"
)

func embeddedOptions(replicas int) *Options {
	options := productionOptions()
	options.Topology.Replicas = replicas
	options.TTL = embeddedTTL
	options.RenewEvery = embeddedStep
	options.RetryEvery = embeddedStep
	options.AttemptTime = embeddedStep
	options.LostAfter = embeddedLost
	options.Logger = slog.New(slog.DiscardHandler)
	options.electionBudget = embeddedElection
	return options
}

func embeddedBucket(t *testing.T, ctx context.Context, url string, replicas int) (jetstream.KeyValue, time.Duration) {
	t.Helper()
	kv, maxAge, err := openBucket(ctx, natsjstest.JetStream(t, url), embeddedOptions(replicas))
	if err != nil {
		t.Fatalf("open bucket: %v", err)
	}
	return kv, maxAge
}

func TestAMaxAgeThatFallsWhileAcquiringGivesTheKeyBack(t *testing.T) {
	cluster := natsjstest.Start(t, singleNode)
	ctx, cancel := context.WithTimeout(t.Context(), embeddedBound)
	defer cancel()
	kv, maxAge := embeddedBucket(t, ctx, cluster.URLs()[0], singleNode)
	shrink(t, ctx, natsjstest.JetStream(t, cluster.URLs()[0]))

	_, err := claimConfirmed(ctx, kv, maxAge, embeddedOptions(singleNode))

	if !errors.Is(err, ErrBucketDrift) {
		t.Fatalf("claimConfirmed = %v, want ErrBucketDrift", err)
	}
	if _, err := kv.Get(ctx, firstKey); !errors.Is(err, jetstream.ErrKeyNotFound) {
		t.Errorf("key after a refused acquire: %v, want it given back", err)
	}
}

func shrink(t *testing.T, ctx context.Context, operator jetstream.JetStream) {
	t.Helper()
	_, err := operator.UpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:   Bucket,
		TTL:      shrunkMaxAge,
		History:  keyHistory,
		Storage:  jetstream.FileStorage,
		Replicas: singleNode,
	})
	if err != nil {
		t.Fatalf("shrink max age: %v", err)
	}
}

type revisions struct {
	movedPast uint64
	current   uint64
}

type fidelityArgs struct {
	write func(ctx context.Context, kv jetstream.KeyValue, key string, at revisions) error
}

type fidelityWant struct {
	Value string
}

func TestNATSAnswersWhatTheFakeAnswers(t *testing.T) {
	tests := []struct {
		name    string
		args    fidelityArgs
		want    fidelityWant
		wantErr error
	}{
		{
			name: "an update at a revision the key has moved past is a revision mismatch",
			args: fidelityArgs{write: func(ctx context.Context, kv jetstream.KeyValue, key string, at revisions) error {
				_, err := kv.Update(ctx, key, []byte(heldValue), at.movedPast)
				return err
			}},
			want:    fidelityWant{Value: movedOnValue},
			wantErr: jetstream.ErrKeyRevisionMismatch,
		},
		{
			name: "a create of a held key is refused as existing",
			args: fidelityArgs{write: func(ctx context.Context, kv jetstream.KeyValue, key string, _ revisions) error {
				_, err := kv.Create(ctx, key, []byte(heldValue))
				return err
			}},
			want:    fidelityWant{Value: movedOnValue},
			wantErr: jetstream.ErrKeyExists,
		},
		{
			name: "a delete at a revision the key has moved past is a revision mismatch and keeps the key",
			args: fidelityArgs{write: func(ctx context.Context, kv jetstream.KeyValue, key string, at revisions) error {
				return deleterOf(kv)(ctx, key, at.movedPast)
			}},
			want:    fidelityWant{Value: movedOnValue},
			wantErr: jetstream.ErrKeyRevisionMismatch,
		},
		{
			name: "a delete at the key's revision removes it",
			args: fidelityArgs{write: func(ctx context.Context, kv jetstream.KeyValue, key string, at revisions) error {
				return deleterOf(kv)(ctx, key, at.current)
			}},
			want: fidelityWant{},
		},
	}
	for _, nodes := range []int{singleNode, threeNodes} {
		t.Run(strconv.Itoa(nodes)+" nodes", func(t *testing.T) {
			opening, cancel := context.WithTimeout(t.Context(), embeddedBound)
			defer cancel()
			kv, _ := embeddedBucket(t, opening, natsjstest.Start(t, nodes).URLs()[0], nodes)
			for i, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					ctx, cancel := context.WithTimeout(t.Context(), embeddedBound)
					defer cancel()
					key := keyPrefix + strconv.Itoa(i)
					at := movedOn(t, ctx, kv, key)

					err := tt.args.write(ctx, kv, key, at)

					if !errors.Is(err, tt.wantErr) {
						t.Fatalf("err = %v, want %v", err, tt.wantErr)
					}
					if diff := cmp.Diff(tt.want, fidelityWant{Value: valueOf(t, ctx, kv, key)}); diff != "" {
						t.Errorf("key afterwards mismatch (-want +got):\n%s", diff)
					}
				})
			}
		})
	}
}

func movedOn(t *testing.T, ctx context.Context, kv jetstream.KeyValue, key string) revisions {
	t.Helper()
	created, err := kv.Create(ctx, key, []byte(heldValue))
	if err != nil {
		t.Fatalf("create %s: %v", key, err)
	}
	updated, err := kv.Update(ctx, key, []byte(movedOnValue), created)
	if err != nil {
		t.Fatalf("update %s: %v", key, err)
	}
	return revisions{movedPast: created, current: updated}
}

func valueOf(t *testing.T, ctx context.Context, kv jetstream.KeyValue, key string) string {
	t.Helper()
	found, err := kv.Get(ctx, key)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return ""
	}
	if err != nil {
		t.Fatalf("get %s: %v", key, err)
	}
	return string(found.Value())
}
