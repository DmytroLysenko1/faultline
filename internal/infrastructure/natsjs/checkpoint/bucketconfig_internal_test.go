package checkpoint

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/natsjstest"
)

const (
	referenceKV      = "reference_kv"
	referenceObjects = "reference_objects"
)

type bucketShape struct {
	Storage           jetstream.StorageType
	Replicas          int
	Discard           jetstream.DiscardPolicy
	AllowRollup       bool
	DenyDelete        bool
	MaxMsgsPerSubject int64
	AllowDirect       bool
}

func shapeOfStream(config *jetstream.StreamConfig) bucketShape {
	return bucketShape{
		Storage:           config.Storage,
		Replicas:          config.Replicas,
		Discard:           config.Discard,
		AllowRollup:       config.AllowRollup,
		DenyDelete:        config.DenyDelete,
		MaxMsgsPerSubject: config.MaxMsgsPerSubject,
		AllowDirect:       config.AllowDirect,
	}
}

func TestTheBucketsAreWhatNATSWouldCreateButReadAtTheLeader(t *testing.T) {
	js := connect(t, natsjstest.Start(t, 1).URLs()[0])
	options := testOptions()
	_, err := js.CreateKeyValue(t.Context(), jetstream.KeyValueConfig{Bucket: referenceKV, History: pointerHistory,
		Storage: jetstream.FileStorage, Replicas: testReplicas})
	if err != nil {
		t.Fatalf("reference kv: %v", err)
	}
	_, err = js.CreateObjectStore(t.Context(), jetstream.ObjectStoreConfig{Bucket: referenceObjects,
		Storage: jetstream.FileStorage, Replicas: testReplicas, MaxBytes: testMaxBytes})
	if err != nil {
		t.Fatalf("reference objects: %v", err)
	}
	if _, err := openBuckets(t.Context(), js, js, options); err != nil {
		t.Fatalf("open buckets: %v", err)
	}
	tests := []struct {
		name string
		args jetstream.StreamConfig
		want jetstream.StreamConfig
	}{
		{
			name: "pointers",
			args: streamConfigOf(t, js, kvStreamPrefix+PointerBucket),
			want: streamConfigOf(t, js, kvStreamPrefix+referenceKV),
		},
		{
			name: "objects",
			args: streamConfigOf(t, js, objStreamPrefix+options.objectBucket()),
			want: streamConfigOf(t, js, objStreamPrefix+referenceObjects),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			want := shapeOfStream(&tt.want)
			want.AllowDirect = false
			if diff := cmp.Diff(want, shapeOfStream(&tt.args)); diff != "" {
				t.Errorf("bucket mismatch (-nats.go, direct reads off +ours):\n%s", diff)
			}
		})
	}
}

func streamConfigOf(t *testing.T, js jetstream.JetStream, name string) jetstream.StreamConfig {
	t.Helper()
	stream, err := js.Stream(t.Context(), name)
	if err != nil {
		t.Fatalf("stream %s: %v", name, err)
	}
	return stream.CachedInfo().Config
}
