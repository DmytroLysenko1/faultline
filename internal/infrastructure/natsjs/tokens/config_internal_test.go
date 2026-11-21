package tokens

import (
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	singleReplica = 1
	threeReplicas = 3
)

func TestEveryStreamConfigFieldHasAClass(t *testing.T) {
	var got []string
	for field := range reflect.TypeFor[jetstream.StreamConfig]().Fields() {
		got = append(got, field.Name)
	}
	var classified []string
	for _, field := range configFields {
		classified = append(classified, field.name)
	}
	slices.Sort(got)
	slices.Sort(classified)
	if diff := cmp.Diff(got, classified); diff != "" {
		t.Errorf("every StreamConfig field must stand in exactly one class (-nats.go +configFields):\n%s", diff)
	}
}

func TestCheckConfigTable(t *testing.T) {
	type want struct {
		BlocksReading []string
		Strict        []string
		Warn          []string
		Irreversible  bool
		Drifted       bool
	}
	tests := []struct {
		name   string
		change func(c *jetstream.StreamConfig)
		want   want
	}{
		{name: "the desired config", change: func(*jetstream.StreamConfig) {}},
		{
			name:   "another subject may inject grants",
			change: func(c *jetstream.StreamConfig) { c.Subjects = []string{">"} },
			want:   want{BlocksReading: []string{"Subjects"}, Drifted: true},
		},
		{
			name:   "a source may inject grants",
			change: func(c *jetstream.StreamConfig) { c.Sources = []*jetstream.StreamSource{{Name: "X"}} },
			want:   want{BlocksReading: []string{"Sources"}, Drifted: true},
		},
		{
			name:   "unlimited messages lift the memory bound",
			change: func(c *jetstream.StreamConfig) { c.MaxMsgs = -1 },
			want:   want{BlocksReading: []string{"MaxMsgs"}, Drifted: true},
		},
		{
			name:   "an age limit hides grants",
			change: func(c *jetstream.StreamConfig) { c.MaxAge = time.Hour },
			want:   want{BlocksReading: []string{"MaxAge"}, Drifted: true},
		},
		{
			name:   "interest retention hides grants",
			change: func(c *jetstream.StreamConfig) { c.Retention = jetstream.InterestPolicy },
			want:   want{BlocksReading: []string{"Retention"}, Drifted: true},
		},
		{
			name:   "no ack is strict for commands only",
			change: func(c *jetstream.StreamConfig) { c.NoAck = true },
			want:   want{Strict: []string{"NoAck"}, Drifted: true},
		},
		{
			name:   "a short duplicate window is strict",
			change: func(c *jetstream.StreamConfig) { c.Duplicates = MinDuplicates - time.Nanosecond },
			want:   want{Strict: []string{"Duplicates"}, Drifted: true},
		},
		{
			name:   "a longer duplicate window is fine",
			change: func(c *jetstream.StreamConfig) { c.Duplicates = time.Hour },
		},
		{
			name:   "replicas and placement only warn",
			change: func(c *jetstream.StreamConfig) { c.Replicas = 5; c.Placement = &jetstream.Placement{Cluster: "east"} },
			want:   want{Warn: []string{"Replicas", "Placement"}},
		},
		{
			name:   "deny purge makes revocation impossible",
			change: func(c *jetstream.StreamConfig) { c.DenyPurge = true },
			want:   want{Warn: []string{"DenyPurge"}, Irreversible: true},
		},
		{
			name:   "a sealed bucket is irreversible",
			change: func(c *jetstream.StreamConfig) { c.Sealed = true },
			want:   want{Warn: []string{"Sealed"}, Irreversible: true},
		},
		{
			name: "server filled fields are ignored",
			change: func(c *jetstream.StreamConfig) {
				c.MaxConsumers = -1
				c.Metadata = map[string]string{"_nats.ver": "2.15.3"}
				c.Description = "tokens"
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			desired := desiredConfig(threeReplicas)
			actual := desiredConfig(threeReplicas)
			tt.change(&actual)
			verdict := checkConfig(&desired, &actual)
			got := want{
				BlocksReading: verdict.byClass[classBlocksReading],
				Strict:        verdict.byClass[classStrict],
				Warn:          slices.Concat(verdict.byClass[classWarn], verdict.byClass[classIrreversible]),
				Irreversible:  verdict.irreversible(),
				Drifted:       verdict.drifted(),
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("verdict mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestRepairedFixesOnlyDriftAndKeepsTheOperatorsFields(t *testing.T) {
	actual := desiredConfig(threeReplicas)
	actual.MaxMsgs = -1
	actual.NoAck = true
	actual.Duplicates = time.Second
	actual.Placement = &jetstream.Placement{Cluster: "east", Tags: []string{"ssd"}}
	actual.Compression = jetstream.S2Compression
	actual.Description = "kept"
	want := desiredConfig(threeReplicas)
	want.Placement = actual.Placement
	want.Compression = jetstream.S2Compression
	want.Description = "kept"
	if diff := cmp.Diff(want, repaired(&actual, singleReplica)); diff != "" {
		t.Errorf("repaired mismatch (-want +got):\n%s", diff)
	}
}

func TestRepairedNeverLowersReplicas(t *testing.T) {
	tests := []struct {
		name     string
		actual   int
		replicas int
		want     int
	}{
		{name: "asked for fewer keeps more", actual: threeReplicas, replicas: singleReplica, want: threeReplicas},
		{name: "asked for more raises", actual: singleReplica, replicas: threeReplicas, want: threeReplicas},
		{name: "equal stays", actual: threeReplicas, replicas: threeReplicas, want: threeReplicas},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := desiredConfig(tt.actual)
			if got := repaired(&actual, tt.replicas).Replicas; got != tt.want {
				t.Errorf("replicas = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestValidReplicas(t *testing.T) {
	for replicas, wantValid := range map[int]bool{0: false, 1: true, 3: true, 5: true, 6: false, -1: false} {
		if got := validReplicas(replicas) == nil; got != wantValid {
			t.Errorf("validReplicas(%d) valid = %v, want %v", replicas, got, wantValid)
		}
	}
}
