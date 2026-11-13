package natsjs_test

import (
	"errors"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs"
)

const (
	testMaxBytes    = 64 << 20
	testWindow      = 5 * time.Minute
	oneReplica      = 1
	threeReplicas   = 3
	firstVersion    = 1
	twoPartitions   = 2
	firstPartition  = uint16(0)
	secondPartition = uint16(1)
)

func topologyOf(partitions, replicas int) natsjs.Topology {
	return natsjs.Topology{
		Partitions:           partitions,
		Replicas:             replicas,
		MaxBytesPerPartition: testMaxBytes,
		DuplicateWindow:      testWindow,
		Compression:          natsjs.CompressionNone,
		ConfigVersion:        firstVersion,
	}
}

func TestTopologyRefusesWhatTheStreamsCannotCarry(t *testing.T) {
	valid := topologyOf(twoPartitions, threeReplicas)
	tests := []struct {
		name    string
		args    func(*natsjs.Topology)
		wantErr error
	}{
		{name: "valid", args: func(*natsjs.Topology) {}},
		{name: "no partitions", args: func(t *natsjs.Topology) { t.Partitions = 0 }, wantErr: natsjs.ErrInvalidTopology},
		{name: "more partitions than routing allows", args: func(t *natsjs.Topology) {
			t.Partitions = natsjs.MaxPartitions + 1
		}, wantErr: natsjs.ErrInvalidTopology},
		{name: "an even replica count", args: func(t *natsjs.Topology) { t.Replicas = 2 },
			wantErr: natsjs.ErrInvalidTopology},
		{name: "more replicas than a quorum needs", args: func(t *natsjs.Topology) { t.Replicas = 7 },
			wantErr: natsjs.ErrInvalidTopology},
		{name: "a stream smaller than one message", args: func(t *natsjs.Topology) {
			t.MaxBytesPerPartition = natsjs.MaxMessageBytes - 1
		}, wantErr: natsjs.ErrInvalidTopology},
		{name: "no dedup window", args: func(t *natsjs.Topology) { t.DuplicateWindow = 0 },
			wantErr: natsjs.ErrInvalidTopology},
		{name: "no config version", args: func(t *natsjs.Topology) { t.ConfigVersion = 0 },
			wantErr: natsjs.ErrInvalidTopology},
		{name: "an unknown compression", args: func(t *natsjs.Topology) { t.Compression = natsjs.CompressionS2 + 1 },
			wantErr: natsjs.ErrInvalidTopology},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			topology := valid
			tt.args(&topology)

			err := topology.Validate()

			if !errors.Is(err, tt.wantErr) {
				t.Errorf("Validate = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestEveryPartitionHasItsOwnStreamAndSubject(t *testing.T) {
	topology := topologyOf(twoPartitions, oneReplica)

	got := []string{
		topology.Stream(firstPartition), topology.Subject(firstPartition),
		topology.Stream(secondPartition), topology.Subject(secondPartition),
	}

	want := []string{"FAULTLINE_SPANS_P0", "faultline.spans.p0", "FAULTLINE_SPANS_P1", "faultline.spans.p1"}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("names (-want +got):\n%s", diff)
	}
}
