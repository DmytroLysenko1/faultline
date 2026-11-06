package replay_test

import (
	"testing"

	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/DmytroLysenko1/faultline/internal/domain/topology"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/connectapi/graphmap"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp/otlptest"
)

const fuzzLimit = 64

func fuzzLimits() topology.Limits {
	return topology.Limits{
		PairingTTL:    pairingTTL,
		MaxPending:    fuzzLimit,
		TombstoneTTL:  tombstoneTTL,
		MaxTombstones: fuzzLimit,
		MaxNodes:      fuzzLimit,
		MaxEdges:      fuzzLimit,
	}
}

func FuzzExportReachesGraph(f *testing.F) {
	for _, line := range otlptest.DemoLines(f) {
		f.Add(otlptest.Encode(f, line))
	}
	f.Fuzz(func(t *testing.T, wire []byte) {
		request := ptraceotlp.NewExportRequest()
		if err := request.UnmarshalProto(wire); err != nil {
			return
		}
		snapshot := replay(t, fuzzLimits(), []ptrace.Traces{
			request.Traces(),
		})
		assertMarshals(t, snapshot)
	})
}

func assertMarshals(t *testing.T, snapshot *topology.Snapshot) {
	t.Helper()
	mapped, err := graphmap.Graph(snapshot)
	if err != nil {
		t.Fatalf("graphmap.Graph: %v", err)
	}
	if _, err := proto.Marshal(mapped); err != nil {
		t.Fatalf("proto.Marshal of an accepted request: %v", err)
	}
	if _, err := protojson.Marshal(mapped); err != nil {
		t.Fatalf("protojson.Marshal of an accepted request: %v", err)
	}
}
