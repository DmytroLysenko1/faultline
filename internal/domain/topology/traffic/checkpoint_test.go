package traffic_test

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/resource"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/telemetrytest"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/handle"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/traffic"
	"github.com/DmytroLysenko1/faultline/pkg/checkpoint"
)

const (
	restoreJournals = 200
	trafficSection  = checkpoint.Tag(1)
	frontendHandle  = handle.Node(testNodes)
)

// generatedSubjects is what a graph would hold for the generator: svc-k at handle k, frontend after
// them, and edge e from frontend to svc-(e mod nodes).
func generatedSubjects() *traffic.Subjects {
	known := &traffic.Subjects{}
	for node := range testNodes {
		known.Nodes = append(known.Nodes, telemetrytest.Service(fmt.Sprintf("svc-%d", node)))
	}
	known.Nodes = append(known.Nodes, telemetrytest.Service("frontend"))
	for edge := range testEdges {
		known.Edges = append(known.Edges, traffic.EdgeSubject{
			Caller: frontendHandle,
			Callee: handle.Node(edge % testNodes),
		})
	}
	return known
}

func trafficCheckpoint(t *testing.T, built *traffic.Traffic) []byte {
	t.Helper()
	var out bytes.Buffer
	w := checkpoint.NewWriter(&out, 1)
	w.Begin(trafficSection)
	built.WriteCheckpoint(w)
	w.End()
	if err := w.Err(); err != nil {
		t.Fatalf("write: %v", err)
	}
	return out.Bytes()
}

func restoreTraffic(t *testing.T, limits *traffic.Limits, known *traffic.Subjects, data []byte) (*harness, error) {
	t.Helper()
	sink := &collector{}
	r := checkpoint.NewReader(bytes.NewReader(data))
	r.Begin(trafficSection)
	restored, err := traffic.Restore(limits, sink, known, r)
	if err != nil {
		return nil, err
	}
	r.End()
	return &harness{t: t, traffic: restored, sink: sink}, r.Finish()
}

type published struct {
	Finals []traffic.Final
	View   traffic.View
	Stats  traffic.Stats
}

func continueWith(h *harness, seed uint64, from time.Time) published {
	g := generator{rng: rand.New(rand.NewPCG(seed, seed))}
	before := len(h.sink.finals)
	receivedAt := from
	for range batchesEach {
		receivedAt = receivedAt.Add(time.Duration(g.rng.IntN(maxStepSeconds)) * time.Second)
		g.batch(h, receivedAt)
		if g.chance(fireChance) {
			h.traffic.Fire()
		}
	}
	h.flush()
	return published{
		Finals: h.sink.finals[before:],
		View:   h.traffic.View(),
		Stats:  h.traffic.Stats(),
	}
}

func playFor(t *testing.T, seed uint64, limits *traffic.Limits, batches int) (*harness, time.Time) {
	t.Helper()
	h := newHarness(t, limits)
	g := generator{rng: rand.New(rand.NewPCG(seed, seed))}
	receivedAt := base
	for range batches {
		receivedAt = receivedAt.Add(time.Duration(g.rng.IntN(maxStepSeconds)) * time.Second)
		g.batch(h, receivedAt)
		if g.chance(fireChance) {
			h.traffic.Fire()
		}
	}
	return h, receivedAt
}

// TestRestoredTrafficContinuesLikeTheOriginal: windows, sketches, the operation registry, the
// subject book and the views come back as they were, so the same events give the same finals, views
// and stats — with the small series limit that sends operations to overflow, too.
func TestRestoredTrafficContinuesLikeTheOriginal(t *testing.T) {
	for seed := range uint64(restoreJournals) {
		limits := testLimits()
		if seed%2 == 0 {
			limits.MaxSeries = smallSeries
		}
		original, last := playFor(t, seed, limits, int(seed%batchesEach))
		data := trafficCheckpoint(t, original.traffic)
		restored, err := restoreTraffic(t, limits, generatedSubjects(), data)
		if err != nil {
			t.Fatalf("seed %d: restore: %v", seed, err)
		}
		if !bytes.Equal(data, trafficCheckpoint(t, restored.traffic)) {
			t.Fatalf("seed %d: a restored traffic writes other bytes", seed)
		}

		want := continueWith(original, seed+1, last)
		got := continueWith(restored, seed+1, last)

		if diff := cmp.Diff(want, got); diff != "" {
			t.Fatalf("seed %d (-original +restored):\n%s", seed, diff)
		}
	}
}

// TestTrafficRefusesABookOtherThanTheGraph: the book names services by the graph's handles; a graph
// that disagrees (another service, another callee, fewer subjects) is a corrupt checkpoint.
func TestTrafficRefusesABookOtherThanTheGraph(t *testing.T) {
	original, _ := playFor(t, 7, testLimits(), batchesEach)
	data := trafficCheckpoint(t, original.traffic)
	tests := []struct {
		name   string
		change func(known *traffic.Subjects)
	}{
		{name: "another service at a handle", change: func(known *traffic.Subjects) {
			known.Nodes[0] = telemetrytest.Service("elsewhere")
		}},
		{name: "another callee on an edge", change: func(known *traffic.Subjects) {
			known.Edges[0].Callee = handle.Node(testNodes - 1)
		}},
		{name: "fewer nodes than the book", change: func(known *traffic.Subjects) {
			known.Nodes = known.Nodes[:1]
		}},
		{name: "fewer edges than the views", change: func(known *traffic.Subjects) {
			known.Edges = known.Edges[:1]
		}},
		{name: "a zero service where the book has one", change: func(known *traffic.Subjects) {
			known.Nodes[1] = resource.ServiceRef{}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			known := generatedSubjects()
			tt.change(known)

			_, err := restoreTraffic(t, testLimits(), known, data)

			if !errors.Is(err, checkpoint.ErrCorrupt) {
				t.Errorf("err = %v, want ErrCorrupt", err)
			}
		})
	}
}

func TestTrafficRestoreRefusesInvalidLimits(t *testing.T) {
	limits := testLimits()
	limits.EarlyFirePeriod = 0

	_, err := restoreTraffic(t, limits, generatedSubjects(), trafficCheckpoint(t, newHarness(t, testLimits()).traffic))

	if !errors.Is(err, traffic.ErrInvalidLimits) {
		t.Errorf("err = %v, want ErrInvalidLimits", err)
	}
}

func TestTrafficRestoreRefusesSmallerEngines(t *testing.T) {
	original, _ := playFor(t, 3, testLimits(), batchesEach)
	data := trafficCheckpoint(t, original.traffic)
	tests := []struct {
		name   string
		change func(*traffic.Limits)
	}{
		{name: "fewer series", change: func(l *traffic.Limits) { l.MaxSeries = 1 }},
		{name: "a smaller arena", change: func(l *traffic.Limits) { l.SketchEntries /= 2 }},
		{name: "fewer nodes", change: func(l *traffic.Limits) { l.MaxNodes = 1 }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			limits := testLimits()
			tt.change(limits)

			_, err := restoreTraffic(t, limits, generatedSubjects(), data)

			if err == nil {
				t.Errorf("a smaller %s accepted the checkpoint", tt.name)
			}
		})
	}
}
