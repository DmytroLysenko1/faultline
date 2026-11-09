package stream_test

import (
	"math/rand/v2"
	"testing"
	"time"

	"github.com/DmytroLysenko1/faultline/pkg/sketch"
	"github.com/DmytroLysenko1/faultline/pkg/stream"
)

const (
	windowSize = 10 * time.Second
	delay      = 30 * time.Second
	lateness   = 30 * time.Second
	slack      = 2
	capacity   = 64
	arenaSize  = 1 << 20
)

var (
	epoch  = time.Date(2026, 11, 9, 12, 0, 0, 0, time.UTC)
	config = stream.Config{Size: windowSize, Delay: delay, AllowedLateness: lateness, SlackWindows: slack}
)

type cell struct {
	requests uint64
	latency  sketch.Sketch
}

type event struct {
	key     int
	at      time.Time
	untimed bool
	latency int64
}

type batch struct {
	receivedAt time.Time
	heartbeat  bool
	events     []event
}

type cellKey struct {
	key    int
	window int64
}

type finalPane struct {
	Key      int
	Window   int64
	Kind     stream.PaneKind
	Revision uint32
	Requests uint64
}

type retired struct {
	requests uint64
	latency  sketch.Sketch
}

type recorder struct {
	t       testing.TB
	arena   *sketch.Arena
	log     []finalPane
	batchOf []int
	batch   int
	retired map[cellKey]*retired
	closed  []int
}

func newRecorder(t testing.TB) *recorder {
	t.Helper()
	arena, err := sketch.NewArena(arenaSize)
	if err != nil {
		t.Fatal(err)
	}
	return &recorder{t: t, arena: arena, retired: make(map[cellKey]*retired)}
}

func (r *recorder) Emit(key int, pane stream.Pane, c *cell) {
	if _, gone := r.retired[cellKey{key: key, window: pane.Window.ID}]; gone {
		r.t.Fatalf("pane %+v for key %d after its window retired", pane, key)
	}
	r.log = append(r.log, finalPane{Key: key, Window: pane.Window.ID, Kind: pane.Kind, Revision: pane.Revision,
		Requests: c.requests})
	r.batchOf = append(r.batchOf, r.batch)
}

func (r *recorder) Retire(key int, window stream.Window, c *cell) {
	r.retired[cellKey{key: key, window: window.ID}] = &retired{requests: c.requests, latency: c.latency}
	c.latency = sketch.Sketch{}
}

func (r *recorder) Closed(key int) {
	r.closed = append(r.closed, key)
}

func (r *recorder) finals() []finalPane {
	finals := make([]finalPane, 0, len(r.log))
	for _, pane := range r.log {
		if pane.Kind != stream.PaneEarly {
			finals = append(finals, pane)
		}
	}
	return finals
}

func (r *recorder) releaseRetired() {
	for _, cell := range r.retired {
		r.arena.Release(&cell.latency)
	}
}

type run struct {
	t         testing.TB
	watermark *stream.Watermark
	windows   *stream.Windows[int, cell]
	recorder  *recorder
}

func newRun(t testing.TB, early bool) *run {
	t.Helper()
	watermark, err := stream.NewWatermark(config)
	if err != nil {
		t.Fatal(err)
	}
	recorder := newRecorder(t)
	windows, err := stream.New[int, cell](config, stream.Options{Capacity: capacity, Early: early}, recorder)
	if err != nil {
		t.Fatal(err)
	}
	return &run{t: t, watermark: watermark, windows: windows, recorder: recorder}
}

func (r *run) apply(b batch) {
	candidate := candidateOf(b)
	r.recorder.batch++
	mark := r.watermark.Advance(b.receivedAt, candidate)
	anchor := r.watermark.Anchor(candidate, b.receivedAt)
	r.windows.Begin(mark, anchor)
	for _, e := range b.events {
		r.place(e, anchor)
	}
	r.windows.End()
}

func (r *run) place(e event, anchor time.Time) {
	t := e.at
	if e.untimed {
		t = anchor
	}
	classified := r.windows.Classify(t)
	c, placement, _ := r.windows.Locate(e.key, t)
	if placement != classified {
		r.t.Fatalf("event at %s: Classify %v, Locate %v", t, classified, placement)
	}
	if c != nil {
		c.requests++
		r.recorder.arena.Add(&c.latency, e.latency)
	}
}

func candidateOf(b batch) stream.Mark {
	if b.heartbeat {
		return stream.Mark{Time: b.receivedAt, Known: true}
	}
	candidate := stream.Mark{}
	for _, e := range b.events {
		if !e.untimed && (!candidate.Known || e.at.After(candidate.Time)) {
			candidate = stream.Mark{Time: e.at, Known: true}
		}
	}
	if candidate.Known && candidate.Time.After(b.receivedAt) {
		candidate.Time = b.receivedAt
	}
	return candidate
}

func heartbeat(at time.Time) batch {
	return batch{receivedAt: at, heartbeat: true}
}

func flushAfter(last time.Time) time.Time {
	return last.Add(delay + lateness + 3*windowSize)
}

type scenario struct {
	batches []batch
	last    time.Time
}

const (
	maxLag       = delay + lateness + 2*time.Minute
	maxFutureLag = 5 * time.Minute
)

func generate(random *rand.Rand) scenario {
	received := epoch
	batches := make([]batch, 0, 80)
	for range 40 + random.IntN(40) {
		received = received.Add(time.Duration(random.IntN(2500)) * time.Millisecond)
		if random.IntN(15) == 0 {
			received = received.Add(time.Duration(random.IntN(90)) * time.Second)
		}
		batches = append(batches, randomBatch(random, received))
	}
	return scenario{batches: batches, last: received}
}

func randomBatch(random *rand.Rand, received time.Time) batch {
	switch random.IntN(10) {
	case 0:
		return heartbeat(received)
	case 1:
		return batch{receivedAt: received, events: []event{{key: random.IntN(capacity / 2), untimed: true}}}
	}
	events := make([]event, 1+random.IntN(12))
	for i := range events {
		events[i] = randomEvent(random, received)
	}
	return batch{receivedAt: received, events: events}
}

func randomEvent(random *rand.Rand, received time.Time) event {
	e := event{key: random.IntN(capacity / 2), latency: random.Int64N(int64(2 * time.Second))}
	switch random.IntN(20) {
	case 0:
		e.untimed = true
	case 1:
		e.at = received.Add(time.Duration(random.Int64N(int64(maxFutureLag))))
	default:
		e.at = received.Add(-time.Duration(random.Int64N(int64(maxLag))))
	}
	return e
}
