package topology_test

import (
	stdcmp "cmp"
	"math/rand/v2"
	"slices"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/batch"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/eventtime"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/kind"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/operation"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/resource"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/span"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/telemetrytest"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/red"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/topologytest"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/traffic"
)

const (
	redWindow       = 10 * time.Second
	redFlushAhead   = time.Hour
	redCallLatency  = 5 * time.Millisecond
	redSeed         = 6
	redScenarios    = 300
	redCallsMax     = 80
	redWindowsSpan  = 6
	redFailOneIn    = 4
	redMaxDisplaced = 5 * time.Second
	redBatchMax     = 7
	redReceiveLag   = 2 * time.Second
)

var redOperations = [...]string{"GET /cart", "POST /cart", "GET /checkout"}

type finalsSink struct {
	got []traffic.Final
}

func (s *finalsSink) Final(final *traffic.Final) {
	s.got = append(s.got, *final)
}

func timedHalf(tb testing.TB, spanKind kind.Kind, trace uint64, service string, end time.Time, failed bool) span.Span {
	tb.Helper()
	half := telemetrytest.NumberedSpan(tb, spanKind, trace, service)
	half.Interval = eventtime.FromUnixNanos(uint64(end.Add(-redCallLatency).UnixNano()), uint64(end.UnixNano()))
	half.Operation = operation.Operation{
		Protocol: operation.ProtocolHTTP,
		Name:     redOperations[trace%uint64(len(redOperations))],
	}
	half.Failed = failed
	return half
}

// timedPair is one call; the failure is on the server half, so the edge and the served node agree.
func timedPair(t *testing.T, trace uint64, caller, callee string, end time.Time, failed bool) []span.Span {
	t.Helper()
	return timedPairTB(t, trace, caller, callee, end, failed)
}

func timedPairTB(tb testing.TB, trace uint64, caller, callee string, end time.Time, failed bool) []span.Span {
	tb.Helper()
	return []span.Span{
		timedHalf(tb, kind.Client, trace, caller, end, false),
		timedHalf(tb, kind.Server, trace, callee, end, failed),
	}
}

func receivedAt(t *testing.T, at time.Time, spans ...span.Span) batch.Batch {
	t.Helper()
	received, err := batch.New(spans, at)
	if err != nil {
		t.Fatalf("batch.New: %v", err)
	}
	return received
}

func flush(builder *topology.Builder, from time.Time) {
	builder.Apply(batch.Heartbeat(from.Add(redFlushAhead)))
}

// redOf keeps what a test can predict exactly; percentiles have their own tests in red and sketch.
func redOf(stats red.WindowStats) red.WindowStats {
	return red.WindowStats{
		StartUnixNano:  stats.StartUnixNano,
		Kind:           stats.Kind,
		Requests:       stats.Requests,
		Failed:         stats.Failed,
		LatencySamples: stats.LatencySamples,
	}
}

type subjectRED struct {
	Live  red.WindowStats
	Final red.WindowStats
}

func edgeRED(t *testing.T, snapshot *topology.Snapshot, caller, callee string) subjectRED {
	t.Helper()
	for i := range snapshot.Edges {
		found := &snapshot.Edges[i]
		if found.Source == node(caller).ID && found.Target == node(callee).ID {
			return subjectRED{Live: redOf(found.Live), Final: redOf(found.Final)}
		}
	}
	t.Fatalf("edge %s → %s is not in the snapshot", caller, callee)
	return subjectRED{}
}

func nodeRED(t *testing.T, snapshot *topology.Snapshot, service string) subjectRED {
	t.Helper()
	for i := range snapshot.Nodes {
		if found := &snapshot.Nodes[i]; found.ID == node(service).ID {
			return subjectRED{Live: redOf(found.Live), Final: redOf(found.Final)}
		}
	}
	t.Fatalf("node %s is not in the snapshot", service)
	return subjectRED{}
}

func threeCallsOneFailed(t *testing.T, end time.Time) []span.Span {
	t.Helper()
	return slices.Concat(
		timedPair(t, 1, "frontend", "cart", end, false),
		timedPair(t, 2, "frontend", "cart", end, true),
		timedPair(t, 3, "frontend", "cart", end, false),
	)
}

func TestSnapshotShowsLiveREDAfterFireAndFinalREDAfterTheWindowCloses(t *testing.T) {
	builder := newTestBuilder(t, builderLimits())
	start := telemetrytest.Epoch()
	builder.Apply(receivedAt(t, start.Add(2*time.Second), threeCallsOneFailed(t, start.Add(time.Second))...))
	summary := red.WindowStats{StartUnixNano: start.UnixNano(), Requests: 3, Failed: 1, LatencySamples: 3}
	live, final := summary, summary
	live.Kind, final.Kind = red.PaneEarly, red.PaneOnTime
	steps := []struct {
		Name string
		Args func()
		Want subjectRED
	}{
		{
			Name: "apply alone shows no summary",
			Args: func() {},
		},
		{
			Name: "fire shows the open window as live",
			Args: builder.Fire,
			Want: subjectRED{Live: live},
		},
		{
			Name: "closing the window moves the summary to final and clears live",
			Args: func() { flush(builder, start) },
			Want: subjectRED{Final: final},
		},
	}
	for _, step := range steps {
		step.Args()
		snapshot := builder.Snapshot()
		if diff := cmp.Diff(step.Want, edgeRED(t, snapshot, "frontend", "cart")); diff != "" {
			t.Errorf("%s: edge RED mismatch (-want +got):\n%s", step.Name, diff)
		}
		if diff := cmp.Diff(step.Want, nodeRED(t, snapshot, "cart")); diff != "" {
			t.Errorf("%s: served RED of the callee mismatch (-want +got):\n%s", step.Name, diff)
		}
		if diff := cmp.Diff(subjectRED{}, nodeRED(t, snapshot, "frontend")); diff != "" {
			t.Errorf("%s: the caller served nothing (-want +got):\n%s", step.Name, diff)
		}
	}
}

func TestRevisionMovesWithREDButNotWithTheWatermark(t *testing.T) {
	builder := newTestBuilder(t, builderLimits())
	start := telemetrytest.Epoch()
	type check struct {
		Revision       uint64
		GeneratedAt    time.Time
		WatermarkMoved bool
	}
	var lastWatermark time.Time
	observe := func() check {
		snapshot := builder.Snapshot()
		moved := !snapshot.Watermark.Equal(lastWatermark)
		lastWatermark = snapshot.Watermark
		return check{Revision: snapshot.Revision, GeneratedAt: snapshot.GeneratedAt, WatermarkMoved: moved}
	}
	steps := []struct {
		Name string
		Args func()
		Want check
	}{
		{
			Name: "a new edge moves the revision",
			Args: func() {
				builder.Apply(receivedAt(t, start.Add(time.Second), threeCallsOneFailed(t, start.Add(time.Second))...))
			},
			Want: check{Revision: 1, GeneratedAt: start.Add(time.Second), WatermarkMoved: true},
		},
		{
			Name: "a live summary moves the revision, dated by the newest batch",
			Args: builder.Fire,
			Want: check{Revision: 2, GeneratedAt: start.Add(time.Second)},
		},
		{
			Name: "a heartbeat that closes no window moves only the watermark",
			Args: func() { builder.Apply(batch.Heartbeat(start.Add(5 * time.Second))) },
			Want: check{Revision: 2, GeneratedAt: start.Add(time.Second), WatermarkMoved: true},
		},
		{
			Name: "fire with the same live summary moves nothing",
			Args: builder.Fire,
			Want: check{Revision: 2, GeneratedAt: start.Add(time.Second)},
		},
		{
			Name: "a final summary moves the revision",
			Args: func() { flush(builder, start) },
			Want: check{Revision: 3, GeneratedAt: start.Add(redFlushAhead), WatermarkMoved: true},
		},
		{
			Name: "a snapshot with nothing new keeps the revision",
			Args: func() {},
			Want: check{Revision: 3, GeneratedAt: start.Add(redFlushAhead)},
		},
	}
	for _, step := range steps {
		step.Args()
		if diff := cmp.Diff(step.Want, observe()); diff != "" {
			t.Errorf("%s: mismatch (-want +got):\n%s", step.Name, diff)
		}
	}
}

func TestSnapshotCarriesTheWindowSizeAndTheWatermark(t *testing.T) {
	builder := newTestBuilder(t, builderLimits())
	type diagnostics struct {
		WindowSize     time.Duration
		Watermark      time.Time
		WatermarkKnown bool
	}
	view := func(snapshot *topology.Snapshot) diagnostics {
		return diagnostics{snapshot.WindowSize, snapshot.Watermark, snapshot.WatermarkKnown}
	}
	if diff := cmp.Diff(diagnostics{WindowSize: redWindow}, view(builder.Snapshot())); diff != "" {
		t.Errorf("before any batch (-want +got):\n%s", diff)
	}
	start := telemetrytest.Epoch()
	builder.Apply(receivedAt(t, start.Add(time.Minute), timedPair(t, 1, "frontend", "cart", start.Add(time.Minute), false)...))
	want := diagnostics{
		WindowSize:     redWindow,
		Watermark:      start.Add(time.Minute - builderLimits().Windows.Delay),
		WatermarkKnown: true,
	}
	if diff := cmp.Diff(want, view(builder.Snapshot())); diff != "" {
		t.Errorf("after a batch (-want +got):\n%s", diff)
	}
}

func TestStructureChangesOnlyWithANewNodeOrEdge(t *testing.T) {
	builder := newTestBuilder(t, builderLimits())
	start := telemetrytest.Epoch()
	steps := []struct {
		Name     string
		Args     func()
		Snapshot bool
		Want     bool
	}{
		{
			Name: "nothing yet",
			Args: func() {},
			Want: false,
		},
		{
			Name: "a new edge and its nodes",
			Args: func() {
				builder.Apply(receivedAt(t, start, timedPair(t, 1, "frontend", "cart", start, false)...))
			},
			Want: true,
		},
		{
			Name:     "a snapshot publishes the structure",
			Args:     func() {},
			Snapshot: true,
			Want:     false,
		},
		{
			Name: "more calls on a known edge and a live summary are not structure",
			Args: func() {
				builder.Apply(receivedAt(t, start, timedPair(t, 2, "frontend", "cart", start, true)...))
				builder.Fire()
			},
			Want: false,
		},
		{
			Name: "a new caller node alone is structure",
			Args: func() {
				builder.Apply(receivedAt(t, start, telemetrytest.Client(t, 3, 1, "checkout", nil)))
			},
			Want: true,
		},
		{
			Name: "a new edge between published nodes is structure",
			Args: func() {
				builder.Snapshot()
				builder.Apply(receivedAt(t, start, timedPair(t, 4, "cart", "frontend", start, false)...))
			},
			Want: true,
		},
	}
	for _, step := range steps {
		step.Args()
		if step.Snapshot {
			builder.Snapshot()
		}
		if got := builder.StructureChanged(); got != step.Want {
			t.Errorf("%s: StructureChanged() = %v, want %v", step.Name, got, step.Want)
		}
	}
}

func TestFinalSinkSeesSubjectAndOperationFinals(t *testing.T) {
	sink := &finalsSink{}
	builder := newTestBuilder(t, builderLimits(), topology.WithFinalSink(sink))
	start := telemetrytest.Epoch()
	builder.Apply(receivedAt(t, start.Add(time.Second), threeCallsOneFailed(t, start.Add(time.Second))...))
	flush(builder, start)
	type final struct {
		Series traffic.SeriesKey
		Stats  red.WindowStats
	}
	got := make([]final, 0, len(sink.got))
	for i := range sink.got {
		got = append(got, final{Series: sink.got[i].Series, Stats: redOf(sink.got[i].Stats)})
	}
	frontend, cart := telemetrytest.Service("frontend"), telemetrytest.Service("cart")
	stats := func(requests, failed, samples uint64) red.WindowStats {
		return red.WindowStats{
			StartUnixNano:  start.UnixNano(),
			Kind:           red.PaneOnTime,
			Requests:       requests,
			Failed:         failed,
			LatencySamples: samples,
		}
	}
	operationSeries := func(subject traffic.SubjectKind, caller resource.ServiceRef, name string) traffic.SeriesKey {
		return traffic.SeriesKey{
			Kind:      traffic.SeriesOperation,
			Subject:   subject,
			Caller:    caller,
			Callee:    cart,
			Protocol:  operation.ProtocolHTTP,
			Operation: name,
		}
	}
	edge := traffic.SeriesKey{Kind: traffic.SeriesSubject, Subject: traffic.SubjectEdge, Caller: frontend, Callee: cart}
	served := traffic.SeriesKey{Kind: traffic.SeriesSubject, Subject: traffic.SubjectNode, Callee: cart}
	want := []final{
		{Series: edge, Stats: stats(3, 1, 3)},
		{Series: served, Stats: stats(3, 1, 3)},
		{Series: operationSeries(traffic.SubjectEdge, frontend, "POST /cart"), Stats: stats(1, 0, 0)},
		{Series: operationSeries(traffic.SubjectEdge, frontend, "GET /checkout"), Stats: stats(1, 1, 0)},
		{Series: operationSeries(traffic.SubjectEdge, frontend, "GET /cart"), Stats: stats(1, 0, 0)},
		{Series: operationSeries(traffic.SubjectNode, resource.ServiceRef{}, "POST /cart"), Stats: stats(1, 0, 0)},
		{Series: operationSeries(traffic.SubjectNode, resource.ServiceRef{}, "GET /checkout"), Stats: stats(1, 1, 0)},
		{Series: operationSeries(traffic.SubjectNode, resource.ServiceRef{}, "GET /cart"), Stats: stats(1, 0, 0)},
	}
	sortFinals := cmpopts.SortSlices(func(a, b final) int {
		return stdcmp.Or(
			stdcmp.Compare(a.Series.Kind, b.Series.Kind),
			stdcmp.Compare(a.Series.Subject, b.Series.Subject),
			stdcmp.Compare(a.Series.Operation, b.Series.Operation),
		)
	})
	if diff := cmp.Diff(want, got, sortFinals); diff != "" {
		t.Errorf("finals mismatch (-want +got):\n%s", diff)
	}
}

func TestBuilderWithoutAFinalSinkStillClosesWindows(t *testing.T) {
	builder := newTestBuilder(t, builderLimits())
	start := telemetrytest.Epoch()
	builder.Apply(receivedAt(t, start.Add(time.Second), threeCallsOneFailed(t, start.Add(time.Second))...))
	flush(builder, start)
	snapshot := builder.Snapshot()
	if got := snapshot.TrafficStats.Finals; got == 0 {
		t.Fatalf("no final panes were counted: %+v", snapshot.TrafficStats)
	}
	if got := edgeRED(t, snapshot, "frontend", "cart").Final.Requests; got != 3 {
		t.Errorf("final requests = %d, want 3", got)
	}
}

type redCall struct {
	Trace  uint64
	Caller string
	Callee string
	End    time.Time
	Failed bool
}

// redScenario returns calls in arrival order: each call arrives at most redMaxDisplaced after
// calls that ended later, well inside the watermark delay, so every event is on time.
func redScenario(rng *rand.Rand) []redCall {
	calls := make([]redCall, 1+rng.IntN(redCallsMax))
	displaced := make(map[uint64]time.Time, len(calls))
	for i := range calls {
		caller := topologytest.RandomService(rng)
		callee := topologytest.RandomService(rng)
		end := telemetrytest.Epoch().Add(time.Duration(rng.Int64N(int64(redWindowsSpan * redWindow))))
		calls[i] = redCall{
			Trace:  uint64(i + 1),
			Caller: caller,
			Callee: callee,
			End:    end,
			Failed: rng.IntN(redFailOneIn) == 0,
		}
		displaced[calls[i].Trace] = end.Add(time.Duration(rng.Int64N(int64(redMaxDisplaced))))
	}
	slices.SortFunc(calls, func(a, b redCall) int {
		return displaced[a.Trace].Compare(displaced[b.Trace])
	})
	return calls
}

// redBatches groups calls in arrival order; a batch is received after its newest call ended.
func redBatches(t *testing.T, rng *rand.Rand, calls []redCall) []batch.Batch {
	t.Helper()
	var (
		batches []batch.Batch
		latest  time.Time
	)
	for len(calls) > 0 {
		size := min(len(calls), 1+rng.IntN(redBatchMax))
		spans := make([]span.Span, 0, 2*size)
		for _, current := range calls[:size] {
			spans = append(spans, timedPair(t, current.Trace, current.Caller, current.Callee, current.End, current.Failed)...)
			latest = later(latest, current.End)
		}
		latest = latest.Add(time.Duration(rng.Int64N(int64(redReceiveLag))))
		batches = append(batches, receivedAt(t, latest, spans...))
		calls = calls[size:]
	}
	return batches
}

func later(first, second time.Time) time.Time {
	if second.After(first) {
		return second
	}
	return first
}

type redKey struct {
	Series traffic.SeriesKey
	Start  int64
}

type redCounts struct {
	Requests, Failed uint64
}

func windowStart(at time.Time) int64 {
	return at.Truncate(redWindow).UnixNano()
}

// oracleRED recounts every call into its edge, its served node and their operations.
func oracleRED(calls []redCall) map[redKey]redCounts {
	truth := make(map[redKey]redCounts)
	add := func(series traffic.SeriesKey, current *redCall) {
		key := redKey{Series: series, Start: windowStart(current.End)}
		counts := truth[key]
		counts.Requests++
		if current.Failed {
			counts.Failed++
		}
		truth[key] = counts
	}
	for i := range calls {
		current := &calls[i]
		caller, callee := telemetrytest.Service(current.Caller), telemetrytest.Service(current.Callee)
		name := redOperations[current.Trace%uint64(len(redOperations))]
		edge := traffic.SeriesKey{Kind: traffic.SeriesSubject, Subject: traffic.SubjectEdge, Caller: caller, Callee: callee}
		served := traffic.SeriesKey{Kind: traffic.SeriesSubject, Subject: traffic.SubjectNode, Callee: callee}
		add(edge, current)
		add(served, current)
		for _, subject := range []traffic.SeriesKey{edge, served} {
			subject.Kind, subject.Protocol, subject.Operation = traffic.SeriesOperation, operation.ProtocolHTTP, name
			add(subject, current)
		}
	}
	return truth
}

// lastFinals keeps the newest revision of every (series, window), as a store of finals would.
func lastFinals(finals []traffic.Final) map[redKey]redCounts {
	got := make(map[redKey]redCounts, len(finals))
	for i := range finals {
		current := &finals[i]
		key := redKey{Series: current.Series, Start: current.Stats.StartUnixNano}
		got[key] = redCounts{Requests: current.Stats.Requests, Failed: current.Stats.Failed}
	}
	return got
}

func TestFinalREDMatchesTheOracle(t *testing.T) {
	rng := rand.New(rand.NewPCG(redSeed, redSeed))
	for scenario := range redScenarios {
		sink := &finalsSink{}
		builder := newTestBuilder(t, builderLimits(), topology.WithFinalSink(sink))
		calls := redScenario(rng)
		for _, received := range redBatches(t, rng, calls) {
			builder.Apply(received)
			if rng.IntN(2) == 0 {
				builder.Fire()
			}
		}
		flush(builder, telemetrytest.Epoch().Add(redWindowsSpan*redWindow))
		if diff := cmp.Diff(oracleRED(calls), lastFinals(sink.got)); diff != "" {
			t.Fatalf("scenario %d: finals differ from the recount of true calls (-want +got):\n%s", scenario, diff)
		}
		checkFlushedAccounting(t, scenario, builder.Snapshot())
	}
}

// checkFlushedAccounting: I10 — subjects were offered exactly the graph's calls and served
// requests; I11 — every subject event went to one operation engine; I12 — nothing is held.
func checkFlushedAccounting(t *testing.T, scenario int, snapshot *topology.Snapshot) {
	t.Helper()
	var graphEvents uint64
	for i := range snapshot.Edges {
		graphEvents += snapshot.Edges[i].Calls
	}
	for i := range snapshot.Nodes {
		graphEvents += snapshot.Nodes[i].Served
	}
	stats := &snapshot.TrafficStats
	type accounting struct {
		SubjectsOffered, OperationsOffered      uint64
		OperationSeries, OverflowSeries, Sketch int
		SubjectSeries, RegisteredOperations     int
	}
	want := accounting{SubjectsOffered: graphEvents, OperationsOffered: graphEvents}
	got := accounting{
		SubjectsOffered:      stats.Subjects.Offered,
		OperationsOffered:    stats.Operations.Offered + stats.Overflow.Offered,
		SubjectSeries:        stats.Subjects.Series,
		OperationSeries:      stats.Operations.Series,
		OverflowSeries:       stats.Overflow.Series,
		Sketch:               stats.Sketch.InUse,
		RegisteredOperations: stats.Cardinality.Operations,
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Fatalf("scenario %d: I10–I12 after the flush (-want +got):\n%s", scenario, diff)
	}
}

// TestSnapshotMatchesSummariesByHandleNotByPosition: handles follow creation order, the snapshot
// follows ID order; here the two orders differ for nodes and for edges.
func TestSnapshotMatchesSummariesByHandleNotByPosition(t *testing.T) {
	builder := newTestBuilder(t, builderLimits())
	start := telemetrytest.Epoch()
	builder.Apply(receivedAt(t, start.Add(time.Second), slices.Concat(
		threeCallsOneFailed(t, start.Add(time.Second)),
		timedPair(t, 4, "cart", "checkout", start.Add(time.Second), false),
	)...))
	builder.Fire()
	snapshot := builder.Snapshot()
	type requests struct {
		FrontendCart, CartCheckout, Cart, Checkout uint64
	}
	got := requests{
		FrontendCart: edgeRED(t, snapshot, "frontend", "cart").Live.Requests,
		CartCheckout: edgeRED(t, snapshot, "cart", "checkout").Live.Requests,
		Cart:         nodeRED(t, snapshot, "cart").Live.Requests,
		Checkout:     nodeRED(t, snapshot, "checkout").Live.Requests,
	}
	if diff := cmp.Diff(requests{FrontendCart: 3, CartCheckout: 1, Cart: 3, Checkout: 1}, got); diff != "" {
		t.Errorf("summaries reached the wrong subjects (-want +got):\n%s", diff)
	}
}

// TestFinalsAreAFunctionOfTheJournal (I8): Fire only shows early panes, so the same journal gives
// the same finals, in the same order and with the same summaries, however often Fire runs and in
// whichever process it is replayed.
func TestFinalsAreAFunctionOfTheJournal(t *testing.T) {
	rng := rand.New(rand.NewPCG(redSeed, redSeed+1))
	for scenario := range redScenarios {
		journal := redBatches(t, rng, redScenario(rng))
		fireEvery := 1 + rng.IntN(len(journal))
		replay := func(fires func(index int) bool) []traffic.Final {
			sink := &finalsSink{}
			builder := newTestBuilder(t, builderLimits(), topology.WithFinalSink(sink))
			for index, received := range journal {
				builder.Apply(received)
				if fires(index) {
					builder.Fire()
				}
			}
			flush(builder, telemetrytest.Epoch().Add(redWindowsSpan*redWindow))
			return sink.got
		}
		never := replay(func(int) bool { return false })
		sometimes := replay(func(index int) bool { return index%fireEvery == 0 })
		if diff := cmp.Diff(never, sometimes); diff != "" {
			t.Fatalf("scenario %d: Fire every %d batches changed the finals (-never +sometimes):\n%s",
				scenario, fireEvery, diff)
		}
	}
}
