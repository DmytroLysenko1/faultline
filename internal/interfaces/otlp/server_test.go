package otlp_test

import (
	"context"
	"fmt"
	"iter"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/DmytroLysenko1/faultline/internal/application/ingest"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/operation"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/resource"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/span"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp/otlptest"
)

const (
	oversizedSpans   = 140_000
	sendLimitBytes   = 16 << 20
	emptySpanBytes   = 2
	gzipCompressor   = "gzip"
	bombSlack        = 64 << 10
	shortKeepalive   = 200 * time.Millisecond
	longKeepalive    = time.Hour
	connectDeadline  = 10 * time.Second
	heldRequests     = otlp.MaxConcurrentStreams
	shortRouteBytes  = 16
	loopbackAddress  = "127.0.0.1:0"
	fieldResource    = 1
	fieldScopeSpans  = 2
	fieldSpans       = 2
	fieldSpanName    = 5
	fieldAttributes  = 9
	fieldValue       = 2
	fieldArray       = 5
	fieldArrayValues = 1
	fieldIntValue    = 3
	tooDeep          = otlp.MaxNestingDepth + 1
	arrivalDeadline  = time.Minute
	wrongWireTypeTag = 0x08
)

type countingAcceptor struct {
	calls atomic.Int64
}

func (c *countingAcceptor) accept(context.Context, ingest.SpanSource) (ingest.AcceptResult, error) {
	c.calls.Add(1)
	return ingest.AcceptResult{}, nil
}

func TestPanicInAcceptorIsInternalAndTheServerKeepsServing(t *testing.T) {
	var calls atomic.Int64
	h := start(t, func(context.Context, ingest.SpanSource) (ingest.AcceptResult, error) {
		if calls.Add(1) == 1 {
			panic(journalSecret)
		}
		return ingest.AcceptResult{}, nil
	})
	_, err := h.Client.Export(t.Context(), requestOf(1))
	if st := status.Convert(err); st.Code() != codes.Internal || st.Message() != "internal error" {
		t.Fatalf("panic answered %v %q, want INTERNAL without the panic value", st.Code(), st.Message())
	}
	if _, err := h.Client.Export(t.Context(), requestOf(1)); err != nil {
		t.Errorf("Export after a panic: %v", err)
	}
	if got := len(h.Logs.Events(t, "rpc.panic")); got != 1 {
		t.Errorf("%d rpc.panic events, want 1", got)
	}
}

func TestGzipRequestIsDecompressed(t *testing.T) {
	var spans atomic.Int64
	h := start(t, func(_ context.Context, source ingest.SpanSource) (ingest.AcceptResult, error) {
		spans.Store(int64(source.Len()))
		return ingest.AcceptResult{}, nil
	})
	const sent = 3
	if _, err := h.Client.Export(t.Context(), requestOf(sent), grpc.UseCompressor(gzipCompressor)); err != nil {
		t.Fatalf("gzip Export: %v", err)
	}
	if spans.Load() != sent {
		t.Errorf("acceptor saw %d spans, want %d", spans.Load(), sent)
	}
}

func TestOversizedRequestIsRejectedByGRPCWithoutRetryInfo(t *testing.T) {
	acceptor := &countingAcceptor{}
	h := start(t, acceptor.accept)
	_, err := h.Client.Export(t.Context(), requestOf(oversizedSpans), grpc.MaxCallSendMsgSize(sendLimitBytes))
	st := status.Convert(err)
	if st.Code() != codes.ResourceExhausted || retryDelay(st) != 0 {
		t.Errorf("oversized request answered %v with retry delay %v, want RESOURCE_EXHAUSTED without RetryInfo",
			st.Code(), retryDelay(st))
	}
	if acceptor.calls.Load() != 0 {
		t.Errorf("acceptor called %d times for an oversized request", acceptor.calls.Load())
	}
}

func spanBomb() []byte {
	spans := otlptest.Repeat(otlptest.Message(fieldSpans, nil), (otlp.MaxRecvMsgBytes-bombSlack)/emptySpanBytes)
	return otlptest.Message(fieldResource, otlptest.Message(fieldScopeSpans, spans))
}

func malformed() []byte {
	span := append([]byte{
		wrongWireTypeTag,
	}, otlptest.Message(fieldSpanName, nil)...)
	return otlptest.Message(fieldResource, otlptest.Message(fieldScopeSpans, otlptest.Message(fieldSpans, span)))
}

func nestedValues(depth int) []byte {
	value := otlptest.Message(fieldIntValue, nil)
	for range depth - 1 {
		value = otlptest.Message(fieldArray, otlptest.Message(fieldArrayValues, value))
	}
	span := otlptest.Message(fieldAttributes, otlptest.Message(fieldValue, value))
	return otlptest.Message(fieldResource, otlptest.Message(fieldScopeSpans, otlptest.Message(fieldSpans, span)))
}

type decodeEvent struct {
	Reason string
	Weight bool
	Limit  any
}

func decodeEventOf(record map[string]any) decodeEvent {
	_, weighed := record["weight"]
	return decodeEvent{
		Reason: fmt.Sprint(record["reason"]),
		Weight: weighed,
		Limit:  record["limit"],
	}
}

func TestDecodeLimitRejectsBeforeTheAcceptor(t *testing.T) {
	tests := []struct {
		Name string
		Args []byte
		Want decodeEvent
	}{
		{
			Name: "4 MiB of empty spans",
			Args: spanBomb(),
			Want: decodeEvent{
				Reason: "too_heavy",
				Weight: true,
				Limit:  float64(otlp.MaxDecodedBytes),
			},
		},
		{
			Name: "values nested past the depth limit",
			Args: nestedValues(tooDeep),
			Want: decodeEvent{
				Reason: "too_deep",
			},
		},
		{
			Name: "bytes pdata cannot decode",
			Args: append(malformed(), 0xff),
			Want: decodeEvent{
				Reason: "malformed",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			acceptor := &countingAcceptor{}
			h := start(t, acceptor.accept)
			err := sendRaw(t.Context(), h.Conn, tt.Args)
			if got := status.Code(err); got != codes.Internal {
				t.Fatalf("code = %v, want %v", got, codes.Internal)
			}
			if acceptor.calls.Load() != 0 {
				t.Errorf("acceptor called %d times for a rejected request", acceptor.calls.Load())
			}
			events := h.Logs.Events(t, "otlp.export.decode_rejected")
			if len(events) != 1 {
				t.Fatalf("decode_rejected events = %v, want exactly one", events)
			}
			if diff := cmp.Diff(tt.Want, decodeEventOf(events[0])); diff != "" {
				t.Errorf("decode_rejected event (-want +got):\n%s", diff)
			}
		})
	}
}

func startTCP(t *testing.T, acceptor acceptorFunc, keepalive otlp.Keepalive) string {
	t.Helper()
	var config net.ListenConfig
	listener, err := config.Listen(t.Context(), "tcp", loopbackAddress)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	settings := options(&logBuffer{}, &fakeClock{})
	settings.Keepalive = keepalive
	serve(t, otlp.LimitListener(listener), acceptor, settings)
	return listener.Addr().String()
}

func tcpClient(t *testing.T, address string) *grpc.ClientConn {
	t.Helper()
	return dial(t, func(ctx context.Context, _ string) (net.Conn, error) {
		var dialer net.Dialer
		return dialer.DialContext(ctx, "tcp", address)
	})
}

func exportWithin(t *testing.T, conn *grpc.ClientConn) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), connectDeadline)
	defer cancel()
	_, err := ptraceotlp.NewGRPCClient(conn).Export(ctx, requestOf(1))
	return err
}

func TestIdleConnectionReleasesTheOnlySlot(t *testing.T) {
	acceptor := &countingAcceptor{}
	address := startTCP(t, acceptor.accept, otlp.Keepalive{
		Time:                  longKeepalive,
		Timeout:               longKeepalive,
		MaxConnectionIdle:     shortKeepalive,
		MaxConnectionAge:      longKeepalive,
		MaxConnectionAgeGrace: longKeepalive,
	})
	if err := exportWithin(t, tcpClient(t, address)); err != nil {
		t.Fatalf("first client: %v", err)
	}
	if err := exportWithin(t, tcpClient(t, address)); err != nil {
		t.Errorf("second client was not served after the idle first one: %v", err)
	}
}

func TestSecondConnectionWaitsWhileTheOnlySlotIsBusy(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int64
	address := startTCP(t, func(ctx context.Context, _ ingest.SpanSource) (ingest.AcceptResult, error) {
		if calls.Add(1) == 1 {
			close(entered)
			select {
			case <-release:
			case <-ctx.Done():
			}
		}
		return ingest.AcceptResult{}, nil
	}, otlp.DefaultOptions(nil, nil).Keepalive)
	busyClient := tcpClient(t, address)
	var busy sync.WaitGroup
	busy.Go(func() {
		if err := exportWithin(t, busyClient); err != nil {
			t.Errorf("busy client: %v", err)
		}
	})
	<-entered
	ctx, cancel := context.WithTimeout(t.Context(), shortKeepalive)
	defer cancel()
	_, err := ptraceotlp.NewGRPCClient(tcpClient(t, address)).Export(ctx, requestOf(1))
	if status.Code(err) != codes.DeadlineExceeded {
		t.Errorf("second connection answered %v while the only slot was busy, want it held in the backlog", err)
	}
	close(release)
	busy.Wait()
}

func TestConnectionWithAStuckStreamIsRotatedByAge(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int64
	address := startTCP(t, func(ctx context.Context, _ ingest.SpanSource) (ingest.AcceptResult, error) {
		if calls.Add(1) == 1 {
			close(entered)
			select {
			case <-release:
			case <-ctx.Done():
			}
		}
		return ingest.AcceptResult{}, nil
	}, otlp.Keepalive{
		Time:                  longKeepalive,
		Timeout:               longKeepalive,
		MaxConnectionIdle:     longKeepalive,
		MaxConnectionAge:      shortKeepalive,
		MaxConnectionAgeGrace: shortKeepalive,
	})
	defer close(release)
	var stuck sync.WaitGroup
	stuckClient := tcpClient(t, address)
	stuck.Go(func() {
		if err := exportWithin(t, stuckClient); err == nil {
			t.Errorf("the stuck request finished, want it cut by the age grace")
		}
	})
	<-entered
	if err := exportWithin(t, tcpClient(t, address)); err != nil {
		t.Errorf("second client was not served after the stuck connection aged out: %v", err)
	}
	stuck.Wait()
}

type holdingSource struct {
	inner   ingest.SpanSource
	arrived chan<- struct{}
	release <-chan struct{}
}

func (h holdingSource) Len() int {
	return h.inner.Len()
}

func (h holdingSource) All() iter.Seq2[resource.Resource, *ingest.Record] {
	return func(yield func(resource.Resource, *ingest.Record) bool) {
		remaining := h.inner.Len()
		for source, fields := range h.inner.All() {
			remaining--
			if remaining == 0 {
				h.arrived <- struct{}{}
				<-h.release
			}
			if !yield(source, fields) {
				return
			}
		}
	}
}

func waitForArrivals(t *testing.T, arrived <-chan struct{}, failed <-chan struct{}) {
	t.Helper()
	deadline := time.After(arrivalDeadline)
	for range heldRequests {
		select {
		case <-arrived:
		case <-failed:
			t.Fatalf("a held request failed before reaching its last span")
		case <-deadline:
			t.Fatalf("held requests did not reach their last span within %v", arrivalDeadline)
		}
	}
}

type discardJournal struct{}

func (discardJournal) Append(context.Context, []span.Span) error {
	return nil
}

type namingJournal struct {
	mu    sync.Mutex
	names map[string]struct{}
	spans int
}

func (j *namingJournal) Append(_ context.Context, spans []span.Span) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	for index := range spans {
		j.names[spans[index].Operation.Name] = struct{}{}
	}
	j.spans += len(spans)
	return nil
}

func (j *namingJournal) counts() (names, spans int) {
	j.mu.Lock()
	defer j.mu.Unlock()
	return len(j.names), j.spans
}

// TestRoutedSpansKeepAnOperationNamePerSpan guards the heap probe below: if the normalizer folded
// the probe's routes into one name, the probe would no longer hold one name per span.
func TestRoutedSpansKeepAnOperationNamePerSpan(t *testing.T) {
	tests := []struct {
		Name string
		Args int
	}{
		{
			Name: "short routes",
			Args: shortRouteBytes,
		},
		{
			Name: "the longest routes",
			Args: operation.MaxNameBytes,
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			journal := &namingJournal{
				names: make(map[string]struct{}),
			}
			accept := ingest.NewAcceptBatch(journal, operation.NewNormalizer(nil))
			h := start(t, accept.Execute)
			traces := otlptest.RoutedSpans(t, tt.Args)
			if err := sendRaw(t.Context(), h.Conn, otlptest.Encode(t, traces)); err != nil {
				t.Fatalf("Export: %v", err)
			}
			names, spans := journal.counts()
			if diff := cmp.Diff([2]int{traces.SpanCount(), traces.SpanCount()}, [2]int{names, spans}); diff != "" {
				t.Errorf("distinct operation names and journaled spans (-want +got):\n%s", diff)
			}
		})
	}
}

func heldHeapPerRequest(t *testing.T, wire []byte) int64 {
	t.Helper()
	arrived := make(chan struct{}, heldRequests)
	release := make(chan struct{})
	accept := ingest.NewAcceptBatch(discardJournal{}, operation.NewNormalizer(nil))
	h := start(t, func(ctx context.Context, spans ingest.SpanSource) (ingest.AcceptResult, error) {
		return accept.Execute(ctx, holdingSource{
			inner:   spans,
			arrived: arrived,
			release: release,
		})
	})
	before := otlptest.HeapInUse()
	var senders sync.WaitGroup
	var signalOnce sync.Once
	failed := make(chan struct{})
	for range heldRequests {
		senders.Go(func() {
			if err := sendRaw(t.Context(), h.Conn, wire); err != nil {
				t.Errorf("held Export: %v", err)
				signalOnce.Do(func() {
					close(failed)
				})
			}
		})
	}
	waitForArrivals(t, arrived, failed)
	perRequest := (otlptest.HeapInUse() - before) / heldRequests
	close(release)
	senders.Wait()
	return perRequest
}

// TestHeldRequestsStayWithinTheMemoryBudget: the most spans a request can carry is the worst
// case; routes add operation names, but their wire bytes leave room for fewer spans.
func TestHeldRequestsStayWithinTheMemoryBudget(t *testing.T) {
	tests := []struct {
		Name string
		Args func(testing.TB) ptrace.Traces
	}{
		{
			Name: "the most spans without attributes",
			Args: otlptest.MinimalSpans,
		},
		{
			Name: "short unique routes",
			Args: func(tb testing.TB) ptrace.Traces {
				return otlptest.RoutedSpans(tb, shortRouteBytes)
			},
		},
		{
			Name: "the longest unique routes",
			Args: func(tb testing.TB) ptrace.Traces {
				return otlptest.RoutedSpans(tb, operation.MaxNameBytes)
			},
		},
	}
	budget := int64(otlp.ExportHeapRatio * otlp.MaxRecvMsgBytes)
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			perRequest := heldHeapPerRequest(t, otlptest.Encode(t, tt.Args(t)))
			if perRequest > budget {
				t.Errorf("each held request takes %d bytes, budget %d (ExportHeapRatio × MaxRecvMsgBytes)", perRequest, budget)
			}
			t.Logf("held heap per request before the last span is validated: %.1f MiB", float64(perRequest)/(1<<20))
		})
	}
}
