package graphservice_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"

	"connectrpc.com/connect"
	"github.com/google/go-cmp/cmp"
	"go.uber.org/goleak"
	"google.golang.org/genproto/googleapis/rpc/errdetails"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/telemetrytest"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/graph"
	faultlinev1 "github.com/DmytroLysenko1/faultline/internal/gen/proto/faultline/v1"
	"github.com/DmytroLysenko1/faultline/internal/gen/proto/faultline/v1/faultlinev1connect"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/connectapi/graphservice"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/connectapi/limit"
)

const admissionNodes = 3

func admissionSnapshot() *topology.Snapshot {
	nodes := make([]topology.NodeView, 0, admissionNodes)
	for _, name := range []string{"cart", "checkout", "frontend"} {
		nodes = append(nodes, topology.NodeView{Node: graph.NewNode(telemetrytest.Service(name))})
	}
	return &topology.Snapshot{Nodes: nodes}
}

func admissionWeight() int64 {
	return graphservice.ResponseWeight(admissionNodes, 0)
}

// gatedQuery keeps the first admitted request open after its response is written, as a slow write
// would, until the test opens the gate; every later request passes at once.
type gatedQuery struct {
	entered chan struct{}
	gate    chan struct{}
	calls   atomic.Int32
	panics  atomic.Bool
}

func (q *gatedQuery) Execute(context.Context) (*topology.Snapshot, error) {
	if q.panics.Load() {
		panic("query broke")
	}
	return admissionSnapshot(), nil
}

func (q *gatedQuery) holdsThisCall() bool {
	return q.gate != nil && q.calls.Add(1) == 1
}

func admittedServer(t *testing.T, query *gatedQuery, budget int64) *httptest.Server {
	t.Helper()
	_, handler := faultlinev1connect.NewGraphServiceHandler(graphservice.NewHandler(query, silentLogger()),
		connect.WithRecover(func(context.Context, connect.Spec, http.Header, any) error {
			return connect.NewError(connect.CodeInternal, graphservice.ErrInternal)
		}))
	holding := http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		handler.ServeHTTP(response, request)
		if query.holdsThisCall() {
			query.entered <- struct{}{}
			<-query.gate
		}
	})
	admitted, err := graphservice.NewAdmission(budget, holding)
	if err != nil {
		t.Fatalf("NewAdmission: %v", err)
	}
	server := httptest.NewServer(admitted)
	t.Cleanup(server.Close)
	return server
}

func call(ctx context.Context, server *httptest.Server) error {
	client := faultlinev1connect.NewGraphServiceClient(server.Client(), server.URL)
	_, err := client.GetGraph(ctx, connect.NewRequest(&faultlinev1.GetGraphRequest{}))
	return err
}

func codeOf(err error) connect.Code {
	if err == nil {
		return 0
	}
	return connect.CodeOf(err)
}

func hasRetryInfo(err error) bool {
	failure, isConnect := errors.AsType[*connect.Error](err)
	if !isConnect {
		return false
	}
	for _, detail := range failure.Details() {
		value, err := detail.Value()
		if retry, isRetry := value.(*errdetails.RetryInfo); err == nil && isRetry {
			return retry.GetRetryDelay().AsDuration() == limit.RetryDelay
		}
	}
	return false
}

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

// TestAResponseLargerThanTheWholeBudgetIsNotRetryable: no wait frees room for it, so a retry hint
// would make clients retry forever; the startup validator keeps it from happening at the limits.
func TestAResponseLargerThanTheWholeBudgetIsNotRetryable(t *testing.T) {
	err := call(t.Context(), admittedServer(t, &gatedQuery{}, admissionWeight()-1))
	if codeOf(err) != connect.CodeFailedPrecondition || hasRetryInfo(err) {
		t.Fatalf("GetGraph above the whole budget = %v, want failed_precondition without a retry", err)
	}
}

func TestEveryResponseReleasesItsWeight(t *testing.T) {
	server := admittedServer(t, &gatedQuery{}, admissionWeight())
	for attempt := range 3 {
		if err := call(t.Context(), server); err != nil {
			t.Fatalf("GetGraph %d with the whole budget free: %v", attempt, err)
		}
	}
}

// TestAHeldReservationTurnsAwayTheNextResponse: one response at a time fits; the next one is
// refused while the first is still being written, and admitted once it is done.
func TestAHeldReservationTurnsAwayTheNextResponse(t *testing.T) {
	query := &gatedQuery{entered: make(chan struct{}), gate: make(chan struct{})}
	var opened sync.Once
	open := func() {
		opened.Do(func() {
			close(query.gate)
		})
	}
	t.Cleanup(open)
	server := admittedServer(t, query, admissionWeight())
	var first error
	var running sync.WaitGroup
	running.Go(func() {
		first = call(t.Context(), server)
	})
	<-query.entered
	second := call(t.Context(), server)
	open()
	running.Wait()
	if first != nil {
		t.Fatalf("first GetGraph: %v", first)
	}
	if codeOf(second) != connect.CodeResourceExhausted || !hasRetryInfo(second) {
		t.Errorf("GetGraph while the budget is held = %v, want resource_exhausted with a retry", second)
	}
	if err := call(t.Context(), server); err != nil {
		t.Errorf("GetGraph after the first response: %v", err)
	}
}

func TestAPanickingResponseStillReleasesItsWeight(t *testing.T) {
	query := &gatedQuery{}
	query.panics.Store(true)
	server := admittedServer(t, query, admissionWeight())
	if err := call(t.Context(), server); codeOf(err) != connect.CodeInternal {
		t.Fatalf("GetGraph with a panicking query = %v, want internal", err)
	}
	query.panics.Store(false)
	if err := call(t.Context(), server); err != nil {
		t.Errorf("GetGraph after a panic: %v, want the budget free again", err)
	}
}

func TestAHandlerWithoutAdmissionFailsClosed(t *testing.T) {
	_, handler := faultlinev1connect.NewGraphServiceHandler(graphservice.NewHandler(&gatedQuery{}, silentLogger()))
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	if err := call(t.Context(), server); codeOf(err) != connect.CodeInternal {
		t.Errorf("GetGraph without admission = %v, want internal", err)
	}
}

func TestNewAdmissionRejectsABudgetWithoutRoom(t *testing.T) {
	tests := []struct {
		Name    string
		Args    int64
		WantErr error
	}{
		{Name: "one byte", Args: 1},
		{Name: "zero", Args: 0, WantErr: graphservice.ErrInvalidBudget},
		{Name: "negative", Args: -1, WantErr: graphservice.ErrInvalidBudget},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			_, err := graphservice.NewAdmission(tt.Args, http.NotFoundHandler())
			if !errors.Is(err, tt.WantErr) {
				t.Fatalf("NewAdmission(%d) = %v, want %v", tt.Args, err, tt.WantErr)
			}
		})
	}
}

func TestResponseWeightGrowsWithTheGraph(t *testing.T) {
	type weights struct {
		Empty, OneNode, OneEdge int64
	}
	got := weights{
		Empty:   graphservice.ResponseWeight(0, 0),
		OneNode: graphservice.ResponseWeight(1, 0),
		OneEdge: graphservice.ResponseWeight(0, 1),
	}
	if got.Empty <= 0 || got.OneNode <= got.Empty || got.OneEdge <= got.Empty {
		t.Errorf("weights do not grow with the graph: %+v", got)
	}
	if diff := cmp.Diff(graphservice.ResponseBytes(0, 0), int64(1<<10)); diff != "" {
		t.Errorf("envelope (-want +got):\n%s", diff)
	}
}
