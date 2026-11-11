package graphservice_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/interfaces/connectapi/graphservice"
)

const (
	shrinkNodes = 40
	shrinkEdges = 120
)

// freeBytes is the room left in the budget; it probes with TryAcquire and gives the room back.
func freeBytes(admission *graphservice.Admission) int64 {
	low, high := int64(0), admission.Capacity
	for low < high {
		probe := (low + high + 1) / 2
		if admission.Budget.TryAcquire(probe) {
			admission.Budget.Release(probe)
			low = probe
			continue
		}
		high = probe - 1
	}
	return low
}

type budgetSteps struct {
	Reserved, Flushed, Written, WrittenTwice, Done int64
}

// TestTheFirstWriteKeepsOnlyWhatTheResponseStillHolds: once marshalled, the snapshot and the
// marshal peak are gone; the tree and the encoded bytes stay until the write ends, and a
// compressed response also keeps its compressed copy.
func TestTheFirstWriteKeepsOnlyWhatTheResponseStillHolds(t *testing.T) {
	weight := graphservice.ResponseWeight(shrinkNodes, shrinkEdges)
	written := graphservice.WrittenWeight(shrinkNodes, shrinkEdges)
	encoded := graphservice.ResponseBytes(shrinkNodes, shrinkEdges)
	tests := []struct {
		Name string
		Args http.Header
		Want budgetSteps
	}{
		{
			Name: "identity",
			Args: http.Header{},
			Want: budgetSteps{Flushed: 0, Written: weight - written, WrittenTwice: weight - written, Done: weight},
		},
		{
			Name: "explicit identity",
			Args: http.Header{"Content-Encoding": {"identity"}},
			Want: budgetSteps{Written: weight - written, WrittenTwice: weight - written, Done: weight},
		},
		{
			Name: "connect gzip",
			Args: http.Header{"Content-Encoding": {"gzip"}},
			Want: budgetSteps{Written: weight - written - encoded, WrittenTwice: weight - written - encoded, Done: weight},
		},
		{
			Name: "grpc gzip",
			Args: http.Header{"Grpc-Encoding": {"gzip"}},
			Want: budgetSteps{Written: weight - written - encoded, WrittenTwice: weight - written - encoded, Done: weight},
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			var got budgetSteps
			var admission *graphservice.Admission
			next := http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
				if err := graphservice.Reserve(request.Context(), shrinkNodes, shrinkEdges); err != nil {
					t.Fatalf("Reserve: %v", err)
				}
				got.Reserved = freeBytes(admission)
				for name, values := range tt.Args {
					response.Header()[name] = values
				}
				flusher, isFlusher := response.(http.Flusher)
				if !isFlusher {
					t.Fatal("the admitted writer hides http.Flusher, which gRPC needs")
				}
				flusher.Flush()
				got.Flushed = freeBytes(admission)
				if _, err := response.Write([]byte("body")); err != nil {
					t.Fatalf("Write: %v", err)
				}
				got.Written = freeBytes(admission)
				if _, err := response.Write([]byte("more")); err != nil {
					t.Fatalf("Write: %v", err)
				}
				got.WrittenTwice = freeBytes(admission)
			})
			var err error
			if admission, err = graphservice.NewAdmission(weight, next); err != nil {
				t.Fatalf("NewAdmission: %v", err)
			}
			admission.ServeHTTP(httptest.NewRecorder(), httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/", nil))
			got.Done = freeBytes(admission)
			if diff := cmp.Diff(tt.Want, got); diff != "" {
				t.Errorf("free budget at each step (-want +got):\n%s", diff)
			}
		})
	}
}

func TestTheAdmittedWriterUnwrapsAndFlushesToTheServerWriter(t *testing.T) {
	recorder := httptest.NewRecorder()
	var unwrapped bool
	next := http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		wrapper, ok := response.(interface{ Unwrap() http.ResponseWriter })
		unwrapped = ok && wrapper.Unwrap() == recorder
		if flusher, isFlusher := response.(http.Flusher); isFlusher {
			flusher.Flush()
		}
	})
	admission, err := graphservice.NewAdmission(1, next)
	if err != nil {
		t.Fatalf("NewAdmission: %v", err)
	}
	admission.ServeHTTP(recorder, httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/", nil))
	if !unwrapped {
		t.Error("the admitted writer does not unwrap to the server writer: http.ResponseController loses deadlines")
	}
	if !recorder.Flushed {
		t.Error("Flush on the admitted writer did not reach the server writer: gRPC trailers would wait for the end")
	}
}
