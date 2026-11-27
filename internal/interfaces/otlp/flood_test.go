package otlp_test

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp/otlptest"
)

const (
	floodCalls            = 20000
	floodDeadline         = 1000 * time.Hour
	floodHeapBytesPerCall = 16
)

type floodCase struct {
	name    string
	harness func(t *testing.T, acceptor *countingAcceptor) harness
	answer  statusWant
}

func floodCases() []floodCase {
	cases := []floodCase{
		{
			name: "unauthenticated",
			harness: func(t *testing.T, acceptor *countingAcceptor) harness {
				t.Helper()
				return startGuarded(t, acceptor.accept, otlptest.Admitting())
			},
			answer: statusWant{
				Code:    codes.Unauthenticated,
				Message: rejectedMessage,
			},
		},
	}
	for _, refusal := range refusalCases() {
		cases = append(cases, floodCase{
			name: refusal.name,
			harness: func(t *testing.T, acceptor *countingAcceptor) harness {
				t.Helper()
				gate := frozenGate(t, refusal.rate)
				refusal.setup(t, *gate)
				return startWith(t, setup{
					acceptor:      acceptor.accept,
					authenticator: otlptest.Admitting(),
					gate:          gate,
					credentials:   tokenCredentials(),
				})
			},
			answer: refusal.want,
		})
	}
	return cases
}

func TestRejectedCallsWithAFarDeadlineLeaveNothingOnTheHeap(t *testing.T) {
	for _, tt := range floodCases() {
		t.Run(tt.name, func(t *testing.T) {
			acceptor := &countingAcceptor{}
			h := tt.harness(t, acceptor)
			body := append(malformed(), 0xff)
			rejectWithFarDeadline(t, h, body)
			before := liveHeap()
			answers := map[statusWant]int{}
			for range floodCalls {
				answers[rejectWithFarDeadline(t, h, body)]++
			}
			grown := liveHeap() - before
			type outcome struct {
				Answers       map[statusWant]int
				DecodeEvents  int
				AcceptorCalls int64
			}
			want := outcome{
				Answers: map[statusWant]int{
					tt.answer: floodCalls,
				},
			}
			got := outcome{
				Answers:       answers,
				DecodeEvents:  len(h.Logs.Events(t, "otlp.export.decode_rejected")),
				AcceptorCalls: acceptor.calls.Load(),
			}
			if diff := cmp.Diff(want, got); diff != "" {
				t.Errorf("rejected flood (-want +got):\n%s", diff)
			}
			if grown >= floodCalls*floodHeapBytesPerCall {
				t.Errorf("%d rejected calls grew the live heap by %d B (%d B a call), want under %d B a call",
					floodCalls, grown, grown/floodCalls, floodHeapBytesPerCall)
			}
		})
	}
}

func rejectWithFarDeadline(t *testing.T, h harness, body []byte) statusWant {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), floodDeadline)
	defer cancel()
	st := status.Convert(sendRaw(ctx, h.Conn, body))
	return statusWant{
		Code:       st.Code(),
		Message:    st.Message(),
		RetryDelay: retryDelay(st),
	}
}

func liveHeap() int64 {
	runtime.GC()
	runtime.GC()
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	return int64(stats.HeapAlloc)
}
