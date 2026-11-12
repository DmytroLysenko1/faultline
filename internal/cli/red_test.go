package cli_test

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/DmytroLysenko1/faultline/internal/cli"
	faultlinev1 "github.com/DmytroLysenko1/faultline/internal/gen/proto/faultline/v1"
)

type panes struct {
	Final *faultlinev1.WindowStats
	Live  *faultlinev1.WindowStats
}

type summaryLabels struct {
	Edge string
	Node string
}

func pane(requests, failed uint64, observed time.Duration) *faultlinev1.WindowStats {
	return &faultlinev1.WindowStats{
		Requests:       requests,
		FailedRequests: failed,
		Observed:       durationpb.New(observed),
	}
}

func timed(summary *faultlinev1.WindowStats, p95 time.Duration) *faultlinev1.WindowStats {
	summary.LatencySamples = 3
	summary.P95 = durationpb.New(p95)
	return summary
}

func summarized(t *testing.T, args panes) summaryLabels {
	t.Helper()
	cart := service{Namespace: demoNamespace, Name: "cart"}
	frontend := service{Namespace: demoNamespace, Name: "frontend"}
	built := graphOf([]namedNode{{service: frontend}, {service: cart}}, []call{{source: 0, target: 1, calls: 7, failed: 1}})
	built.Nodes[1].Final, built.Nodes[1].Live = args.Final, args.Live
	built.Edges[0].Final, built.Edges[0].Live = args.Final, args.Live
	parsed := parseDOT(t, rendered(t, cli.DOT, built))
	return summaryLabels{
		Edge: parsed.Edges[0][2],
		Node: parsed.Labels[nodeID(cart)],
	}
}

func shown(line string) summaryLabels {
	if line == "" {
		return summaryLabels{
			Edge: "calls 7, failed 1",
			Node: "demo/cart",
		}
	}
	return summaryLabels{
		Edge: "calls 7, failed 1\n" + line,
		Node: "demo/cart\nserved " + line,
	}
}

func TestDOTLabelsShowRateErrorsAndP95OfTheSettledWindowOrElseTheLiveOne(t *testing.T) {
	tests := []struct {
		Name string
		Args panes
		Want summaryLabels
	}{
		{
			Name: "a final window wins over the live one",
			Args: panes{
				Final: timed(pane(25, 1, 10*time.Second), 45678*time.Microsecond),
				Live:  pane(99, 9, time.Second),
			},
			Want: shown("2.5/s err 4.0% p95 45.7ms"),
		},
		{
			Name: "only a live window is marked as not settled",
			Args: panes{
				Live: timed(pane(10, 1, 4*time.Second), 45678*time.Microsecond),
			},
			Want: shown("~2.5/s err 10.0% p95 45.7ms"),
		},
		{
			Name: "a final window without observed time falls back to the live one",
			Args: panes{
				Final: pane(25, 1, 0),
				Live:  pane(10, 0, 4*time.Second),
			},
			Want: shown("~2.5/s err 0.0%"),
		},
		{
			Name: "a live window without observed time shows nothing",
			Args: panes{
				Live: pane(10, 0, 0),
			},
			Want: shown(""),
		},
		{
			Name: "no windows show only the counters",
			Want: shown(""),
		},
		{
			Name: "a window without latency samples has no p95",
			Args: panes{
				Final: pane(1, 1, 3*time.Second),
			},
			Want: shown("0.3/s err 100.0%"),
		},
		{
			Name: "a window without requests has no error share to divide",
			Args: panes{
				Final: pane(0, 0, 10*time.Second),
			},
			Want: shown("0.0/s err 0.0%"),
		},
		{
			Name: "one failure in three",
			Args: panes{
				Final: pane(3, 1, time.Second),
			},
			Want: shown("3.0/s err 33.3%"),
		},
		{
			Name: "p95 above a second keeps three digits",
			Args: panes{
				Final: timed(pane(1, 0, time.Second), 1234567*time.Microsecond),
			},
			Want: shown("1.0/s err 0.0% p95 1.23s"),
		},
		{
			Name: "p95 rounding carries into the next unit",
			Args: panes{
				Final: timed(pane(1, 0, time.Second), 999600*time.Nanosecond),
			},
			Want: shown("1.0/s err 0.0% p95 1ms"),
		},
		{
			Name: "p95 below a millisecond",
			Args: panes{
				Final: timed(pane(1, 0, time.Second), 512340*time.Nanosecond),
			},
			Want: shown("1.0/s err 0.0% p95 512µs"),
		},
		{
			Name: "p95 of a few nanoseconds stays exact",
			Args: panes{
				Final: timed(pane(1, 0, time.Second), 7*time.Nanosecond),
			},
			Want: shown("1.0/s err 0.0% p95 7ns"),
		},
		{
			Name: "p95 of exactly a thousand steps still drops to three digits",
			Args: panes{
				Final: timed(pane(1, 0, time.Second), 10005*time.Nanosecond),
			},
			Want: shown("1.0/s err 0.0% p95 10µs"),
		},
		{
			Name: "a p95 sent without latency samples is not shown",
			Args: panes{
				Final: &faultlinev1.WindowStats{
					Requests: 2,
					Observed: durationpb.New(time.Second),
					P95:      durationpb.New(time.Millisecond),
				},
			},
			Want: shown("2.0/s err 0.0%"),
		},
		{
			Name: "one failure in many is not rounded away",
			Args: panes{
				Final: pane(2001, 1, time.Second),
			},
			Want: shown("2001.0/s err <0.1%"),
		},
		{
			Name: "a share that one decimal would show as a tenth still reads as below it",
			Args: panes{
				Final: pane(1500, 1, time.Second),
			},
			Want: shown("1500.0/s err <0.1%"),
		},
		{
			Name: "a tenth of a percent is shown as is",
			Args: panes{
				Final: pane(1000, 1, time.Second),
			},
			Want: shown("1000.0/s err 0.1%"),
		},
		{
			Name: "one success in many is not rounded away",
			Args: panes{
				Final: pane(10000, 9996, time.Second),
			},
			Want: shown("10000.0/s err >99.9%"),
		},
		{
			Name: "every request failed",
			Args: panes{
				Final: pane(4, 4, time.Second),
			},
			Want: shown("4.0/s err 100.0%"),
		},
		{
			Name: "more failures than requests from a broken server stop at every request",
			Args: panes{
				Final: pane(1, 5, time.Second),
			},
			Want: shown("1.0/s err 100.0%"),
		},
		{
			Name: "a negative p95 from a broken server is not shown",
			Args: panes{
				Final: timed(pane(1, 0, time.Second), -1234567*time.Nanosecond),
			},
			Want: shown("1.0/s err 0.0%"),
		},
		{
			Name: "latency samples without a p95 do not show a zero p95",
			Args: panes{
				Final: &faultlinev1.WindowStats{
					Requests:       1,
					LatencySamples: 1,
					Observed:       durationpb.New(time.Second),
				},
			},
			Want: shown("1.0/s err 0.0%"),
		},
		{
			Name: "a malformed observed time makes the window unusable",
			Args: panes{
				Final: &faultlinev1.WindowStats{
					Requests: 1,
					Observed: &durationpb.Duration{
						Seconds: 1,
						Nanos:   -999999999,
					},
				},
				Live: pane(10, 0, 4*time.Second),
			},
			Want: shown("~2.5/s err 0.0%"),
		},
		{
			Name: "a negative observed time makes the window unusable",
			Args: panes{
				Final: pane(1, 0, -time.Second),
			},
			Want: shown(""),
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			if diff := cmp.Diff(tt.Want, summarized(t, tt.Args)); diff != "" {
				t.Errorf("labels (-want +got):\n%s", diff)
			}
		})
	}
}

func TestNodeAndEdgeEachShowTheirOwnWindows(t *testing.T) {
	cart := service{Namespace: demoNamespace, Name: "cart"}
	frontend := service{Namespace: demoNamespace, Name: "frontend"}
	built := graphOf([]namedNode{{service: frontend}, {service: cart}}, []call{{source: 0, target: 1, calls: 7, failed: 1}})
	built.Nodes[1].Final = pane(30, 3, 10*time.Second)
	built.Edges[0].Live = pane(8, 0, 4*time.Second)
	parsed := parseDOT(t, rendered(t, cli.DOT, built))
	got := summaryLabels{
		Edge: parsed.Edges[0][2],
		Node: parsed.Labels[nodeID(cart)],
	}
	want := summaryLabels{
		Edge: "calls 7, failed 1\n~2.0/s err 0.0%",
		Node: "demo/cart\nserved 3.0/s err 10.0%",
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("labels (-want +got):\n%s", diff)
	}
}
