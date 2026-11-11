package selfalert_test

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/application/selfalert"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/resource"
)

const (
	lagThreshold = time.Minute
	behind       = 2 * time.Minute

	specRaiseRatio      = 0.01
	specLagRaiseReports = 3
)

var start = time.Date(2026, time.November, 11, 12, 0, 0, 0, time.UTC)

type outcome struct {
	Event      string
	Raised     bool
	LowTraffic bool
}

func thresholds() selfalert.Thresholds {
	return selfalert.Thresholds{
		WatermarkLag: lagThreshold,
	}
}

func replay(samples []selfalert.Sample) [][]outcome {
	evaluator := selfalert.NewEvaluator(thresholds())
	steps := make([][]outcome, 0, len(samples))
	for index := range samples {
		var step []outcome
		for _, transition := range evaluator.Evaluate(&samples[index]) {
			step = append(step, outcome{
				Event:      transition.Event,
				Raised:     transition.Raised,
				LowTraffic: transition.LowTraffic,
			})
		}
		steps = append(steps, step)
	}
	return steps
}

func skew(offered, reassigned uint64) selfalert.Sample {
	return selfalert.Sample{
		Offered:    offered,
		Reassigned: reassigned,
	}
}

func lagged(lag time.Duration) selfalert.Sample {
	return selfalert.Sample{
		Now:            start.Add(lag),
		Watermark:      start,
		WatermarkKnown: true,
	}
}

func owners(count int) selfalert.Sample {
	return selfalert.Sample{
		OwnersOverLimit: count,
	}
}

func raised(event string) []outcome {
	return []outcome{
		{
			Event:  event,
			Raised: true,
		},
	}
}

func cleared(event string, lowTraffic bool) []outcome {
	return []outcome{
		{
			Event:      event,
			LowTraffic: lowTraffic,
		},
	}
}

func TestRatioAlertsNeedAThousandEventsAndClearAfterTwoQuietReports(t *testing.T) {
	tests := []struct {
		Name string
		Args []selfalert.Sample
		Want [][]outcome
	}{
		{
			Name: "999 events at 50 percent do not raise",
			Args: []selfalert.Sample{
				skew(999, 500),
			},
			Want: [][]outcome{nil},
		},
		{
			Name: "1000 events above 1 percent raise",
			Args: []selfalert.Sample{
				skew(1_000, 11),
			},
			Want: [][]outcome{raised(selfalert.ClockSkewRatioHigh)},
		},
		{
			Name: "exactly 1 percent does not raise",
			Args: []selfalert.Sample{
				skew(1_000, 10),
			},
			Want: [][]outcome{nil},
		},
		{
			Name: "a clean report alone does not clear, the second one does",
			Args: []selfalert.Sample{
				skew(1_000, 20),
				skew(2_000, 20),
				skew(3_000, 20),
			},
			Want: [][]outcome{
				raised(selfalert.ClockSkewRatioHigh),
				nil,
				cleared(selfalert.ClockSkewRatioHigh, false),
			},
		},
		{
			Name: "between 0.5 and 1 percent holds the alert",
			Args: []selfalert.Sample{
				skew(1_000, 20),
				skew(2_000, 27),
				skew(3_000, 34),
				skew(4_000, 41),
			},
			Want: [][]outcome{
				raised(selfalert.ClockSkewRatioHigh),
				nil,
				nil,
				nil,
			},
		},
		{
			Name: "no traffic for two reports clears with the low traffic reason",
			Args: []selfalert.Sample{
				skew(1_000, 20),
				skew(1_000, 20),
				skew(1_000, 20),
			},
			Want: [][]outcome{
				raised(selfalert.ClockSkewRatioHigh),
				nil,
				cleared(selfalert.ClockSkewRatioHigh, true),
			},
		},
		{
			Name: "a quiet pause shorter than two reports keeps the alert",
			Args: []selfalert.Sample{
				skew(1_000, 20),
				skew(1_000, 20),
				skew(2_000, 40),
			},
			Want: [][]outcome{
				raised(selfalert.ClockSkewRatioHigh),
				nil,
				nil,
			},
		},
		{
			Name: "a stalled pipeline that repeats its snapshot clears with the low traffic reason",
			Args: []selfalert.Sample{
				skew(1_000, 20),
				skew(1_000, 20),
				skew(1_000, 20),
			},
			Want: [][]outcome{
				raised(selfalert.ClockSkewRatioHigh),
				nil,
				cleared(selfalert.ClockSkewRatioHigh, true),
			},
		},
		{
			Name: "a report between the thresholds breaks the run of clean reports",
			Args: []selfalert.Sample{
				skew(1_000, 20),
				skew(2_000, 20),
				skew(3_000, 27),
				skew(4_000, 27),
			},
			Want: [][]outcome{
				raised(selfalert.ClockSkewRatioHigh),
				nil,
				nil,
				nil,
			},
		},
		{
			Name: "counters that start over count from zero instead of wrapping",
			Args: []selfalert.Sample{
				skew(50_000, 100),
				skew(1_000, 5),
			},
			Want: [][]outcome{nil, nil},
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			if diff := cmp.Diff(tt.Want, replay(tt.Args)); diff != "" {
				t.Errorf("transitions per report (-want +got):\n%s", diff)
			}
		})
	}
}

func TestLateDropsRaiseTheirOwnAlert(t *testing.T) {
	got := replay([]selfalert.Sample{
		{
			Offered:     2_000,
			LateDropped: 30,
		},
	})
	if diff := cmp.Diff([][]outcome{raised(selfalert.LateDropRatioHigh)}, got); diff != "" {
		t.Errorf("transitions per report (-want +got):\n%s", diff)
	}
}

func TestWatermarkLagRaisesAfterThreeReportsAndClearsAfterTwo(t *testing.T) {
	got := replay([]selfalert.Sample{
		lagged(behind),
		lagged(behind),
		lagged(behind),
		lagged(time.Second),
		{},
	})
	want := [][]outcome{
		nil,
		nil,
		raised(selfalert.WatermarkLagHigh),
		nil,
		cleared(selfalert.WatermarkLagHigh, false),
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("transitions per report (-want +got):\n%s", diff)
	}
}

func TestAGrowingSetOfOwnersOverTheLimitWarnsAgain(t *testing.T) {
	got := replay([]selfalert.Sample{
		owners(1),
		owners(1),
		owners(3),
		owners(2),
		owners(0),
		owners(0),
	})
	want := [][]outcome{
		raised(selfalert.CardinalityLimitReached),
		nil,
		raised(selfalert.CardinalityLimitReached),
		nil,
		nil,
		cleared(selfalert.CardinalityLimitReached, false),
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("transitions per report (-want +got):\n%s", diff)
	}
}

// TestAStalledSnapshotRaisesTheLagAlert: a pipeline that stopped publishing repeats its last
// snapshot, and only the clock moves; that is the stall the lag alert exists for.
func TestAStalledSnapshotRaisesTheLagAlert(t *testing.T) {
	stalled := make([]selfalert.Sample, 0, specLagRaiseReports)
	for range specLagRaiseReports {
		stalled = append(stalled, lagged(behind))
	}
	want := [][]outcome{nil, nil, raised(selfalert.WatermarkLagHigh)}
	if diff := cmp.Diff(want, replay(stalled)); diff != "" {
		t.Errorf("transitions per report (-want +got):\n%s", diff)
	}
}

func TestAFlappingGaugeWarnsOnce(t *testing.T) {
	samples := make([]selfalert.Sample, 0, 6)
	for report := range 6 {
		samples = append(samples, selfalert.Sample{
			SeriesLimitReached: report%2 == 0,
		})
	}
	want := [][]outcome{raised(selfalert.SeriesLimitReached), nil, nil, nil, nil, nil}
	if diff := cmp.Diff(want, replay(samples)); diff != "" {
		t.Errorf("transitions per report (-want +got):\n%s", diff)
	}
}

func TestDroppedSamplesRaiseOnGrowthOnly(t *testing.T) {
	got := replay([]selfalert.Sample{
		{
			DroppedSamples: 3,
		},
		{
			DroppedSamples: 3,
		},
		{
			DroppedSamples: 3,
		},
	})
	want := [][]outcome{
		raised(selfalert.SketchBudgetExhausted),
		nil,
		cleared(selfalert.SketchBudgetExhausted, false),
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("transitions per report (-want +got):\n%s", diff)
	}
}

func TestTransitionsCarryTheDecidingValuesAndOwners(t *testing.T) {
	listed := []resource.ServiceRef{
		{
			Namespace: "shop",
			Name:      "cart",
		},
	}
	evaluator := selfalert.NewEvaluator(thresholds())
	got := evaluator.Evaluate(&selfalert.Sample{
		Offered:         4_000,
		Reassigned:      80,
		OwnersOverLimit: 1,
		Owners:          listed,
	})
	want := []selfalert.Transition{
		{
			Event:  selfalert.ClockSkewRatioHigh,
			Raised: true,
			Reading: selfalert.Reading{
				Value:     0.02,
				Threshold: specRaiseRatio,
				Volume:    4_000,
			},
		},
		{
			Event:  selfalert.CardinalityLimitReached,
			Raised: true,
			Reading: selfalert.Reading{
				Value: 1,
			},
			Owners: listed,
		},
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("transitions (-want +got):\n%s", diff)
	}
}

func TestAlertsReportTheCurrentStateOfEveryRule(t *testing.T) {
	evaluator := selfalert.NewEvaluator(thresholds())
	evaluator.Evaluate(&selfalert.Sample{
		SeriesLimitReached: true,
	})
	want := []selfalert.Alert{
		{Event: selfalert.ClockSkewRatioHigh},
		{Event: selfalert.LateDropRatioHigh},
		{Event: selfalert.WatermarkLagHigh},
		{Event: selfalert.CardinalityLimitReached},
		{Event: selfalert.SeriesLimitReached, Raised: true},
		{Event: selfalert.SketchBudgetExhausted},
	}
	if diff := cmp.Diff(want, evaluator.Alerts()); diff != "" {
		t.Errorf("alerts (-want +got):\n%s", diff)
	}
}
