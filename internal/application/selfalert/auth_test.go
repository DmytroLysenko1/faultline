package selfalert_test

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/application/selfalert"
)

const (
	specReportInterval  = 30 * time.Second
	quickReportInterval = 10 * time.Second
	staleAlert          = "auth.token_directory_stale"
	regressedAlert      = "auth.token_sequence_regressed"
	blockedAlert        = "auth.token_revoke_blocked"
	driftAlert          = "auth.token_bucket_drift"
	epochAlert          = "auth.token_epoch_changed"
	massAlert           = "auth.token_mass_revoke"
)

func flagged(set func(sample *selfalert.Sample), pattern ...bool) []selfalert.Sample {
	samples := make([]selfalert.Sample, 0, len(pattern))
	for _, on := range pattern {
		sample := selfalert.Sample{}
		if on {
			set(&sample)
		}
		samples = append(samples, sample)
	}
	return samples
}

func TestAuthStateAlertsRaiseAtOnceAndClearAfterTwoQuietReports(t *testing.T) {
	tests := []struct {
		name string
		args func(sample *selfalert.Sample)
		want string
	}{
		{
			name: "a stale directory",
			args: func(sample *selfalert.Sample) {
				sample.TokensStale = true
			},
			want: staleAlert,
		},
		{
			name: "a bucket that went back in its epoch",
			args: func(sample *selfalert.Sample) {
				sample.TokensRegressed = true
			},
			want: regressedAlert,
		},
		{
			name: "a bucket that refuses revocation",
			args: func(sample *selfalert.Sample) {
				sample.RevokeBlocked = true
			},
			want: blockedAlert,
		},
		{
			name: "a bucket whose config drifted",
			args: func(sample *selfalert.Sample) {
				sample.BucketDrift = true
			},
			want: driftAlert,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := replay(flagged(tt.args, true, true, false, true, false, false, false))
			want := [][]outcome{
				raised(tt.want),
				nil,
				nil,
				nil,
				nil,
				cleared(tt.want, false),
				nil,
			}
			if diff := cmp.Diff(want, got); diff != "" {
				t.Errorf("transitions per report (-want +got):\n%s", diff)
			}
		})
	}
}

type paced struct {
	interval  time.Duration
	reports   int
	increases map[int]uint64
	skipped   map[int]bool
}

func TestAStateAlertClearsAfterAClockStepBack(t *testing.T) {
	samples := flagged(func(sample *selfalert.Sample) {
		sample.TokensStale = true
	}, true, false, false)
	samples[0].Now = start.Add(time.Hour)
	samples[1].Now = start
	samples[2].Now = start

	got := replay(samples)

	want := [][]outcome{
		raised(staleAlert),
		nil,
		cleared(staleAlert, false),
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("transitions per report (-want +got):\n%s", diff)
	}
}

func counted(set func(sample *selfalert.Sample, count uint64), pace paced) []selfalert.Sample {
	samples := make([]selfalert.Sample, 0, pace.reports)
	var total uint64
	for report := range pace.reports {
		total += pace.increases[report]
		if pace.skipped[report] {
			continue
		}
		sample := selfalert.Sample{
			Now: start.Add(time.Duration(report) * pace.interval),
		}
		set(&sample, total)
		samples = append(samples, sample)
	}
	return samples
}

func onlyAt(reports int, at map[int][]outcome) [][]outcome {
	steps := make([][]outcome, reports)
	for report, step := range at {
		steps[report] = step
	}
	return steps
}

func reportsIn(span, interval time.Duration) int {
	return int(span / interval)
}

func TestAuthCountAlertsHoldForAnHourAfterTheLastIncrease(t *testing.T) {
	type args struct {
		set  func(sample *selfalert.Sample, count uint64)
		pace paced
	}
	epochs := func(sample *selfalert.Sample, count uint64) {
		sample.EpochChanges = count
	}
	revokes := func(sample *selfalert.Sample, count uint64) {
		sample.MassRevokes = count
	}
	hourAt30s := reportsIn(time.Hour, specReportInterval)
	hourAt10s := reportsIn(time.Hour, quickReportInterval)
	tests := []struct {
		name string
		args args
		want [][]outcome
	}{
		{
			name: "an epoch change stays raised for an hour of quiet reports",
			args: args{
				set: epochs,
				pace: paced{
					interval: specReportInterval,
					reports:  hourAt30s + 2,
					increases: map[int]uint64{
						0: 1,
					},
				},
			},
			want: onlyAt(hourAt30s+2, map[int][]outcome{
				0:         raised(epochAlert),
				hourAt30s: cleared(epochAlert, false),
			}),
		},
		{
			name: "a shorter report interval keeps the hour",
			args: args{
				set: epochs,
				pace: paced{
					interval: quickReportInterval,
					reports:  hourAt10s + 2,
					increases: map[int]uint64{
						0: 1,
					},
				},
			},
			want: onlyAt(hourAt10s+2, map[int][]outcome{
				0:         raised(epochAlert),
				hourAt10s: cleared(epochAlert, false),
			}),
		},
		{
			name: "missed reports keep the hour",
			args: args{
				set: epochs,
				pace: paced{
					interval: specReportInterval,
					reports:  hourAt30s + 2,
					increases: map[int]uint64{
						0: 1,
					},
					skipped: skippedBetween(1, hourAt30s-2),
				},
			},
			want: [][]outcome{
				raised(epochAlert),
				nil,
				cleared(epochAlert, false),
				nil,
			},
		},
		{
			name: "the first quiet report after the hour clears",
			args: args{
				set: epochs,
				pace: paced{
					interval: specReportInterval,
					reports:  hourAt30s + 2,
					increases: map[int]uint64{
						0: 1,
					},
					skipped: skippedBetween(1, hourAt30s-1),
				},
			},
			want: [][]outcome{
				raised(epochAlert),
				cleared(epochAlert, false),
				nil,
			},
		},
		{
			name: "a second epoch change restarts the hour",
			args: args{
				set: epochs,
				pace: paced{
					interval: specReportInterval,
					reports:  61 + hourAt30s + 1,
					increases: map[int]uint64{
						0:  1,
						60: 1,
					},
				},
			},
			want: onlyAt(61+hourAt30s+1, map[int][]outcome{
				0:              raised(epochAlert),
				60 + hourAt30s: cleared(epochAlert, false),
			}),
		},
		{
			name: "a mass revoke stays raised for an hour of quiet reports",
			args: args{
				set: revokes,
				pace: paced{
					interval: specReportInterval,
					reports:  hourAt30s + 2,
					increases: map[int]uint64{
						0: 2,
					},
				},
			},
			want: onlyAt(hourAt30s+2, map[int][]outcome{
				0:         raised(massAlert),
				hourAt30s: cleared(massAlert, false),
			}),
		},
		{
			name: "a count that never grows raises nothing",
			args: args{
				set: revokes,
				pace: paced{
					interval: specReportInterval,
					reports:  3,
				},
			},
			want: onlyAt(3, nil),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := replay(counted(tt.args.set, tt.args.pace))
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("transitions per report (-want +got):\n%s", diff)
			}
		})
	}
}

func skippedBetween(first, last int) map[int]bool {
	skipped := map[int]bool{}
	for report := first; report <= last; report++ {
		skipped[report] = true
	}
	return skipped
}
