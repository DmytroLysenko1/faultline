package main

import (
	"bytes"
	"errors"
	"log/slog"
	"os"
	"strings"
	"testing"

	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"

	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp/admission"
)

const ingestRateName = "INGEST_MAX_REQUESTS_PER_SEC"

func testIngest() ingestConfig {
	return ingestConfig{
		RequestsPerSecond: 2000,
		MaxConnections:    64,
	}
}

func TestTheIngestLimitsComeFromTheEnvironment(t *testing.T) {
	tests := []struct {
		name    string
		args    map[string]string
		want    ingestConfig
		wantErr error
	}{
		{
			name: "the default",
			args: map[string]string{},
			want: ingestConfig{
				RequestsPerSecond: 2000,
				MaxConnections:    64,
			},
		},
		{
			name: "the lowest",
			args: map[string]string{
				ingestRateName: "1",
			},
			want: ingestConfig{
				RequestsPerSecond: 1,
				MaxConnections:    64,
			},
		},
		{
			name: "the highest",
			args: map[string]string{
				ingestRateName: "100000",
			},
			want: ingestConfig{
				RequestsPerSecond: 100_000,
				MaxConnections:    64,
			},
		},
		{
			name: "zero",
			args: map[string]string{
				ingestRateName: "0",
			},
			want: ingestConfig{
				RequestsPerSecond: 2000,
			},
			wantErr: errOutOfRange,
		},
		{
			name: "above the highest",
			args: map[string]string{
				ingestRateName: "100001",
			},
			want: ingestConfig{
				RequestsPerSecond: 2000,
			},
			wantErr: errOutOfRange,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := loadIngest(environment(tt.args))
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if err == nil {
				assertDiff(t, "ingest", tt.want, got)
			}
		})
	}
}

func TestEveryIngestRoleReadsTheRequestRate(t *testing.T) {
	values := processorEnvironment(t, map[string]string{
		ingestRateName: "300",
	})
	ingester, ingesterErr := loadIngester(processOf(values))
	all, allErr := loadAll(processOf(values))
	if err := errors.Join(ingesterErr, allErr); err != nil {
		t.Fatalf("load: %v", err)
	}
	want := []int{300, 300}
	assertDiff(t, "requests per second", want, []int{ingester.Ingest.RequestsPerSecond, all.Ingest.RequestsPerSecond})
}

func TestARequestRateOutOfRangeRefusesToStartWithCodeTwo(t *testing.T) {
	for _, command := range []string{commandIngester, commandAll} {
		t.Run(command, func(t *testing.T) {
			var stderr bytes.Buffer
			code := realMain(&process{
				Args: []string{command},
				Lookup: environment(processorEnvironment(t, map[string]string{
					ingestRateName: "100001",
					envMemoryLimit: "1MiB",
				})),
				ReadCgroup:     func() ([]byte, error) { return nil, os.ErrNotExist },
				SetMemoryLimit: (&recordedLimit{}).setLimit,
				Stderr:         &stderr,
				Hostname:       podHostname,
			})
			assertDiff(t, "code", exitConfig, code)
			if !strings.Contains(stderr.String(), ingestRateName) {
				t.Errorf("stderr = %q, want it to name %s", stderr.String(), ingestRateName)
			}
		})
	}
}

func TestTheOTLPLayerRefusesLimitsThatCannotAdmit(t *testing.T) {
	logs := &logBuffer{}
	current := otlpLayer(&Config{
		OTLPAddr: localAddress(t),
	}, ingestConfig{}, acceptingJournal(t), admittingTaps(), testLogger(logs))
	view := newLayerSupervisor(t, logs, &current)
	err := current.unit.start(t.Context(), view)
	failure, found := errors.AsType[*stageError](err)
	if !found {
		t.Fatalf("start = %v, want a stage error", err)
	}
	assertDiff(t, "stage", "otlp/admission", failure.stage)
}

func TestTheOTLPLayerReportsItsAdmission(t *testing.T) {
	logs := &logBuffer{}
	address := localAddress(t)
	current := otlpLayer(&Config{
		OTLPAddr: address,
	}, testIngest(), acceptingJournal(t), admittingTaps(), testLogger(logs))
	view := newLayerSupervisor(t, logs, &current)
	if err := current.unit.start(t.Context(), view); err != nil {
		t.Fatalf("start: %v", err)
	}
	if _, err := ptraceotlp.NewGRPCClient(dialOTLP(t, address)).Export(t.Context(), routedRequest()); err != nil {
		t.Fatalf("Export: %v", err)
	}
	groups := current.statsGroups(t.Context())
	if err := stopLayer(t, &current, view); err != nil {
		t.Errorf("stop = %v, want nil", err)
	}
	want := map[string]any{
		"in_use":        int64(0),
		"active_tokens": int64(1),
		"admitted":      uint64(1),
		"rejected": map[string]any{
			"share": uint64(0),
			"slots": uint64(0),
			"rate":  uint64(0),
		},
		"too_slow": uint64(0),
	}
	assertDiff(t, "admission group", want, groupNamed(t, groups, "admission"))
}

func groupNamed(t *testing.T, groups []slog.Attr, name string) map[string]any {
	t.Helper()
	for _, group := range groups {
		if group.Key == name {
			return attrValues(group.Value.Group())
		}
	}
	t.Fatalf("no %s group in %v", name, groups)
	return nil
}

func attrValues(attrs []slog.Attr) map[string]any {
	values := map[string]any{}
	for _, attr := range attrs {
		if attr.Value.Kind() == slog.KindGroup {
			values[attr.Key] = attrValues(attr.Value.Group())
			continue
		}
		values[attr.Key] = attr.Value.Any()
	}
	return values
}

func TestTheAdmissionGroupNamesEveryCounter(t *testing.T) {
	stats := admission.Stats{
		InUse:        1,
		ActiveTokens: 2,
		Admitted:     3,
		Rejected: [admission.ReasonCount]uint64{
			admission.ReasonShare: 4,
			admission.ReasonSlots: 5,
			admission.ReasonRate:  6,
		},
		TooSlow: 7,
	}
	want := map[string]any{
		"in_use":        int64(1),
		"active_tokens": int64(2),
		"admitted":      uint64(3),
		"rejected": map[string]any{
			"share": uint64(4),
			"slots": uint64(5),
			"rate":  uint64(6),
		},
		"too_slow": uint64(7),
	}
	assertDiff(t, "admission group", want, groupNamed(t, []slog.Attr{admissionGroup(stats)}, "admission"))
}
