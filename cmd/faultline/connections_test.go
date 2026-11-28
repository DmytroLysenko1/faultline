package main

import (
	"errors"
	"log/slog"
	"net"
	"strings"
	"testing"

	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"

	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp/connlimit"
)

const connectionsName = "OTLP_GRPC_MAX_CONNECTIONS"

func TestTheConnectionLimitComesFromTheEnvironment(t *testing.T) {
	tests := []struct {
		name    string
		args    map[string]string
		want    int
		wantErr error
	}{
		{
			name: "the default",
			args: map[string]string{},
			want: 64,
		},
		{
			name: "the lowest",
			args: map[string]string{
				connectionsName: "1",
			},
			want: 1,
		},
		{
			name: "the highest",
			args: map[string]string{
				connectionsName: "1024",
			},
			want: 1024,
		},
		{
			name: "zero",
			args: map[string]string{
				connectionsName: "0",
			},
			wantErr: errOutOfRange,
		},
		{
			name: "above the highest",
			args: map[string]string{
				connectionsName: "1025",
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
				assertDiff(t, "max connections", tt.want, got.MaxConnections)
			}
		})
	}
}

func TestEachIngestRoleHasItsConnectionLimit(t *testing.T) {
	tests := []struct {
		name string
		args map[string]string
		want []int
	}{
		{
			name: "the defaults: 64 for the ingester, 4 for all",
			args: map[string]string{},
			want: []int{64, 4},
		},
		{
			name: "the environment wins over the default of all",
			args: map[string]string{
				connectionsName: "16",
			},
			want: []int{16, 16},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			values := processorEnvironment(t, tt.args)
			ingester, ingesterErr := loadIngester(processOf(values))
			all, allErr := loadAll(processOf(values))
			if err := errors.Join(ingesterErr, allErr); err != nil {
				t.Fatalf("load: %v", err)
			}
			got := []int{ingester.Ingest.MaxConnections, all.Ingest.MaxConnections}
			assertDiff(t, "max connections of the ingester and all", tt.want, got)
		})
	}
}

func TestAnOverBudgetConnectionLimitRefusesToStart(t *testing.T) {
	err := checkParts(allContributions(t, map[string]string{
		connectionsName: "64",
	}), defaultMemoryLimit)
	if !errors.Is(err, errOverBudget) {
		t.Fatalf("all with 64 connections = %v, want %v", err, errOverBudget)
	}
	if !strings.Contains(err.Error(), "otlp connections 145.0 MiB") {
		t.Errorf("the refusal %q does not name the otlp connections", err)
	}
}

func TestTheOTLPConnectionsContributionFollowsTheLimit(t *testing.T) {
	tests := []struct {
		name string
		args int
		want int64
	}{
		{
			name: "one connection",
			args: 1,
			want: 14_761_984,
		},
		{
			name: "the limit of all",
			args: 4,
			want: 21_299_200,
		},
		{
			name: "the limit of the ingester",
			args: 64,
			want: 152_043_520,
		},
		{
			name: "the highest limit",
			args: 1024,
			want: 2_243_952_640,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			limits := testIngest()
			limits.MaxConnections = tt.args
			got := map[string]int64{}
			for _, part := range otlpContributions(limits) {
				got[part.name] = part.bytes
			}
			assertDiff(t, "otlp connections", tt.want, got["otlp connections"])
		})
	}
}

func TestTheOTLPLayerRefusesAConnectionLimitThatHoldsNoConnection(t *testing.T) {
	logs := &logBuffer{}
	address := localAddress(t)
	limits := testIngest()
	limits.MaxConnections = 0
	current := otlpLayer(&Config{
		OTLPAddr: address,
	}, limits, acceptingJournal(t), admittingTaps(), testLogger(logs))
	view := newLayerSupervisor(t, logs, &current)
	err := current.unit.start(t.Context(), view)
	failure, found := errors.AsType[*stageError](err)
	if !found {
		t.Fatalf("start = %v, want a stage error", err)
	}
	assertDiff(t, "stage", "otlp/listen", failure.stage)
	if !errors.Is(err, connlimit.ErrInvalidLimits) {
		t.Errorf("start = %v, want %v", err, connlimit.ErrInvalidLimits)
	}
	again, err := new(net.ListenConfig).Listen(t.Context(), network, address)
	if err != nil {
		t.Fatalf("the refused layer kept %s: %v", address, err)
	}
	if err := again.Close(); err != nil {
		t.Errorf("close: %v", err)
	}
}

func TestTheOTLPLayerReportsItsConnections(t *testing.T) {
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
		"open":                    int64(1),
		"accepted":                uint64(1),
		"evicted_unauthenticated": uint64(0),
		"evicted_lapsed":          uint64(0),
		"evicted_over_share":      uint64(0),
		"refused":                 uint64(0),
	}
	assertDiff(t, "connections group", want, groupNamed(t, groups, "connections"))
}

func TestTheConnectionsGroupNamesEveryCounter(t *testing.T) {
	stats := connlimit.Stats{
		Open:                   1,
		Accepted:               2,
		EvictedUnauthenticated: 3,
		EvictedLapsed:          4,
		EvictedOverShare:       5,
		Refused:                6,
	}
	want := map[string]any{
		"open":                    int64(1),
		"accepted":                uint64(2),
		"evicted_unauthenticated": uint64(3),
		"evicted_lapsed":          uint64(4),
		"evicted_over_share":      uint64(5),
		"refused":                 uint64(6),
	}
	assertDiff(t, "connections group", want, groupNamed(t, []slog.Attr{connectionsGroup(stats)}, "connections"))
}

func TestTheOTLPLayerKeepsAnAuthenticatedConnectionAgainstANewcomer(t *testing.T) {
	logs := &logBuffer{}
	address := localAddress(t)
	limits := testIngest()
	limits.MaxConnections = 1
	current := otlpLayer(&Config{
		OTLPAddr: address,
	}, limits, acceptingJournal(t), admittingTaps(), testLogger(logs))
	view := newLayerSupervisor(t, logs, &current)
	if err := current.unit.start(t.Context(), view); err != nil {
		t.Fatalf("start: %v", err)
	}
	client := ptraceotlp.NewGRPCClient(dialOTLP(t, address))
	if _, err := client.Export(t.Context(), routedRequest()); err != nil {
		t.Fatalf("Export: %v", err)
	}
	type want struct {
		NewcomerClosed bool
		Exported       bool
		Refused        any
	}
	newcomerClosed := closedByThePeer(t, dialRawOTLP(t, address))
	_, err := client.Export(t.Context(), routedRequest())
	got := want{
		NewcomerClosed: newcomerClosed,
		Exported:       err == nil,
		Refused:        groupNamed(t, current.statsGroups(t.Context()), "connections")["refused"],
	}
	if err := stopLayer(t, &current, view); err != nil {
		t.Errorf("stop = %v, want nil", err)
	}
	assertDiff(t, "a newcomer next to the only authenticated connection", want{
		NewcomerClosed: true,
		Exported:       true,
		Refused:        uint64(1),
	}, got)
}
