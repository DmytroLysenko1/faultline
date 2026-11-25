package main

import (
	"bytes"
	"io"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/samber/lo"

	"github.com/DmytroLysenko1/faultline/internal/application/selfalert"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/span"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/natsjstest"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/record"
)

const (
	lagReportsAtMost     = 10
	watermarkFarBehind   = 24 * time.Hour
	belowWorstResponse   = "1MiB"
	pairingAboveTomb     = "2s"
	tombstoneBelowPair   = "1s"
	brokenTenant         = "not a tenant!"
	oldJournalBytes      = "256MiB"
	oldJournalBatches    = "100"
	removedJournalBytes  = "JOURNAL_MAX_BYTES: removed, use FEED_MAX_BYTES"
	removedJournalBatchs = "JOURNAL_MAX_BATCHES: removed"
	sameAsOTLP           = "SELF_TRACING_ENDPOINT: "
)

func roleConfigRows() []struct {
	name string
	args map[string]string
	want string
} {
	return []struct {
		name string
		args map[string]string
		want string
	}{
		{
			name: "a tombstone ttl below the pairing ttl",
			args: map[string]string{
				envPairingTTL:   pairingAboveTomb,
				envTombstoneTTL: tombstoneBelowPair,
			},
			want: envTombstoneTTL + ": ",
		},
		{
			name: "an api response budget below the worst response",
			args: map[string]string{
				envAPIResponseBudget: belowWorstResponse,
			},
			want: envAPIResponseBudget + ": ",
		},
		{
			name: "self-tracing to its own otlp port",
			args: map[string]string{
				envSelfTracing: defaultOTLPAddr,
			},
			want: sameAsOTLP,
		},
		{
			name: "the old journal byte limit points to the feed",
			args: map[string]string{
				envJournalMaxBytes: oldJournalBytes,
			},
			want: removedJournalBytes,
		},
		{
			name: "the old journal batch limit is removed",
			args: map[string]string{
				envJournalMaxBatches: oldJournalBatches,
			},
			want: removedJournalBatchs,
		},
	}
}

func TestTheAllCommandRefusesItsConfigWithCodeTwo(t *testing.T) {
	tests := append(roleConfigRows(), []struct {
		name string
		args map[string]string
		want string
	}{
		{
			name: "a publish timeout that does not parse",
			args: map[string]string{
				envPublishTimeout: "abc",
			},
			want: envPublishTimeout + ": ",
		},
		{
			name: "a tenant the records cannot carry",
			args: map[string]string{
				envTenant: brokenTenant,
			},
			want: envTenant + ": ",
		},
	}...)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			values := processorEnvironment(t, tt.args)
			if _, err := loadAll(processOf(values)); err == nil {
				t.Fatalf("loadAll accepted the config, want an error containing %q", tt.want)
			}
			assertRefused(t, commandAll, values, tt.want)
		})
	}
}

func TestTheProcessorRefusesTheGraphAndSharedChecksWithCodeTwo(t *testing.T) {
	for _, tt := range roleConfigRows() {
		t.Run(tt.name, func(t *testing.T) {
			values := processorEnvironment(t, tt.args)
			if _, err := loadProcessor(processOf(values)); err == nil {
				t.Fatalf("loadProcessor accepted the config, want an error containing %q", tt.want)
			}
			assertRefused(t, commandProcessor, values, tt.want)
		})
	}
}

func TestTheIngesterRefusesTheSharedChecksWithCodeTwo(t *testing.T) {
	tests := []struct {
		name string
		args map[string]string
		want string
	}{
		{
			name: "self-tracing to its own otlp port",
			args: map[string]string{
				envSelfTracing: defaultOTLPAddr,
			},
			want: sameAsOTLP,
		},
		{
			name: "the old journal byte limit points to the feed",
			args: map[string]string{
				envJournalMaxBytes: oldJournalBytes,
			},
			want: removedJournalBytes,
		},
		{
			name: "the old journal batch limit is removed",
			args: map[string]string{
				envJournalMaxBatches: oldJournalBatches,
			},
			want: removedJournalBatchs,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.args[envNATSURL] = testNATSURL
			if _, err := loadIngester(processOf(tt.args)); err == nil {
				t.Fatalf("loadIngester accepted the config, want an error containing %q", tt.want)
			}
			assertRefused(t, commandIngester, tt.args, tt.want)
		})
	}
}

func assertRefused(t *testing.T, command string, values map[string]string, want string) {
	t.Helper()
	var stderr bytes.Buffer
	code := realMain(process{
		Args: []string{
			command,
		},
		Lookup:         environment(values),
		ReadCgroup:     func() ([]byte, error) { return nil, os.ErrNotExist },
		SetMemoryLimit: (&recordedLimit{}).setLimit,
		Stderr:         &stderr,
		Hostname:       podHostname,
	})
	assertDiff(t, "code", exitConfig, code)
	if !strings.Contains(stderr.String(), want) {
		t.Errorf("stderr = %q, want it to contain %q", stderr.String(), want)
	}
}

func assertAllRefusedBeforeItsStack(t *testing.T, current process) {
	t.Helper()
	if len(current.Args) != 1 || current.Args[0] != commandAll {
		return
	}
	current.SetMemoryLimit = (&recordedLimit{}).setLimit
	current.Stderr = io.Discard
	if _, _, err := prepareAll(current); err == nil {
		t.Fatalf("all accepted its config and memory budget, realMain would start its stack and dial NATS")
	}
}

func TestTheAllLagThresholdFollowsTheIdleAdvance(t *testing.T) {
	tests := []struct {
		name string
		idle string
		want time.Duration
	}{
		{"the defaults", "", defaultLagSeconds * time.Second},
		{"a longer idle advance", "5m", (defaultLagSeconds - 35 + 300) * time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			values := processorEnvironment(t, nil)
			if tt.idle != "" {
				values[envIdleAdvanceAfter] = tt.idle
			}
			run := buildAll(t, values)
			stats := run.layer(t, layerStats).unit.(*statsUnit)
			assertDiff(t, "lag threshold seconds", tt.want.Seconds(), raisedLagThreshold(stats.alerts))
		})
	}
}

func raisedLagThreshold(alerts *selfalert.Evaluator) float64 {
	now := time.Now()
	sample := selfalert.Sample{
		Now:            now,
		Watermark:      now.Add(-watermarkFarBehind),
		WatermarkKnown: true,
	}
	for range lagReportsAtMost {
		for _, moved := range alerts.Evaluate(&sample) {
			if moved.Event == selfalert.WatermarkLagHigh {
				return moved.Reading.Threshold
			}
		}
	}
	return 0
}

func TestAllNamesOperationsWithItsRouteRules(t *testing.T) {
	cluster := natsjstest.Start(t, singleNATSNode)
	values := allEnvironment(t, cluster.URLs())
	values[envRouteRulesFile] = writeRules(t, catalogRules)
	run := startAll(t, values)

	response, err := run.exporter(t).Export(t.Context(), routedRequest())
	if err != nil {
		t.Fatalf("Export: %v", err)
	}

	assertDiff(t, "rejected spans", int64(0), response.PartialSuccess().RejectedSpans())
	got := streamOperations(t, cluster.URLs()[0], run.setup.Journal.Topology)
	assertDiff(t, "operations in the stream the graph reads", []string{routedTemplate}, got)
	run.sig.send()
	assertCode(t, <-run.done, exitOK, run.logs)
}

func streamOperations(t *testing.T, url string, streams natsjs.Topology) []string {
	t.Helper()
	js := natsjstest.JetStream(t, url)
	var names []string
	for partition := range uint16(streams.Partitions) {
		stream, err := js.Stream(t.Context(), streams.Stream(partition))
		if err != nil {
			t.Fatalf("stream %d: %v", partition, err)
		}
		for sequence := range stream.CachedInfo().State.LastSeq {
			stored, err := stream.GetMsg(t.Context(), sequence+1)
			if err != nil {
				t.Fatalf("GetMsg %d: %v", sequence+1, err)
			}
			names = append(names, recordOperations(t, stored.Data, partition)...)
		}
	}
	return names
}

func recordOperations(t *testing.T, data []byte, partition uint16) []string {
	t.Helper()
	header, err := record.ReadHeader(data, partition, recordFootprint)
	if err != nil {
		t.Fatalf("ReadHeader: %v", err)
	}
	spans, err := record.Decode(data, header)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	return lo.Map(spans, func(stored span.Span, _ int) string { return stored.Operation.Name })
}

func TestAllServesOnOneNATSConnection(t *testing.T) {
	cluster := natsjstest.Start(t, singleNATSNode)
	run := startAll(t, allEnvironment(t, cluster.URLs()))

	connections, err := cluster.Servers[0].Connz(&server.ConnzOptions{})
	if err != nil {
		t.Fatalf("Connz: %v", err)
	}

	run.sig.send()
	assertCode(t, <-run.done, exitOK, run.logs)
	instance := run.setup.NATS.Instance
	want := []string{
		instance + "-control",
		instance + "-feed",
		instance + "-publish",
	}
	names := lo.Map(connections.Conns, func(open *server.ConnInfo, _ int) string { return open.Name })
	got := slices.Sorted(slices.Values(names))
	assertDiff(t, "client connections to NATS while serving, one natsjs connection", want, got)
}
