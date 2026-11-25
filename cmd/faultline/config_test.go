package main

import (
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/domain/topology"
	"github.com/DmytroLysenko1/faultline/pkg/stream"
)

func environment(values map[string]string) lookupFunc {
	return func(name string) (string, bool) {
		value, found := values[name]
		return value, found
	}
}

type loadedVariables struct {
	Shared       Config
	Graph        graphConfig
	FeedMaxBytes int64
}

func loadVariables(t *testing.T, values map[string]string) (loadedVariables, error) {
	t.Helper()
	lookup := func(name string) (string, bool) {
		if name == envCheckpointDir {
			return t.TempDir(), true
		}
		value, found := values[name]
		return value, found
	}
	shared, sharedErr := Load(lookup)
	graph, graphErr := loadGraph(lookup)
	partition, partitionErr := loadPartition(lookup)
	return loadedVariables{
		Shared:       shared,
		Graph:        graph,
		FeedMaxBytes: partition.FeedMaxBytes,
	}, errors.Join(sharedErr, graphErr, partitionErr)
}

func defaultConfig() Config {
	return Config{
		OTLPAddr: "127.0.0.1:4317",
		APIAddr:  "127.0.0.1:8080",
		LogLevel: slog.LevelInfo,
	}
}

func defaultGraph() graphConfig {
	return graphConfig{
		Topology: topology.Limits{
			PairingTTL:    10 * time.Second,
			MaxPending:    100_000,
			TombstoneTTL:  time.Minute,
			MaxTombstones: 1_000_000,
			MaxNodes:      10_000,
			MaxEdges:      50_000,
			Windows: stream.Config{
				Size:            10 * time.Second,
				Delay:           30 * time.Second,
				AllowedLateness: 30 * time.Second,
				SlackWindows:    2,
			},
			EarlyFirePeriod:         time.Second,
			MaxSeries:               100_000,
			MaxOperationsPerService: 500,
			SketchEntries:           8 << 20,
		},
		APIResponseBudget: 256 << 20,
		IdleAfter:         35 * time.Second,
	}
}

func defaultVariables() loadedVariables {
	return loadedVariables{
		Shared:       defaultConfig(),
		Graph:        defaultGraph(),
		FeedMaxBytes: 64 << 20,
	}
}

func variablesWith(edit func(loaded *loadedVariables)) loadedVariables {
	loaded := defaultVariables()
	edit(&loaded)
	return loaded
}

func TestLoadReadsEveryVariable(t *testing.T) {
	tests := []struct {
		Name string
		Args map[string]string
		Want loadedVariables
	}{
		{
			Name: "empty environment gives the documented defaults",
			Args: map[string]string{},
			Want: defaultVariables(),
		},
		{
			Name: "unrelated variables are ignored",
			Args: map[string]string{
				"UNKNOWN": "x",
			},
			Want: defaultVariables(),
		},
		{
			Name: "every variable lands in its field",
			Args: map[string]string{
				envOTLPAddr:          "0.0.0.0:4317",
				envAPIAddr:           "[::1]:8081",
				envFeedMaxBytes:      "128MiB",
				envIdleAdvanceAfter:  "1m",
				envPairingTTL:        "20s",
				envPairingMaxPending: "500",
				envTombstoneTTL:      "2m",
				envTombstoneMax:      "600",
				envGraphMaxNodes:     "20000",
				envGraphMaxEdges:     "1",
				envLogLevel:          "debug",
				envSelfTracing:       "collector:4317",
				envAPIResponseBudget: "320MiB",
				envWindowSize:        "30s",
				envWatermarkDelay:    "0s",
				envAllowedLateness:   "2m",
				envSlackWindows:      "4",
				envEarlyFirePeriod:   "500ms",
				envMaxSeries:         "200000",
				envOperationsEach:    "999",
				envSketchEntries:     "16777216",
				envRouteRulesFile:    "/etc/faultline/routes.yaml",
			},
			Want: loadedVariables{
				Shared: Config{
					OTLPAddr:            "0.0.0.0:4317",
					APIAddr:             "[::1]:8081",
					RouteRulesFile:      "/etc/faultline/routes.yaml",
					LogLevel:            slog.LevelDebug,
					SelfTracingEndpoint: "collector:4317",
				},
				Graph: graphConfig{
					Topology: topology.Limits{
						PairingTTL:    20 * time.Second,
						MaxPending:    500,
						TombstoneTTL:  2 * time.Minute,
						MaxTombstones: 600,
						MaxNodes:      20_000,
						MaxEdges:      1,
						Windows: stream.Config{
							Size:            30 * time.Second,
							Delay:           0,
							AllowedLateness: 2 * time.Minute,
							SlackWindows:    4,
						},
						EarlyFirePeriod:         500 * time.Millisecond,
						MaxSeries:               200_000,
						MaxOperationsPerService: 999,
						SketchEntries:           16 << 20,
					},
					APIResponseBudget: 320 << 20,
					IdleAfter:         time.Minute,
				},
				FeedMaxBytes: 128 << 20,
			},
		},
		{
			Name: "empty self-tracing endpoint means disabled",
			Args: map[string]string{
				envSelfTracing: "",
			},
			Want: defaultVariables(),
		},
		{
			Name: "size at the upper range edge in MiB",
			Args: map[string]string{
				envFeedMaxBytes: "512MiB",
			},
			Want: variablesWith(func(loaded *loadedVariables) {
				loaded.FeedMaxBytes = 512 << 20
			}),
		},
		{
			Name: "size at the lower range edge in plain bytes",
			Args: map[string]string{
				envFeedMaxBytes: "50331648B",
			},
			Want: variablesWith(func(loaded *loadedVariables) {
				loaded.FeedMaxBytes = 48 << 20
			}),
		},
		{
			Name: "size in KiB and GiB",
			Args: map[string]string{
				envFeedMaxBytes: "131072KiB",
			},
			Want: variablesWith(func(loaded *loadedVariables) {
				loaded.FeedMaxBytes = 128 << 20
			}),
		},
		{
			Name: "tombstone ttl equal to pairing ttl is allowed",
			Args: map[string]string{
				envPairingTTL:   "30s",
				envTombstoneTTL: "30s",
			},
			Want: variablesWith(func(loaded *loadedVariables) {
				loaded.Graph.Topology.PairingTTL = 30 * time.Second
				loaded.Graph.Topology.TombstoneTTL = 30 * time.Second
			}),
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			got, err := loadVariables(t, tt.Args)
			if err != nil {
				t.Fatalf("loadVariables: %v", err)
			}
			if diff := cmp.Diff(tt.Want, got); diff != "" {
				t.Errorf("config (-want +got):\n%s", diff)
			}
		})
	}
}

func TestLoadRejectsWithAnExactMessage(t *testing.T) {
	tests := []struct {
		Name    string
		Args    map[string]string
		Want    string
		WantErr error
	}{
		{
			Name: "duration without unit",
			Args: map[string]string{
				envPairingTTL: "10",
			},
			Want:    `PAIRING_TTL: invalid duration "10", want e.g. 10s`,
			WantErr: errInvalid,
		},
		{
			Name: "duration with a sign",
			Args: map[string]string{
				envPairingTTL: "+10s",
			},
			Want:    `PAIRING_TTL: invalid duration "+10s", want e.g. 10s`,
			WantErr: errInvalid,
		},
		{
			Name: "duration with a space",
			Args: map[string]string{
				envPairingTTL: " 10s",
			},
			Want:    `PAIRING_TTL: invalid duration " 10s", want e.g. 10s`,
			WantErr: errInvalid,
		},
		{
			Name: "duration in the wrong case",
			Args: map[string]string{
				envPairingTTL: "10S",
			},
			Want:    `PAIRING_TTL: invalid duration "10S", want e.g. 10s`,
			WantErr: errInvalid,
		},
		{
			Name: "duration below range",
			Args: map[string]string{
				envPairingTTL: "999ms",
			},
			Want:    "PAIRING_TTL: 999ms out of range, want 1s … 5m0s",
			WantErr: errOutOfRange,
		},
		{
			Name: "duration above range",
			Args: map[string]string{
				envTombstoneTTL: "11m",
			},
			Want:    "TOMBSTONE_TTL: 11m0s out of range, want 1s … 10m0s",
			WantErr: errOutOfRange,
		},
		{
			Name: "idle advance below range",
			Args: map[string]string{
				envIdleAdvanceAfter: "50ms",
			},
			Want:    "IDLE_ADVANCE_AFTER: 50ms out of range, want 100ms … 5m0s",
			WantErr: errOutOfRange,
		},
		{
			Name: "idle advance above range",
			Args: map[string]string{
				envIdleAdvanceAfter: "6m",
			},
			Want:    "IDLE_ADVANCE_AFTER: 6m0s out of range, want 100ms … 5m0s",
			WantErr: errOutOfRange,
		},
		{
			Name: "count above range",
			Args: map[string]string{
				envGraphMaxNodes: "30000",
			},
			Want:    "GRAPH_MAX_NODES: 30000 out of range, want 1 … 20000",
			WantErr: errOutOfRange,
		},
		{
			Name: "count of zero",
			Args: map[string]string{
				envGraphMaxEdges: "0",
			},
			Want:    "GRAPH_MAX_EDGES: 0 out of range, want 1 … 100000",
			WantErr: errOutOfRange,
		},
		{
			Name: "count with a sign",
			Args: map[string]string{
				envTombstoneMax: "+5",
			},
			Want:    `TOMBSTONE_MAX: invalid integer "+5" (not an unsigned decimal integer), want e.g. 1000000`,
			WantErr: errInvalid,
		},
		{
			Name: "count overflowing int64",
			Args: map[string]string{
				envTombstoneMax: "99999999999999999999",
			},
			Want: `TOMBSTONE_MAX: invalid integer "99999999999999999999" (value overflows int64: ` +
				`strconv.ParseInt: parsing "99999999999999999999": value out of range), want e.g. 1000000`,
			WantErr: errOverflow,
		},
		{
			Name: "size without unit",
			Args: map[string]string{
				envFeedMaxBytes: "64",
			},
			Want:    `FEED_MAX_BYTES: invalid size "64" (unknown size unit), want e.g. 64MiB`,
			WantErr: errUnknownUnit,
		},
		{
			Name: "size in the wrong case",
			Args: map[string]string{
				envFeedMaxBytes: "64mib",
			},
			Want:    `FEED_MAX_BYTES: invalid size "64mib" (unknown size unit), want e.g. 64MiB`,
			WantErr: errUnknownUnit,
		},
		{
			Name: "size below range",
			Args: map[string]string{
				envFeedMaxBytes: "32MiB",
			},
			Want:    "FEED_MAX_BYTES: 32MiB out of range, want 48MiB … 512MiB",
			WantErr: errOutOfRange,
		},
		{
			Name: "size above range",
			Args: map[string]string{
				envFeedMaxBytes: "1GiB",
			},
			Want:    "FEED_MAX_BYTES: 1GiB out of range, want 48MiB … 512MiB",
			WantErr: errOutOfRange,
		},
		{
			Name: "size overflowing before multiplication",
			Args: map[string]string{
				envFeedMaxBytes: "9999999999GiB",
			},
			Want:    `FEED_MAX_BYTES: invalid size "9999999999GiB" (value overflows int64), want e.g. 64MiB`,
			WantErr: errOverflow,
		},
		{
			Name: "empty value is a typo, not the default",
			Args: map[string]string{
				envGraphMaxNodes: "",
			},
			Want:    "GRAPH_MAX_NODES: empty value, unset the variable to use the default",
			WantErr: errEmpty,
		},
		{
			Name: "address without host",
			Args: map[string]string{
				envOTLPAddr: ":4317",
			},
			Want:    `OTLP_GRPC_ADDR: invalid address ":4317" (host is empty), want host:port`,
			WantErr: errEmptyHost,
		},
		{
			Name: "address with port zero",
			Args: map[string]string{
				envAPIAddr: "127.0.0.1:0",
			},
			Want:    `API_ADDR: invalid address "127.0.0.1:0" (port must be 1 … 65535), want host:port`,
			WantErr: errPort,
		},
		{
			Name: "address with a leading space",
			Args: map[string]string{
				envOTLPAddr: " 127.0.0.1:4317",
			},
			Want:    `OTLP_GRPC_ADDR: invalid address " 127.0.0.1:4317" (contains whitespace), want host:port`,
			WantErr: errWhitespace,
		},
		{
			Name: "address without port",
			Args: map[string]string{
				envAPIAddr: "localhost",
			},
			Want: `API_ADDR: invalid address "localhost" ` +
				"(address localhost: missing port in address), want host:port",
			WantErr: errAddress,
		},
		{
			Name: "endpoint with port out of range",
			Args: map[string]string{
				envSelfTracing: "collector:70000",
			},
			Want: `SELF_TRACING_ENDPOINT: invalid address "collector:70000" ` +
				"(port must be 1 … 65535), want host:port",
			WantErr: errPort,
		},
		{
			Name: "self-tracing endpoint equal to the otlp address",
			Args: map[string]string{
				envSelfTracing: defaultOTLPAddr,
			},
			Want:    `SELF_TRACING_ENDPOINT: "127.0.0.1:4317" must differ from OTLP_GRPC_ADDR`,
			WantErr: errSameAddress,
		},
		{
			Name: "unknown log level",
			Args: map[string]string{
				envLogLevel: "Info",
			},
			Want:    `LOG_LEVEL: unknown log level "Info", want debug, info, warn or error`,
			WantErr: errUnknownLevel,
		},
		{
			Name: "tombstone ttl below pairing ttl",
			Args: map[string]string{
				envPairingTTL:   "30s",
				envTombstoneTTL: "20s",
			},
			Want:    "TOMBSTONE_TTL: 20s must not be below PAIRING_TTL (30s)",
			WantErr: errBelow,
		},
		{
			Name: "window that does not divide an hour",
			Args: map[string]string{
				envWindowSize: "7s",
			},
			Want:    "WINDOW_SIZE: 7s must divide 1h0m0s without a remainder",
			WantErr: errNotDivisor,
		},
		{
			Name: "window above five minutes",
			Args: map[string]string{
				envWindowSize: "10m",
			},
			Want:    "WINDOW_SIZE: 10m0s out of range, want 1s … 5m0s",
			WantErr: errOutOfRange,
		},
		{
			Name: "allowed lateness above ten minutes",
			Args: map[string]string{
				envAllowedLateness: "11m",
			},
			Want:    "ALLOWED_LATENESS: 11m0s out of range, want 0s … 10m0s",
			WantErr: errOutOfRange,
		},
		{
			Name: "slack windows above sixteen",
			Args: map[string]string{
				envSlackWindows: "17",
			},
			Want:    "HORIZON_SLACK_WINDOWS: 17 out of range, want 1 … 16",
			WantErr: errOutOfRange,
		},
		{
			Name: "delay and lateness that need too many live windows",
			Args: map[string]string{
				envWindowSize:      "1s",
				envWatermarkDelay:  "5m",
				envAllowedLateness: "10m",
			},
			Want: "WINDOW_SIZE: invalid window config: delay 5m0s and lateness 10m0s need more than 256 " +
				"live windows of 1s per series",
			WantErr: stream.ErrInvalidConfig,
		},
		{
			Name: "early fire period below 100ms",
			Args: map[string]string{
				envEarlyFirePeriod: "50ms",
			},
			Want:    "EARLY_FIRE_PERIOD: 50ms out of range, want 100ms … 5m0s",
			WantErr: errOutOfRange,
		},
		{
			Name: "early fire period above the window",
			Args: map[string]string{
				envEarlyFirePeriod: "15s",
			},
			Want:    "EARLY_FIRE_PERIOD: 15s must not be above WINDOW_SIZE (10s)",
			WantErr: errAbove,
		},
		{
			Name: "idle pulse sooner than the early fire period",
			Args: map[string]string{
				envEarlyFirePeriod:  "2s",
				envIdleAdvanceAfter: "1s",
			},
			Want:    "IDLE_ADVANCE_AFTER: 1s must not be below EARLY_FIRE_PERIOD (2s)",
			WantErr: errBelow,
		},
		{
			Name: "operations per service above 999",
			Args: map[string]string{
				envOperationsEach: "1000",
			},
			Want:    "MAX_OPERATIONS_PER_SERVICE: 1000 out of range, want 1 … 999",
			WantErr: errOutOfRange,
		},
		{
			Name: "series above a million",
			Args: map[string]string{
				envMaxSeries: "1000001",
			},
			Want:    "MAX_SERIES: 1000001 out of range, want 1 … 1000000",
			WantErr: errOutOfRange,
		},
		{
			Name: "sketch arena that is not a power of two",
			Args: map[string]string{
				envSketchEntries: "3000000",
			},
			Want:    "SKETCH_MAX_ENTRIES: 3000000 must be a power of two",
			WantErr: errPowerOfTwo,
		},
		{
			Name: "sketch arena below one Mi entries",
			Args: map[string]string{
				envSketchEntries: "1024",
			},
			Want:    "SKETCH_MAX_ENTRIES: 1024 out of range, want 1048576 … 268435456",
			WantErr: errOutOfRange,
		},
		{
			Name: "empty route rules path",
			Args: map[string]string{
				envRouteRulesFile: "",
			},
			Want:    "ROUTE_RULES_FILE: empty value, unset the variable to use the default",
			WantErr: errEmpty,
		},
		{
			Name: "response budget below the worst response at the graph limits",
			Args: map[string]string{
				envAPIResponseBudget: "128MiB",
			},
			Want: "API_RESPONSE_BUDGET: 134217728 must not be below the worst response at the graph limits " +
				"(257762560)",
			WantErr: errBelow,
		},
		{
			Name: "response budget above 1 GiB",
			Args: map[string]string{
				envAPIResponseBudget: "2GiB",
			},
			Want:    "API_RESPONSE_BUDGET: 2GiB out of range, want 1MiB … 1GiB",
			WantErr: errOutOfRange,
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			_, err := loadVariables(t, tt.Args)
			if !errors.Is(err, tt.WantErr) {
				t.Fatalf("loadVariables = %v, want %v", err, tt.WantErr)
			}
			if diff := cmp.Diff(tt.Want, err.Error()); diff != "" {
				t.Errorf("message (-want +got):\n%s", diff)
			}
		})
	}
}

func TestLoadReportsEveryErrorAtOnce(t *testing.T) {
	_, err := loadVariables(t, map[string]string{
		envPairingTTL:    "10",
		envGraphMaxNodes: "30000",
		envFeedMaxBytes:  "64",
	})
	want := []string{
		`PAIRING_TTL: invalid duration "10", want e.g. 10s`,
		"GRAPH_MAX_NODES: 30000 out of range, want 1 … 20000",
		`FEED_MAX_BYTES: invalid size "64" (unknown size unit), want e.g. 64MiB`,
	}
	if err == nil {
		t.Fatal("Load accepted three invalid variables")
	}
	if diff := cmp.Diff(want, strings.Split(err.Error(), "\n")); diff != "" {
		t.Errorf("messages (-want +got):\n%s", diff)
	}
}

func TestCrossCheckIsSkippedWhenAnInputFailed(t *testing.T) {
	_, err := loadGraph(environment(map[string]string{
		envPairingTTL:   "1h",
		envTombstoneTTL: "5s",
	}))
	if errors.Is(err, errBelow) {
		t.Errorf("cross-check reported against an unparsed pairing ttl: %v", err)
	}
	if !errors.Is(err, errOutOfRange) {
		t.Errorf("Load = %v, want only the pairing ttl range error", err)
	}
}
