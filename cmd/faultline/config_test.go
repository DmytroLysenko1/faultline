package main

import (
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/domain/topology"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/memjournal"
)

func environment(values map[string]string) lookupFunc {
	return func(name string) (string, bool) {
		value, found := values[name]
		return value, found
	}
}

func defaultConfig() Config {
	return Config{
		OTLPAddr: "127.0.0.1:4317",
		APIAddr:  "127.0.0.1:8080",
		Journal: memjournal.Limits{
			MaxBytes:   64 << 20,
			MaxBatches: 4096,
		},
		Topology: topology.Limits{
			PairingTTL:    10 * time.Second,
			MaxPending:    100_000,
			TombstoneTTL:  time.Minute,
			MaxTombstones: 1_000_000,
			MaxNodes:      10_000,
			MaxEdges:      50_000,
		},
		LogLevel: slog.LevelInfo,
	}
}

func configWith(edit func(cfg *Config)) Config {
	cfg := defaultConfig()
	edit(&cfg)
	return cfg
}

func TestLoadReadsEveryVariable(t *testing.T) {
	tests := []struct {
		Name string
		Args map[string]string
		Want Config
	}{
		{
			Name: "empty environment gives the documented defaults",
			Args: map[string]string{},
			Want: defaultConfig(),
		},
		{
			Name: "unrelated FAULTLINE variables are ignored",
			Args: map[string]string{
				"FAULTLINE_UNKNOWN": "x",
			},
			Want: defaultConfig(),
		},
		{
			Name: "every variable lands in its field",
			Args: map[string]string{
				envOTLPAddr:          "0.0.0.0:4317",
				envAPIAddr:           "[::1]:8081",
				envJournalMaxBytes:   "128MiB",
				envJournalMaxBatches: "10",
				envPairingTTL:        "20s",
				envPairingMaxPending: "500",
				envTombstoneTTL:      "2m",
				envTombstoneMax:      "600",
				envGraphMaxNodes:     "20000",
				envGraphMaxEdges:     "1",
				envLogLevel:          "debug",
				envSelfTracing:       "collector:4317",
			},
			Want: Config{
				OTLPAddr: "0.0.0.0:4317",
				APIAddr:  "[::1]:8081",
				Journal: memjournal.Limits{
					MaxBytes:   128 << 20,
					MaxBatches: 10,
				},
				Topology: topology.Limits{
					PairingTTL:    20 * time.Second,
					MaxPending:    500,
					TombstoneTTL:  2 * time.Minute,
					MaxTombstones: 600,
					MaxNodes:      20_000,
					MaxEdges:      1,
				},
				LogLevel:            slog.LevelDebug,
				SelfTracingEndpoint: "collector:4317",
			},
		},
		{
			Name: "empty self-tracing endpoint means disabled",
			Args: map[string]string{
				envSelfTracing: "",
			},
			Want: defaultConfig(),
		},
		{
			Name: "size at the upper range edge in MiB",
			Args: map[string]string{
				envJournalMaxBytes: "512MiB",
			},
			Want: configWith(func(cfg *Config) {
				cfg.Journal.MaxBytes = 512 << 20
			}),
		},
		{
			Name: "size at the lower range edge in plain bytes",
			Args: map[string]string{
				envJournalMaxBytes: "50331648B",
			},
			Want: configWith(func(cfg *Config) {
				cfg.Journal.MaxBytes = 48 << 20
			}),
		},
		{
			Name: "size in KiB and GiB",
			Args: map[string]string{
				envJournalMaxBytes: "131072KiB",
			},
			Want: configWith(func(cfg *Config) {
				cfg.Journal.MaxBytes = 128 << 20
			}),
		},
		{
			Name: "tombstone ttl equal to pairing ttl is allowed",
			Args: map[string]string{
				envPairingTTL:   "30s",
				envTombstoneTTL: "30s",
			},
			Want: configWith(func(cfg *Config) {
				cfg.Topology.PairingTTL = 30 * time.Second
				cfg.Topology.TombstoneTTL = 30 * time.Second
			}),
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			got, err := Load(environment(tt.Args))
			if err != nil {
				t.Fatalf("Load: %v", err)
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
			Want:    `FAULTLINE_PAIRING_TTL: invalid duration "10", want e.g. 10s`,
			WantErr: errInvalid,
		},
		{
			Name: "duration with a sign",
			Args: map[string]string{
				envPairingTTL: "+10s",
			},
			Want:    `FAULTLINE_PAIRING_TTL: invalid duration "+10s", want e.g. 10s`,
			WantErr: errInvalid,
		},
		{
			Name: "duration with a space",
			Args: map[string]string{
				envPairingTTL: " 10s",
			},
			Want:    `FAULTLINE_PAIRING_TTL: invalid duration " 10s", want e.g. 10s`,
			WantErr: errInvalid,
		},
		{
			Name: "duration in the wrong case",
			Args: map[string]string{
				envPairingTTL: "10S",
			},
			Want:    `FAULTLINE_PAIRING_TTL: invalid duration "10S", want e.g. 10s`,
			WantErr: errInvalid,
		},
		{
			Name: "duration below range",
			Args: map[string]string{
				envPairingTTL: "999ms",
			},
			Want:    "FAULTLINE_PAIRING_TTL: 999ms out of range, want 1s … 5m0s",
			WantErr: errOutOfRange,
		},
		{
			Name: "duration above range",
			Args: map[string]string{
				envTombstoneTTL: "11m",
			},
			Want:    "FAULTLINE_TOMBSTONE_TTL: 11m0s out of range, want 1s … 10m0s",
			WantErr: errOutOfRange,
		},
		{
			Name: "count above range",
			Args: map[string]string{
				envGraphMaxNodes: "30000",
			},
			Want:    "FAULTLINE_GRAPH_MAX_NODES: 30000 out of range, want 1 … 20000",
			WantErr: errOutOfRange,
		},
		{
			Name: "count of zero",
			Args: map[string]string{
				envGraphMaxEdges: "0",
			},
			Want:    "FAULTLINE_GRAPH_MAX_EDGES: 0 out of range, want 1 … 100000",
			WantErr: errOutOfRange,
		},
		{
			Name: "count with a sign",
			Args: map[string]string{
				envJournalMaxBatches: "+5",
			},
			Want:    `FAULTLINE_JOURNAL_MAX_BATCHES: invalid integer "+5" (not an unsigned decimal integer), want e.g. 4096`,
			WantErr: errInvalid,
		},
		{
			Name: "count overflowing int64",
			Args: map[string]string{
				envTombstoneMax: "99999999999999999999",
			},
			Want: `FAULTLINE_TOMBSTONE_MAX: invalid integer "99999999999999999999" (value overflows int64: ` +
				`strconv.ParseInt: parsing "99999999999999999999": value out of range), want e.g. 1000000`,
			WantErr: errOverflow,
		},
		{
			Name: "size without unit",
			Args: map[string]string{
				envJournalMaxBytes: "64",
			},
			Want:    `FAULTLINE_JOURNAL_MAX_BYTES: invalid size "64" (unknown size unit), want e.g. 64MiB`,
			WantErr: errUnknownUnit,
		},
		{
			Name: "size in the wrong case",
			Args: map[string]string{
				envJournalMaxBytes: "64mib",
			},
			Want:    `FAULTLINE_JOURNAL_MAX_BYTES: invalid size "64mib" (unknown size unit), want e.g. 64MiB`,
			WantErr: errUnknownUnit,
		},
		{
			Name: "size below range",
			Args: map[string]string{
				envJournalMaxBytes: "32MiB",
			},
			Want:    "FAULTLINE_JOURNAL_MAX_BYTES: 32MiB out of range, want 48MiB … 512MiB",
			WantErr: errOutOfRange,
		},
		{
			Name: "size above range",
			Args: map[string]string{
				envJournalMaxBytes: "1GiB",
			},
			Want:    "FAULTLINE_JOURNAL_MAX_BYTES: 1GiB out of range, want 48MiB … 512MiB",
			WantErr: errOutOfRange,
		},
		{
			Name: "size overflowing before multiplication",
			Args: map[string]string{
				envJournalMaxBytes: "9999999999GiB",
			},
			Want:    `FAULTLINE_JOURNAL_MAX_BYTES: invalid size "9999999999GiB" (value overflows int64), want e.g. 64MiB`,
			WantErr: errOverflow,
		},
		{
			Name: "empty value is a typo, not the default",
			Args: map[string]string{
				envGraphMaxNodes: "",
			},
			Want:    "FAULTLINE_GRAPH_MAX_NODES: empty value, unset the variable to use the default",
			WantErr: errEmpty,
		},
		{
			Name: "address without host",
			Args: map[string]string{
				envOTLPAddr: ":4317",
			},
			Want:    `FAULTLINE_OTLP_GRPC_ADDR: invalid address ":4317" (host is empty), want host:port`,
			WantErr: errEmptyHost,
		},
		{
			Name: "address with port zero",
			Args: map[string]string{
				envAPIAddr: "127.0.0.1:0",
			},
			Want:    `FAULTLINE_API_ADDR: invalid address "127.0.0.1:0" (port must be 1 … 65535), want host:port`,
			WantErr: errPort,
		},
		{
			Name: "address with a leading space",
			Args: map[string]string{
				envOTLPAddr: " 127.0.0.1:4317",
			},
			Want:    `FAULTLINE_OTLP_GRPC_ADDR: invalid address " 127.0.0.1:4317" (contains whitespace), want host:port`,
			WantErr: errWhitespace,
		},
		{
			Name: "address without port",
			Args: map[string]string{
				envAPIAddr: "localhost",
			},
			Want: `FAULTLINE_API_ADDR: invalid address "localhost" ` +
				"(address localhost: missing port in address), want host:port",
			WantErr: errAddress,
		},
		{
			Name: "endpoint with port out of range",
			Args: map[string]string{
				envSelfTracing: "collector:70000",
			},
			Want: `FAULTLINE_SELF_TRACING_ENDPOINT: invalid address "collector:70000" ` +
				"(port must be 1 … 65535), want host:port",
			WantErr: errPort,
		},
		{
			Name: "self-tracing endpoint equal to the otlp address",
			Args: map[string]string{
				envSelfTracing: defaultOTLPAddr,
			},
			Want:    `FAULTLINE_SELF_TRACING_ENDPOINT: "127.0.0.1:4317" must differ from FAULTLINE_OTLP_GRPC_ADDR`,
			WantErr: errSameAddress,
		},
		{
			Name: "unknown log level",
			Args: map[string]string{
				envLogLevel: "Info",
			},
			Want:    `FAULTLINE_LOG_LEVEL: unknown log level "Info", want debug, info, warn or error`,
			WantErr: errUnknownLevel,
		},
		{
			Name: "tombstone ttl below pairing ttl",
			Args: map[string]string{
				envPairingTTL:   "30s",
				envTombstoneTTL: "20s",
			},
			Want:    "FAULTLINE_TOMBSTONE_TTL: 20s must not be below FAULTLINE_PAIRING_TTL (30s)",
			WantErr: errBelow,
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			_, err := Load(environment(tt.Args))
			if !errors.Is(err, tt.WantErr) {
				t.Fatalf("Load = %v, want %v", err, tt.WantErr)
			}
			if diff := cmp.Diff(tt.Want, err.Error()); diff != "" {
				t.Errorf("message (-want +got):\n%s", diff)
			}
		})
	}
}

func TestLoadReportsEveryErrorAtOnce(t *testing.T) {
	_, err := Load(environment(map[string]string{
		envPairingTTL:      "10",
		envGraphMaxNodes:   "30000",
		envJournalMaxBytes: "64",
	}))
	want := []string{
		`FAULTLINE_JOURNAL_MAX_BYTES: invalid size "64" (unknown size unit), want e.g. 64MiB`,
		`FAULTLINE_PAIRING_TTL: invalid duration "10", want e.g. 10s`,
		"FAULTLINE_GRAPH_MAX_NODES: 30000 out of range, want 1 … 20000",
	}
	if err == nil {
		t.Fatal("Load accepted three invalid variables")
	}
	if diff := cmp.Diff(want, strings.Split(err.Error(), "\n")); diff != "" {
		t.Errorf("messages (-want +got):\n%s", diff)
	}
}

func TestCrossCheckIsSkippedWhenAnInputFailed(t *testing.T) {
	_, err := Load(environment(map[string]string{
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
