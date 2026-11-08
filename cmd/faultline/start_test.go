package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"net"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
)

const (
	tinyLimit      = "2GiB"
	tinyLimitBytes = 2 << 30
)

func occupy(t *testing.T) string {
	t.Helper()
	var config net.ListenConfig
	busy, err := config.Listen(t.Context(), network, loopback)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	t.Cleanup(func() {
		if err := busy.Close(); err != nil {
			t.Errorf("close busy listener: %v", err)
		}
	})
	return busy.Addr().String()
}

func freeAddress(t *testing.T) string {
	t.Helper()
	var config net.ListenConfig
	probe, err := config.Listen(t.Context(), network, loopback)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	address := probe.Addr().String()
	if err := probe.Close(); err != nil {
		t.Fatalf("close probe: %v", err)
	}
	return address
}

func startWith(t *testing.T, values map[string]string) (int, []map[string]any) {
	t.Helper()
	values[envMemoryLimit] = tinyLimit
	var stderr bytes.Buffer
	limits := &recordedLimit{}
	code := realMain(process{
		Args: []string{
			commandAll,
		},
		Lookup:         environment(values),
		ReadCgroup:     noCgroup,
		SetMemoryLimit: limits.setLimit,
		Stderr:         &stderr,
	})
	if diff := cmp.Diff([]int64{tinyLimitBytes}, limits.set); diff != "" {
		t.Errorf("memory limit set (-want +got):\n%s", diff)
	}
	return code, jsonLines(t, stderr.String())
}

func jsonLines(t *testing.T, output string) []map[string]any {
	t.Helper()
	var lines []map[string]any
	scanner := bufio.NewScanner(strings.NewReader(output))
	for scanner.Scan() {
		line := map[string]any{}
		if err := json.Unmarshal(scanner.Bytes(), &line); err != nil {
			t.Fatalf("stderr line is not JSON: %v\n%s", err, scanner.Text())
		}
		lines = append(lines, line)
	}
	return lines
}

type startFailure struct {
	Code         int
	MemoryEvent  string
	MemorySource string
	FailureEvent string
	NamesAddress bool
}

func failureOf(lines []map[string]any, address string) startFailure {
	var got startFailure
	for _, line := range lines {
		switch line["msg"] {
		case "config.memory_limit":
			got.MemoryEvent = "config.memory_limit"
			got.MemorySource, _ = line["source"].(string)
		case "lifecycle.start_failed":
			got.FailureEvent = "lifecycle.start_failed"
			reason, _ := line["error"].(string)
			got.NamesAddress = strings.Contains(reason, address)
		}
	}
	return got
}

func TestBusyPortFailsWithItsAddress(t *testing.T) {
	want := startFailure{
		Code:         exitFailure,
		MemoryEvent:  "config.memory_limit",
		MemorySource: string(sourceEnv),
		FailureEvent: "lifecycle.start_failed",
		NamesAddress: true,
	}
	tests := []struct {
		Name string
		Args func(t *testing.T) (map[string]string, string)
		Want startFailure
	}{
		{
			Name: "busy otlp port",
			Args: func(t *testing.T) (map[string]string, string) {
				busy := occupy(t)
				return map[string]string{
					envOTLPAddr: busy,
					envAPIAddr:  freeAddress(t),
				}, busy
			},
			Want: want,
		},
		{
			Name: "busy port with self-tracing leaves no exporter behind",
			Args: func(t *testing.T) (map[string]string, string) {
				busy := occupy(t)
				return map[string]string{
					envOTLPAddr:    busy,
					envAPIAddr:     freeAddress(t),
					envSelfTracing: freeAddress(t),
				}, busy
			},
			Want: want,
		},
		{
			Name: "busy api port closes the otlp listener",
			Args: func(t *testing.T) (map[string]string, string) {
				busy := occupy(t)
				return map[string]string{
					envOTLPAddr: freeAddress(t),
					envAPIAddr:  busy,
				}, busy
			},
			Want: want,
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			values, busy := tt.Args(t)
			code, lines := startWith(t, values)
			got := failureOf(lines, busy)
			got.Code = code
			if diff := cmp.Diff(tt.Want, got); diff != "" {
				t.Errorf("start (-want +got):\n%s", diff)
			}
			assertFree(t, values[envOTLPAddr], busy)
		})
	}
}

func assertFree(t *testing.T, address, busy string) {
	t.Helper()
	if address == busy {
		return
	}
	var config net.ListenConfig
	reopened, err := config.Listen(t.Context(), network, address)
	if err != nil {
		t.Fatalf("otlp address %s still held after a failed start: %v", address, err)
	}
	if err := reopened.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
		t.Errorf("close reopened: %v", err)
	}
}
