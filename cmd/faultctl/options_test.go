package main

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"go.uber.org/goleak"

	"github.com/DmytroLysenko1/faultline/internal/interfaces/connectapi"
)

const (
	secretPassword = "s3cret"
	envServerValue = "http://from-env:9090"
	flagServerURL  = "http://from-flag:7070"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func environment(values map[string]string) lookupFunc {
	return func(name string) (string, bool) {
		value, set := values[name]
		return value, set
	}
}

type outcome struct {
	Code        int
	Stdout      string
	ShowsUsage  bool
	LeaksSecret bool
}

func runCLI(t *testing.T, args []string, env map[string]string) (outcome, string) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	code := realMain(process{
		Args:   args,
		Lookup: environment(env),
		Stdout: &stdout,
		Stderr: &stderr,
	})
	return outcome{
		Code:        code,
		Stdout:      stdout.String(),
		ShowsUsage:  strings.Contains(stderr.String(), usage),
		LeaksSecret: strings.Contains(stderr.String(), secretPassword),
	}, stderr.String()
}

func exportArgs(extra ...string) []string {
	return append([]string{commandGraph, commandExport, "-o", formatJSON}, extra...)
}

func TestUsageErrorsExitWithTwoBeforeAnyCall(t *testing.T) {
	credentials := "http://user:" + secretPassword + "@h"
	tests := []struct {
		Name    string
		Args    []string
		Env     map[string]string
		WantErr error
	}{
		{Name: "no arguments", WantErr: errCommand},
		{Name: "unknown command", Args: []string{"graph", "import"}, WantErr: errCommand},
		{Name: "extra argument", Args: exportArgs("more"), WantErr: errArgument},
		{Name: "no format", Args: []string{commandGraph, commandExport}, WantErr: errFormat},
		{Name: "unknown format", Args: []string{commandGraph, commandExport, "-o", "xml"}, WantErr: errFormat},
		{Name: "zero timeout", Args: exportArgs("--timeout", "0s"), WantErr: errTimeout},
		{Name: "timeout above the limit", Args: exportArgs("--timeout", "6m"), WantErr: errTimeout},
		{Name: "server with another scheme", Args: exportArgs("--server", "ftp://x"), WantErr: errServer},
		{Name: "server without a scheme", Args: exportArgs("--server", "localhost:8080"), WantErr: errServer},
		{Name: "server without a host", Args: exportArgs("--server", "http://"), WantErr: errServer},
		{Name: "server with a query", Args: exportArgs("--server", "http://h?x=1"), WantErr: errServer},
		{Name: "server with an empty query", Args: exportArgs("--server", "http://h?"), WantErr: errServer},
		{Name: "server with a fragment", Args: exportArgs("--server", "http://h#x"), WantErr: errServer},
		{Name: "server with credentials", Args: exportArgs("--server", credentials), WantErr: errCredentials},
		{Name: "credentials as a positional argument", Args: exportArgs(credentials), WantErr: errArgument},
		{
			Name:    "credentials without a scheme",
			Args:    exportArgs("--server", "user:"+secretPassword+"@h:8080"),
			WantErr: errServer,
		},
		{
			Name:    "credentials after one slash",
			Args:    exportArgs("--server", "http:/user:"+secretPassword+"@h"),
			WantErr: errServer,
		},
		{Name: "token in the query", Args: exportArgs("--server", "http://h/?token="+secretPassword), WantErr: errServer},
		{
			Name:    "credentials in the environment",
			Args:    exportArgs(),
			Env:     map[string]string{envServer: credentials},
			WantErr: errCredentials,
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			_, err := parseExport(tt.Args, environment(tt.Env), renderers())
			if !errors.Is(err, tt.WantErr) {
				t.Fatalf("parseExport = %v, want %v", err, tt.WantErr)
			}
			got, stderr := runCLI(t, tt.Args, tt.Env)
			want := outcome{
				Code:       exitUsage,
				ShowsUsage: true,
			}
			if diff := cmp.Diff(want, got); diff != "" {
				t.Errorf("faultctl %v (-want +got):\n%s\nstderr:\n%s", tt.Args, diff, stderr)
			}
		})
	}
}

func TestUnparsableTimeoutIsAUsageError(t *testing.T) {
	got, stderr := runCLI(t, exportArgs("--timeout", "abc"), nil)
	want := outcome{
		Code:       exitUsage,
		ShowsUsage: true,
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("faultctl --timeout abc (-want +got):\n%s\nstderr:\n%s", diff, stderr)
	}
}

func TestServerAndTimeoutDefaults(t *testing.T) {
	type want struct {
		Server  string
		Timeout time.Duration
	}
	tests := []struct {
		Name string
		Args map[string]string
		Flag []string
		Want want
	}{
		{
			Name: "built-in defaults",
			Want: want{Server: defaultServer, Timeout: defaultTimeout},
		},
		{
			Name: "server from the environment",
			Args: map[string]string{envServer: envServerValue},
			Want: want{Server: envServerValue, Timeout: defaultTimeout},
		},
		{
			Name: "empty environment value falls back to the default",
			Args: map[string]string{envServer: ""},
			Want: want{Server: defaultServer, Timeout: defaultTimeout},
		},
		{
			Name: "longest allowed timeout",
			Flag: []string{"--timeout", "5m"},
			Want: want{Server: defaultServer, Timeout: maxTimeout},
		},
		{
			Name: "flag wins over the environment",
			Args: map[string]string{envServer: envServerValue},
			Flag: []string{"--server", flagServerURL, "--timeout", "30s"},
			Want: want{Server: flagServerURL, Timeout: 30 * time.Second},
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			options, err := parseExport(exportArgs(tt.Flag...), environment(tt.Args), renderers())
			if err != nil {
				t.Fatalf("parseExport: %v", err)
			}
			got := want{Server: options.Server, Timeout: options.Timeout}
			if diff := cmp.Diff(tt.Want, got); diff != "" {
				t.Errorf("options (-want +got):\n%s", diff)
			}
		})
	}
}

func TestClientReadsEverythingTheServerMaySend(t *testing.T) {
	if readMaxBytes < connectapi.SendMaxBytes {
		t.Errorf("readMaxBytes = %d, below the server's SendMaxBytes %d", readMaxBytes, connectapi.SendMaxBytes)
	}
}

func TestHelpPrintsUsageAndSucceeds(t *testing.T) {
	for _, flagName := range []string{"-h", "--help"} {
		t.Run(flagName, func(t *testing.T) {
			got, stderr := runCLI(t, []string{commandGraph, commandExport, flagName}, nil)
			want := outcome{
				Code:   exitOK,
				Stdout: usage + "\n",
			}
			if diff := cmp.Diff(want, got); diff != "" {
				t.Errorf("faultctl %s (-want +got):\n%s\nstderr:\n%s", flagName, diff, stderr)
			}
		})
	}
}
