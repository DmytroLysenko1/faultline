package main

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"go.uber.org/goleak"
)

const reexecFlag = "FAULTLINE_TEST_REEXEC"

func TestMain(m *testing.M) {
	if os.Getenv(reexecFlag) != "" {
		main()
	}
	goleak.VerifyTestMain(m, goleak.IgnoreAnyFunction("os/signal.loop"))
}

type recordedLimit struct {
	set []int64
}

func (r *recordedLimit) setLimit(limit int64) int64 {
	r.set = append(r.set, limit)
	return limit
}

type outcome struct {
	Code   int
	Limits []int64
	Stderr string
}

func TestRealMain(t *testing.T) {
	tests := []struct {
		Name string
		Args process
		Want outcome
	}{
		{
			Name: "no subcommand prints usage",
			Args: process{
				Lookup: environment(map[string]string{}),
			},
			Want: outcome{
				Code:   exitConfig,
				Stderr: "usage: faultline all\n",
			},
		},
		{
			Name: "unknown subcommand prints usage",
			Args: process{
				Args: []string{
					"ingest",
				},
				Lookup: environment(map[string]string{}),
			},
			Want: outcome{
				Code:   exitConfig,
				Stderr: "usage: faultline all\n",
			},
		},
		{
			Name: "invalid variable names the variable",
			Args: process{
				Args: []string{
					commandAll,
				},
				Lookup: environment(map[string]string{
					envGraphMaxNodes: "abc",
				}),
			},
			Want: outcome{
				Code: exitConfig,
				Stderr: `FAULTLINE_GRAPH_MAX_NODES: invalid integer "abc" (not an unsigned decimal integer), ` +
					"want e.g. 10000\n",
			},
		},
		{
			Name: "memory limit off is refused before the budget",
			Args: process{
				Args: []string{
					commandAll,
				},
				Lookup: environment(map[string]string{
					envMemoryLimit: "off",
				}),
			},
			Want: outcome{
				Code:   exitConfig,
				Stderr: "GOMEMLIMIT=off disables the memory budget\n",
			},
		},
		{
			Name: "over budget keeps the limit set and lists the contributions",
			Args: process{
				Args: []string{
					commandAll,
				},
				Lookup: environment(map[string]string{
					envMemoryLimit: "1GiB",
				}),
			},
			Want: outcome{
				Code: exitConfig,
				Limits: []int64{
					1 << 30,
				},
				Stderr: "worst-case memory exceeds the budget: 1137.5 MiB > 85% of GOMEMLIMIT 1024.0 MiB " +
					"(otlp requests 440.0 MiB, api responses 345.5 MiB, pairing and graph 253.1 MiB, journal 64.4 MiB, " +
					"batch apply 27.8 MiB, snapshots 6.9 MiB)\n",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			var stderr bytes.Buffer
			limits := &recordedLimit{}
			current := tt.Args
			current.ReadCgroup = noCgroup
			current.SetMemoryLimit = limits.setLimit
			current.Stderr = &stderr
			code := realMain(current)
			got := outcome{
				Code:   code,
				Limits: limits.set,
				Stderr: stderr.String(),
			}
			if diff := cmp.Diff(tt.Want, got); diff != "" {
				t.Errorf("realMain (-want +got):\n%s", diff)
			}
		})
	}
}

func TestProcessExitCodeIsTwoForAnInvalidVariable(t *testing.T) {
	command := exec.CommandContext(t.Context(), os.Args[0], commandAll)
	command.Env = append(os.Environ(), reexecFlag+"=1", envGraphMaxNodes+"=abc")
	var stderr bytes.Buffer
	command.Stderr = &stderr
	err := command.Run()
	exit, isExit := errors.AsType[*exec.ExitError](err)
	if !isExit || exit.ExitCode() != exitConfig {
		t.Fatalf("process exit = %v, want code %d", err, exitConfig)
	}
	if !strings.Contains(stderr.String(), envGraphMaxNodes) {
		t.Errorf("stderr %q does not name %s", stderr.String(), envGraphMaxNodes)
	}
}
