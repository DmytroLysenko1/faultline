package main

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
)

const reexecFlag = "FAULTLINE_TEST_REEXEC"

func TestMain(m *testing.M) {
	if os.Getenv(reexecFlag) != "" {
		main()
	}
	os.Exit(m.Run())
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
}

func TestRealMain(t *testing.T) {
	tests := []struct {
		Name       string
		Args       process
		Want       outcome
		WantStderr string
	}{
		{
			Name: "no subcommand prints usage",
			Args: process{
				Lookup: environment(map[string]string{}),
			},
			Want: outcome{
				Code: exitConfig,
			},
			WantStderr: "usage: faultline all\n",
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
				Code: exitConfig,
			},
			WantStderr: "usage: faultline all\n",
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
			},
			WantStderr: `FAULTLINE_GRAPH_MAX_NODES: invalid integer "abc" (not an unsigned decimal integer), ` +
				"want e.g. 10000\n",
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
				Code: exitConfig,
			},
			WantStderr: "GOMEMLIMIT=off disables the memory budget\n",
		},
		{
			Name: "over budget keeps the limit set and lists the contributions",
			Args: process{
				Args: []string{
					commandAll,
				},
				Lookup: environment(map[string]string{
					envMemoryLimit: "1280MiB",
				}),
			},
			Want: outcome{
				Code: exitConfig,
				Limits: []int64{
					formerMemoryLimit,
				},
			},
			WantStderr: "worst-case memory exceeds the budget: 1090.5 MiB > 85% of GOMEMLIMIT 1280.0 MiB " +
				"(otlp requests 440.0 MiB, api responses 343.3 MiB, pairing and graph 222.5 MiB, journal 64.2 MiB, " +
				"batch apply 14.7 MiB, snapshots 5.8 MiB)\n",
		},
		{
			Name: "valid config sets the memory limit and exits cleanly",
			Args: process{
				Args: []string{
					commandAll,
				},
				Lookup: environment(map[string]string{}),
			},
			Want: outcome{
				Code: exitOK,
				Limits: []int64{
					manifestMemoryLimit,
				},
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
			got := outcome{
				Code:   realMain(current),
				Limits: limits.set,
			}
			if diff := cmp.Diff(tt.Want, got); diff != "" {
				t.Errorf("realMain (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(tt.WantStderr, stderr.String()); diff != "" {
				t.Errorf("stderr (-want +got):\n%s", diff)
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
