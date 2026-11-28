package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"go.uber.org/goleak"
)

const (
	reexecFlag      = "TEST_REEXEC"
	stackFlag       = "TEST_STACK"
	stackSignal     = "signal"
	heldStopBudget  = time.Hour
	secondSignalCap = 10 * time.Second
)

func TestMain(m *testing.M) {
	if os.Getenv(reexecFlag) != "" {
		main()
	}
	if os.Getenv(stackFlag) == stackSignal {
		os.Exit(runSignalledStack())
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
	checkpoints := t.TempDir()
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
				Stderr: "usage: faultline all | ingester | processor | tokens init|issue|revoke|list\n",
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
				Stderr: "usage: faultline all | ingester | processor | tokens init|issue|revoke|list\n",
			},
		},
		{
			Name: "a command with extra arguments prints usage",
			Args: process{
				Args: []string{
					commandAll,
					"extra",
				},
				Lookup: environment(map[string]string{}),
			},
			Want: outcome{
				Code:   exitConfig,
				Stderr: "usage: faultline all | ingester | processor | tokens init|issue|revoke|list\n",
			},
		},
		{
			Name: "invalid variable names the variable",
			Args: process{
				Args: []string{
					commandAll,
				},
				Lookup: environment(map[string]string{
					envNATSURL:       testNATSURL,
					envCheckpointDir: checkpoints,
					envGraphMaxNodes: "abc",
				}),
			},
			Want: outcome{
				Code: exitConfig,
				Stderr: `GRAPH_MAX_NODES: invalid integer "abc" (not an unsigned decimal integer), ` +
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
					envNATSURL:       testNATSURL,
					envCheckpointDir: checkpoints,
					envMemoryLimit:   "off",
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
					envNATSURL:       testNATSURL,
					envCheckpointDir: checkpoints,
					envMemoryLimit:   "1GiB",
				}),
			},
			Want: outcome{
				Code: exitConfig,
				Limits: []int64{
					1 << 30,
				},
				Stderr: "worst-case memory exceeds the budget: 1340.1 MiB > 85% of GOMEMLIMIT 1024.0 MiB " +
					"(otlp requests 440.0 MiB, pairing, graph and traffic 402.1 MiB, api responses 256.0 MiB, " +
					"feed credit and pull 88.0 MiB, records before pubAck 60.0 MiB, batch apply 27.8 MiB, " +
					"snapshots 23.5 MiB, otlp connections 20.3 MiB, publish buffer 8.0 MiB, nats connections 7.0 MiB, " +
					"checkpoint 6.2 MiB, token directory 0.8 MiB, admission tokens 0.4 MiB)\n",
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
			current.Hostname = podHostname
			if tt.Want.Code == exitConfig {
				assertAllRefusedBeforeItsStack(t, current)
			}
			code := realMain(&current)
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

func TestTheUsageNamesEveryCommand(t *testing.T) {
	for name := range commands() {
		if !slices.Contains(strings.Fields(usage), name) {
			t.Errorf("usage %q does not name the command %q", usage, name)
		}
	}
}

type heldStop struct{}

func (heldStop) start(context.Context, unitSupervisor) error {
	return nil
}

func (heldStop) stop(ctx context.Context) error {
	<-ctx.Done()
	return nil
}

func runSignalledStack() int {
	role := &stack{
		layers: []layer{
			{
				name:       "held",
				unit:       heldStop{},
				stopBudget: heldStopBudget,
			},
		},
	}
	life := newLifecycle(slog.New(slog.NewJSONHandler(os.Stderr, nil)))
	return launchStack(context.Background(), signalNotify, role, life)
}

func TestASecondSignalIsLeftToTheSystem(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), secondSignalCap)
	defer cancel()
	command := exec.CommandContext(ctx, os.Args[0], "-test.run=^$")
	command.Env = append(os.Environ(), stackFlag+"="+stackSignal)
	stderr, err := command.StderrPipe()
	if err != nil {
		t.Fatalf("stderr pipe: %v", err)
	}
	if err := command.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	lines := bufio.NewScanner(stderr)
	awaitState(t, lines, stateServing)
	signalChild(t, command)
	awaitState(t, lines, stateDraining)
	signalChild(t, command)
	if _, err := io.Copy(io.Discard, stderr); err != nil {
		t.Errorf("drain stderr: %v", err)
	}
	waitErr := command.Wait()
	status, _ := command.ProcessState.Sys().(syscall.WaitStatus)
	if !status.Signaled() || status.Signal() != syscall.SIGTERM {
		t.Fatalf("child ended with %v (%v), want killed by the second SIGTERM", command.ProcessState, waitErr)
	}
}

func awaitState(t *testing.T, lines *bufio.Scanner, state lifecycleState) {
	t.Helper()
	want := `"state":"` + string(state) + `"`
	for lines.Scan() {
		if strings.Contains(lines.Text(), want) {
			return
		}
	}
	t.Fatalf("the child never logged %s: %v", want, lines.Err())
}

func signalChild(t *testing.T, command *exec.Cmd) {
	t.Helper()
	if err := command.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("SIGTERM: %v", err)
	}
}
