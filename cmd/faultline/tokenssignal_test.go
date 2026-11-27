package main

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/natsjstest"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/tokens"
)

const (
	tokenPutPrefix   = "$KV." + tokens.Bucket + "."
	signalSettles    = 100 * time.Millisecond
	showBlocks       = 300 * time.Millisecond
	commandExitLimit = 10 * time.Second
	pipeChunk        = 64 << 10
)

type issueChild struct {
	command *exec.Cmd
	stderr  bytes.Buffer
	exited  chan error
}

type issueExit struct {
	Signaled bool
	Code     int
	Grants   int
	Hinted   bool
	Revoked  bool
}

func startIssue(t *testing.T, url string, stdout *os.File) *issueChild {
	t.Helper()
	child := &issueChild{
		exited: make(chan error, 1),
	}
	child.command = exec.CommandContext(t.Context(), os.Args[0], commandTokens, actionIssue, "--stdout")
	child.command.Env = append(os.Environ(), reexecFlag+"=1", envNATSURL+"="+url, envInstance+"="+testInstance)
	child.command.Stdout = stdout
	child.command.Stderr = &child.stderr
	if err := child.command.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	go func() {
		child.exited <- child.command.Wait()
	}()
	return child
}

func (c *issueChild) signal(t *testing.T) {
	t.Helper()
	if err := c.command.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("signal: %v", err)
	}
}

func (c *issueChild) exit(t *testing.T, url string) issueExit {
	t.Helper()
	select {
	case waitErr := <-c.exited:
		t.Logf("wait: %v", waitErr)
	case <-time.After(commandExitLimit):
		if err := c.command.Process.Kill(); err != nil {
			t.Errorf("kill: %v", err)
		}
		t.Logf("wait after the kill: %v", <-c.exited)
		t.Fatalf("the command kept running %s after its signal; stderr %s", commandExitLimit, c.stderr.String())
	}
	status, _ := c.command.ProcessState.Sys().(syscall.WaitStatus)
	stderr := c.stderr.String()
	id := strings.Fields(strings.SplitN(stderr, "\n", 2)[0] + " _ _ _ _")[3]
	return issueExit{
		Signaled: status.Signaled(),
		Code:     c.command.ProcessState.ExitCode(),
		Grants:   len(bucketGrants(t, url)),
		Hinted:   strings.Contains(stderr, "run faultline tokens revoke "+id+", then faultline tokens list"),
		Revoked:  strings.Contains(stderr, shownAndRevoked),
	}
}

func heldCompensation(t *testing.T, proxy *natsjstest.Proxy) (*atomic.Bool, *atomic.Bool, *natsjstest.HeldFrame) {
	t.Helper()
	purgeVerdict, purgeHeld := natsjstest.Hold()
	t.Cleanup(purgeHeld.Release)
	var registered, purging atomic.Bool
	proxy.Intercept(func(f *natsjstest.Frame) natsjstest.Verdict {
		if strings.HasPrefix(f.Subject, tokenPutPrefix) && registered.CompareAndSwap(false, true) {
			return natsjstest.LoseReply()
		}
		if strings.HasPrefix(f.Subject, tokenPurgePrefix) && purging.CompareAndSwap(false, true) {
			return purgeVerdict
		}
		return natsjstest.Forward()
	})
	return &registered, &purging, purgeHeld
}

func TestASignalDuringTheCompensationNeverLeavesTheTokenLive(t *testing.T) {
	tests := []struct {
		name string
		args int
		want issueExit
	}{
		{
			name: "one signal",
			args: 1,
			want: issueExit{
				Code:   exitFailure,
				Hinted: true,
			},
		},
		{
			name: "two signals",
			args: 2,
			want: issueExit{
				Code:   exitFailure,
				Hinted: true,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cluster := natsjstest.Start(t, singleNATSNode)
			proxy := natsjstest.StartProxy(t, cluster.URLs()[0])
			registered, purging, purge := heldCompensation(t, proxy)
			child := startIssue(t, proxy.URL(), capturedStdout(t))

			eventually(t, "the register sent", registered.Load)
			child.signal(t)
			eventually(t, "the compensation sent", purging.Load)
			for range tt.args - 1 {
				child.signal(t)
			}
			time.Sleep(signalSettles)
			purge.Release()

			assertDiff(t, "exit, grants left and hint", tt.want, child.exit(t, cluster.URLs()[0]))
		})
	}
}

type stalledStdout struct {
	writer      int
	stdout      *os.File
	closeReader func() error
}

func pipeDescriptors() ([]int, error) {
	descriptors := make([]int, 2)
	return descriptors, syscall.Pipe(descriptors)
}

func socketDescriptors() ([]int, error) {
	descriptors, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
	return descriptors[:], err
}

func stalledOutput(t *testing.T, open func() ([]int, error)) *stalledStdout {
	t.Helper()
	descriptors, err := open()
	if err != nil {
		t.Fatalf("open the descriptors: %v", err)
	}
	reader := descriptorFile(t, descriptors[0], "reader")
	output := &stalledStdout{
		writer:      descriptors[1],
		stdout:      descriptorFile(t, descriptors[1], "stdout"),
		closeReader: sync.OnceValue(reader.Close),
	}
	t.Cleanup(func() {
		if err := errors.Join(output.closeReader(), output.stdout.Close()); err != nil {
			t.Errorf("close the descriptors: %v", err)
		}
	})
	fillDescriptor(t, descriptors[1])
	return output
}

func descriptorFile(t *testing.T, descriptor int, name string) *os.File {
	t.Helper()
	file := os.NewFile(uintptr(descriptor), name)
	if file == nil {
		t.Fatalf("descriptor %d is not a valid file for %s", descriptor, name)
	}
	return file
}

func fillDescriptor(t *testing.T, descriptor int) {
	t.Helper()
	if err := syscall.SetNonblock(descriptor, true); err != nil {
		t.Fatalf("SetNonblock: %v", err)
	}
	chunk := make([]byte, pipeChunk)
	for {
		_, err := syscall.Write(descriptor, chunk)
		if errors.Is(err, syscall.EAGAIN) {
			break
		}
		if err != nil {
			t.Fatalf("fill the descriptor: %v", err)
		}
	}
	if err := syscall.SetNonblock(descriptor, false); err != nil {
		t.Fatalf("SetNonblock: %v", err)
	}
}

func nonblocking(t *testing.T, descriptor int) bool {
	t.Helper()
	flags, _, errno := syscall.Syscall(syscall.SYS_FCNTL, uintptr(descriptor), syscall.F_GETFL, 0)
	if errno != 0 {
		t.Fatalf("F_GETFL: %v", errno)
	}
	return flags&syscall.O_NONBLOCK != 0
}

func TestASignalEndsAShowIntoAStalledPipe(t *testing.T) {
	tests := []struct {
		name string
		args func() ([]int, error)
		want issueExit
	}{
		{
			name: "a pipe",
			args: pipeDescriptors,
			want: issueExit{
				Code:    exitFailure,
				Revoked: true,
			},
		},
		{
			name: "a socket",
			args: socketDescriptors,
			want: issueExit{
				Code:    exitFailure,
				Revoked: true,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cluster := natsjstest.Start(t, singleNATSNode)
			url := cluster.URLs()[0]
			createTokenBucket(t, url)
			child := startIssue(t, url, stalledOutput(t, tt.args).stdout)

			eventually(t, "the grant stored", func() bool {
				return len(bucketGrants(t, url)) == 1
			})
			time.Sleep(showBlocks)
			child.signal(t)

			assertDiff(t, "exit, grants left and the revoked show", tt.want, child.exit(t, url))
		})
	}
}
