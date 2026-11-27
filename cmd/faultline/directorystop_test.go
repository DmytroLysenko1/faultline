package main

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/natsjstest"
)

const (
	tokenStreamInfo = "$JS.API.STREAM.INFO." + tokenStream
	runExitWithin   = 30 * time.Millisecond
	runExitNever    = time.Hour
)

type exitReportingSupervisor struct {
	exited chan struct{}
}

func (s *exitReportingSupervisor) spawn(string, func() error) {}

func (s *exitReportingSupervisor) wait(ctx context.Context, _ string) (bool, error) {
	select {
	case <-s.exited:
		return true, nil
	case <-ctx.Done():
		return false, nil
	}
}

type stopOutcome struct {
	Err              error
	ReturnedAfterRun bool
}

func TestTheDirectoryStopWaitsForItsRunWithinTheGrace(t *testing.T) {
	tests := []struct {
		name string
		args time.Duration
		want stopOutcome
	}{
		{
			name: "a run that ends soon after the cancel",
			args: runExitWithin,
			want: stopOutcome{
				ReturnedAfterRun: true,
			},
		},
		{
			name: "a run that outlives the grace",
			args: runExitNever,
			want: stopOutcome{
				Err: errStepAbandoned,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				runCtx, cancel := context.WithCancel(t.Context())
				sup := &exitReportingSupervisor{
					exited: make(chan struct{}),
				}
				var ran atomic.Bool
				go func() {
					<-runCtx.Done()
					timer := time.NewTimer(tt.args)
					defer timer.Stop()
					select {
					case <-timer.C:
						ran.Store(true)
						close(sup.exited)
					case <-t.Context().Done():
					}
				}()
				unit := &directoryUnit{
					sup:    sup,
					cancel: cancel,
				}
				err := unit.stop(t.Context())
				got := stopOutcome{
					Err:              err,
					ReturnedAfterRun: ran.Load(),
				}
				if !errors.Is(got.Err, tt.want.Err) || got.ReturnedAfterRun != tt.want.ReturnedAfterRun {
					t.Errorf("stop = %+v, want %+v", got, tt.want)
				}
			})
		})
	}
}

func TestASignalWhileTheTokenDirectoryOpensExitsZero(t *testing.T) {
	cluster := natsjstest.Start(t, singleNATSNode)
	ensureStreams(t, cluster.URLs()...)
	grantTestToken(t, defaultTenant, cluster.URLs()...)
	proxy := natsjstest.StartProxy(t, cluster.URLs()[0])
	verdict, held := natsjstest.Hold()
	t.Cleanup(held.Release)
	var seen atomic.Bool
	proxy.Intercept(func(f *natsjstest.Frame) natsjstest.Verdict {
		if f.Subject == tokenStreamInfo && seen.CompareAndSwap(false, true) {
			return verdict
		}
		return natsjstest.Forward()
	})
	logs := &logBuffer{}
	setup, _, _ := ingesterOn(t, []string{proxy.URL()})
	notify, done, _ := launchIngester(t, setup, logs)
	eventually(t, "the held token stream info", seen.Load)

	notify.send()

	assertCode(t, <-done, exitOK, logs)
	assertDiff(t, "canceled stage", stageAwaitTokens, logs.event(t, eventStartCanceled)["stage"])
	assertNoLine(t, logs, eventStartFailed)
	assertNoLine(t, logs, eventTokenWaitPrefix+reasonJetStreamUnavailable)
}
