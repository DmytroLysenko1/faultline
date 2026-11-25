package main

import (
	"context"
	"fmt"
	"testing"
	"testing/synctest"
	"time"

	"github.com/DmytroLysenko1/faultline/internal/application/processing"
)

var errLeaseGone = fmt.Errorf("renew: %w", processing.ErrLeaseLost)

type leaseUnit struct {
	trigger chan struct{}
	quit    chan struct{}
	waits   bool
	sup     unitSupervisor
}

func (u *leaseUnit) start(_ context.Context, sup unitSupervisor) error {
	u.sup, u.quit = sup, make(chan struct{})
	sup.spawn("p0/lease", func() error {
		select {
		case <-u.trigger:
		case <-u.quit:
		}
		return errLeaseGone
	})
	return nil
}

func (u *leaseUnit) stop(ctx context.Context) error {
	close(u.quit)
	if !u.waits {
		return nil
	}
	_, err := u.sup.wait(ctx, "p0/lease")
	return err
}

type triggersThenFails struct {
	trigger  chan struct{}
	startErr error
	stopErr  error
}

func (u *triggersThenFails) start(context.Context, unitSupervisor) error { return u.startErr }

func (u *triggersThenFails) stop(context.Context) error {
	if u.trigger != nil {
		close(u.trigger)
		time.Sleep(10 * time.Millisecond)
	}
	return u.stopErr
}

func leaseStack(lease *leaseUnit, top *triggersThenFails) *stack {
	return &stack{layers: []layer{
		{
			name:       "p0",
			unit:       lease,
			components: []string{"p0/lease"},
			stopBudget: time.Second,
		},
		{
			name:       "c",
			unit:       top,
			stopBudget: time.Second,
		},
	}}
}

func TestStartFailedThenALateLeaseLossIsAFollowup(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		logs, signal := &logBuffer{}, newFakeNotify()
		trigger := make(chan struct{})
		top := &triggersThenFails{
			trigger: trigger,
			startErr: &stageError{
				stage: "c/step",
				err:   errB,
			},
		}
		role := leaseStack(&leaseUnit{
			trigger: trigger,
		}, top)
		if code := <-launchRole(t, role, signal.notify, logs); code != 1 {
			t.Fatalf("code %d", code)
		}
		found := errorLines(t, logs)
		if len(found) != 1 || found[0]["msg"] != eventStartFailed {
			t.Fatalf("errors %v\n%s", found, logs.String())
		}
		checkLeaseLine(t, logs, "WARN", true)
	})
}

func TestARuleTwoExitThenALateLeaseLossIsAFollowup(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		logs, signal := &logBuffer{}, newFakeNotify()
		trigger := make(chan struct{})
		role := leaseStack(&leaseUnit{
			trigger: trigger,
		}, &triggersThenFails{
			trigger: trigger,
		})
		unit := &exitsOnTrigger{
			trigger: make(chan struct{}),
		}
		close(unit.trigger)
		role.layers = append(role.layers, layer{
			name:       "a",
			unit:       unit,
			components: []string{"a/run"},
			stopBudget: time.Second,
		})
		if code := <-launchRole(t, role, signal.notify, logs); code != 1 {
			t.Fatalf("code %d", code)
		}
		found := errorLines(t, logs)
		if len(found) != 1 || found[0]["msg"] != eventComponentExit || found[0]["component"] != "a/run" {
			t.Fatalf("errors %v\n%s", found, logs.String())
		}
		checkLeaseLine(t, logs, "WARN", true)
	})
}

func TestADrainingRunWithALateLeaseLossLogsOneError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		logs, signal := &logBuffer{}, newFakeNotify()
		trigger := make(chan struct{})
		role := leaseStack(&leaseUnit{
			trigger: trigger,
		}, &triggersThenFails{
			trigger: trigger,
			stopErr: errC,
		})
		done := launchRole(t, role, signal.notify, logs)
		synctest.Wait()
		signal.send()
		if code := <-done; code != 1 {
			t.Fatalf("code %d", code)
		}
		found := errorLines(t, logs)
		if len(found) != 1 || found[0]["msg"] != eventLeaseLost {
			t.Fatalf("errors %v\n%s", found, logs.String())
		}
		failed := logs.event(t, eventStopFailed)
		if failed["level"] != "WARN" || failed["followup"] != true {
			t.Fatalf("stop_failed %v", failed)
		}
		checkLeaseLine(t, logs, "ERROR", false)
	})
}

func TestALeaseLossAndAStopFailureLogOneError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		logs, signal := &logBuffer{}, newFakeNotify()
		role := leaseStack(&leaseUnit{
			waits: true,
		}, &triggersThenFails{
			stopErr: errC,
		})
		done := launchRole(t, role, signal.notify, logs)
		synctest.Wait()
		signal.send()
		if code := <-done; code != 1 {
			t.Fatalf("code %d", code)
		}
		found := errorLines(t, logs)
		if len(found) != 1 || found[0]["msg"] != eventLeaseLost {
			t.Fatalf("errors %v\n%s", found, logs.String())
		}
		failed := logs.event(t, eventStopFailed)
		if failed["level"] != "WARN" || failed["followup"] != true {
			t.Fatalf("stop_failed %v", failed)
		}
		checkLeaseLine(t, logs, "ERROR", false)
		if got := logs.event(t, eventLeaseLost)["error"]; got != "p0: "+errLeaseGone.Error() {
			t.Fatalf("lease.lost error %v", got)
		}
	})
}
