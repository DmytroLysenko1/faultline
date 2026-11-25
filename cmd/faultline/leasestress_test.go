package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DmytroLysenko1/faultline/internal/application/processing"
)

var (
	errOrderMoved     = errors.New("checkpoint moved")
	errOrderRestore   = errors.New("restore broken")
	errOrderStop      = errors.New("stop failed")
	errOrderStopLease = fmt.Errorf("final stake: %w", processing.ErrLeaseLost)
)

const (
	startServes = iota
	startLosesTheStake
	startResumesOnCause
	startFailsRestore
	startModes
)

type orderPlan struct {
	unitExit   time.Duration
	leaseExit  time.Duration
	startMode  int
	startDelay time.Duration
	leaseStop  error
	topStop    error
	awaits     bool
	signal     time.Duration
}

type orderUnit struct {
	plan *orderPlan
	quit chan struct{}
}

func (u *orderUnit) start(_ context.Context, sup unitSupervisor) error {
	u.quit = make(chan struct{})
	sup.spawn("a/run", func() error { return exitAfter(u.plan.unitExit, u.quit, errA, nil) })
	return nil
}

func (u *orderUnit) stop(context.Context) error {
	close(u.quit)
	return nil
}

func exitAfter(delay time.Duration, quit <-chan struct{}, err error, fired *atomic.Bool) error {
	if delay < 0 {
		<-quit
		return nil
	}
	select {
	case <-time.After(delay):
		if fired != nil {
			fired.Store(true)
		}
		return err
	case <-quit:
		return nil
	}
}

type orderPartition struct {
	plan    *orderPlan
	quit    chan struct{}
	sup     unitSupervisor
	visible *atomic.Bool
}

func (u *orderPartition) start(ctx context.Context, sup unitSupervisor) error {
	u.sup, u.quit = sup, make(chan struct{})
	sup.spawn("p0/lease", func() error { return exitAfter(u.plan.leaseExit, u.quit, errLeaseGone, u.visible) })
	switch u.plan.startMode {
	case startLosesTheStake:
		u.pause(ctx)
		u.visible.Store(true)
		return errStakeLease
	case startResumesOnCause:
		<-ctx.Done()
		cause := context.Cause(ctx)
		if errors.Is(cause, processing.ErrLeaseLost) {
			u.visible.Store(true)
		}
		return &stageError{
			stage: "p0/resume",
			err:   errors.Join(errOrderMoved, cause),
		}
	case startFailsRestore:
		u.pause(ctx)
		return &stageError{
			stage: "p0/restore",
			err:   errOrderRestore,
		}
	}
	return nil
}

func (u *orderPartition) pause(ctx context.Context) {
	select {
	case <-time.After(u.plan.startDelay):
	case <-ctx.Done():
	}
}

func (u *orderPartition) stop(ctx context.Context) error {
	close(u.quit)
	_, err := u.sup.wait(ctx, "p0/lease")
	if errors.Is(u.plan.leaseStop, processing.ErrLeaseLost) {
		u.visible.Store(true)
	}
	return errors.Join(u.plan.leaseStop, err)
}

type orderTop struct{ plan *orderPlan }

func (u *orderTop) start(context.Context, unitSupervisor) error { return nil }

func (u *orderTop) stop(context.Context) error { return u.plan.topStop }

type orderAwait struct{}

func (orderAwait) start(ctx context.Context, _ unitSupervisor) error {
	<-ctx.Done()
	return &stageError{
		stage: "journal/await_streams",
		err:   fmt.Errorf("await streams: %w", context.Cause(ctx)),
	}
}

func (orderAwait) stop(context.Context) error { return nil }

func orderDelay(r *rand.Rand, never bool) time.Duration {
	if never && r.IntN(3) == 0 {
		return -1
	}
	return time.Duration(r.IntN(1500)) * time.Microsecond
}

func orderPlanOf(r *rand.Rand) *orderPlan {
	stops := []error{nil, errOrderStop, errOrderStopLease}
	return &orderPlan{
		unitExit:   orderDelay(r, true),
		leaseExit:  orderDelay(r, true),
		startMode:  r.IntN(startModes),
		startDelay: orderDelay(r, false),
		leaseStop:  stops[r.IntN(3)],
		topStop:    stops[r.IntN(2)],
		awaits:     r.IntN(2) == 0,
		signal:     orderDelay(r, false) + time.Duration(r.IntN(2))*time.Millisecond,
	}
}

func orderStack(plan *orderPlan, visible *atomic.Bool) *stack {
	layers := []layer{
		{
			name: "a",
			unit: &orderUnit{
				plan: plan,
			},
			components: []string{"a/run"},
			stopBudget: time.Second,
		},
		{name: "p0", unit: &orderPartition{
			plan:    plan,
			visible: visible,
		}, components: []string{"p0/lease"},
			stopBudget: time.Second},
		{
			name: "c",
			unit: &orderTop{
				plan: plan,
			},
			stopBudget: time.Second,
		},
	}
	if plan.awaits {
		layers = append(layers, layer{
			name:       "journal",
			unit:       orderAwait{},
			stopBudget: time.Second,
		})
	}
	return &stack{
		layers: layers,
	}
}

func orderProblem(code int, visible bool, logs *logBuffer, t *testing.T) string {
	problem := ""
	if errs := errorLines(t, logs); len(errs) != min(code, 1) {
		problem += fmt.Sprintf(" errors=%d code=%d", len(errs), code)
	}
	lines := logs.all(t, eventLeaseLost)
	if visible != (len(lines) == 1) || len(lines) > 1 {
		problem += fmt.Sprintf(" lease.lost=%d visible=%v", len(lines), visible)
	}
	for _, line := range lines {
		if line["partition"] != "p0" || line["stage"] == "" ||
			!strings.Contains(fmt.Sprint(line["error"]), processing.ErrLeaseLost.Error()) {
			problem += fmt.Sprintf(" attrs %v", line)
		}
	}
	return problem
}

func TestOneErrorAndOneLeaseLineInEveryOrder(t *testing.T) {
	forEachScheduler(t, func(t *testing.T) {
		const runs = 6000
		bad := 0
		for run := range runs {
			if problem := orderRun(t, run); problem != "" {
				bad++
				t.Logf("run %d:%s", run, problem)
			}
		}
		if bad > 0 {
			t.Errorf("%d of %d runs broke one Error per run or one lease.lost line", bad, runs)
		}
	})
}

func orderRun(t *testing.T, run int) string {
	plan := orderPlanOf(rand.New(rand.NewPCG(uint64(run), 15)))
	var visible atomic.Bool
	var signaller sync.WaitGroup
	logs := &logBuffer{}
	signal := newFakeNotify()
	signaller.Go(func() {
		time.Sleep(plan.signal)
		signal.send()
	})
	code := launchStack(t.Context(), signal.notify, orderStack(plan, &visible), newLifecycle(testLogger(logs)))
	signaller.Wait()
	problem := orderProblem(code, visible.Load(), logs, t)
	if problem == "" {
		return ""
	}
	return fmt.Sprintf(" %+v:%s\n%s", *plan, problem, logs.String())
}

func forEachScheduler(t *testing.T, check func(t *testing.T)) {
	for _, procs := range []int{runtime.GOMAXPROCS(0), 1} {
		t.Run(fmt.Sprintf("GOMAXPROCS=%d", procs), func(t *testing.T) {
			previous := runtime.GOMAXPROCS(procs)
			t.Cleanup(func() { runtime.GOMAXPROCS(previous) })
			check(t)
		})
	}
}

func TestTheFirstLeaseExitIsKept(t *testing.T) {
	logs := &logBuffer{}
	_, fail := context.WithCancelCause(t.Context())
	sup := newSupervisor(fail, testLogger(logs))
	checkpointLost := fmt.Errorf("sweep: %w", processing.ErrLeaseLost)
	sup.spawn("p0/lease", func() error { return errLeaseGone })
	<-sup.units["p0/lease"].done
	sup.spawn("p0/checkpoint", func() error { return checkpointLost })
	<-sup.units["p0/checkpoint"].done
	kept := sup.lostLease()
	exit, found := errors.AsType[*unitExitError](kept.err)
	if !found || exit.name != "p0/lease" || !kept.beforeSnapshot {
		t.Fatalf("kept %+v", kept)
	}
}
