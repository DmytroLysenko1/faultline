package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/samber/lo"
)

var (
	errTestSignal = fmt.Errorf("test signal: %w", context.Canceled)
	errA          = errors.New("a failed")
	errB          = errors.New("b failed")
	errC          = errors.New("c failed")
	errX          = errors.New("x failed")
	errZ          = errors.New("z failed")
)

const (
	testBudget = time.Second
	levelError = "ERROR"
	levelWarn  = "WARN"
	levelInfo  = "INFO"
	settleTime = 10 * time.Millisecond
)

type fakeNotify struct {
	trigger chan struct{}
	once    sync.Once
	stops   atomic.Int32
}

func newFakeNotify() *fakeNotify {
	return &fakeNotify{
		trigger: make(chan struct{}),
	}
}

func (n *fakeNotify) notify(parent context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancelCause(parent)
	go func() {
		select {
		case <-n.trigger:
			cancel(errTestSignal)
		case <-ctx.Done():
		}
	}()
	return ctx, func() {
		n.stops.Add(1)
		cancel(context.Canceled)
	}
}

func (n *fakeNotify) send() {
	n.once.Do(func() {
		close(n.trigger)
	})
}

func alreadySignalled(parent context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancelCause(parent)
	cancel(errTestSignal)
	return ctx, func() {
		cancel(context.Canceled)
	}
}

type unitExit int

const (
	exitOnQuit unitExit = iota
	exitAtOnce
	exitOnRelease
)

type fakeUnit struct {
	name  string
	err   error
	exit  unitExit
	delay time.Duration
}

type startMode int

const (
	startReturns startMode = iota
	startIgnoresTheSignal
	startReturnsTheCause
)

type stopEntry struct {
	err       error
	remaining time.Duration
	at        time.Time
}

type fakeLayer struct {
	name         string
	components   []string
	budget       time.Duration
	units        []fakeUnit
	starting     startMode
	startDelay   time.Duration
	startErr     error
	onStart      func()
	onStop       func()
	stopErr      error
	stopHold     chan struct{}
	stopWaitsCtx bool
	waitFor      []string
	waitBudget   time.Duration
	events       *eventLog
	sup          unitSupervisor
	runs         atomic.Int32
	quit         chan struct{}
	quitOnce     sync.Once
	release      chan struct{}
	releaseOnce  sync.Once
	lock         sync.Mutex
	entry        stopEntry
}

func fakeStack(events *eventLog, layers ...*fakeLayer) *stack {
	role := &stack{}
	for _, current := range layers {
		role.layers = append(role.layers, current.layer(events))
	}
	return role
}

func (f *fakeLayer) layer(events *eventLog) layer {
	f.events = events
	f.quit = make(chan struct{})
	f.release = make(chan struct{})
	return layer{
		name:       f.name,
		unit:       f,
		components: f.declared(),
		stopBudget: f.stopBudget(),
	}
}

func (f *fakeLayer) declared() []string {
	if f.components != nil {
		return f.components
	}
	return lo.Map(f.units, func(current fakeUnit, _ int) string {
		return current.name
	})
}

func (f *fakeLayer) stopBudget() time.Duration {
	if f.budget == 0 {
		return testBudget
	}
	return f.budget
}

func (f *fakeLayer) start(ctx context.Context, sup unitSupervisor) error {
	f.events.add("start " + f.name)
	f.sup = sup
	for _, current := range f.units {
		sup.spawn(current.name, f.runOf(current))
	}
	if f.onStart != nil {
		f.onStart()
	}
	time.Sleep(f.startDelay)
	return f.startResult(ctx)
}

func (f *fakeLayer) startResult(ctx context.Context) error {
	if f.startErr != nil {
		return f.startErr
	}
	if f.starting == startReturns {
		return nil
	}
	<-ctx.Done()
	if f.starting == startIgnoresTheSignal {
		return nil
	}
	return fmt.Errorf("%s: %w", f.name, context.Cause(ctx))
}

func (f *fakeLayer) runOf(current fakeUnit) func() error {
	return func() error {
		f.runs.Add(1)
		f.awaitExit(current.exit)
		time.Sleep(current.delay)
		return current.err
	}
}

func (f *fakeLayer) awaitExit(exit unitExit) {
	switch exit {
	case exitOnQuit:
		<-f.quit
	case exitOnRelease:
		<-f.release
	case exitAtOnce:
	}
}

func (f *fakeLayer) free() {
	f.releaseOnce.Do(func() {
		close(f.release)
	})
}

func (f *fakeLayer) stop(ctx context.Context) error {
	f.events.add("stop " + f.name)
	f.noteEntry(ctx)
	f.quitOnce.Do(func() {
		close(f.quit)
	})
	if f.onStop != nil {
		f.onStop()
	}
	if f.stopHold != nil {
		<-f.stopHold
	}
	if f.stopWaitsCtx {
		<-ctx.Done()
	}
	return errors.Join(f.stopErr, f.awaitUnits(ctx))
}

func (f *fakeLayer) noteEntry(ctx context.Context) {
	deadline, _ := ctx.Deadline()
	f.lock.Lock()
	defer f.lock.Unlock()
	f.entry = stopEntry{
		err:       ctx.Err(),
		remaining: time.Until(deadline),
		at:        time.Now(),
	}
}

func (f *fakeLayer) entered() stopEntry {
	f.lock.Lock()
	defer f.lock.Unlock()
	return f.entry
}

func (f *fakeLayer) awaitUnits(ctx context.Context) error {
	var failures []error
	for _, name := range f.waitFor {
		failures = append(failures, f.awaitUnit(ctx, name))
	}
	return errors.Join(failures...)
}

func (f *fakeLayer) awaitUnit(ctx context.Context, name string) error {
	waitCtx, cancel := f.waitWindow(ctx)
	defer cancel()
	exited, err := f.sup.wait(waitCtx, name)
	if !exited {
		return errStepAbandoned
	}
	return err
}

func (f *fakeLayer) waitWindow(ctx context.Context) (context.Context, context.CancelFunc) {
	if f.waitBudget == 0 {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, f.waitBudget)
}

func launchRole(t *testing.T, role *stack, notify notifyFunc, logs *logBuffer) <-chan int {
	t.Helper()
	return launchRoleWith(t, role, notify, newLifecycle(testLogger(logs)))
}

func launchRoleWith(t *testing.T, role *stack, notify notifyFunc, life *lifecycle) <-chan int {
	t.Helper()
	done := make(chan int, 1)
	go func() {
		done <- launchStack(t.Context(), notify, role, life)
	}()
	return done
}

func stoppedCode(t *testing.T, logs *logBuffer) int {
	t.Helper()
	for _, record := range logs.all(t, eventState) {
		if record["state"] == string(stateStopped) {
			code, _ := record["code"].(float64)
			return int(code)
		}
	}
	t.Fatalf("no stopped line:\n%s", logs.String())
	return -1
}

func errorLines(t *testing.T, logs *logBuffer) []map[string]any {
	t.Helper()
	return lo.Filter(logs.records(t), func(record map[string]any, _ int) bool {
		return record["level"] == levelError
	})
}

func messages(records []map[string]any) []string {
	return lo.Map(records, func(record map[string]any, _ int) string {
		message, _ := record["msg"].(string)
		return message
	})
}

func picked(records []map[string]any, keys ...string) []map[string]any {
	return lo.Map(records, func(record map[string]any, _ int) map[string]any {
		return lo.PickByKeys(record, keys)
	})
}

func stopLines(t *testing.T, logs *logBuffer) []string {
	t.Helper()
	stops := lo.Filter(logs.records(t), func(record map[string]any, _ int) bool {
		return record["msg"] == eventLayerStopped || record["msg"] == eventLayerStopFailed
	})
	return lo.Map(stops, func(record map[string]any, _ int) string {
		return fmt.Sprintf("%s %s", strings.TrimPrefix(record["msg"].(string), "lifecycle."), record["layer"])
	})
}
