package checkpoint

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/application/processing"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/natsjstest"
)

const (
	staleBytes          = 8 << 10
	smallBytes          = 9
	overWorstImage      = 1 << 10
	firstChunkID        = ".0"
	boundAttempts       = 3
	boundTolerance      = time.Second
	chunkWindow         = 28 * time.Second
	unavailableAnswer   = `{"error":{"code":503,"err_code":10008,"description":"JetStream system temporarily unavailable"}}`
	accountLimitsAnswer = `{"error":{"code":400,"err_code":10002,"description":"resource limits exceeded for account"}}`
	resourcesAnswer     = `{"error":{"code":503,"err_code":10047,"description":"insufficient storage resources available"}}`
	stopAtTheFits       = 3 * time.Second
	keptObjects         = 4
	heldPosition        = 5
	closeTheSweep       = 3 * time.Second
	staleLeft           = 1
	shortfall           = staleBytes
)

type stepClock struct {
	mu sync.Mutex
	at time.Time
}

func (c *stepClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.at
}

func (c *stepClock) set(at time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.at = at
}

func (w *world) used(t *testing.T) uint64 {
	t.Helper()
	info, err := w.objectStream(t).Info(t.Context())
	if err != nil {
		t.Fatalf("bucket info: %v", err)
	}
	return info.State.Bytes
}

func (w *world) lastSequence(t *testing.T) uint64 {
	t.Helper()
	info, err := w.objectStream(t).Info(t.Context())
	if err != nil {
		t.Fatalf("bucket info: %v", err)
	}
	return info.State.LastSeq
}

func (w *world) orphan(t *testing.T, nuid string) (string, time.Time) {
	t.Helper()
	subject := w.store.chunkSubject(nuid)
	if _, err := w.operator.Publish(t.Context(), subject, imageOf(smallBytes)); err != nil {
		t.Fatalf("publish orphan: %v", err)
	}
	return subject, w.lastTime(t, subject)
}

func (w *world) tightFor(t *testing.T, image []byte) func() {
	t.Helper()
	return func() {
		w.store.options.MaxBytes = signed(w.used(t)) + int64(len(image)) - 1
	}
}

func (w *world) clocked(clock *stepClock) {
	w.store.now = clock.now
}

func cycled(count uint64) func(stats *Stats) bool {
	return func(stats *Stats) bool {
		return stats.Cycles == count
	}
}

func TestASpaceSweepTakesOnlyWhatNoHolderCanStillNeed(t *testing.T) {
	w := startWorld(t)
	stale := w.putObject(t, testEpoch-1, firstPosition, imageOf(staleBytes))
	own := w.putObject(t, testEpoch, secondPosition, imageOf(smallBytes))
	later := w.putObject(t, testEpoch+1, thirdPosition, imageOf(smallBytes))
	if _, err := w.objects.PutBytes(t.Context(), runbookName, imageOf(smallBytes)); err != nil {
		t.Fatalf("put %s: %v", runbookName, err)
	}
	old, oldAt := w.orphan(t, "OLD")
	young, youngAt := w.orphan(t, "YOUNG")
	if !youngAt.After(oldAt.Add(time.Nanosecond)) {
		t.Fatalf("the young orphan at %s is not later than the old one at %s", youngAt, oldAt)
	}
	held := w.putObject(t, olderEpoch, heldPosition, imageOf(staleBytes))
	w.clocked(&stepClock{at: oldAt.Add(w.options.orphanAge() + time.Nanosecond)})
	image := checkpointImage(t)
	tight := w.tightFor(t, image)
	sink, retention := w.startedAfter(t, func() {
		tight()
		w.setPointer(t, &pointerValue{Version: pointerVersion, Epoch: testEpoch,
			StreamCreated: w.streamCreated(t).Format(createdLayout), Current: held})
	})

	w.spoolAndSettleImage(t, sink, fourthPosition, image)

	subjects := w.subjects(t)
	for _, gone := range []string{w.metaSubject(stale.Name), w.store.chunkSubject(stale.NUID), old} {
		if slices.Contains(subjects, gone) {
			t.Errorf("%s was not swept", gone)
		}
	}
	kept := []string{w.metaSubject(own.Name), w.metaSubject(later.Name), w.metaSubject(runbookName), young,
		w.store.chunkSubject(own.NUID), w.store.chunkSubject(later.NUID), w.metaSubject(held.Name),
		w.store.chunkSubject(held.NUID)}
	for _, subject := range kept {
		if !slices.Contains(subjects, subject) {
			t.Errorf("%s was swept", subject)
		}
	}
	w.assertSwept(t, &sweptCounts{Sweeps: 1, Objects: 1, Orphans: 1, Bytes: stale.Size})
	w.assertInvariants(t, retention, keptObjects)
}

func (w *world) spoolAndSettleImage(t *testing.T, sink *Sink, position uint64, image []byte) {
	t.Helper()
	if err := spoolCheckpoint(t, sink, position, image); err != nil {
		t.Fatalf("spool: %v", err)
	}
	w.settled(t, committedThrough(position))
}

type sweptCounts struct {
	Sweeps   uint64
	Objects  uint64
	Orphans  uint64
	Bytes    uint64
	Failures uint64
	Full     uint64
}

func (w *world) assertSwept(t *testing.T, want *sweptCounts) {
	t.Helper()
	stats := w.store.Stats()
	got := &sweptCounts{Sweeps: stats.SpaceSweeps, Objects: stats.SweptObjects, Orphans: stats.SweptOrphans,
		Bytes: stats.SweptBytes, Failures: stats.SweepFailures, Full: stats.Skipped["bucket_full"]}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("sweep counts (-want +got):\n%s", diff)
	}
}

func TestABucketThatStaysFullPausesTheSweepUntilTheOldestOrphanMatures(t *testing.T) {
	w := startWorld(t)
	older, olderAt := w.orphan(t, "OLDER")
	younger, youngerAt := w.orphan(t, "YOUNGER")
	if !youngerAt.After(olderAt.Add(time.Nanosecond)) {
		t.Fatalf("the younger orphan at %s is not later than the older one at %s", youngerAt, olderAt)
	}
	clock := &stepClock{at: youngerAt}
	w.clocked(clock)
	image := checkpointImage(t)
	sink, retention := w.startedAfter(t, w.tightFor(t, image))
	mature := olderAt.Add(w.options.orphanAge())

	w.spoolAndCycle(t, sink, firstPosition, image, 1)
	w.assertSwept(t, &sweptCounts{Sweeps: 1, Full: 1})
	if diff := cmp.Diff([]string{"OLDER", "YOUNGER"}, w.ownObjects(t)); diff != "" {
		t.Errorf("chunks after a skipped upload (-want +got):\n%s", diff)
	}
	clock.set(mature)
	w.spoolAndCycle(t, sink, secondPosition, image, 2)
	w.assertSwept(t, &sweptCounts{Sweeps: 1, Full: 2})
	clock.set(mature.Add(time.Nanosecond))
	w.spoolAndSettleImage(t, sink, thirdPosition, image)

	w.assertSwept(t, &sweptCounts{Sweeps: 2, Orphans: 1, Full: 2})
	subjects := w.subjects(t)
	if slices.Contains(subjects, older) || !slices.Contains(subjects, younger) {
		t.Errorf("subjects %v, want %s swept and %s kept", subjects, older, younger)
	}
	w.assertInvariants(t, retention, 1)
}

func (w *world) spoolAndCycle(t *testing.T, sink *Sink, position uint64, image []byte, cycles uint64) {
	t.Helper()
	if err := spoolCheckpoint(t, sink, position, image); err != nil {
		t.Fatalf("spool: %v", err)
	}
	w.settled(t, cycled(cycles))
}

func TestAFailedSpaceSweepIsTriedAgainByTheNextCycle(t *testing.T) {
	w := startWorld(t)
	stale := w.putObject(t, testEpoch-1, firstPosition, imageOf(staleBytes))
	image := checkpointImage(t)
	tight := w.tightFor(t, image)
	sink, retention := w.startedAfter(t, func() {
		tight()
		w.once(isInfoLookup, natsjstest.Answer([]byte(unavailableAnswer)))
	})

	w.spoolAndCycle(t, sink, secondPosition, image, 1)
	w.assertSwept(t, &sweptCounts{Sweeps: 1, Failures: 1, Full: 1})
	w.spoolAndSettleImage(t, sink, thirdPosition, image)

	w.assertSwept(t, &sweptCounts{Sweeps: 2, Objects: 1, Bytes: stale.Size, Failures: 1, Full: 1})
	w.assertInvariants(t, retention, 1)
}

func TestEachRefusalOfSpaceHasItsClassAndOnlyAFullBucketSweepsTheNextCycleOnce(t *testing.T) {
	tests := []struct {
		name string
		args string
		want sweptClass
	}{
		{name: "maximum bytes exceeded", args: storeFullAnswer, want: sweptClass{Class: "bucket_full", Sweeps: 1}},
		{name: "account limits", args: accountLimitsAnswer, want: sweptClass{Class: "account_full"}},
		{name: "insufficient resources", args: resourcesAnswer, want: sweptClass{Class: "insufficient_resources"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := startWorld(t)
			sink, retention := w.started(t)
			w.once(isChunk, natsjstest.Answer([]byte(tt.args)))
			image := checkpointImage(t)

			w.spoolAndCycle(t, sink, firstPosition, image, 1)
			w.spoolAndSettleImage(t, sink, secondPosition, image)
			w.spoolAndSettleImage(t, sink, thirdPosition, image)

			stats := w.store.Stats()
			got := sweptClass{Class: classOf(stats.UploadFailed), Sweeps: stats.SpaceSweeps}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("(-want +got):\n%s", diff)
			}
			w.assertInvariants(t, retention, 0)
		})
	}
}

type sweptClass struct {
	Class  string
	Sweeps uint64
}

func TestAnImageOverTheWorstIsCountedAndStillUploaded(t *testing.T) {
	options := testOptions()
	options.WorstImage = overWorstImage
	w := startWorldWith(t, options, dedupWindow)
	sink, retention := w.started(t)

	image := checkpointImage(t)
	w.spoolAndSettleImage(t, sink, firstPosition, image)

	if got := w.store.Stats(); got.ImageOverWorst != 1 || got.LastImageBytes != int64(len(image)) {
		t.Errorf("images over the worst %d, last image %d bytes; want 1 and %d", got.ImageOverWorst,
			got.LastImageBytes, len(image))
	}
	w.assertInvariants(t, retention, 0)
}

type attempts struct {
	mu    sync.Mutex
	times []time.Time
}

func (a *attempts) add() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.times = append(a.times, time.Now())
}

func (a *attempts) seen() []time.Time {
	a.mu.Lock()
	defer a.mu.Unlock()
	return slices.Clone(a.times)
}

func (w *world) loseEvery(match func(f *natsjstest.Frame) bool) *attempts {
	lost := &attempts{}
	w.proxy.Intercept(func(f *natsjstest.Frame) natsjstest.Verdict {
		if !match(f) {
			return natsjstest.Forward()
		}
		lost.add()
		return natsjstest.LoseReply()
	})
	return lost
}

func isFirstChunk(f *natsjstest.Frame) bool {
	return isChunk(f) && strings.HasSuffix(msgIDOf(f.Payload), firstChunkID)
}

func TestTheRetryBoundEndsAChunkWithoutAnAnswerInsideTheWindow(t *testing.T) {
	options := testOptions()
	options.WorstImage = boundWorst
	options.UploadRate = boundRate
	w := startWorldWith(t, options, chunkWindow)
	sink, retention := w.started(t)
	lost := w.loseEvery(isFirstChunk)
	before := w.lastSequence(t)

	if err := spoolCheckpoint(t, sink, firstPosition, imageOf(boundWorst)); err != nil {
		t.Fatalf("spool: %v", err)
	}
	stats := w.settled(t, failedFor("upload"))
	ended := time.Now()

	sent := lost.seen()
	bound := retryWithin(w.store.buckets.window, &w.store.options)
	if len(sent) < boundAttempts || ended.Sub(sent[0]) > bound+boundTolerance {
		t.Errorf("%d attempts ending %s after the first, want at least %d within %s", len(sent),
			ended.Sub(sent[0]), boundAttempts, bound)
	}
	if got := classOf(stats.UploadFailed); got != "timeout" {
		t.Errorf("upload class %q, want timeout", got)
	}
	if grown := w.lastSequence(t) - before; grown != 1 {
		t.Errorf("the stream grew by %d messages, want the one first copy", grown)
	}
	w.assertInvariants(t, retention, 0)
}

func TestTheRetryBoundEndsAMetaWithoutAnAnswerAfterOneAttempt(t *testing.T) {
	w := startWorldWith(t, testOptions(), shortWindow)
	sink, retention := w.started(t)
	lost := w.loseEvery(isMeta)

	if err := spoolCheckpoint(t, sink, firstPosition, checkpointImage(t)); err != nil {
		t.Fatalf("spool: %v", err)
	}
	stats := w.settled(t, failedFor("upload"))

	if sent := lost.seen(); len(sent) != 1 || classOf(stats.UploadFailed) != "timeout" {
		t.Errorf("%d meta attempts, class %q; want one attempt failed as a timeout", len(sent),
			classOf(stats.UploadFailed))
	}
	if metas := w.ownMetas(t); len(metas) != 0 {
		t.Errorf("metas %v left after the failed upload", metas)
	}
	w.assertInvariants(t, retention, 0)
}

func TestAStopDuringASpaceSweepIsNoFailureAndTheFinalLands(t *testing.T) {
	w := startWorld(t)
	w.putObject(t, testEpoch-1, firstPosition, imageOf(smallBytes))
	sink, retention := w.started(t)
	image := checkpointImage(t)
	w.once(isChunk, natsjstest.Answer([]byte(storeFullAnswer)))
	w.spoolAndCycle(t, sink, secondPosition, image, 1)
	verdict, held := natsjstest.Hold()
	t.Cleanup(held.Release)
	fired := w.once(isInfoLookup, verdict)
	if err := spoolCheckpoint(t, sink, thirdPosition, image); err != nil {
		t.Fatalf("spool: %v", err)
	}
	waitFor(t, fired)
	if err := spoolFinal(t, sink, fourthPosition, image); err != nil {
		t.Fatalf("final: %v", err)
	}

	w.closeWithin(t, newGate(t), bothFitStop)

	stats := w.store.Stats()
	if stats.Finals != 1 || stats.SweepFailures != 0 || stats.LastSweepError != nil || stats.SpaceSweeps != 1 {
		t.Errorf("finals %d, sweeps %d, sweep failures %d (%v); want the final landed after a sweep cut without "+
			"a failure", stats.Finals, stats.SpaceSweeps, stats.SweepFailures, stats.LastSweepError)
	}
	w.assertInvariants(t, retention, 1)
	w.assertStopInvariants(t)
}

func isBucketUsage(f *natsjstest.Frame) bool {
	return f.Subject == bucketInfo && !strings.Contains(string(f.Payload), "subjects_filter")
}

func TestABucketReadCutByTheStopIsAStopNotAFailure(t *testing.T) {
	w := startWorld(t)
	sink, retention := w.started(t)
	verdict, held := natsjstest.Hold()
	t.Cleanup(held.Release)
	fired := w.once(isBucketUsage, verdict)
	if err := spoolFinal(t, sink, firstPosition, checkpointImage(t)); err != nil {
		t.Fatalf("final: %v", err)
	}
	waitFor(t, fired)

	w.closeWithin(t, newGate(t), stopAtTheFits)

	want := stopOutcome{Stopped: 1, FinalSkipped: 1}
	if diff := cmp.Diff(want, w.outcome(retention), cmpEmptyAsNil()); diff != "" {
		t.Errorf("outcome (-want +got):\n%s", diff)
	}
	w.assertInvariants(t, retention, 0)
	w.assertStopInvariants(t)
}

func TestASpaceSweepEndsAtAForeignOrLaterPointerAndRemovesNothing(t *testing.T) {
	tests := []struct {
		name    string
		args    uint64
		wantErr error
	}{
		{name: "a later epoch holds the pointer", args: testEpoch + 1, wantErr: processing.ErrLeaseLost},
		{name: "an earlier epoch wrote the pointer", args: testEpoch - 1, wantErr: processing.ErrCheckpointRefused},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := startWorld(t)
			stale := w.putObject(t, testEpoch-2, firstPosition, imageOf(staleBytes))
			image := checkpointImage(t)
			sink, _ := w.startedAfter(t, w.tightFor(t, image))
			w.setPointer(t, &pointerValue{Version: pointerVersion, Epoch: tt.args,
				StreamCreated: w.streamCreated(t).Format(createdLayout)})

			if err := spoolCheckpoint(t, sink, secondPosition, image); err != nil {
				t.Fatalf("spool: %v", err)
			}

			if err := w.ended(t); !errors.Is(err, tt.wantErr) {
				t.Errorf("Err() = %v, want %v", err, tt.wantErr)
			}
			if !slices.Contains(w.subjects(t), w.metaSubject(stale.Name)) {
				t.Errorf("%s was swept under a pointer this holder does not own", stale.Name)
			}
		})
	}
}

type infoRequest struct {
	SubjectsFilter string `json:"subjects_filter"`
}

func (w *world) isSweepListing(f *natsjstest.Frame) bool {
	var request infoRequest
	return w.isBucketInfo(f) && json.Unmarshal(f.Payload, &request) == nil &&
		request.SubjectsFilter == w.store.subjectPrefix()+subjectFilterAll
}

func TestACloseDuringTheStakeSweepIsNoSweepFailure(t *testing.T) {
	w := startWorld(t)
	w.putObject(t, testEpoch-1, firstPosition, imageOf(smallBytes))
	_, meta := w.restorable(t)
	verdict, held := natsjstest.Hold()
	t.Cleanup(held.Release)
	fired := w.once(w.isSweepListing, verdict)
	staked := make(chan error, 1)
	go func() {
		_, err := w.store.Stake(t.Context(), &meta)
		staked <- err
	}()
	waitFor(t, fired)

	ctx, cancel := context.WithTimeout(t.Context(), stopWithin)
	defer cancel()
	w.store.Close(ctx)
	if err := <-staked; err != nil {
		t.Errorf("Stake = %v, want the stake standing: only its sweep was cut", err)
	}
	if err := w.store.Wait(ctx); err != nil {
		t.Fatalf("Wait: %v", err)
	}

	if stats := w.store.Stats(); stats.SweepFailures != 0 || stats.LastSweepError != nil {
		t.Errorf("sweep failures %d (%v), want none for a sweep the close cut", stats.SweepFailures,
			stats.LastSweepError)
	}
}

func TestASpaceSweepCutByTheStopEndsTheCycleAsAStopNotAFullBucket(t *testing.T) {
	w := startWorld(t)
	w.putObject(t, testEpoch-1, firstPosition, imageOf(staleBytes))
	image := checkpointImage(t)
	tight := w.tightFor(t, image)
	verdict, held := natsjstest.Hold()
	t.Cleanup(held.Release)
	var fired *atomic.Bool
	sink, retention := w.startedAfter(t, func() {
		tight()
		fired = w.once(isInfoLookup, verdict)
	})
	if err := spoolCheckpoint(t, sink, secondPosition, image); err != nil {
		t.Fatalf("spool: %v", err)
	}
	waitFor(t, fired)

	w.closeWithin(t, newGate(t), closeTheSweep)

	w.assertSwept(t, &sweptCounts{Sweeps: 1})
	if stopped := w.store.Stats().Skipped["stopped"]; stopped != 1 {
		t.Errorf("stopped %d, want the cycle the sweep was cut in", stopped)
	}
	w.assertInvariants(t, retention, staleLeft)
}

func TestAnUnreadableMetaStillEndsTheDueSweepAndPausesTheNext(t *testing.T) {
	w := startWorld(t)
	w.publishRaw(t, w.metaSubject(unreadableMetaName), []byte("{not json"))
	sink, retention := w.started(t)
	w.once(isChunk, natsjstest.Answer([]byte(storeFullAnswer)))
	image := checkpointImage(t)

	w.spoolAndCycle(t, sink, firstPosition, image, 1)
	w.spoolAndSettleImage(t, sink, secondPosition, image)
	w.spoolAndSettleImage(t, sink, thirdPosition, image)

	stats := w.store.Stats()
	if stats.SpaceSweeps != 1 || stats.UnreadableMeta != 1 || stats.SweepFailures != 1 {
		t.Errorf("sweeps %d, unreadable metas %d, failures %d; want one due sweep that saw the one unreadable meta",
			stats.SpaceSweeps, stats.UnreadableMeta, stats.SweepFailures)
	}
	w.assertInvariants(t, retention, 0)
}

func TestAnUnreadableMetaInAFullBucketPausesTheSweepUntilTheOrphanAge(t *testing.T) {
	w := startWorld(t)
	w.publishRaw(t, w.metaSubject(unreadableMetaName), []byte("{not json"))
	clock := &stepClock{at: testStart}
	w.clocked(clock)
	image := checkpointImage(t)
	sink, _ := w.startedAfter(t, w.tightFor(t, image))

	w.spoolAndCycle(t, sink, firstPosition, image, 1)
	w.spoolAndCycle(t, sink, secondPosition, image, 2)
	w.assertSwept(t, &sweptCounts{Sweeps: 1, Failures: 1, Full: 2})
	clock.set(testStart.Add(w.options.orphanAge() + time.Nanosecond))
	w.spoolAndCycle(t, sink, thirdPosition, image, 3)

	w.assertSwept(t, &sweptCounts{Sweeps: 2, Failures: 2, Full: 3})
}

func TestASweepThatFreesTooLittleStillSkipsTheUpload(t *testing.T) {
	w := startWorld(t)
	w.putObject(t, testEpoch-1, firstPosition, imageOf(smallBytes))
	image := checkpointImage(t)
	sink, retention := w.startedAfter(t, func() {
		w.store.options.MaxBytes = signed(w.used(t)) + int64(len(image)) - shortfall
	})

	w.spoolAndCycle(t, sink, secondPosition, image, 1)

	w.assertSwept(t, &sweptCounts{Sweeps: 1, Objects: 1, Bytes: smallBytes, Full: 1})
	if chunks := w.ownObjects(t); len(chunks) != 0 {
		t.Errorf("chunks %v uploaded into a bucket the sweep could not free enough", chunks)
	}
	w.assertInvariants(t, retention, 0)
}
