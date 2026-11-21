package checkpoint

import (
	"bytes"
	"context"
	"errors"
	"math/rand/v2"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/DmytroLysenko1/faultline/internal/application/processing"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/natsjstest"
)

const (
	answeredWithin    = 3 * time.Second
	wrongLastSequence = "10071"
	propertyRuns      = 10_000
	rareOneIn         = 10_000
	propertySeed      = 34
	propertyEpochs    = 4
	propertyRevisions = 6
	lateChunk         = "late"
	decimalBase       = 10
)

type writes struct {
	mu       sync.Mutex
	seen     int
	verdicts []func(f *natsjstest.Frame) natsjstest.Verdict
	after    func(f *natsjstest.Frame)
}

func (w *world) judgeWrites(rules *writes) {
	w.proxy.Intercept(func(f *natsjstest.Frame) natsjstest.Verdict {
		rules.mu.Lock()
		after := rules.after
		if !isPointerWrite(f) {
			rules.mu.Unlock()
			if after != nil {
				after(f)
			}
			return natsjstest.Forward()
		}
		n := rules.seen
		rules.seen++
		rules.mu.Unlock()
		if n < len(rules.verdicts) && rules.verdicts[n] != nil {
			return rules.verdicts[n](f)
		}
		return natsjstest.Forward()
	})
}

func dropping(*natsjstest.Frame) natsjstest.Verdict {
	return natsjstest.Drop()
}

func holding(verdict natsjstest.Verdict) func(*natsjstest.Frame) natsjstest.Verdict {
	return func(*natsjstest.Frame) natsjstest.Verdict {
		return verdict
	}
}

func waitAnswered(t *testing.T, held *natsjstest.HeldFrame) []byte {
	t.Helper()
	select {
	case <-held.Answered():
		return held.Answer()
	case <-time.After(answeredWithin):
		t.Fatalf("the held write had no answer within %s", answeredWithin)
		return nil
	}
}

func TestAFenceMakesAHangingUpdateHarmless(t *testing.T) {
	for _, nodes := range clusterSizes {
		t.Run(strconv.Itoa(nodes)+" nodes", func(t *testing.T) {
			w := startWorldOn(t, nodes)
			sink, retention := w.started(t)
			verdict, held := natsjstest.Hold()
			var fenced atomic.Bool
			rules := &writes{verdicts: []func(*natsjstest.Frame) natsjstest.Verdict{
				holding(verdict),
				dropping,
				func(*natsjstest.Frame) natsjstest.Verdict {
					fenced.Store(true)
					return natsjstest.Forward()
				},
			}}
			rules.after = func(*natsjstest.Frame) {
				if fenced.Load() {
					held.Release()
				}
			}
			w.judgeWrites(rules)

			w.spoolAndSettle(t, sink, firstPosition, failedFor("cas"))
			answer := waitAnswered(t, held)

			if !bytes.Contains(answer, []byte(wrongLastSequence)) {
				t.Errorf("the hanging Update was answered %s, want %s", answer, wrongLastSequence)
			}
			stats := w.store.Stats()
			got := waitingCounts{Pending: stats.Pending, Unknown: stats.CASUnknown, Fences: stats.Fences}
			if diff := cmp.Diff(waitingCounts{Fences: 1}, got); diff != "" {
				t.Errorf("counts mismatch (-want +got):\n%s", diff)
			}
			if value, _ := w.pointer(t); value.Current != nil {
				t.Errorf("the pointer names %v, want it as it was", value.Current)
			}
			if objects := w.ownObjects(t); len(objects) != 0 {
				t.Errorf("objects %v, want the fenced one purged", objects)
			}
			w.assertInvariants(t, retention, 0)
		})
	}
}

func TestAHangingUpdateThatLandsBeforeTheFenceIsAccepted(t *testing.T) {
	for _, nodes := range clusterSizes {
		t.Run(strconv.Itoa(nodes)+" nodes", func(t *testing.T) {
			w := startWorldOn(t, nodes)
			sink, retention := w.started(t)
			verdict, held := natsjstest.Hold()
			w.judgeWrites(&writes{verdicts: []func(*natsjstest.Frame) natsjstest.Verdict{
				holding(verdict),
				dropping,
				func(*natsjstest.Frame) natsjstest.Verdict {
					held.Release()
					select {
					case <-held.Answered():
						if bytes.Contains(held.Answer(), []byte(`"error"`)) {
							t.Errorf("the hanging Update was refused: %s", held.Answer())
						}
					case <-time.After(answeredWithin):
						t.Errorf("the hanging Update had no answer within %s", answeredWithin)
					}
					return natsjstest.Forward()
				},
			}})

			w.spoolAndSettle(t, sink, firstPosition, committedThrough(firstPosition))

			if stats := w.store.Stats(); stats.Fences != 1 || stats.Pending != 0 {
				t.Errorf("fences %d, pending %d; want one fence and nothing waiting", stats.Fences, stats.Pending)
			}
			if diff := cmp.Diff([]uint64{firstPosition}, retention.committed()); diff != "" {
				t.Errorf("feed commits mismatch (-want +got):\n%s", diff)
			}
			w.assertInvariants(t, retention, 0)
		})
	}
}

func TestAWaitingObjectThePointerNamesIsCommittedByTheNextCycle(t *testing.T) {
	w := startWorld(t)
	sink, retention := w.started(t)
	verdict, held := natsjstest.Hold()
	w.judgeWrites(&writes{verdicts: []func(*natsjstest.Frame) natsjstest.Verdict{
		holding(verdict),
		dropping,
		dropping,
	}})

	w.spoolAndSettle(t, sink, firstPosition, failedFor("cas"))
	held.Release()
	waitAnswered(t, held)
	w.proxy.Intercept(nil)
	w.spoolAndSettle(t, sink, secondPosition, committedThrough(secondPosition))

	if diff := cmp.Diff([]uint64{firstPosition, secondPosition}, retention.committed()); diff != "" {
		t.Errorf("feed commits mismatch (-want +got):\n%s", diff)
	}
	if stats := w.store.Stats(); stats.Pending != 0 || stats.Committed != 2 {
		t.Errorf("pending %d, committed %d; want none waiting and both committed", stats.Pending, stats.Committed)
	}
	w.assertInvariants(t, retention, 0)
}

func TestAChunkThatLandsAfterThePurgeWaitsAndIsPurgedLater(t *testing.T) {
	w := startWorld(t)
	sink, retention := w.started(t)
	var chunks atomic.Int32
	var subject atomic.Value
	var purged, ghost atomic.Bool
	w.proxy.Intercept(func(f *natsjstest.Frame) natsjstest.Verdict {
		if isChunk(f) {
			subject.Store(f.Subject)
			if chunks.Add(1) == changedChunk {
				f.Payload[len(f.Payload)-1] ^= 1
			}
		}
		if isPurge(f) {
			purged.Store(true)
		}
		if isChunkCount(f) && purged.Load() && ghost.CompareAndSwap(false, true) {
			chunk, _ := subject.Load().(string)
			if _, err := w.operator.Publish(context.WithoutCancel(t.Context()), chunk, []byte(lateChunk)); err != nil {
				t.Errorf("publish a late chunk: %v", err)
			}
		}
		return natsjstest.Forward()
	})

	w.spoolAndSettle(t, sink, firstPosition, failedFor("verify"))
	waiting := w.store.Stats()
	w.proxy.Intercept(nil)
	w.spoolAndSettle(t, sink, secondPosition, committedThrough(secondPosition))

	if waiting.Pending != 1 || waiting.PurgeLeftover != 1 {
		t.Errorf("pending %d, leftovers %d; want the late chunk waiting", waiting.Pending, waiting.PurgeLeftover)
	}
	if stats := w.store.Stats(); stats.Pending != 0 {
		t.Errorf("pending = %d after the next cycle", stats.Pending)
	}
	w.assertInvariants(t, retention, 0)
}

func TestNoFenceGoesOutWhileTheLeaseIsSuspended(t *testing.T) {
	w := startWorld(t)
	sink, retention := w.started(t)
	w.judgeWrites(&writes{verdicts: []func(*natsjstest.Frame) natsjstest.Verdict{
		dropping,
		func(*natsjstest.Frame) natsjstest.Verdict {
			w.lease.set(false)
			return natsjstest.Drop()
		},
	}})

	w.spoolAndSettle(t, sink, firstPosition, failedFor("cas"))
	waiting := w.store.Stats()
	w.lease.set(true)
	w.proxy.Intercept(nil)
	w.spoolAndSettle(t, sink, secondPosition, committedThrough(secondPosition))

	got := waitingCounts{Pending: waiting.Pending, Unknown: waiting.CASUnknown, Fences: waiting.Fences}
	if diff := cmp.Diff(waitingCounts{Pending: 1, Unknown: 1}, got); diff != "" {
		t.Errorf("counts while waiting mismatch (-want +got):\n%s", diff)
	}
	if stats := w.store.Stats(); stats.Fences != 1 || stats.Pending != 0 {
		t.Errorf("fences %d, pending %d; want the fence of the next cycle", stats.Fences, stats.Pending)
	}
	w.assertInvariants(t, retention, 0)
}

type stopInCASArgs struct {
	held int
}

type stopInCASWant struct {
	Current   uint64
	Objects   int
	Committed []uint64
}

func TestCloseDuringTheRetryOrTheFenceCompletesIt(t *testing.T) {
	tests := []struct {
		name string
		args stopInCASArgs
		want stopInCASWant
	}{
		{name: "the retry", args: stopInCASArgs{held: 1}, want: stopInCASWant{Current: firstPosition, Objects: 1}},
		{name: "the fence", args: stopInCASArgs{held: 2}, want: stopInCASWant{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := startWorld(t)
			sink, retention := w.started(t)
			held := newGate(t)
			verdicts := make([]func(*natsjstest.Frame) natsjstest.Verdict, tt.args.held+1)
			for i := range tt.args.held {
				verdicts[i] = dropping
			}
			verdicts[tt.args.held] = func(*natsjstest.Frame) natsjstest.Verdict {
				held.held.Store(true)
				<-held.open
				return natsjstest.Forward()
			}
			w.judgeWrites(&writes{verdicts: verdicts})
			if err := spoolCheckpoint(t, sink, firstPosition, checkpointImage(t)); err != nil {
				t.Fatalf("spool: %v", err)
			}
			held.waitHeld(t)

			w.closeWhile(t, held)

			value, _ := w.pointer(t)
			got := stopInCASWant{Objects: len(w.ownObjects(t)), Committed: retention.committed()}
			if value.Current != nil {
				got.Current = value.Current.Position
			}
			if diff := cmp.Diff(tt.want, got, cmpEmptyAsNil()); diff != "" {
				t.Errorf("after the stop (-want +got):\n%s", diff)
			}
			w.assertInvariants(t, retention, 0)
		})
	}
}

func TestALateCASOfAnEarlierHolderMeetsTheStakeOfTheNextOne(t *testing.T) {
	w := startWorld(t)
	sink, retention := w.started(t)
	w.spoolAndSettle(t, sink, firstPosition, committedThrough(firstPosition))
	verdict, held := natsjstest.Hold()
	var seen atomic.Bool
	w.judgeWrites(&writes{verdicts: []func(*natsjstest.Frame) natsjstest.Verdict{
		func(*natsjstest.Frame) natsjstest.Verdict {
			seen.Store(true)
			return verdict
		},
	}})
	if err := spoolCheckpoint(t, sink, secondPosition, checkpointImage(t)); err != nil {
		t.Fatalf("spool: %v", err)
	}
	waitFor(t, &seen)

	next := w.laterHolder(t)
	held.Release()

	if err := w.ended(t); !errors.Is(err, processing.ErrLeaseLost) {
		t.Fatalf("Err() = %v, want the lease lost", err)
	}
	if answer := waitAnswered(t, held); !bytes.Contains(answer, []byte(wrongLastSequence)) {
		t.Errorf("the late Update was answered %s, want %s", answer, wrongLastSequence)
	}
	value, _ := w.pointer(t)
	if value.Epoch != testEpoch+1 || value.Current == nil || value.Current.Position != firstPosition {
		t.Errorf("pointer %+v, want the next holder's stake of %d", value, firstPosition)
	}
	if err := latestErr(next.Latest(t.Context(), testPartition)); err != nil {
		t.Errorf("Latest of the next holder: %v", err)
	}
	if diff := cmp.Diff([]uint64{firstPosition}, retention.committed()); diff != "" {
		t.Errorf("feed commits mismatch (-want +got):\n%s", diff)
	}
}

func (w *world) laterHolder(t *testing.T) *Store {
	t.Helper()
	options := *w.options
	options.Epoch = testEpoch + 1
	next := newStore(&options, &fakeLease{valid: true}, &w.store.buckets, time.Now)
	meta, reader, err := next.Latest(t.Context(), testPartition)
	if err != nil {
		t.Fatalf("Latest of the next holder: %v", err)
	}
	if err := reader.Close(); err != nil {
		t.Fatalf("close the latest: %v", err)
	}
	if _, err := next.Stake(t.Context(), &meta); err != nil {
		t.Fatalf("Stake of the next holder: %v", err)
	}
	return next
}

func TestTheServerRefusesAnUpdateAtARevisionTheFenceMovedPast(t *testing.T) {
	for _, nodes := range clusterSizes {
		t.Run(strconv.Itoa(nodes)+" nodes", func(t *testing.T) {
			w := startWorldOn(t, nodes)
			if err := latestErr(w.store.Latest(t.Context(), testPartition)); !errors.Is(err, processing.ErrNoCheckpoint) {
				t.Fatalf("Latest: %v", err)
			}
			if _, err := w.store.StakeEmpty(t.Context()); err != nil {
				t.Fatalf("StakeEmpty: %v", err)
			}
			entry, err := w.kv.Get(t.Context(), w.options.pointerKey())
			if err != nil {
				t.Fatalf("Get: %v", err)
			}
			if _, err := w.kv.Update(t.Context(), w.options.pointerKey(), entry.Value(), entry.Revision()); err != nil {
				t.Fatalf("the fence: %v", err)
			}

			late := &nats.Msg{
				Subject: "$KV." + PointerBucket + "." + w.options.pointerKey(),
				Data:    []byte(`{"late":true}`),
				Header: nats.Header{
					jetstream.ExpectedLastSubjSeqHeader: []string{strconv.FormatUint(entry.Revision(), decimalBase)},
				},
			}
			_, err = w.operator.PublishMsg(t.Context(), late)

			if apiErr, ok := errors.AsType[*jetstream.APIError](err); !ok || apiErr.ErrorCode != 10071 {
				t.Fatalf("the late Update = %v, want 10071", err)
			}
			after, err := w.kv.Get(t.Context(), w.options.pointerKey())
			if err != nil || !bytes.Equal(after.Value(), entry.Value()) {
				t.Errorf("the pointer after the late Update = %s, %v; want it as fenced", after.Value(), err)
			}
		})
	}
}

func TestTheJudgeNeverPurgesWhatAPointerMayStillName(t *testing.T) {
	random := rand.New(rand.NewPCG(propertySeed, propertySeed))
	objects := []objectRef{*ownNext(), {Name: "7-10", NUID: "A"}, {Name: "7-15", NUID: "B"}}
	for range propertyRuns {
		found := randomPointer(random, objects)
		item := pendingObject{
			ref:      objects[random.IntN(len(objects))],
			updated:  random.IntN(2) == 0,
			revision: random.Uint64N(propertyRevisions),
		}
		decided := judgePending(found, ownEpoch, &item)
		if decided == verdictPurge && (names(found, &item.ref) || (item.updated && found.revision <= item.revision)) {
			t.Fatalf("judgePending purges %+v under %+v", item, found)
		}
		after := judgeAfter(found, ownEpoch, &item.ref, casStep(random.IntN(len(nextAfter))))
		if after == verdictAccepted && !sameObject(found.value.Current, &item.ref) {
			t.Fatalf("judgeAfter accepts %+v under %+v", item.ref, found)
		}
		if foreign(found, ownEpoch) != (decided == verdictForeign) {
			t.Fatalf("judgePending = %d under %+v, foreign %t", decided, found, foreign(found, ownEpoch))
		}
	}
}

func randomPointer(random *rand.Rand, objects []objectRef) *pointer {
	if random.IntN(rareOneIn) == 0 {
		return nil
	}
	pick := func() *objectRef {
		if random.IntN(len(objects)+1) == len(objects) {
			return nil
		}
		return &objects[random.IntN(len(objects))]
	}
	found := pointerAt(ownEpoch-2+random.Uint64N(propertyEpochs), pick(), created)
	found.value.Previous = pick()
	found.revision = random.Uint64N(propertyRevisions)
	found.corrupt = random.IntN(rareOneIn) == 1
	return found
}

func names(found *pointer, ref *objectRef) bool {
	return found != nil &&
		(sameObject(found.value.Current, ref) || sameObject(found.value.Previous, ref))
}

func cmpEmptyAsNil() cmp.Option {
	return cmp.Transformer("emptyAsNil", func(in []uint64) []uint64 {
		if len(in) == 0 {
			return nil
		}
		return in
	})
}

func TestACheckpointOfAnotherPartitionIsRefusedAtBegin(t *testing.T) {
	w := startWorld(t)
	sink, _ := w.started(t)
	meta := spoolAt(firstPosition)
	meta.Position.Partition = testPartition + 1

	_, err := sink.Begin(t.Context(), meta)

	if !errors.Is(err, processing.ErrForeignCheckpoint) {
		t.Fatalf("Begin = %v, want ErrForeignCheckpoint", err)
	}
	if left := spooled(t, w.options.SpoolDir); len(left) != 0 {
		t.Errorf("spools written: %v", left)
	}
}

func TestLatestAnswersThePartitionTheObjectHolds(t *testing.T) {
	w := startWorld(t)
	ref := w.putShaped(t, &storedObject{partition: testPartition + 1, epoch: olderEpoch, position: restoredAt},
		imageOf(imageBytes))
	w.setPointer(t, &pointerValue{Version: pointerVersion, Epoch: olderEpoch, Current: ref})

	meta, reader, err := w.store.Latest(t.Context(), testPartition)
	if err != nil {
		t.Fatalf("Latest: %v", err)
	}
	if err := reader.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	if meta.Position.Partition != testPartition+1 {
		t.Errorf("Latest partition = %d, want the %d the object holds", meta.Position.Partition, testPartition+1)
	}
}

func TestAnObjectPutByHandLeavesThePointerWhole(t *testing.T) {
	w := startWorld(t)
	previous := w.putObject(t, testEpoch, firstPosition, checkpointImage(t))
	current := w.putObject(t, testEpoch, secondPosition, checkpointImage(t))
	w.setPointer(t, &pointerValue{Version: pointerVersion, Epoch: testEpoch, Current: current, Previous: previous})
	sink, retention := w.restarted(t)

	w.spoolAndSettle(t, sink, thirdPosition, committedThrough(thirdPosition))

	for _, name := range w.ownMetas(t) {
		if name == previous.Name {
			t.Errorf("the meta of %s stayed after its chunks went", name)
		}
	}
	w.assertInvariants(t, retention, 0)
}

func (w *world) restarted(t *testing.T) (*Sink, *fakeRetention) {
	t.Helper()
	meta, reader, err := w.store.Latest(t.Context(), testPartition)
	if err != nil {
		t.Fatalf("Latest: %v", err)
	}
	if err := reader.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if _, err := w.store.Stake(t.Context(), &meta); err != nil {
		t.Fatalf("Stake: %v", err)
	}
	w.store.chunkBytes = uploadChunk
	retention := &fakeRetention{created: w.streamCreated(t)}
	sink, err := w.store.Start(t.Context(), retention)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { w.stop(t) })
	return sink, retention
}

func TestNoRetryGoesOutWithoutTheLease(t *testing.T) {
	w := startWorld(t)
	sink, retention := w.started(t)
	rules := &writes{verdicts: []func(*natsjstest.Frame) natsjstest.Verdict{
		func(*natsjstest.Frame) natsjstest.Verdict {
			w.lease.set(false)
			return natsjstest.Drop()
		},
	}}
	w.judgeWrites(rules)

	w.spoolAndSettle(t, sink, firstPosition, failedFor("cas"))

	rules.mu.Lock()
	sent := rules.seen
	rules.mu.Unlock()
	stats := w.store.Stats()
	got := waitingCounts{Pending: stats.Pending, Unknown: stats.CASUnknown, Fences: stats.Fences}
	if diff := cmp.Diff(waitingCounts{Pending: 1, Unknown: 1}, got); diff != "" || sent != 1 {
		t.Errorf("pointer writes %d, counts mismatch (-want +got):\n%s", sent, diff)
	}
	w.lease.set(true)
	w.assertInvariants(t, retention, 0)
}

func TestAWaitingObjectNamedAsPreviousLeavesItsLeftObjectsAndTheList(t *testing.T) {
	w := startWorld(t)
	sink, retention := w.started(t)
	for _, position := range []uint64{firstPosition, secondPosition} {
		w.spoolAndSettle(t, sink, position, committedThrough(position))
	}
	w.stop(t)
	value, _ := w.pointer(t)
	left := w.putObject(t, testEpoch, restoredAt, checkpointImage(t))
	idle := &uploader{store: w.store, feed: retention}
	idle.wait(&pendingObject{ref: *value.Previous, updated: true, left: []objectRef{*left}})

	if err := idle.resolve(t.Context()); err != nil {
		t.Fatalf("resolve: %v", err)
	}

	if len(idle.pending) != 0 {
		t.Errorf("pending %v, want the referenced object off the list", idle.pending)
	}
	for _, name := range w.ownMetas(t) {
		if name == left.Name {
			t.Errorf("the left object %s stayed", name)
		}
	}
	w.assertInvariants(t, retention, 0)
}

type retentionWant struct {
	Commits   []uint64
	Calls     int
	Failed    uint64
	Suspended uint64
}

func TestEachRefusalOfTheFeedCommitGetsItsOwnReaction(t *testing.T) {
	tests := []struct {
		name string
		args func(w *world, retention *fakeRetention)
		want retentionWant
	}{
		{
			name: "a closed feed is never asked again",
			args: func(_ *world, retention *fakeRetention) {
				retention.refuse = 1
				retention.refusal = processing.ErrFeedClosed
			},
			want: retentionWant{Calls: 1, Failed: 1},
		},
		{
			name: "a suspended lease skips the retention of this cycle",
			args: func(w *world, retention *fakeRetention) {
				retention.refuse = 1
				retention.refused = func() { w.lease.set(false) }
			},
			want: retentionWant{Commits: []uint64{secondPosition, thirdPosition}, Calls: 3, Suspended: 1},
		},
		{
			name: "any other refusal is a failed commit and the next cycle commits",
			args: func(_ *world, retention *fakeRetention) {
				retention.refuse = 1
			},
			want: retentionWant{Commits: []uint64{secondPosition, thirdPosition}, Calls: 3, Failed: 1},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := startWorld(t)
			sink, retention := w.started(t)
			retention.mu.Lock()
			tt.args(w, retention)
			retention.mu.Unlock()

			for _, position := range []uint64{firstPosition, secondPosition, thirdPosition} {
				w.lease.set(true)
				w.spoolAndSettle(t, sink, position, committedThrough(position))
			}

			stats := w.store.Stats()
			retention.mu.Lock()
			got := retentionWant{Commits: retention.commits, Calls: retention.calls, Failed: stats.Failed["commit"],
				Suspended: stats.Skipped["retention_suspended"]}
			retention.mu.Unlock()
			if diff := cmp.Diff(tt.want, got, cmpEmptyAsNil()); diff != "" {
				t.Errorf("retention mismatch (-want +got):\n%s", diff)
			}
			w.assertInvariants(t, retention, 0)
		})
	}
}

func TestAnAcceptedWaitingObjectTakesItsLeftObjectsBack(t *testing.T) {
	w := startWorld(t)
	sink, retention := w.started(t)
	for _, position := range []uint64{firstPosition, secondPosition} {
		w.spoolAndSettle(t, sink, position, committedThrough(position))
	}
	first, _ := w.pointer(t)
	verdict, held := natsjstest.Hold()
	w.judgeWrites(&writes{verdicts: []func(*natsjstest.Frame) natsjstest.Verdict{
		holding(verdict),
		dropping,
		dropping,
	}})
	w.spoolAndSettle(t, sink, thirdPosition, failedFor("cas"))
	held.Release()
	waitAnswered(t, held)
	w.proxy.Intercept(nil)

	w.spoolAndSettle(t, sink, fourthPosition, committedThrough(fourthPosition))

	for _, name := range w.ownMetas(t) {
		if name == first.Previous.Name {
			t.Errorf("the object %s that left with the accepted one stayed", name)
		}
	}
	if diff := cmp.Diff([]uint64{firstPosition, secondPosition, thirdPosition, fourthPosition},
		retention.committed()); diff != "" {
		t.Errorf("feed commits mismatch (-want +got):\n%s", diff)
	}
	w.assertInvariants(t, retention, 0)
}

type acceptedWant struct {
	Commits   []uint64
	Kept      bool
	LastBytes int64
	Committed uint64
}

func TestAWaitingObjectAcceptedAtTheStopKeepsWhatThePointerNamesAndCommitsNoFeed(t *testing.T) {
	w := startWorld(t)
	sink, retention := w.started(t)
	for _, position := range []uint64{firstPosition, secondPosition} {
		w.spoolAndSettle(t, sink, position, committedThrough(position))
	}
	w.stop(t)
	value, _ := w.pointer(t)
	before := w.store.Stats()
	stopping := make(chan struct{})
	close(stopping)
	idle := &uploader{store: w.store, feed: retention, stopping: stopping}
	idle.wait(&pendingObject{ref: *value.Current, updated: true, left: []objectRef{*value.Previous}})

	if err := idle.resolve(t.Context()); err != nil {
		t.Fatalf("resolve: %v", err)
	}
	idle.publishCommits()

	stats := w.store.Stats()
	got := acceptedWant{Commits: retention.committed(), LastBytes: stats.LastBytes, Committed: stats.Committed}
	for _, name := range w.ownMetas(t) {
		got.Kept = got.Kept || name == value.Previous.Name
	}
	want := acceptedWant{Commits: []uint64{firstPosition, secondPosition}, Kept: true, LastBytes: before.LastBytes,
		Committed: before.Committed + 1}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("after the accepted object (-want +got):\n%s", diff)
	}
}

func TestALaterEpochFoundWhileTheListIsSettledAtTheStopEndsTheStore(t *testing.T) {
	w := startWorld(t)
	sink, retention := w.started(t)
	w.spoolAndSettle(t, sink, firstPosition, committedThrough(firstPosition))
	w.stop(t)
	value, _ := w.pointer(t)
	w.setPointer(t, &pointerValue{Version: pointerVersion, Epoch: testEpoch + 1, Current: value.Current})
	stopping := make(chan struct{})
	close(stopping)
	idle := &uploader{store: w.store, feed: retention, stopping: stopping}
	idle.wait(&pendingObject{ref: *value.Current, updated: true})

	idle.leave(t.Context(), t.Context())

	if err := w.store.Err(); !errors.Is(err, processing.ErrLeaseLost) {
		t.Errorf("Err() = %v, want the lease lost", err)
	}
	if stats := w.store.Stats(); stats.LeftAtStop != 0 {
		t.Errorf("left at stop = %d, want none counted for an ended store", stats.LeftAtStop)
	}
}
