package checkpoint

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/natsjstest"
)

const (
	unreadableMetaName = "9-1"
	unparsedName       = "operator-notes"
)

func TestAFailedSweepLeavesTheStakeStanding(t *testing.T) {
	w := startWorld(t)
	_, meta := w.restorable(t)
	w.once(w.isBucketInfo, natsjstest.Drop())

	_, err := w.store.Stake(t.Context(), &meta)

	if got, _ := w.pointer(t); err != nil || got.Epoch != testEpoch {
		t.Fatalf("Stake: err = %v, epoch %d; want a standing stake", err, got.Epoch)
	}
	if got := w.store.Stats(); got.SweepFailures != 1 || !errors.Is(got.LastSweepError, context.DeadlineExceeded) {
		t.Errorf("stats %+v, want the sweep failure counted", got)
	}
}

func TestAnUnreadableMetaKeepsEveryChunkWithoutAMeta(t *testing.T) {
	w := startWorld(t)
	stale := w.putObject(t, olderEpoch, 20, imageOf(9))
	_, meta := w.restorable(t)
	w.publishRaw(t, w.metaSubject(unreadableMetaName), []byte("{not json"))
	orphan := w.store.chunkSubject("OLD")
	if _, err := w.operator.Publish(t.Context(), orphan, imageOf(9)); err != nil {
		t.Fatalf("publish orphan: %v", err)
	}
	published := w.lastTime(t, orphan)
	w.store.now = func() time.Time { return published.Add(time.Hour + w.options.orphanAge()) }

	if _, err := w.store.Stake(t.Context(), &meta); err != nil {
		t.Fatalf("Stake: %v", err)
	}

	subjects := w.subjects(t)
	if slices.Contains(subjects, w.metaSubject(stale.Name)) {
		t.Errorf("the stale object %s was not swept", stale.Name)
	}
	if !slices.Contains(subjects, orphan) {
		t.Error("an old chunk was swept although a meta could not be read")
	}
	got := w.store.Stats()
	if got.UnreadableMeta != 1 || !errors.Is(got.LastSweepError, errUnreadableMeta) {
		t.Errorf("stats %+v, want one unreadable meta reported", got)
	}
}

func TestASweepKeepsAnObjectWhoseNameDoesNotParse(t *testing.T) {
	w := startWorld(t)
	if _, err := w.objects.PutBytes(t.Context(), unparsedName, imageOf(9)); err != nil {
		t.Fatalf("put %s: %v", unparsedName, err)
	}
	_, meta := w.restorable(t)

	if _, err := w.store.Stake(t.Context(), &meta); err != nil {
		t.Fatalf("Stake: %v", err)
	}

	if !slices.Contains(w.subjects(t), w.metaSubject(unparsedName)) {
		t.Errorf("the sweep removed %q, whose name tells no epoch", unparsedName)
	}
}

func isChunkPurge(f *natsjstest.Frame) bool {
	return isPurge(f) && strings.Contains(string(f.Payload), chunkSubjectPart)
}

func TestAMetaSweptBeforeItsChunksFailedIsStillCounted(t *testing.T) {
	w := startWorld(t)
	stale := w.putObject(t, olderEpoch, 20, imageOf(9))
	_, meta := w.restorable(t)
	w.once(isChunkPurge, natsjstest.Answer([]byte(unavailableAnswer)))

	if _, err := w.store.Stake(t.Context(), &meta); err != nil {
		t.Fatalf("Stake: %v", err)
	}

	got := w.store.Stats()
	want := []uint64{1, stale.Size, 1}
	if diff := cmp.Diff(want, []uint64{got.SweptObjects, got.SweptBytes, got.SweepFailures}); diff != "" {
		t.Errorf("swept objects, bytes, failures (-want +got):\n%s", diff)
	}
}
