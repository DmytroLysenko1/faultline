package tokens

import (
	"fmt"
	"maps"
	"math"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/DmytroLysenko1/faultline/internal/domain/credential"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/record"
)

const (
	pinnedSnapshotBytes = 864 << 10
	heldSnapshots       = 2
	idSymbols           = "abcdefghijklmnopqrstuvwxyz234567"
)

func numberedID(number int) string {
	id := make([]byte, credential.IDLength)
	for position := range id {
		id[position] = idSymbols[number%len(idSymbols)]
		number /= len(idSymbols)
	}
	return string(id)
}

func streamSubjects(seed int) map[string]uint64 {
	subjects := make(map[string]uint64, MaxGrants)
	for index := range MaxGrants {
		subjects[fmt.Sprintf("%s%s", subjectPrefix, numberedID(seed*MaxGrants+index))] = 1
	}
	return subjects
}

func decodedSnapshot(t *testing.T, subjects map[string]uint64) credential.Grants {
	t.Helper()
	template := testGrant(t, 1)
	tenant := strings.Repeat("t", record.MaxTenantBytes)
	grants := credential.Grants{}
	for _, subject := range slices.Sorted(maps.Keys(subjects)) {
		id, err := idOf(subject)
		if err != nil {
			t.Fatalf("idOf: %v", err)
		}
		written, err := credential.NewGrant(id, tenant, template.Digest, time.Now())
		if err != nil {
			t.Fatalf("NewGrant: %v", err)
		}
		value, err := encodeEntry(&written)
		if err != nil {
			t.Fatalf("encodeEntry: %v", err)
		}
		grant, err := decodeEntry(id, value)
		if err != nil {
			t.Fatalf("decodeEntry: %v", err)
		}
		grants[grant.ID] = grant
	}
	return grants
}

func liveHeap() int64 {
	runtime.GC()
	runtime.GC()
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	return int64(min(stats.HeapAlloc, math.MaxInt64))
}

func TestTwoFullSnapshotsAndARereadFitTheirPinnedShare(t *testing.T) {
	before := liveHeap()
	snapshots := make([]credential.Grants, 0, heldSnapshots)
	var reread map[string]uint64
	for seed := range heldSnapshots {
		reread = streamSubjects(seed)
		snapshots = append(snapshots, decodedSnapshot(t, reread))
	}
	held := liveHeap() - before
	runtime.KeepAlive(snapshots)
	runtime.KeepAlive(reread)
	for _, snapshot := range snapshots {
		if len(snapshot) != MaxGrants {
			t.Fatalf("a snapshot holds %d grants, want %d", len(snapshot), MaxGrants)
		}
	}
	if held > pinnedSnapshotBytes {
		t.Errorf("two full snapshots and a reread hold %d bytes, the pinned share is %d", held, pinnedSnapshotBytes)
	}
	t.Logf("held %.2f MiB of %.2f MiB", float64(held)/(1<<20), float64(pinnedSnapshotBytes)/(1<<20))
}
