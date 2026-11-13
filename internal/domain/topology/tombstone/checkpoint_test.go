package tombstone_test

import (
	"bytes"
	"errors"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/ids"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/role"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/tombstone"
	"github.com/DmytroLysenko1/faultline/pkg/checkpoint"
)

const (
	restoreScenarios = 1_000
	restoreSteps     = 80
	restoreLimit     = 20
	restoreKeys      = 40
	restoreTTL       = 20 * time.Second
	tombstoneSection = checkpoint.Tag(1)
)

func keyOf(n int) ids.CallKey {
	return ids.NewCallKey(ids.TraceID{byte(n)}, ids.SpanID{byte(n + 1)})
}

type setLog struct {
	Evicted    []bool
	Suppressed []bool
}

// steps adds tombstones the way the matcher does — a fixed TTL after a clock that never goes back —
// expires some and asks which sides they suppress.
func steps(set *tombstone.Set, rng *rand.Rand, now *time.Duration, count int) setLog {
	var log setLog
	for range count {
		*now += time.Duration(rng.IntN(5)) * time.Second
		key := keyOf(rng.IntN(restoreKeys))
		switch rng.IntN(3) {
		case 0:
			set.Expire(*now)
		case 1:
			log.Suppressed = append(log.Suppressed, set.Suppresses(key, role.Caller), set.Suppresses(key, role.Callee))
		default:
			scope := tombstone.Scope(rng.IntN(2))
			log.Evicted = append(log.Evicted, set.Add(key, scope, *now+restoreTTL))
		}
	}
	return log
}

func setCheckpoint(t *testing.T, set *tombstone.Set) []byte {
	t.Helper()
	var out bytes.Buffer
	w := checkpoint.NewWriter(&out, 1)
	w.Begin(tombstoneSection)
	set.WriteCheckpoint(w)
	w.End()
	if err := w.Err(); err != nil {
		t.Fatalf("write: %v", err)
	}
	return out.Bytes()
}

func restoreSet(limit int, data []byte) (tombstone.Set, error) {
	r := checkpoint.NewReader(bytes.NewReader(data))
	r.Begin(tombstoneSection)
	restored, err := tombstone.Restore(limit, r)
	if err != nil {
		return tombstone.Set{}, err
	}
	r.End()
	return restored, r.Finish()
}

// TestARestoredSetSuppressesAndExpiresLikeTheOriginal: entries keep their order, deadlines and scope,
// so the same adds, expiries and questions get the same answers, evictions included.
func TestARestoredSetSuppressesAndExpiresLikeTheOriginal(t *testing.T) {
	rng := rand.New(rand.NewPCG(20261113, 4))
	for scenario := range restoreScenarios {
		original, err := tombstone.New(restoreLimit)
		if err != nil {
			t.Fatal(err)
		}
		var now time.Duration
		steps(&original, rng, &now, rng.IntN(restoreSteps))
		data := setCheckpoint(t, &original)
		restored, err := restoreSet(restoreLimit, data)
		if err != nil {
			t.Fatalf("scenario %d: restore: %v", scenario, err)
		}
		if !bytes.Equal(data, setCheckpoint(t, &restored)) {
			t.Fatalf("scenario %d: a restored set writes other bytes", scenario)
		}
		seed := rng.Uint64()

		want := steps(&original, rand.New(rand.NewPCG(seed, 1)), new(now), restoreSteps)
		got := steps(&restored, rand.New(rand.NewPCG(seed, 1)), new(now), restoreSteps)

		if diff := cmp.Diff(want, got); diff != "" {
			t.Fatalf("scenario %d (-original +restored):\n%s", scenario, diff)
		}
	}
}

type rawEntry struct {
	key      int
	deadline time.Duration
	scope    uint64
}

func rawSet(entries ...rawEntry) []byte {
	var out bytes.Buffer
	w := checkpoint.NewWriter(&out, 1)
	w.Begin(tombstoneSection)
	w.Count(len(entries))
	for _, e := range entries {
		key := keyOf(e.key)
		w.Fixed(key.TraceID[:])
		w.Fixed(key.ID[:])
		w.Duration(e.deadline)
		w.Uint(e.scope)
	}
	w.End()
	return out.Bytes()
}

func TestASetCheckpointRefusesWhatExpireCouldNotHandle(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		limit   int
		wantErr error
	}{
		{name: "a repeated key", data: rawSet(rawEntry{key: 1}, rawEntry{key: 1, deadline: 1}), limit: restoreLimit,
			wantErr: checkpoint.ErrCorrupt},
		{name: "deadlines out of order", data: rawSet(rawEntry{key: 1, deadline: 2}, rawEntry{key: 2, deadline: 1}),
			limit: restoreLimit, wantErr: checkpoint.ErrCorrupt},
		{name: "an unknown scope", data: rawSet(rawEntry{key: 1, scope: 2}), limit: restoreLimit,
			wantErr: checkpoint.ErrCorrupt},
		{name: "more entries than the limit", data: rawSet(rawEntry{key: 1}, rawEntry{key: 2, deadline: 1}), limit: 1,
			wantErr: checkpoint.ErrCorrupt},
		{name: "an invalid limit", data: rawSet(), limit: 0, wantErr: tombstone.ErrInvalidLimit},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := restoreSet(tt.limit, tt.data)

			if !errors.Is(err, tt.wantErr) {
				t.Errorf("err = %v, want %v", err, tt.wantErr)
			}
		})
	}
}
