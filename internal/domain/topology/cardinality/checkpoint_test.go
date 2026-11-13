package cardinality_test

import (
	"bytes"
	"errors"
	"math/rand/v2"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/operation"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/cardinality"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/handle"
	"github.com/DmytroLysenko1/faultline/pkg/checkpoint"
)

const (
	restoreScenarios  = 500
	restoreSteps      = 60
	restoreOwners     = 4
	restoreOperations = 5
	restoreMaxIDs     = restoreOwners * smallLimit
	registrySection   = checkpoint.Tag(1)
)

type holding struct {
	owner handle.Node
	id    cardinality.ID
}

// registryRun acquires and releases at random, so ids are freed, reused and owners go over the limit.
type registryRun struct {
	registry *cardinality.Registry
	held     []holding
}

type registryLog struct {
	Acquired []cardinality.ID
	Admitted []bool
	Stats    cardinality.Stats
	Over     []handle.Node
}

func (run *registryRun) steps(rng *rand.Rand, count int) registryLog {
	var log registryLog
	for range count {
		if len(run.held) > 0 && rng.IntN(3) == 0 {
			at := rng.IntN(len(run.held))
			run.registry.Release(run.held[at].owner, run.held[at].id)
			run.held = append(run.held[:at], run.held[at+1:]...)
			continue
		}
		owner := handle.Node(rng.IntN(restoreOwners))
		op := operation.Operation{
			Protocol: operation.ProtocolHTTP,
			Name:     "GET /" + string(rune('a'+rng.IntN(restoreOperations))),
		}
		id, admitted := run.registry.Acquire(owner, op)
		log.Acquired, log.Admitted = append(log.Acquired, id), append(log.Admitted, admitted)
		if admitted {
			run.held = append(run.held, holding{owner: owner, id: id})
		}
	}
	log.Stats, log.Over = run.registry.Stats(), run.registry.OwnersOverLimit()
	return log
}

func registryCheckpoint(t *testing.T, registry *cardinality.Registry) []byte {
	t.Helper()
	var out bytes.Buffer
	w := checkpoint.NewWriter(&out, 1)
	w.Begin(registrySection)
	registry.WriteCheckpoint(w)
	w.End()
	if err := w.Err(); err != nil {
		t.Fatalf("write: %v", err)
	}
	return out.Bytes()
}

func restoreRegistry(limit int, data []byte) (*cardinality.Registry, error) {
	r := checkpoint.NewReader(bytes.NewReader(data))
	r.Begin(registrySection)
	restored, err := cardinality.RestoreRegistry(cardinality.Limits{MaxPerService: limit}, restoreOwners, restoreMaxIDs, r)
	if err != nil {
		return nil, err
	}
	r.End()
	return restored, r.Finish()
}

// TestARestoredRegistryContinuesLikeTheOriginal: live operations keep their ids and references, the
// free ids their order and the owners over the limit their mark, so the same acquires and releases
// hand out the same ids and refuse the same operations.
func TestARestoredRegistryContinuesLikeTheOriginal(t *testing.T) {
	rng := rand.New(rand.NewPCG(20261113, 3))
	for scenario := range restoreScenarios {
		registry, err := cardinality.NewRegistry(cardinality.Limits{MaxPerService: smallLimit}, restoreOwners)
		if err != nil {
			t.Fatal(err)
		}
		original := &registryRun{registry: registry}
		original.steps(rng, rng.IntN(restoreSteps))
		data := registryCheckpoint(t, registry)
		restoredRegistry, err := restoreRegistry(smallLimit, data)
		if err != nil {
			t.Fatalf("scenario %d: restore: %v", scenario, err)
		}
		if !bytes.Equal(data, registryCheckpoint(t, restoredRegistry)) {
			t.Fatalf("scenario %d: a restored registry writes other bytes", scenario)
		}
		restored := &registryRun{registry: restoredRegistry, held: append([]holding(nil), original.held...)}
		seed := rng.Uint64()

		want := original.steps(rand.New(rand.NewPCG(seed, 1)), restoreSteps)
		got := restored.steps(rand.New(rand.NewPCG(seed, 1)), restoreSteps)

		if diff := cmp.Diff(want, got); diff != "" {
			t.Fatalf("scenario %d (-original +restored):\n%s", scenario, diff)
		}
		for _, h := range original.held {
			if !restoredRegistry.Holds(h.id, h.owner, references(original.held, h.id)) {
				t.Fatalf("scenario %d: operation %d of owner %d is not held as in the original", scenario, h.id, h.owner)
			}
		}
	}
}

func references(held []holding, id cardinality.ID) int {
	count := 0
	for _, h := range held {
		if h.id == id {
			count++
		}
	}
	return count
}

type rawEntry struct {
	id, owner, references int
	name                  string
}

func rawRegistry(next int, live []rawEntry, free, over []int) []byte {
	var out bytes.Buffer
	w := checkpoint.NewWriter(&out, 1)
	w.Begin(registrySection)
	w.Count(next)
	w.Count(len(live))
	for _, e := range live {
		w.Count(e.id)
		w.Count(e.owner)
		operation.Operation{Protocol: operation.ProtocolHTTP, Name: e.name}.WriteCheckpoint(w)
		w.Count(e.references)
	}
	for _, list := range [][]int{free, over} {
		w.Count(len(list))
		for _, value := range list {
			w.Count(value)
		}
	}
	w.End()
	return out.Bytes()
}

func TestARegistryCheckpointRefusesWhatCannotBeLive(t *testing.T) {
	get := rawEntry{id: 0, owner: 0, references: 1, name: "GET /"}
	tests := []struct {
		name string
		data []byte
	}{
		{name: "ids out of order", data: rawRegistry(2, []rawEntry{{id: 1, references: 1, name: "a"}, get}, nil, nil)},
		{name: "the same operation twice", data: rawRegistry(2, []rawEntry{get, {id: 1, references: 1, name: "GET /"}},
			nil, nil)},
		{name: "an unreferenced operation", data: rawRegistry(1, []rawEntry{{id: 0, references: 0, name: "a"}}, nil, nil)},
		{name: "an owner past its limit", data: rawRegistry(3, []rawEntry{
			{id: 0, references: 1, name: "a"}, {id: 1, references: 1, name: "b"}, {id: 2, references: 1, name: "c"},
		}, nil, nil)},
		{name: "a free id that is live", data: rawRegistry(2, []rawEntry{get}, []int{0}, nil)},
		{name: "a free id twice", data: rawRegistry(3, []rawEntry{get}, []int{1, 1}, nil)},
		{name: "free ids that do not complete the live ones", data: rawRegistry(3, []rawEntry{get}, []int{1}, nil)},
		{name: "an owner over the limit twice", data: rawRegistry(1, []rawEntry{get}, nil, []int{2, 2})},
		{name: "an invalid operation", data: rawRegistry(1, []rawEntry{{id: 0, references: 1, name: ""}}, nil, nil)},
	}
	if _, err := restoreRegistry(smallLimit, rawRegistry(2, []rawEntry{get}, []int{1}, []int{2})); err != nil {
		t.Fatalf("a valid raw registry: %v", err)
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := restoreRegistry(smallLimit, tt.data)

			if !errors.Is(err, checkpoint.ErrCorrupt) {
				t.Errorf("err = %v, want ErrCorrupt", err)
			}
		})
	}
}

func TestARegistryRestoreRefusesInvalidLimits(t *testing.T) {
	_, err := restoreRegistry(0, rawRegistry(0, nil, nil, nil))

	if !errors.Is(err, cardinality.ErrInvalidLimits) {
		t.Errorf("err = %v, want ErrInvalidLimits", err)
	}
}
