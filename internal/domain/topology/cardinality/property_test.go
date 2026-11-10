package cardinality_test

import (
	"maps"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/operation"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/cardinality"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/handle"
)

const (
	scenarios      = 10_000
	stepsPerRun    = 200
	maxModelLimit  = 5
	maxModelOwners = 4
	operationPool  = 8
	acquireShare   = 3
	actionChoices  = 5
)

// choices is the randomness a scenario draws from: a seeded generator in the property test,
// the fuzzer's bytes in the fuzz test.
type choices interface {
	IntN(n int) int
}

type modelKey struct {
	Owner     handle.Node
	Operation operation.Operation
}

type held struct {
	key modelKey
	id  cardinality.ID
}

type model struct {
	limit      int
	references map[modelKey]int
	ids        map[modelKey]cardinality.ID
	operations map[handle.Node]int
	overLimit  map[handle.Node]bool
	held       []held
}

func newModel(limit int) *model {
	return &model{
		limit:      limit,
		references: make(map[modelKey]int),
		ids:        make(map[modelKey]cardinality.ID),
		operations: make(map[handle.Node]int),
		overLimit:  make(map[handle.Node]bool),
	}
}

func (m *model) stats() cardinality.Stats {
	return cardinality.Stats{Operations: len(m.references), OwnersOverLimit: len(m.overLimit)}
}

func (m *model) ownersOverLimit() []handle.Node {
	return slices.Sorted(maps.Keys(m.overLimit))
}

func (m *model) liveIDs() map[cardinality.ID]bool {
	live := make(map[cardinality.ID]bool, len(m.ids))
	for _, id := range m.ids {
		live[id] = true
	}
	return live
}

type scenario struct {
	t        *testing.T
	registry *cardinality.Registry
	model    *model
	owners   int
	pool     []operation.Operation
	pick     choices
}

func newScenario(t *testing.T, pick choices) *scenario {
	t.Helper()
	limit, owners := 1+pick.IntN(maxModelLimit), 1+pick.IntN(maxModelOwners)
	pool := make([]operation.Operation, 0, operationPool)
	for n := range operationPool {
		pool = append(pool, path(n))
	}
	return &scenario{
		t:        t,
		registry: newRegistry(t, limit, owners),
		model:    newModel(limit),
		owners:   owners,
		pool:     pool,
		pick:     pick,
	}
}

func (s *scenario) randomKey() modelKey {
	return modelKey{
		Owner:     handle.Node(s.pick.IntN(s.owners)),
		Operation: s.pool[s.pick.IntN(operationPool)],
	}
}

func (s *scenario) step() {
	s.act()
	s.compare()
	if !s.overLimitOwnersAreAtTheLimit() {
		s.t.Fatalf("an owner is over limit below the limit: %v, operations %v", s.model.overLimit, s.model.operations)
	}
}

func (s *scenario) act() {
	if len(s.model.held) > 0 && s.pick.IntN(actionChoices) >= acquireShare {
		s.release(s.pick.IntN(len(s.model.held)))
		return
	}
	s.acquire(s.randomKey())
}

func (s *scenario) acquire(wanted modelKey) {
	live := s.model.liveIDs()
	id, ok := s.registry.Acquire(wanted.Owner, wanted.Operation)
	switch {
	case s.model.references[wanted] > 0:
		s.expect(ok && id == s.model.ids[wanted], "Acquire of a known operation", id, ok)
	case s.model.operations[wanted.Owner] < s.model.limit:
		s.expect(ok && !live[id] && s.dense(id), "Acquire under the limit", id, ok)
		s.model.ids[wanted] = id
		s.model.operations[wanted.Owner]++
	default:
		s.expect(!ok, "Acquire at the limit", id, ok)
		s.model.overLimit[wanted.Owner] = true
		return
	}
	s.model.references[wanted]++
	s.model.held = append(s.model.held, held{key: wanted, id: id})
}

func (s *scenario) release(index int) {
	released := s.model.held[index]
	s.model.held = slices.Delete(s.model.held, index, index+1)
	s.registry.Release(released.key.Owner, released.id)
	s.model.references[released.key]--
	if s.model.references[released.key] > 0 {
		return
	}
	delete(s.model.references, released.key)
	delete(s.model.ids, released.key)
	s.model.operations[released.key.Owner]--
	if s.model.operations[released.key.Owner] < s.model.limit {
		delete(s.model.overLimit, released.key.Owner)
	}
}

// dense: every live operation needs its own id, so ids never pass owners × limit.
func (s *scenario) dense(id cardinality.ID) bool {
	return id >= 0 && int(id) < s.owners*s.model.limit
}

func (s *scenario) overLimitOwnersAreAtTheLimit() bool {
	for owner := range s.model.overLimit {
		if s.model.operations[owner] != s.model.limit {
			return false
		}
	}
	return true
}

func (s *scenario) expect(holds bool, what string, id cardinality.ID, ok bool) {
	s.t.Helper()
	if !holds {
		s.t.Fatalf("%s returned (%d, %v), model disagrees", what, id, ok)
	}
}

type observed struct {
	Stats     cardinality.Stats
	OverLimit []handle.Node
	Known     map[modelKey]cardinality.ID
}

// same is the cheap check run on every step; cmp.Diff only formats a divergence.
func same(want, got *observed) bool {
	return want.Stats == got.Stats &&
		slices.Equal(want.OverLimit, got.OverLimit) &&
		maps.Equal(want.Known, got.Known)
}

func (s *scenario) compare() {
	s.t.Helper()
	want := observed{Stats: s.model.stats(), OverLimit: s.model.ownersOverLimit(), Known: s.model.ids}
	got := observed{Stats: s.registry.Stats(), OverLimit: s.registry.OwnersOverLimit(), Known: s.known()}
	if same(&want, &got) {
		return
	}
	if diff := cmp.Diff(want, got); diff != "" {
		s.t.Fatalf("registry diverged from the model (-want +got):\n%s", diff)
	}
}

func (s *scenario) known() map[modelKey]cardinality.ID {
	known := make(map[modelKey]cardinality.ID)
	for owner := range s.owners {
		for _, op := range s.pool {
			candidate := modelKey{Owner: handle.Node(owner), Operation: op}
			if id, ok := s.registry.Lookup(candidate.Owner, candidate.Operation); ok {
				known[candidate] = id
				s.expect(s.registry.Operation(id) == candidate.Operation, "Operation(Lookup)", id, ok)
			}
		}
	}
	return known
}

func (s *scenario) releaseEverything() {
	for len(s.model.held) > 0 {
		s.release(len(s.model.held) - 1)
	}
	s.compare()
	if diff := cmp.Diff(cardinality.Stats{}, s.registry.Stats()); diff != "" {
		s.t.Fatalf("registry leaked after every reference was released (-want +got):\n%s", diff)
	}
}

func run(t *testing.T, pick choices, steps int) {
	t.Helper()
	played := newScenario(t, pick)
	for range steps {
		played.step()
	}
	played.releaseEverything()
}

func TestRegistryMatchesTheReferenceModel(t *testing.T) {
	for seed := range uint64(scenarios) {
		run(t, rand.New(rand.NewPCG(seed, seed)), stepsPerRun)
	}
}

type fuzzChoices struct {
	data []byte
}

func (c *fuzzChoices) IntN(n int) int {
	if len(c.data) == 0 {
		return 0
	}
	next := int(c.data[0])
	c.data = c.data[1:]
	return next % n
}

func FuzzRegistry(f *testing.F) {
	f.Add([]byte{0, 0, 1, 2, 3, 4, 5, 6, 7})
	f.Add([]byte{4, 3, 0, 0, 0, 1, 0, 2, 4, 0, 4, 1})
	f.Fuzz(func(t *testing.T, data []byte) {
		pick := &fuzzChoices{data: data}
		run(t, pick, len(data))
	})
}
