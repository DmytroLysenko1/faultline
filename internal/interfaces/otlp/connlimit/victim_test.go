package connlimit_test

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/domain/credential"
)

const (
	victimAt        = 70 * time.Second
	firstFreshMark  = 11 * time.Second
	freshMarkStep   = 100 * time.Millisecond
	floodSteps      = 1000
	floodStep       = 10 * time.Millisecond
	rpcPeriod       = time.Second
	seniorRefreshes = 100
	replicaCount    = 2
	limitOfAll      = 4
	limitOfIngester = 64
	attackerAccept  = 'a'
)

type held struct {
	name     string
	token    credential.ID
	markedAt time.Duration
}

type block struct {
	token credential.ID
	count int
}

type rejoin struct {
	left string
	as   held
}

type eviction struct {
	Closed []string
	Class  string
}

func never(name string) held {
	return held{
		name: name,
	}
}

func markedBy(name string, token credential.ID, markedAt time.Duration) held {
	return held{
		name:     name,
		token:    token,
		markedAt: markedAt,
	}
}

func blocks(parts ...block) []held {
	var conns []held
	for _, part := range parts {
		for range part.count {
			index := len(conns)
			conns = append(conns, markedBy(
				fmt.Sprintf("c%d", index+1),
				part.token,
				firstFreshMark+time.Duration(index)*freshMarkStep,
			))
		}
	}
	return conns
}

func tokensOf(count, each int) []block {
	parts := make([]block, 0, count)
	for index := range count {
		parts = append(parts, block{
			token: credential.ID(fmt.Sprintf("t%02d", index)),
			count: each,
		})
	}
	return parts
}

func settle(t *testing.T, limit int, conns []held) *fixture {
	t.Helper()
	f := newFixture(t, limit)
	base := f.base
	for _, conn := range conns {
		if f.offer(conn.name) == nil {
			t.Fatalf("%s was refused while the limit %d was not reached", conn.name, limit)
		}
	}
	for _, conn := range conns {
		if conn.token != "" {
			f.markAt(conn)
		}
	}
	f.clock.now = base
	return f
}

func (f *fixture) markAt(conn held) {
	f.t.Helper()
	f.clock.now = f.base.Add(conn.markedAt)
	f.mark(conn.name, conn.token)
	f.clock.now = f.base
}

func (f *fixture) rejoin(r rejoin) {
	f.t.Helper()
	if err := f.accepted[r.left].Close(); err != nil {
		f.t.Fatalf("%s leaves: %v", r.left, err)
	}
	if f.offer(r.as.name) == nil {
		f.t.Fatalf("%s was refused after %s left", r.as.name, r.left)
	}
	f.markAt(r.as)
}

func evictionAt(f *fixture, offset time.Duration) eviction {
	f.clock.now = f.base.Add(offset)
	before := f.listener.Stats()
	closedBefore := f.closedByTheListener()
	f.offer(newcomerName)
	closed := slices.DeleteFunc(f.closedByTheListener(), func(name string) bool {
		return slices.Contains(closedBefore, name)
	})
	return eviction{
		Closed: sorted(closed),
		Class:  classOf(before, f.listener.Stats()),
	}
}

func TestVictimOrder(t *testing.T) {
	type args struct {
		limit   int
		conns   []held
		rejoins []rejoin
		remarks []held
	}
	tests := []struct {
		name string
		args args
		want eviction
	}{
		{
			name: "a never authenticated connection goes before a lapsed one",
			args: args{
				limit: limitOfAll,
				conns: []held{
					markedBy("c1", "A", time.Second),
					never("c2"),
					markedBy("c3", "B", 2*time.Second),
					markedBy("c4", "C", 30*time.Second),
				},
			},
			want: eviction{
				Closed: []string{"c2"},
				Class:  "unauthenticated",
			},
		},
		{
			name: "a lapsed connection goes before one over the share",
			args: args{
				limit: limitOfAll,
				conns: []held{
					markedBy("c1", "A", 40*time.Second),
					markedBy("c2", "A", 41*time.Second),
					markedBy("c3", "A", 42*time.Second),
					markedBy("c4", "B", 5*time.Second),
				},
			},
			want: eviction{
				Closed: []string{"c4"},
				Class:  "lapsed",
			},
		},
		{
			name: "one token holds every connection: its oldest mark goes",
			args: args{
				limit: limitOfAll,
				conns: []held{
					markedBy("c1", "A", 43*time.Second),
					markedBy("c2", "A", 41*time.Second),
					markedBy("c3", "A", 42*time.Second),
					markedBy("c4", "A", 44*time.Second),
				},
			},
			want: eviction{
				Closed: []string{"c2"},
				Class:  "over share",
			},
		},
		{
			name: "two tokens of two at four: the junior token yields its oldest mark",
			args: args{
				limit: limitOfAll,
				conns: []held{
					markedBy("c1", "B", 30*time.Second),
					markedBy("c2", "B", 31*time.Second),
					markedBy("c3", "A", 21*time.Second),
					markedBy("c4", "A", 20*time.Second),
				},
			},
			want: eviction{
				Closed: []string{"c4"},
				Class:  "over share",
			},
		},
		{
			name: "four tokens of one at four: the newcomer is refused",
			args: args{
				limit: limitOfAll,
				conns: blocks(tokensOf(4, 1)...),
			},
			want: eviction{
				Closed: []string{newcomerName},
				Class:  refusedClass,
			},
		},
		{
			name: "two tokens of 32 at 64: share 21, the junior token yields",
			args: args{
				limit: limitOfIngester,
				conns: blocks(tokensOf(2, 32)...),
			},
			want: eviction{
				Closed: []string{"c33"},
				Class:  "over share",
			},
		},
		{
			name: "21, 21 and 22 at 64: share 16, the heaviest yields",
			args: args{
				limit: limitOfIngester,
				conns: blocks(block{"A", 21}, block{"B", 21}, block{"C", 22}),
			},
			want: eviction{
				Closed: []string{"c43"},
				Class:  "over share",
			},
		},
		{
			name: "16 tokens of 4 at 64: share 3, the junior token yields",
			args: args{
				limit: limitOfIngester,
				conns: blocks(tokensOf(16, 4)...),
			},
			want: eviction{
				Closed: []string{"c61"},
				Class:  "over share",
			},
		},
		{
			name: "32 tokens of 2 at 64: share 1, the junior token yields",
			args: args{
				limit: limitOfIngester,
				conns: blocks(tokensOf(32, 2)...),
			},
			want: eviction{
				Closed: []string{"c63"},
				Class:  "over share",
			},
		},
		{
			name: "64 tokens of 1 at 64: the newcomer is refused",
			args: args{
				limit: limitOfIngester,
				conns: blocks(tokensOf(64, 1)...),
			},
			want: eviction{
				Closed: []string{newcomerName},
				Class:  refusedClass,
			},
		},
		{
			name: "accepts of two tied tokens interleave: the token accepted first stays senior",
			args: args{
				limit: limitOfAll,
				conns: []held{
					markedBy("b1", "B", 15*time.Second),
					markedBy("a1", "A", 20*time.Second),
					markedBy("b2", "B", 16*time.Second),
					markedBy("a2", "A", 21*time.Second),
				},
			},
			want: eviction{
				Closed: []string{"a1"},
				Class:  "over share",
			},
		},
		{
			name: "a replica of the senior token rejoins: its earliest connection keeps the token senior",
			args: args{
				limit: limitOfAll,
				conns: []held{
					markedBy("b1", "B", 15*time.Second),
					markedBy("b2", "B", 16*time.Second),
					markedBy("a1", "A", 20*time.Second),
					markedBy("a2", "A", 21*time.Second),
				},
				rejoins: []rejoin{
					{
						left: "b2",
						as:   markedBy("b3", "B", 25*time.Second),
					},
				},
			},
			want: eviction{
				Closed: []string{"a1"},
				Class:  "over share",
			},
		},
		{
			name: "a connection marked again by another token counts under the new token only",
			args: args{
				limit: 3,
				conns: []held{
					markedBy("c1", "A", 15*time.Second),
					markedBy("c2", "B", 20*time.Second),
					markedBy("c3", "A", 25*time.Second),
				},
				remarks: []held{
					markedBy("c1", "B", 30*time.Second),
				},
			},
			want: eviction{
				Closed: []string{"c2"},
				Class:  "over share",
			},
		},
		{
			name: "of two never authenticated the earlier accepted goes",
			args: args{
				limit: 3,
				conns: []held{
					never("c1"),
					never("c2"),
					markedBy("c3", "A", 30*time.Second),
				},
			},
			want: eviction{
				Closed: []string{"c1"},
				Class:  "unauthenticated",
			},
		},
		{
			name: "of two lapsed the earlier mark goes",
			args: args{
				limit: 3,
				conns: []held{
					markedBy("c1", "A", 8*time.Second),
					markedBy("c2", "B", 3*time.Second),
					markedBy("c3", "C", 30*time.Second),
				},
			},
			want: eviction{
				Closed: []string{"c2"},
				Class:  "lapsed",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := settle(t, tt.args.limit, tt.args.conns)
			for _, r := range tt.args.rejoins {
				f.rejoin(r)
			}
			for _, conn := range tt.args.remarks {
				f.markAt(conn)
			}
			if diff := cmp.Diff(tt.want, evictionAt(f, victimAt)); diff != "" {
				t.Errorf("mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestAMarkLapsesAfterItsLifetime(t *testing.T) {
	type args struct {
		marks      []time.Duration
		newcomerAt time.Duration
	}
	tests := []struct {
		name string
		args args
		want eviction
	}{
		{
			name: "a newcomer a nanosecond before the mark lapses is refused",
			args: args{
				marks:      []time.Duration{0},
				newcomerAt: markLifetime - time.Nanosecond,
			},
			want: eviction{
				Closed: []string{newcomerName},
				Class:  refusedClass,
			},
		},
		{
			name: "a newcomer at the end of the lifetime evicts the lapsed connection",
			args: args{
				marks:      []time.Duration{0},
				newcomerAt: markLifetime,
			},
			want: eviction{
				Closed: []string{"c1"},
				Class:  "lapsed",
			},
		},
		{
			name: "a mark renewed half way keeps the connection fresh",
			args: args{
				marks:      []time.Duration{0, 30 * time.Second},
				newcomerAt: markLifetime,
			},
			want: eviction{
				Closed: []string{newcomerName},
				Class:  refusedClass,
			},
		},
		{
			name: "a renewed mark lapses a lifetime after its renewal",
			args: args{
				marks:      []time.Duration{0, 30 * time.Second},
				newcomerAt: 30*time.Second + markLifetime,
			},
			want: eviction{
				Closed: []string{"c1"},
				Class:  "lapsed",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(t, 1)
			base := f.base
			f.offer("c1")
			for _, at := range tt.args.marks {
				f.clock.now = base.Add(at)
				f.mark("c1", "A")
			}
			if diff := cmp.Diff(tt.want, evictionAt(f, tt.args.newcomerAt)); diff != "" {
				t.Errorf("mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

type flood struct {
	f        *fixture
	replicas map[string]int
	current  []string
	evicted  []int
	offered  int
}

func newFlood(t *testing.T, accepts string) *flood {
	t.Helper()
	fl := &flood{
		f:        newFixture(t, limitOfAll),
		replicas: map[string]int{},
		current:  make([]string, replicaCount),
		evicted:  make([]int, replicaCount),
	}
	replica := 0
	for _, accept := range accepts {
		if accept == attackerAccept {
			fl.connectAttacker()
			continue
		}
		fl.connectReplica(replica)
		replica++
	}
	if replica != replicaCount {
		t.Fatalf("accepts %q connect %d replicas, want %d", accepts, replica, replicaCount)
	}
	return fl
}

func (fl *flood) name(prefix string) string {
	fl.offered++
	return fmt.Sprintf("%s%d", prefix, fl.offered)
}

func (fl *flood) connectAttacker() {
	name := fl.name("a")
	if fl.f.offer(name) != nil {
		fl.f.mark(name, "A")
	}
}

func (fl *flood) connectReplica(replica int) {
	name := fl.name("b")
	if fl.f.offer(name) == nil {
		fl.f.t.Fatalf("replica %d of B was refused", replica)
	}
	fl.f.mark(name, "B")
	fl.replicas[name] = replica
	fl.current[replica] = name
}

func (fl *flood) rejoinReplica(replica int) {
	name := fl.current[replica]
	if err := fl.f.accepted[name].Close(); err != nil {
		fl.f.t.Fatalf("replica %d leaves: %v", replica, err)
	}
	fl.connectReplica(replica)
}

func (fl *flood) reconnectEvictedReplicas() {
	for replica, name := range fl.current {
		if fl.f.conns[name].closes > 0 {
			fl.evicted[replica]++
			fl.connectReplica(replica)
		}
	}
}

func (fl *flood) replicasCall(elapsed time.Duration) {
	for replica, name := range fl.current {
		phase := time.Duration(replica) * rpcPeriod / replicaCount
		if (elapsed+phase)%rpcPeriod < floodStep {
			fl.f.mark(name, "B")
		}
	}
}

func (fl *flood) replicasAllCall() {
	for _, name := range fl.current {
		fl.f.mark(name, "B")
	}
}

func (fl *flood) run(staggered, rejoining bool) (int, int) {
	fewestOpen := replicaCount
	base := fl.f.clock.now
	for step := range floodSteps {
		elapsed := time.Duration(step+1) * floodStep
		fl.f.clock.now = base.Add(elapsed)
		fl.call(staggered, step, elapsed)
		if rejoining {
			fl.rejoinReplica(replicaCount - 1)
		}
		fl.connectAttacker()
		fewestOpen = min(fewestOpen, fl.f.open(fl.current))
		fl.reconnectEvictedReplicas()
	}
	evictions := 0
	for _, count := range fl.evicted {
		evictions += count
	}
	return evictions, fewestOpen
}

func (fl *flood) call(staggered bool, step int, elapsed time.Duration) {
	if staggered {
		fl.replicasCall(elapsed)
		return
	}
	if step%seniorRefreshes == 0 {
		fl.replicasAllCall()
	}
}

func TestATieAmongTheHeaviestEvictsTheJuniorToken(t *testing.T) {
	type args struct {
		accepts   string
		staggered bool
		rejoining bool
	}
	type want struct {
		ReplicasEvicted bool
		FewestOpen      int
	}
	tests := []struct {
		name string
		args args
		want want
	}{
		{
			name: "a junior token floods two senior replicas: no replica is evicted",
			args: args{
				accepts: "bbaa",
			},
			want: want{
				FewestOpen: replicaCount,
			},
		},
		{
			name: "a senior token floods two junior replicas calling out of phase: the share of one stays open",
			args: args{
				accepts:   "aabb",
				staggered: true,
			},
			want: want{
				ReplicasEvicted: true,
				FewestOpen:      limitOfAll / (2 + 1),
			},
		},
		{
			name: "accepts interleave and a senior replica rejoins before every newcomer: no replica is evicted",
			args: args{
				accepts:   "baba",
				rejoining: true,
			},
			want: want{
				FewestOpen: replicaCount,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fl := newFlood(t, tt.args.accepts)
			evictions, fewestOpen := fl.run(tt.args.staggered, tt.args.rejoining)
			got := want{
				ReplicasEvicted: evictions > 0,
				FewestOpen:      fewestOpen,
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("%d replica evictions in %d newcomers (-want +got):\n%s", evictions, floodSteps, diff)
			}
		})
	}
}

func TestAThirdTokenEntersPastTwoTiedTokens(t *testing.T) {
	type args struct {
		limit int
		parts []block
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "two tokens of 2 at 4",
			args: args{
				limit: limitOfAll,
				parts: tokensOf(2, 2),
			},
		},
		{
			name: "two tokens of 32 at 64",
			args: args{
				limit: limitOfIngester,
				parts: tokensOf(2, 32),
			},
		},
		{
			name: "16 tokens of 4 at 64",
			args: args{
				limit: limitOfIngester,
				parts: tokensOf(16, 4),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := settle(t, tt.args.limit, blocks(tt.args.parts...))
			f.clock.now = f.base.Add(victimAt)
			if f.offer(newcomerName) == nil {
				t.Errorf("the newcomer of a third token was refused, want one tied token to yield")
			}
		})
	}
}
