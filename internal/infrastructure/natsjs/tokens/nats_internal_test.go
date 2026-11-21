package tokens

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.uber.org/goleak"

	"github.com/DmytroLysenko1/faultline/internal/application/tokenadmin"
	"github.com/DmytroLysenko1/faultline/internal/domain/credential"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/natsjstest"
)

const (
	connectTimeout = 2 * time.Second
	drainTimeout   = time.Second
	closeTimeout   = 5 * time.Second
	singleNode     = 1
	threeNodes     = 3
	clusterName    = "faultline"
	settleBudget   = 7 * time.Second
	lostBudget     = 4 * time.Second
	quorumWithin   = 20 * time.Second
	pollEvery      = 100 * time.Millisecond
	deadlineSlack  = 200 * time.Millisecond
	metaSettles    = 11 * time.Second
	metaLeaderless = 40 * time.Second
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

type bed struct {
	cluster *natsjstest.Cluster
	conn    *natsjs.Conn
	raw     *nats.Conn
	js      jetstream.JetStream
}

func startBed(t *testing.T, nodes int) *bed {
	t.Helper()
	cluster := natsjstest.Start(t, nodes)
	raw, err := nats.Connect(strings.Join(cluster.URLs(), ","))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(raw.Close)
	js, err := jetstream.New(raw)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	return &bed{
		cluster: cluster,
		conn:    dial(t, cluster.URLs()),
		raw:     raw,
		js:      js,
	}
}

func dial(t *testing.T, urls []string) *natsjs.Conn {
	t.Helper()
	conn, err := natsjs.Dial(t.Context(), &natsjs.ConnOptions{
		URLs:           urls,
		Name:           t.Name(),
		ConnectTimeout: connectTimeout,
		DrainTimeout:   drainTimeout,
	})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), closeTimeout)
		defer cancel()
		if err := conn.Close(ctx); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return conn
}

func (b *bed) createWith(t *testing.T, replicas int, change func(c *jetstream.StreamConfig)) {
	t.Helper()
	config := desiredConfig(replicas)
	change(&config)
	if _, err := b.js.CreateStream(t.Context(), config); err != nil {
		t.Fatalf("CreateStream: %v", err)
	}
}

func (b *bed) config(t *testing.T) jetstream.StreamConfig {
	t.Helper()
	stream, err := b.js.Stream(t.Context(), streamName)
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	return stream.CachedInfo().Config
}

func (b *bed) store(t *testing.T) *Store {
	t.Helper()
	return openStore(t, b.conn)
}

func openStore(t *testing.T, conn *natsjs.Conn) *Store {
	t.Helper()
	store, err := OpenStore(t.Context(), conn)
	if err != nil {
		t.Fatalf("OpenStore: %v", err)
	}
	return store
}

func settleContext(t *testing.T, budget time.Duration) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), budget)
	t.Cleanup(cancel)
	return ctx
}

func ids(grants []credential.Grant) []credential.ID {
	listed := make([]credential.ID, 0, len(grants))
	for i := range grants {
		listed = append(listed, grants[i].ID)
	}
	return listed
}

func mustCreate(t *testing.T, conn *natsjs.Conn, replicas int) {
	t.Helper()
	if err := CreateBucket(t.Context(), conn, replicas); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
}

func TestAFreshBucketMatchesTheDesiredConfig(t *testing.T) {
	for _, nodes := range []int{singleNode, threeNodes} {
		t.Run(map[int]string{singleNode: "R1", threeNodes: "R3"}[nodes], func(t *testing.T) {
			b := startBed(t, nodes)
			mustCreate(t, b.conn, nodes)
			actual := b.config(t)
			verdict := verdictOf(&actual)
			if notes := verdict.notes(); len(notes) > 0 || actual.Replicas != nodes {
				t.Errorf("a fresh bucket differs in %v, replicas %d", notes, actual.Replicas)
			}
			if err := CreateBucket(t.Context(), b.conn, nodes); err != nil {
				t.Errorf("a second CreateBucket = %v, want nil", err)
			}
		})
	}
}

func TestTheServerReportsNoUnknownConfigField(t *testing.T) {
	b := startBed(t, singleNode)
	mustCreate(t, b.conn, singleNode)
	reply, err := b.raw.RequestWithContext(t.Context(), infoSubject+streamName, nil)
	if err != nil {
		t.Fatalf("STREAM.INFO: %v", err)
	}
	var raw struct {
		Config map[string]json.RawMessage `json:"config"`
	}
	if err := json.Unmarshal(reply.Data, &raw); err != nil {
		t.Fatalf("decode: %v", err)
	}
	known := map[string]bool{}
	for field := range reflect.TypeFor[jetstream.StreamConfig]().Fields() {
		known[strings.Split(field.Tag.Get("json"), ",")[0]] = true
	}
	var unknown []string
	for key := range raw.Config {
		if !known[key] {
			unknown = append(unknown, key)
		}
	}
	if len(unknown) > 0 {
		slices.Sort(unknown)
		t.Errorf("the server reports config fields nats.go drops, so checkConfig cannot see them: %v", unknown)
	}
}

func TestReadStateOnASingleServerHasALeader(t *testing.T) {
	b := startBed(t, singleNode)
	mustCreate(t, b.conn, singleNode)
	reader := bucketReader{nc: b.conn.Control.NATS, stream: streamName}
	state, verdict, err := reader.readState(t.Context())
	if err != nil || state.Created.IsZero() || len(verdict.notes()) > 0 {
		t.Errorf("readState = %+v, %v, %v", state, verdict.notes(), err)
	}
}

func TestRegisterListRevokeOnTheRealBucket(t *testing.T) {
	b := startBed(t, singleNode)
	mustCreate(t, b.conn, singleNode)
	store := b.store(t)
	first, second := testGrant(t, 1), testGrant(t, 2)
	for _, grant := range []*credential.Grant{&first, &second} {
		if err := store.Register(settleContext(t, settleBudget), grant); err != nil {
			t.Fatalf("Register: %v", err)
		}
	}
	if err := store.Register(settleContext(t, settleBudget), &first); err != nil {
		t.Fatalf("a repeated Register of the same grant = %v, want its own record found", err)
	}
	listed, err := store.List(t.Context())
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if diff := cmp.Diff(ids([]credential.Grant{first, second}), ids(listed)); diff != "" {
		t.Errorf("listed ids (-want +got):\n%s", diff)
	}
	var got []tokenadmin.Revocation
	for range 2 {
		revocation, err := store.Revoke(settleContext(t, settleBudget), first.ID)
		if err != nil {
			t.Fatalf("Revoke: %v", err)
		}
		got = append(got, revocation)
	}
	if diff := cmp.Diff([]tokenadmin.Revocation{tokenadmin.Revoked, tokenadmin.NothingToRevoke}, got); diff != "" {
		t.Errorf("revocations (-want +got):\n%s", diff)
	}
}

func TestARegisteredGrantReadsBackWithItsDigest(t *testing.T) {
	b := startBed(t, singleNode)
	mustCreate(t, b.conn, singleNode)
	store := b.store(t)
	grant := testGrant(t, 3)
	if err := store.Register(settleContext(t, settleBudget), &grant); err != nil {
		t.Fatalf("Register: %v", err)
	}
	listed, err := store.List(t.Context())
	if err != nil || len(listed) != 1 {
		t.Fatalf("List = %v, %v", listed, err)
	}
	if !listed[0].Digest.Equal(grant.Digest) ||
		listed[0].Tenant != grant.Tenant ||
		!listed[0].IssuedAt.Equal(grant.IssuedAt) {
		t.Errorf("listed %+v, want %+v", listed[0], grant)
	}
}

func TestLeaderGetTable(t *testing.T) {
	b := startBed(t, singleNode)
	mustCreate(t, b.conn, singleNode)
	grant := testGrant(t, 4)
	if err := b.store(t).Register(settleContext(t, settleBudget), &grant); err != nil {
		t.Fatalf("Register: %v", err)
	}
	nc := b.conn.Control.NATS
	entry, found, err := leaderGet(t.Context(), nc, streamName, keyOf(grant.ID))
	if err != nil || !found || entry.Sequence != 1 || entry.Header.Get(nats.MsgIdHdr) != string(grant.ID) {
		t.Errorf("present key: %+v, %v, %v", entry, found, err)
	}
	if _, found, err := leaderGet(t.Context(), nc, streamName, keyOf("aaaaaaaaaaaa")); found || err != nil {
		t.Errorf("missing key: found %v, err %v", found, err)
	}
	_, _, err = leaderGet(t.Context(), nc, "KV_missing", keyOf(grant.ID))
	apiErr, isAPI := errors.AsType[*jetstream.APIError](err)
	if !isAPI || apiErr.ErrorCode != jetstream.JSErrCodeStreamNotFound {
		t.Errorf("missing stream: err = %v, want an API error 10059", err)
	}
}

func TestPurgeSubjectReportsTheCount(t *testing.T) {
	b := startBed(t, threeNodes)
	mustCreate(t, b.conn, threeNodes)
	grant := testGrant(t, 5)
	if err := b.store(t).Register(settleContext(t, settleBudget), &grant); err != nil {
		t.Fatalf("Register: %v", err)
	}
	var got []uint64
	for range 2 {
		purged, err := purge(t.Context(), b.conn.Control.NATS, streamName, keyOf(grant.ID))
		if err != nil {
			t.Fatalf("purge: %v", err)
		}
		got = append(got, purged)
	}
	if diff := cmp.Diff([]uint64{1, 0}, got); diff != "" {
		t.Errorf("purged (-want +got):\n%s", diff)
	}
}

func TestAFullBucketRefusesIssueButAlwaysRevokes(t *testing.T) {
	b := startBed(t, threeNodes)
	mustCreate(t, b.conn, threeNodes)
	store := b.store(t)
	ctx := settleContext(t, time.Minute)
	for seed := range MaxGrants {
		grant := uniqueGrant(t, seed)
		if err := store.Register(ctx, &grant); err != nil {
			t.Fatalf("Register %d: %v", seed, err)
		}
	}
	extra := uniqueGrant(t, MaxGrants)
	if err := store.Register(settleContext(t, settleBudget), &extra); !errors.Is(err, tokenadmin.ErrTokenLimit) {
		t.Fatalf("Register past the limit = %v, want ErrTokenLimit", err)
	}
	revocation, err := store.Revoke(settleContext(t, settleBudget), uniqueGrant(t, 0).ID)
	if err != nil || revocation != tokenadmin.Revoked {
		t.Fatalf("Revoke on a full bucket = %v, %v", revocation, err)
	}
	if err := store.Register(settleContext(t, settleBudget), &extra); err != nil {
		t.Errorf("Register after a revoke on a full bucket = %v", err)
	}
	listed, err := store.List(t.Context())
	if err != nil || len(listed) != MaxGrants {
		t.Errorf("List = %d grants, %v; want %d", len(listed), err, MaxGrants)
	}
}

func uniqueGrant(t *testing.T, index int) credential.Grant {
	t.Helper()
	random := make([]byte, tokenBytes)
	for position := range credential.IDLength {
		random[position] = byte(index >> (idSymbolBits * position))
	}
	random[credential.IDLength] = byte(index)
	_, grant, err := credential.Generate(strings.NewReader(string(random)), testTenant, issuedAt)
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	return grant
}

func TestReconcileNeedsRecreateForAnIrreversibleBucket(t *testing.T) {
	for name, change := range map[string]func(c *jetstream.StreamConfig){
		"deny purge": func(c *jetstream.StreamConfig) { c.DenyPurge = true },
		"sealed":     func(c *jetstream.StreamConfig) { c.Sealed = true },
	} {
		t.Run(name, func(t *testing.T) {
			b := startBed(t, singleNode)
			mustCreate(t, b.conn, singleNode)
			config := b.config(t)
			change(&config)
			if _, err := b.js.UpdateStream(t.Context(), config); err != nil {
				t.Fatalf("UpdateStream: %v", err)
			}
			if err := ReconcileBucket(t.Context(), b.conn, singleNode); !errors.Is(err, ErrNeedsRecreate) {
				t.Errorf("ReconcileBucket = %v, want ErrNeedsRecreate", err)
			}
		})
	}
}

func TestReconcileFixesDriftAndReadsItBack(t *testing.T) {
	b := startBed(t, singleNode)
	b.createWith(t, singleNode, func(c *jetstream.StreamConfig) {
		c.MaxAge = time.Hour
		c.NoAck = true
		c.Duplicates = time.Second
	})
	if err := ReconcileBucket(t.Context(), b.conn, singleNode); err != nil {
		t.Fatalf("ReconcileBucket: %v", err)
	}
	actual := b.config(t)
	if verdict := verdictOf(&actual); verdict.drifted() {
		t.Errorf("drift left after reconcile: %v", verdict.notes())
	}
}

func TestReconcileCreatesAMissingBucket(t *testing.T) {
	b := startBed(t, singleNode)
	if err := ReconcileBucket(t.Context(), b.conn, singleNode); err != nil {
		t.Fatalf("ReconcileBucket: %v", err)
	}
	if actual := b.config(t); actual.Name != streamName {
		t.Errorf("stream %q", actual.Name)
	}
}

func TestIssueNeverChangesAnExistingBucket(t *testing.T) {
	b := startBed(t, singleNode)
	b.createWith(t, singleNode, func(c *jetstream.StreamConfig) { c.MaxAge = time.Hour })
	before := b.config(t)
	if err := CreateBucket(t.Context(), b.conn, singleNode); err != nil {
		t.Fatalf("CreateBucket on a drifted bucket = %v", err)
	}
	if after := b.config(t); after.MaxAge != before.MaxAge {
		t.Errorf("CreateBucket changed MaxAge %s → %s", before.MaxAge, after.MaxAge)
	}
}

func TestInitNeverLowersReplicasOrDropsPlacement(t *testing.T) {
	b := startBed(t, threeNodes)
	placement := &jetstream.Placement{Cluster: clusterName}
	b.createWith(t, threeNodes, func(c *jetstream.StreamConfig) {
		c.Placement = placement
		c.NoAck = true
	})
	if err := ReconcileBucket(t.Context(), b.conn, singleNode); err != nil {
		t.Fatalf("ReconcileBucket: %v", err)
	}
	actual := b.config(t)
	if actual.Replicas != threeNodes || actual.Placement == nil || actual.Placement.Cluster != clusterName || actual.NoAck {
		t.Errorf("replicas %d, placement %+v, no ack %v", actual.Replicas, actual.Placement, actual.NoAck)
	}
}

func TestInitRaisesReplicas(t *testing.T) {
	b := startBed(t, threeNodes)
	mustCreate(t, b.conn, singleNode)
	if err := ReconcileBucket(t.Context(), b.conn, threeNodes); err != nil {
		t.Fatalf("ReconcileBucket: %v", err)
	}
	if actual := b.config(t); actual.Replicas != threeNodes {
		t.Errorf("replicas %d, want %d", actual.Replicas, threeNodes)
	}
}

func TestAnIrreversibleBucketRefusesWritesButLists(t *testing.T) {
	b := startBed(t, singleNode)
	mustCreate(t, b.conn, singleNode)
	grant := testGrant(t, 6)
	if err := b.store(t).Register(settleContext(t, settleBudget), &grant); err != nil {
		t.Fatalf("Register: %v", err)
	}
	config := b.config(t)
	config.DenyPurge = true
	if _, err := b.js.UpdateStream(t.Context(), config); err != nil {
		t.Fatalf("UpdateStream: %v", err)
	}
	store := b.store(t)
	other := testGrant(t, 7)
	if err := store.Register(settleContext(t, settleBudget), &other); !errors.Is(err, tokenadmin.ErrStoreUnavailable) ||
		!errors.Is(err, ErrIrreversible) {
		t.Errorf("Register = %v, want ErrStoreUnavailable and ErrIrreversible", err)
	}
	if _, err := store.Revoke(settleContext(t, settleBudget), grant.ID); !errors.Is(err, tokenadmin.ErrRevokeBlocked) ||
		!errors.Is(err, ErrIrreversible) {
		t.Errorf("Revoke = %v, want ErrRevokeBlocked from the bucket check, before any purge", err)
	}
	if listed, err := store.List(t.Context()); err != nil || len(listed) != 1 {
		t.Errorf("List = %v, %v", listed, err)
	}
	directory, err := OpenDirectory(t.Context(), b.conn, testOptions())
	if err != nil || !directory.Stats().RevokeBlocked || directory.Stats().Tokens != 1 {
		t.Errorf("OpenDirectory = %v; stats %+v", err, directory.Stats())
	}
}

func TestADriftedBucketStillRevokesAndLists(t *testing.T) {
	b := startBed(t, singleNode)
	mustCreate(t, b.conn, singleNode)
	grant := testGrant(t, 8)
	if err := b.store(t).Register(settleContext(t, settleBudget), &grant); err != nil {
		t.Fatalf("Register: %v", err)
	}
	config := b.config(t)
	config.MaxAge = time.Hour
	config.NoAck = true
	if _, err := b.js.UpdateStream(t.Context(), config); err != nil {
		t.Fatalf("UpdateStream: %v", err)
	}
	store := b.store(t)
	if diff := cmp.Diff([]string{"MaxAge", "NoAck"}, store.ConfigNotes()); diff != "" {
		t.Errorf("notes (-want +got):\n%s", diff)
	}
	other := testGrant(t, 9)
	if err := store.Register(settleContext(t, settleBudget), &other); !errors.Is(err, ErrBucketDrift) {
		t.Errorf("Register = %v, want ErrBucketDrift", err)
	}
	if listed, err := store.List(t.Context()); err != nil || len(listed) != 1 {
		t.Errorf("List = %v, %v", listed, err)
	}
	revocation, err := store.Revoke(settleContext(t, settleBudget), grant.ID)
	if err != nil || revocation != tokenadmin.Revoked {
		t.Errorf("Revoke = %v, %v; a drift must never block a revocation", revocation, err)
	}
	if _, err := OpenDirectory(t.Context(), b.conn, testOptions()); !errors.Is(err, ErrBucketDrift) || WaitsFor(err) {
		t.Errorf("OpenDirectory = %v, want ErrBucketDrift that does not wait", err)
	}
}

func TestOpenOnAMissingBucketWaits(t *testing.T) {
	b := startBed(t, singleNode)
	_, storeErr := OpenStore(t.Context(), b.conn)
	_, directoryErr := OpenDirectory(t.Context(), b.conn, testOptions())
	for _, err := range []error{storeErr, directoryErr} {
		if !errors.Is(err, ErrBucketMissing) || !WaitsFor(err) {
			t.Errorf("err = %v, want ErrBucketMissing that waits", err)
		}
	}
}

func TestAnInvalidEntryDisablesOnlyItsToken(t *testing.T) {
	b := startBed(t, singleNode)
	mustCreate(t, b.conn, singleNode)
	good := testGrant(t, 10)
	if err := b.store(t).Register(settleContext(t, settleBudget), &good); err != nil {
		t.Fatalf("Register: %v", err)
	}
	for _, value := range []string{`{"v":2}`, strings.Repeat("x", MaxValueBytes+1)} {
		bad := uniqueGrant(t, len(value))
		if _, err := b.js.Publish(t.Context(), keyOf(bad.ID), []byte(value)); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}
	directory, err := OpenDirectory(t.Context(), b.conn, testOptions())
	if err != nil {
		t.Fatalf("OpenDirectory: %v", err)
	}
	stats := directory.Stats()
	_, found := directory.Snapshot(t.Context()).Grants[good.ID]
	if !found || stats.Tokens != 1 || stats.InvalidEntries != 2 {
		t.Errorf("stats %+v, good grant found %v", stats, found)
	}
}

func TestMoreKeysThanTheLimitAreRefused(t *testing.T) {
	b := startBed(t, singleNode)
	b.createWith(t, singleNode, func(c *jetstream.StreamConfig) { c.MaxMsgs = -1; c.MaxBytes = -1 })
	for index := range MaxGrants + 1 {
		if _, err := b.js.Publish(t.Context(), keyOf(uniqueGrant(t, index).ID), []byte(`{}`)); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}
	reader := bucketReader{nc: b.conn.Control.NATS, stream: streamName}
	if _, _, err := reader.readEntries(t.Context()); !errors.Is(err, ErrTooManyKeys) {
		t.Errorf("readEntries = %v, want ErrTooManyKeys", err)
	}
}

func TestAnUnknownTopLevelSubjectIsNotAGrant(t *testing.T) {
	b := startBed(t, singleNode)
	mustCreate(t, b.conn, singleNode)
	if _, err := b.js.Publish(t.Context(), subjectPrefix+"NOT-AN-ID", []byte(`{}`)); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	directory, err := OpenDirectory(t.Context(), b.conn, testOptions())
	if err != nil || directory.Stats().Tokens != 0 || directory.Stats().InvalidEntries != 1 {
		t.Errorf("OpenDirectory = %v, stats %+v", err, directory.Stats())
	}
}

type lossyBed struct {
	*bed
	proxy *natsjstest.Proxy
	conn  *natsjs.Conn
}

func startLossy(t *testing.T) *lossyBed {
	t.Helper()
	b := startBed(t, singleNode)
	mustCreate(t, b.conn, singleNode)
	proxy := natsjstest.StartProxy(t, b.cluster.URLs()[0])
	return &lossyBed{
		bed:   b,
		proxy: proxy,
		conn:  dial(t, []string{proxy.URL()}),
	}
}

func once(prefix string, verdict func() natsjstest.Verdict) func(f *natsjstest.Frame) natsjstest.Verdict {
	spent := false
	return func(f *natsjstest.Frame) natsjstest.Verdict {
		if !strings.HasPrefix(f.Subject, prefix) || spent {
			return natsjstest.Forward()
		}
		spent = true
		return verdict()
	}
}

func TestRegisterSettlesALostReply(t *testing.T) {
	l := startLossy(t)
	store := openStore(t, l.conn)
	l.proxy.Intercept(once(subjectPrefix, natsjstest.LoseReply))
	grant := testGrant(t, 11)
	if err := store.Register(settleContext(t, settleBudget), &grant); err != nil {
		t.Fatalf("Register after a lost reply = %v, want the settle to find its own record", err)
	}
	if l.proxy.AnswersAltered() != 1 {
		t.Errorf("replies lost %d, want 1", l.proxy.AnswersAltered())
	}
}

func TestRegisterSettlesAnInProcessReply(t *testing.T) {
	l := startLossy(t)
	store := openStore(t, l.conn)
	inProcess := []byte(`{"error":{"code":503,"err_code":10158,"description":"duplicate message id is in process"}}`)
	l.proxy.Intercept(once(subjectPrefix, func() natsjstest.Verdict { return natsjstest.Answer(inProcess) }))
	grant := testGrant(t, 12)
	if err := store.Register(settleContext(t, settleBudget), &grant); err != nil {
		t.Fatalf("Register after a 10158 = %v", err)
	}
}

func TestADuplicateWithoutTheKeyIsUnknown(t *testing.T) {
	l := startLossy(t)
	store := openStore(t, l.conn)
	duplicate := []byte(`{"stream":"` + streamName + `","seq":1,"duplicate":true}`)
	noMessage := []byte(`{"error":{"code":404,"err_code":10037,"description":"no message found"}}`)
	l.proxy.Intercept(func(f *natsjstest.Frame) natsjstest.Verdict {
		switch {
		case strings.HasPrefix(f.Subject, subjectPrefix):
			return natsjstest.Answer(duplicate)
		case strings.HasPrefix(f.Subject, msgGetSubject):
			return natsjstest.Answer(noMessage)
		default:
			return natsjstest.Forward()
		}
	})
	grant := testGrant(t, 13)
	started := time.Now()
	err := store.Register(settleContext(t, settleBudget), &grant)
	if !errors.Is(err, tokenadmin.ErrOutcomeUnknown) || time.Since(started) > writeAttempt+settleRead {
		t.Errorf("Register = %v after %s; want unknown at once, without retrying a duplicate", err, time.Since(started))
	}
}

func TestRegisterWithLostRepliesEndsUnknownWithinTheBudget(t *testing.T) {
	l := startLossy(t)
	store := openStore(t, l.conn)
	l.proxy.Intercept(func(f *natsjstest.Frame) natsjstest.Verdict {
		if strings.HasPrefix(f.Subject, subjectPrefix) || strings.HasPrefix(f.Subject, msgGetSubject) {
			return natsjstest.LoseReply()
		}
		return natsjstest.Forward()
	})
	grant := testGrant(t, 14)
	started := time.Now()
	err := store.Register(settleContext(t, lostBudget), &grant)
	if !errors.Is(err, tokenadmin.ErrOutcomeUnknown) || time.Since(started) > lostBudget+deadlineSlack {
		t.Errorf("Register = %v after %s; want ErrOutcomeUnknown within %s", err, time.Since(started), lostBudget)
	}
}

func TestRevokeSettlesALostReply(t *testing.T) {
	l := startLossy(t)
	grant := testGrant(t, 15)
	if err := l.store(t).Register(settleContext(t, settleBudget), &grant); err != nil {
		t.Fatalf("Register: %v", err)
	}
	store := openStore(t, l.conn)
	l.proxy.Intercept(once(purgeSubject, natsjstest.LoseReply))
	revocation, err := store.Revoke(settleContext(t, settleBudget), grant.ID)
	if err != nil || revocation != tokenadmin.Absent {
		t.Errorf("Revoke after a lost reply = %v, %v; want Absent", revocation, err)
	}
	if _, found, err := leaderGet(t.Context(), l.raw, streamName, keyOf(grant.ID)); found || err != nil {
		t.Errorf("the key survived the revoke: found %v, err %v", found, err)
	}
}

func TestLostKVQuorumMakesReadsFail(t *testing.T) {
	b := startBed(t, threeNodes)
	mustCreate(t, b.conn, threeNodes)
	reader := bucketReader{nc: b.conn.Control.NATS, stream: streamName}
	if _, _, err := reader.readState(t.Context()); err != nil {
		t.Fatalf("readState with a quorum: %v", err)
	}
	b.cluster.Stop(1)
	b.cluster.Stop(2)
	deadline := time.Now().Add(quorumWithin)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(t.Context(), checkTime)
		_, _, err := reader.readState(ctx)
		cancel()
		if err != nil {
			if !WaitsFor(err) {
				t.Errorf("a lost quorum gives %v, which does not wait", err)
			}
			return
		}
		time.Sleep(pollEvery)
	}
	t.Errorf("reads still succeed %s after two of three nodes stopped", quorumWithin)
}

func TestWritesDuringAReconnectEndUnavailable(t *testing.T) {
	b := startBed(t, singleNode)
	mustCreate(t, b.conn, singleNode)
	store := b.store(t)
	b.cluster.Stop(0)
	awaitReconnecting(t, b.conn.Control.NATS)
	grant := testGrant(t, 16)
	calls := []func(ctx context.Context) error{
		func(ctx context.Context) error { return store.Register(ctx, &grant) },
		func(ctx context.Context) error {
			_, err := store.Revoke(ctx, grant.ID)
			return err
		},
	}
	for _, call := range calls {
		started := time.Now()
		err := call(settleContext(t, lostBudget))
		took := time.Since(started)
		if !errors.Is(err, tokenadmin.ErrStoreUnavailable) || errors.Is(err, tokenadmin.ErrOutcomeUnknown) {
			t.Errorf("err = %v; nothing was sent, so the store is unavailable, not unknown", err)
		}
		if took < lostBudget-writeAttempt-deliveryPause || took > lostBudget {
			t.Errorf("took %s; want retries until less than %s of the %s budget is left", took, writeAttempt, lostBudget)
		}
	}
}

func TestAFailedKeyReadFailsTheRereadInsteadOfDroppingTheGrant(t *testing.T) {
	l := startLossy(t)
	grant := testGrant(t, 17)
	if err := l.store(t).Register(settleContext(t, settleBudget), &grant); err != nil {
		t.Fatalf("Register: %v", err)
	}
	unavailable := []byte(`{"error":{"code":503,"err_code":10008,"description":"jetstream cluster not available"}}`)
	l.proxy.Intercept(func(f *natsjstest.Frame) natsjstest.Verdict {
		if strings.HasPrefix(f.Subject, msgGetSubject) {
			return natsjstest.Answer(unavailable)
		}
		return natsjstest.Forward()
	})
	reader := bucketReader{nc: l.conn.Control.NATS, stream: streamName}
	entries, _, err := reader.readEntries(t.Context())
	if err == nil || errors.Is(err, errInvalidEntry) {
		t.Errorf("readEntries = %+v, %v; a failed key read must fail the whole reread", entries, err)
	}
}

func TestAKeyPurgedDuringARereadIsAbsent(t *testing.T) {
	l := startLossy(t)
	kept, purged := testGrant(t, 18), testGrant(t, 19)
	for _, grant := range []*credential.Grant{&kept, &purged} {
		if err := l.store(t).Register(settleContext(t, settleBudget), grant); err != nil {
			t.Fatalf("Register: %v", err)
		}
	}
	raw := l.raw
	l.proxy.Intercept(func(f *natsjstest.Frame) natsjstest.Verdict {
		if strings.HasPrefix(f.Subject, msgGetSubject) && strings.Contains(string(f.Payload), string(purged.ID)) {
			if _, err := purge(t.Context(), raw, streamName, keyOf(purged.ID)); err != nil {
				t.Errorf("purge during the reread: %v", err)
			}
		}
		return natsjstest.Forward()
	})
	reader := bucketReader{nc: l.conn.Control.NATS, stream: streamName}
	entries, _, err := reader.readEntries(t.Context())
	if err != nil {
		t.Fatalf("readEntries: %v", err)
	}
	_, keptFound := entries.Grants[kept.ID]
	_, purgedFound := entries.Grants[purged.ID]
	if !keptFound || purgedFound || entries.Invalid != 0 {
		t.Errorf("entries %+v; want the purged key absent and nothing invalid", entries)
	}
}

func TestAnIDHeldByAnotherTenantIsTaken(t *testing.T) {
	b := startBed(t, singleNode)
	mustCreate(t, b.conn, singleNode)
	grant := testGrant(t, 20)
	other := grant
	other.Tenant = "bank"
	if err := b.store(t).Register(settleContext(t, settleBudget), &other); err != nil {
		t.Fatalf("Register of the other tenant: %v", err)
	}
	if err := b.store(t).Register(settleContext(t, settleBudget), &grant); !errors.Is(err, tokenadmin.ErrIDTaken) {
		t.Errorf("Register = %v, want ErrIDTaken for a grant of another tenant", err)
	}
	if listed, err := b.store(t).List(t.Context()); err != nil || len(listed) != 1 || listed[0].Tenant != "bank" {
		t.Errorf("List = %v, %v; the other tenant's grant must stay", listed, err)
	}
}

func TestAnInvalidRecordAtTheIDIsWithdrawn(t *testing.T) {
	b := startBed(t, singleNode)
	mustCreate(t, b.conn, singleNode)
	grant := testGrant(t, 24)
	value, err := encodeEntry(&grant)
	if err != nil {
		t.Fatalf("encodeEntry: %v", err)
	}
	if _, err := b.js.Publish(t.Context(), keyOf(grant.ID), value); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	err = b.store(t).Register(settleContext(t, settleBudget), &grant)
	if !errors.Is(err, ErrUnacceptedGrant) || !errors.Is(err, tokenadmin.ErrStoreUnavailable) {
		t.Errorf("Register = %v; a record the directory refuses is not a taken id", err)
	}
	if _, found, err := leaderGet(t.Context(), b.raw, streamName, keyOf(grant.ID)); found || err != nil {
		t.Errorf("the refused record stayed: found %v, err %v", found, err)
	}
}

func TestAReadFailureAfterAFreshPublishIsASuccess(t *testing.T) {
	l := startLossy(t)
	store := openStore(t, l.conn)
	l.proxy.Intercept(once(msgGetSubject, natsjstest.LoseReply))
	grant := testGrant(t, 25)
	if err := store.Register(settleContext(t, settleBudget), &grant); err != nil {
		t.Errorf("Register = %v; an acknowledged publish stays a success when the read-back is lost", err)
	}
}

func TestAKVDeleteMarkerIsNeitherAGrantNorInvalid(t *testing.T) {
	b := startBed(t, singleNode)
	mustCreate(t, b.conn, singleNode)
	kept, deleted := testGrant(t, 21), testGrant(t, 22)
	if err := b.store(t).Register(settleContext(t, settleBudget), &kept); err != nil {
		t.Fatalf("Register: %v", err)
	}
	marker := &nats.Msg{Subject: keyOf(deleted.ID), Header: nats.Header{operationHeader: []string{operationDelete}}}
	if _, err := b.js.PublishMsg(t.Context(), marker); err != nil {
		t.Fatalf("PublishMsg: %v", err)
	}
	directory, err := OpenDirectory(t.Context(), b.conn, testOptions())
	if err != nil {
		t.Fatalf("OpenDirectory: %v", err)
	}
	if stats := directory.Stats(); stats.Tokens != 1 || stats.InvalidEntries != 0 {
		t.Errorf("stats %+v; a delete marker hides its key and is not invalid", stats)
	}
}

func TestAFullBucketAfterALostPublishIsUnknown(t *testing.T) {
	l := startLossy(t)
	store := openStore(t, l.conn)
	filler := l.store(t)
	ctx := settleContext(t, time.Minute)
	for index := range MaxGrants {
		grant := uniqueGrant(t, index)
		if err := filler.Register(ctx, &grant); err != nil {
			t.Fatalf("Register %d: %v", index, err)
		}
	}
	l.proxy.Intercept(once(subjectPrefix, natsjstest.Drop))
	extra := uniqueGrant(t, MaxGrants)
	err := store.Register(settleContext(t, settleBudget), &extra)
	if !errors.Is(err, tokenadmin.ErrOutcomeUnknown) || errors.Is(err, tokenadmin.ErrTokenLimit) {
		t.Errorf("Register = %v; a full bucket after a publish that may have landed proves nothing", err)
	}
}

func TestAClosedConnectionIsUnavailable(t *testing.T) {
	b := startBed(t, singleNode)
	mustCreate(t, b.conn, singleNode)
	conn := dial(t, b.cluster.URLs())
	store := openStore(t, conn)
	conn.Control.NATS.Close()
	grant := testGrant(t, 23)
	registerErr := store.Register(settleContext(t, settleBudget), &grant)
	_, revokeErr := store.Revoke(settleContext(t, settleBudget), grant.ID)
	for _, err := range []error{registerErr, revokeErr} {
		if !errors.Is(err, tokenadmin.ErrStoreUnavailable) || errors.Is(err, tokenadmin.ErrOutcomeUnknown) {
			t.Errorf("err = %v; a closed connection sends nothing", err)
		}
	}
}

func TestListWithoutACallerDeadlineEnds(t *testing.T) {
	l := startLossy(t)
	store := openStore(t, l.conn)
	l.proxy.Intercept(func(f *natsjstest.Frame) natsjstest.Verdict {
		if strings.HasPrefix(f.Subject, infoSubject) {
			return natsjstest.LoseReply()
		}
		return natsjstest.Forward()
	})
	started := time.Now()
	_, err := store.List(t.Context())
	if !errors.Is(err, tokenadmin.ErrStoreUnavailable) || time.Since(started) > defaultBudget+deadlineSlack {
		t.Errorf("List = %v after %s; want unavailable within %s", err, time.Since(started), defaultBudget)
	}
}

func awaitReconnecting(t *testing.T, nc *nats.Conn) {
	t.Helper()
	deadline := time.Now().Add(quorumWithin)
	for !nc.IsReconnecting() {
		if time.Now().After(deadline) {
			t.Fatalf("the client did not notice the stopped server within %s", quorumWithin)
		}
		time.Sleep(pollEvery / 10)
	}
}

func TestOnlyABucketPublishIsAGrant(t *testing.T) {
	b := startBed(t, singleNode)
	mustCreate(t, b.conn, singleNode)
	legit := testGrant(t, 30)
	if err := b.store(t).Register(settleContext(t, settleBudget), &legit); err != nil {
		t.Fatalf("Register: %v", err)
	}
	expected := func(id credential.ID) nats.Header {
		return nats.Header{
			jetstream.MsgIDHeader:               []string{string(id)},
			jetstream.ExpectedStreamHeader:      []string{streamName},
			jetstream.ExpectedLastSubjSeqHeader: []string{"0"},
		}
	}
	forged := map[string]func(id credential.ID) nats.Header{
		"no headers": func(credential.ID) nats.Header { return nil },
		"an empty Nats-Stream before the real ones": func(id credential.ID) nats.Header {
			header := expected(id)
			header["Nats-Stream"] = []string{""}
			return header
		},
		"another message id": func(id credential.ID) nats.Header {
			header := expected(id)
			header[jetstream.MsgIDHeader] = []string{"other"}
			return header
		},
		"a doubled expected stream": func(id credential.ID) nats.Header {
			header := expected(id)
			header[jetstream.ExpectedStreamHeader] = []string{streamName, streamName}
			return header
		},
		"a lower case key": func(id credential.ID) nats.Header {
			header := expected(id)
			delete(header, jetstream.MsgIDHeader)
			header["nats-msg-id"] = []string{string(id)}
			return header
		},
	}
	index := 0
	for name, header := range forged {
		grant := uniqueGrant(t, 900+index)
		index++
		value, err := encodeEntry(&grant)
		if err != nil {
			t.Fatalf("encodeEntry: %v", err)
		}
		msg := &nats.Msg{Subject: keyOf(grant.ID), Data: value, Header: header(grant.ID)}
		if _, err := b.raw.RequestMsgWithContext(t.Context(), msg); err != nil {
			t.Fatalf("%s: publish: %v", name, err)
		}
	}
	reader := bucketReader{nc: b.conn.Control.NATS, stream: streamName}
	entries, _, err := reader.readEntries(t.Context())
	if err != nil {
		t.Fatalf("readEntries: %v", err)
	}
	_, found := entries.Grants[legit.ID]
	if !found || len(entries.Grants) != 1 || entries.Invalid != uint64(len(forged)) {
		t.Errorf("grants %d, invalid %d; want only the registered grant and %d invalid",
			len(entries.Grants), entries.Invalid, len(forged))
	}
}

func TestARegisterWritesExactlyItsThreeHeaders(t *testing.T) {
	b := startBed(t, singleNode)
	mustCreate(t, b.conn, singleNode)
	grant := testGrant(t, 31)
	if err := b.store(t).Register(settleContext(t, settleBudget), &grant); err != nil {
		t.Fatalf("Register: %v", err)
	}
	entry, found, err := leaderGet(t.Context(), b.raw, streamName, keyOf(grant.ID))
	if err != nil || !found {
		t.Fatalf("leaderGet = %v, %v", found, err)
	}
	if !published(entry, grant.ID) {
		t.Errorf("a registered entry carries %v; the reader admits only the three publish headers", entry.Header)
	}
}

func TestARepublishIntoTheBucketIsNoGrant(t *testing.T) {
	b := startBed(t, singleNode)
	mustCreate(t, b.conn, singleNode)
	grant := testGrant(t, 32)
	value, err := encodeEntry(&grant)
	if err != nil {
		t.Fatalf("encodeEntry: %v", err)
	}
	_, err = b.js.CreateStream(t.Context(), jetstream.StreamConfig{
		Name:      "EVIL",
		Subjects:  []string{"evil.x"},
		Storage:   jetstream.MemoryStorage,
		RePublish: &jetstream.RePublish{Source: "evil.x", Destination: keyOf(grant.ID)},
	})
	if err != nil {
		t.Fatalf("CreateStream: %v", err)
	}
	msg := &nats.Msg{Subject: "evil.x", Data: value, Header: nats.Header{
		jetstream.MsgIDHeader:               []string{string(grant.ID)},
		jetstream.ExpectedLastSubjSeqHeader: []string{"0"},
	}}
	if _, err := b.js.PublishMsg(t.Context(), msg); err != nil {
		t.Fatalf("PublishMsg: %v", err)
	}
	reader := bucketReader{nc: b.conn.Control.NATS, stream: streamName}
	deadline := time.Now().Add(quorumWithin)
	for time.Now().Before(deadline) {
		entries, _, err := reader.readEntries(t.Context())
		if err != nil {
			t.Fatalf("readEntries: %v", err)
		}
		if entries.Invalid == 1 {
			if len(entries.Grants) != 0 {
				t.Errorf("a republished value became a grant")
			}
			return
		}
		time.Sleep(pollEvery)
	}
	t.Errorf("the republished value never reached the bucket")
}

func rawPublish(t *testing.T, url, subject, header, payload string) {
	t.Helper()
	conn, err := new(net.Dialer).DialContext(t.Context(), "tcp", strings.TrimPrefix(url, "nats://"))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Logf("close the raw connection: %v", err)
		}
	})
	reader := bufio.NewReader(conn)
	if _, err := reader.ReadString('\n'); err != nil {
		t.Fatalf("INFO: %v", err)
	}
	frame := fmt.Sprintf("CONNECT {\"headers\":true,\"verbose\":false,\"protocol\":1}\r\nHPUB %s %d %d\r\n%s%s\r\nPING\r\n",
		subject, len(header), len(header)+len(payload), header, payload)
	if _, err := conn.Write([]byte(frame)); err != nil {
		t.Fatalf("write: %v", err)
	}
	if line, err := reader.ReadString('\n'); err != nil || !strings.HasPrefix(line, "PONG") {
		t.Fatalf("server answered %q, %v", line, err)
	}
}

func TestAnUndecodableHeaderDisablesOnlyItsKey(t *testing.T) {
	b := startBed(t, singleNode)
	mustCreate(t, b.conn, singleNode)
	good := testGrant(t, 33)
	if err := b.store(t).Register(settleContext(t, settleBudget), &good); err != nil {
		t.Fatalf("Register: %v", err)
	}
	rawPublish(t, b.cluster.URLs()[0], keyOf(uniqueGrant(t, 950).ID), "NATS/1.0\r\nbogus line without a colon\r\n\r\n", "x")
	directory, err := OpenDirectory(t.Context(), b.conn, testOptions())
	if err != nil {
		t.Fatalf("OpenDirectory: %v", err)
	}
	if stats := directory.Stats(); stats.Tokens != 1 || stats.InvalidEntries != 1 {
		t.Errorf("stats %+v; a broken header must disable only its own key", stats)
	}
}

func lineFeedHeader(id credential.ID, tail string) string {
	return "NATS/1.0\r\nNats-Msg-Id: " + string(id) +
		"\nNats-Expected-Stream: " + streamName +
		"\nNats-Expected-Last-Subject-Sequence: 0" + tail
}

func TestHeadersSplitByABareLineFeedAreNoGrant(t *testing.T) {
	b := startBed(t, singleNode)
	mustCreate(t, b.conn, singleNode)
	direct, republished := uniqueGrant(t, 960), uniqueGrant(t, 961)
	_, err := b.js.CreateStream(t.Context(), jetstream.StreamConfig{
		Name:      "EVILRP",
		Subjects:  []string{"evil.rp"},
		Storage:   jetstream.MemoryStorage,
		RePublish: &jetstream.RePublish{Source: "evil.rp", Destination: keyOf(republished.ID)},
	})
	if err != nil {
		t.Fatalf("CreateStream: %v", err)
	}
	for _, forged := range []struct {
		subject string
		grant   credential.Grant
		tail    string
	}{
		{subject: keyOf(direct.ID), grant: direct, tail: "\r\n\r\n"},
		{subject: "evil.rp", grant: republished, tail: "\n\nPad: x\r\n\r\n"},
	} {
		value, err := encodeEntry(&forged.grant)
		if err != nil {
			t.Fatalf("encodeEntry: %v", err)
		}
		rawPublish(t, b.cluster.URLs()[0], forged.subject, lineFeedHeader(forged.grant.ID, forged.tail), string(value))
	}
	reader := bucketReader{nc: b.conn.Control.NATS, stream: streamName}
	deadline := time.Now().Add(quorumWithin)
	for time.Now().Before(deadline) {
		entries, _, err := reader.readEntries(t.Context())
		if err != nil {
			t.Fatalf("readEntries: %v", err)
		}
		if entries.Invalid == 2 {
			if len(entries.Grants) != 0 {
				t.Errorf("headers the server never parsed became a grant")
			}
			return
		}
		time.Sleep(pollEvery)
	}
	t.Errorf("the forged values never reached the bucket")
}

func TestARevokeAnsweredByANonLeaderIsUnknownNotUnavailable(t *testing.T) {
	const nodes = 5
	cluster := natsjstest.Start(t, nodes)
	conn := dial(t, cluster.URLs())
	mustCreate(t, conn, threeNodes)
	store := openStore(t, conn)
	grant := testGrant(t, 34)
	if err := store.Register(settleContext(t, settleBudget), &grant); err != nil {
		t.Fatalf("Register: %v", err)
	}
	reader := bucketReader{nc: conn.Control.NATS, stream: streamName}
	info, err := reader.info(t.Context(), "")
	if err != nil {
		t.Fatalf("info: %v", err)
	}
	kept := map[string]bool{info.Cluster.Leader: true, info.Cluster.Replicas[0].Name: true}
	time.Sleep(metaSettles)
	var keptNode int
	for node, running := range cluster.Servers {
		name := running.Name()
		if !kept[name] {
			cluster.Stop(node)
			continue
		}
		if name != info.Cluster.Leader {
			keptNode = node
		}
	}
	client, err := nats.Connect(cluster.Servers[keptNode].ClientURL())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer client.Close()
	awaitMetaLeaderless(t, client)
	js, err := jetstream.New(client)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	nonLeader := &Store{reader: bucketReader{nc: client, stream: streamName}, js: js}
	_, revokeErr := nonLeader.Revoke(settleContext(t, settleBudget), grant.ID)
	if errors.Is(revokeErr, tokenadmin.ErrStoreUnavailable) {
		t.Errorf("Revoke = %v; a non-leader answer may follow an applied purge, so it is never unavailable", revokeErr)
	}
}

func awaitMetaLeaderless(t *testing.T, client *nats.Conn) {
	t.Helper()
	deadline := time.Now().Add(metaLeaderless)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		_, err := purge(ctx, client, streamName, keyOf("aaaaaaaaaaaa"))
		cancel()
		if natsjs.HasCode(err, codeClusterUnavailable) {
			return
		}
		time.Sleep(pollEvery)
	}
	t.Fatalf("the meta group kept a leader for %s", metaLeaderless)
}

func TestARegisterTheDirectoryWouldRefuseIsAnError(t *testing.T) {
	l := startLossy(t)
	store := openStore(t, l.conn)
	grant := testGrant(t, 35)
	value, err := encodeEntry(&grant)
	if err != nil {
		t.Fatalf("encodeEntry: %v", err)
	}
	stripped := fmt.Sprintf(`{"message":{"subject":%q,"seq":1,"hdrs":%q,"data":%q,"time":"2026-11-22T00:00:00Z"}}`,
		keyOf(grant.ID),
		base64.StdEncoding.EncodeToString([]byte("NATS/1.0\r\nNats-Msg-Id: "+string(grant.ID)+"\r\n\r\n")),
		base64.StdEncoding.EncodeToString(value),
	)
	l.proxy.Intercept(once(msgGetSubject, func() natsjstest.Verdict { return natsjstest.Answer([]byte(stripped)) }))
	err = store.Register(settleContext(t, settleBudget), &grant)
	if !errors.Is(err, ErrUnacceptedGrant) || !errors.Is(err, tokenadmin.ErrStoreUnavailable) {
		t.Errorf("Register = %v; a grant stored without its publish headers is dead and must not be reported as issued", err)
	}
	if _, found, err := leaderGet(t.Context(), l.raw, streamName, keyOf(grant.ID)); found || err != nil {
		t.Errorf("the refused record stayed in the bucket: found %v, err %v", found, err)
	}
}

func TestARefusedRecordThatCannotBeWithdrawnIsUnknown(t *testing.T) {
	l := startLossy(t)
	store := openStore(t, l.conn)
	grant := testGrant(t, 36)
	value, err := encodeEntry(&grant)
	if err != nil {
		t.Fatalf("encodeEntry: %v", err)
	}
	stripped := fmt.Sprintf(`{"message":{"subject":%q,"seq":1,"hdrs":%q,"data":%q,"time":"2026-11-22T00:00:00Z"}}`,
		keyOf(grant.ID),
		base64.StdEncoding.EncodeToString([]byte("NATS/1.0\r\nNats-Msg-Id: "+string(grant.ID)+"\r\n\r\n")),
		base64.StdEncoding.EncodeToString(value),
	)
	l.proxy.Intercept(func(f *natsjstest.Frame) natsjstest.Verdict {
		switch {
		case strings.HasPrefix(f.Subject, msgGetSubject):
			return natsjstest.Answer([]byte(stripped))
		case strings.HasPrefix(f.Subject, purgeSubject):
			return natsjstest.Drop()
		default:
			return natsjstest.Forward()
		}
	})
	err = store.Register(settleContext(t, settleBudget), &grant)
	if !errors.Is(err, ErrUnacceptedGrant) ||
		!errors.Is(err, tokenadmin.ErrOutcomeUnknown) ||
		errors.Is(err, tokenadmin.ErrStoreUnavailable) {
		t.Errorf("Register = %v; a refused record that may remain must be compensated", err)
	}
}

func TestAWithdrawalWhileOurPublishMayLandIsUnknown(t *testing.T) {
	l := startLossy(t)
	grant := testGrant(t, 26)
	value, err := encodeEntry(&grant)
	if err != nil {
		t.Fatalf("encodeEntry: %v", err)
	}
	if _, err := l.js.Publish(t.Context(), keyOf(grant.ID), value); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	store := openStore(t, l.conn)
	l.proxy.Intercept(once(subjectPrefix, natsjstest.LoseReply))
	err = store.Register(settleContext(t, settleBudget), &grant)
	if !errors.Is(err, ErrUnacceptedGrant) ||
		!errors.Is(err, tokenadmin.ErrOutcomeUnknown) ||
		errors.Is(err, tokenadmin.ErrStoreUnavailable) {
		t.Errorf("Register = %v; after a lost reply our publish may still land, so nothing is proven", err)
	}
}

func TestADuplicateOfARefusedRecordIsUnknown(t *testing.T) {
	l := startLossy(t)
	store := openStore(t, l.conn)
	grant := testGrant(t, 27)
	value, err := encodeEntry(&grant)
	if err != nil {
		t.Fatalf("encodeEntry: %v", err)
	}
	duplicate := []byte(`{"stream":"` + streamName + `","seq":1,"duplicate":true}`)
	stripped := fmt.Sprintf(`{"message":{"subject":%q,"seq":1,"hdrs":%q,"data":%q,"time":"2026-11-22T00:00:00Z"}}`,
		keyOf(grant.ID),
		base64.StdEncoding.EncodeToString([]byte("NATS/1.0\r\nNats-Msg-Id: "+string(grant.ID)+"\r\n\r\n")),
		base64.StdEncoding.EncodeToString(value),
	)
	l.proxy.Intercept(func(f *natsjstest.Frame) natsjstest.Verdict {
		switch {
		case strings.HasPrefix(f.Subject, subjectPrefix):
			return natsjstest.Answer(duplicate)
		case strings.HasPrefix(f.Subject, msgGetSubject):
			return natsjstest.Answer([]byte(stripped))
		default:
			return natsjstest.Forward()
		}
	})
	err = store.Register(settleContext(t, settleBudget), &grant)
	if !errors.Is(err, ErrUnacceptedGrant) || !errors.Is(err, tokenadmin.ErrOutcomeUnknown) {
		t.Errorf("Register = %v; a deduplicated attempt may still have a twin in flight", err)
	}
}

func TestAForeignGrantSeenAfterAFreshPublishIsTaken(t *testing.T) {
	l := startLossy(t)
	store := openStore(t, l.conn)
	grant := testGrant(t, 28)
	other := grant
	other.Tenant = "bank"
	value, err := encodeEntry(&other)
	if err != nil {
		t.Fatalf("encodeEntry: %v", err)
	}
	header := "NATS/1.0\r\nNats-Msg-Id: " + string(grant.ID) +
		"\r\nNats-Expected-Stream: " + streamName +
		"\r\nNats-Expected-Last-Subject-Sequence: 0\r\n\r\n"
	foreign := fmt.Sprintf(`{"message":{"subject":%q,"seq":1,"hdrs":%q,"data":%q,"time":"2026-11-22T00:00:00Z"}}`,
		keyOf(grant.ID),
		base64.StdEncoding.EncodeToString([]byte(header)),
		base64.StdEncoding.EncodeToString(value),
	)
	l.proxy.Intercept(once(msgGetSubject, func() natsjstest.Verdict { return natsjstest.Answer([]byte(foreign)) }))
	if err := store.Register(settleContext(t, settleBudget), &grant); !errors.Is(err, tokenadmin.ErrIDTaken) {
		t.Errorf("Register = %v; a grant of another tenant under the key is not our success", err)
	}
}
