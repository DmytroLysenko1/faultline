package otlp_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/DmytroLysenko1/faultline/internal/application/ingest"
	"github.com/DmytroLysenko1/faultline/internal/application/ingestauth"
	"github.com/DmytroLysenko1/faultline/internal/domain/credential"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp/admission"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp/otlptest"
)

const (
	shareMessage        = "ingest token uses its share of request slots, retry later"
	slotsMessage        = "ingest request slots are busy, retry later"
	rateMessage         = "ingest request rate exceeded, retry later"
	eventAdmission      = "otlp.admission.rejected"
	eventTooSlow        = "otlp.export.too_slow"
	releaseWait         = 5 * time.Second
	releasePoll         = time.Millisecond
	notReached          = -1
	grantedTokenID      = "grantedid001"
	admissionRefusals   = 1000
	otherTokensForSlots = 10
)

var errAcceptorFailed = errors.New("acceptor failed")

type gateSetup func(t *testing.T, gate admission.Gate)

type admissionMarks interface {
	Authenticated(remote net.Addr, id credential.ID)
}

func gateWith(t *testing.T, limits admission.Limits, marks admissionMarks) *admission.Gate {
	t.Helper()
	books, err := admission.New(limits, &fakeClock{
		now: time.Unix(0, 0),
	})
	if err != nil {
		t.Fatalf("admission.New: %v", err)
	}
	if marks == nil {
		marks = otlptest.NoMarks{}
	}
	return &admission.Gate{
		Admission: books,
		Marks:     marks,
	}
}

func tokenCredentials() []grpc.DialOption {
	return []grpc.DialOption{otlptest.WithTestToken()}
}

func frozenGate(t *testing.T, requestsPerSecond int) *admission.Gate {
	t.Helper()
	return gateWith(t, otlp.AdmissionLimits(requestsPerSecond), nil)
}

func holdDirectly(t *testing.T, gate admission.Gate, id credential.ID, count int) {
	t.Helper()
	for index := range count {
		if _, err := gate.Enter(id, nil); err != nil {
			t.Fatalf("%s hold %d: %v", id, index+1, err)
		}
	}
}

func shareSpent(t *testing.T, gate admission.Gate) {
	t.Helper()
	holdDirectly(t, gate, otlptest.TokenID, otlp.RequestSlots)
}

func slotsTaken(t *testing.T, gate admission.Gate) {
	t.Helper()
	for index := range otherTokensForSlots {
		holdDirectly(t, gate, otherToken(index), 1)
	}
}

func rateSpent(t *testing.T, gate admission.Gate) {
	t.Helper()
	release, err := gate.Enter(otlptest.TokenID, nil)
	if err != nil {
		t.Fatalf("spend the only token of the budget: %v", err)
	}
	release()
}

func otherToken(index int) credential.ID {
	return credential.ID(fmt.Sprintf("other%07d", index))
}

type refusalCase struct {
	name  string
	rate  int
	setup gateSetup
	want  statusWant
}

func refusalCases() []refusalCase {
	return []refusalCase{
		{
			name:  "over the share",
			rate:  otlp.MaxRequestsPerSecond,
			setup: shareSpent,
			want: statusWant{
				Code:       codes.ResourceExhausted,
				Message:    shareMessage,
				RetryDelay: time.Second,
			},
		},
		{
			name:  "no free slot",
			rate:  otlp.MaxRequestsPerSecond,
			setup: slotsTaken,
			want: statusWant{
				Code:       codes.ResourceExhausted,
				Message:    slotsMessage,
				RetryDelay: time.Second,
			},
		},
		{
			name:  "the rate budget spent",
			rate:  1,
			setup: rateSpent,
			want: statusWant{
				Code:       codes.ResourceExhausted,
				Message:    rateMessage,
				RetryDelay: time.Second,
			},
		},
	}
}

func statusOf(err error) statusWant {
	st := status.Convert(err)
	return statusWant{
		Code:       st.Code(),
		Message:    st.Message(),
		RetryDelay: retryDelay(st),
	}
}

func TestEveryAdmissionRejectionIsResourceExhaustedWithRetryInfo(t *testing.T) {
	for _, tt := range refusalCases() {
		t.Run(tt.name, func(t *testing.T) {
			acceptor := &countingAcceptor{}
			gate := frozenGate(t, tt.rate)
			tt.setup(t, *gate)
			h := startWith(t, setup{
				acceptor:      acceptor.accept,
				authenticator: otlptest.Admitting(),
				gate:          gate,
				credentials:   tokenCredentials(),
			})
			_, err := h.Client.Export(t.Context(), requestOf(1))
			type outcome struct {
				Status        statusWant
				AcceptorCalls int64
			}
			want := outcome{
				Status: tt.want,
			}
			got := outcome{
				Status:        statusOf(err),
				AcceptorCalls: acceptor.calls.Load(),
			}
			if diff := cmp.Diff(want, got); diff != "" {
				t.Errorf("mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

type slotProbe struct {
	gate    *admission.Gate
	lock    sync.Mutex
	insides []int
}

func (p *slotProbe) record() {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.insides = append(p.insides, p.gate.Admission.Stats().InUse)
}

func (p *slotProbe) seen() []int {
	p.lock.Lock()
	defer p.lock.Unlock()
	if len(p.insides) == 0 {
		return []int{notReached}
	}
	return append([]int(nil), p.insides...)
}

type releaseCase struct {
	name     string
	acceptor func(probe *slotProbe) acceptorFunc
	call     func(t *testing.T, h harness) codes.Code
	want     releaseOutcome
}

type releaseOutcome struct {
	Code         codes.Code
	InsideExport []int
	After        int
	Admitted     uint64
}

func recordingThen(answer func(ctx context.Context) (ingest.AcceptResult, error)) func(probe *slotProbe) acceptorFunc {
	return func(probe *slotProbe) acceptorFunc {
		return func(ctx context.Context, _ ingest.AcceptCommand) (ingest.AcceptResult, error) {
			probe.record()
			return answer(ctx)
		}
	}
}

func exportOne(t *testing.T, h harness) codes.Code {
	t.Helper()
	_, err := h.Client.Export(t.Context(), requestOf(1))
	return status.Code(err)
}

func exportMalformed(t *testing.T, h harness) codes.Code {
	t.Helper()
	return status.Code(sendRaw(t.Context(), h.Conn, append(malformed(), 0xff)))
}

func exportThenCancel(t *testing.T, h harness) codes.Code {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()
	_, err := h.Client.Export(ctx, requestOf(1))
	return status.Code(err)
}

func releaseCases() []releaseCase {
	return []releaseCase{
		{
			name: "accepted",
			acceptor: recordingThen(func(context.Context) (ingest.AcceptResult, error) {
				return ingest.AcceptResult{}, nil
			}),
			call: exportOne,
			want: releaseOutcome{
				Code:         codes.OK,
				InsideExport: []int{1},
			},
		},
		{
			name: "a malformed protobuf",
			acceptor: recordingThen(func(context.Context) (ingest.AcceptResult, error) {
				return ingest.AcceptResult{}, nil
			}),
			call: exportMalformed,
			want: releaseOutcome{
				Code:         codes.Internal,
				InsideExport: []int{notReached},
			},
		},
		{
			name: "the acceptor fails",
			acceptor: recordingThen(func(context.Context) (ingest.AcceptResult, error) {
				return ingest.AcceptResult{}, fmt.Errorf("append: %w", errors.Join(ingest.ErrUnavailable, errAcceptorFailed))
			}),
			call: exportOne,
			want: releaseOutcome{
				Code:         codes.Unavailable,
				InsideExport: []int{1},
			},
		},
		{
			name: "the client cancels",
			acceptor: recordingThen(func(ctx context.Context) (ingest.AcceptResult, error) {
				<-ctx.Done()
				return ingest.AcceptResult{}, fmt.Errorf("append: %w", ctx.Err())
			}),
			call: exportThenCancel,
			want: releaseOutcome{
				Code:         codes.Canceled,
				InsideExport: []int{1},
			},
		},
		{
			name: "the acceptor panics",
			acceptor: recordingThen(func(context.Context) (ingest.AcceptResult, error) {
				panic(journalSecret)
			}),
			call: exportOne,
			want: releaseOutcome{
				Code:         codes.Internal,
				InsideExport: []int{1},
			},
		},
	}
}

func TestEveryAdmittedRPCReleasesItsSlot(t *testing.T) {
	for _, tt := range releaseCases() {
		t.Run(tt.name, func(t *testing.T) {
			probe := &slotProbe{
				gate: frozenGate(t, otlp.MaxRequestsPerSecond),
			}
			h := startWith(t, setup{
				acceptor:      tt.acceptor(probe),
				authenticator: otlptest.Admitting(),
				gate:          probe.gate,
				credentials:   tokenCredentials(),
			})
			code := tt.call(t, h)
			stats := settledStats(probe.gate)
			got := releaseOutcome{
				Code:         code,
				InsideExport: probe.seen(),
				After:        stats.InUse,
				Admitted:     stats.Admitted,
			}
			want := tt.want
			want.Admitted = 1
			if diff := cmp.Diff(want, got); diff != "" {
				t.Errorf("mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func settledStats(gate *admission.Gate) admission.Stats {
	deadline := time.Now().Add(releaseWait)
	for {
		stats := gate.Admission.Stats()
		if stats.InUse == 0 || time.Now().After(deadline) {
			return stats
		}
		time.Sleep(releasePoll)
	}
}

type markRecorder struct {
	lock  sync.Mutex
	marks []string
}

func (m *markRecorder) Authenticated(remote net.Addr, id credential.ID) {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.marks = append(m.marks, remote.String()+" "+string(id))
}

func (m *markRecorder) recorded() []string {
	m.lock.Lock()
	defer m.lock.Unlock()
	return append([]string(nil), m.marks...)
}

func grantingAnother(ctx context.Context, token string) (ingestauth.Principal, error) {
	principal, err := byToken(ctx, token)
	if err != nil {
		return principal, err
	}
	return ingestauth.Principal{
		TokenID: grantedTokenID,
	}, nil
}

func TestEveryTapOutcomeMovesTheMarkAsRuled(t *testing.T) {
	tests := []struct {
		name string
		args sent
		want []string
	}{
		{
			name: "an admitted token marks with the id of its grant",
			args: carrying(bearerPrefix + otlptest.Token),
			want: []string{"bufconn " + grantedTokenID},
		},
		{
			name: "no authorization header",
			args: carrying(),
		},
		{
			name: "another scheme",
			args: carrying(otherSchemePrefix + otlptest.Token),
		},
		{
			name: "a rejected token",
			args: bearing(unknownTokenID),
		},
		{
			name: "a stale directory",
			args: bearing(staleTokenID),
		},
		{
			name: "a broken directory",
			args: bearing(brokenTokenID),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			marks := &markRecorder{}
			gate := gateWith(t, otlp.AdmissionLimits(otlp.MaxRequestsPerSecond), marks)
			h := startWith(t, setup{
				acceptor:      returning(ingest.AcceptResult{}, nil),
				authenticator: grantingAnother,
				gate:          gate,
			})
			tt.args.export(t, h)
			if diff := cmp.Diff(tt.want, marks.recorded()); diff != "" {
				t.Errorf("marks (-want +got):\n%s", diff)
			}
		})
	}
}

func TestAnUnauthenticatedRequestTakesNoSlotNoRateNoMark(t *testing.T) {
	marks := &markRecorder{}
	limits := otlp.AdmissionLimits(1)
	gate := gateWith(t, limits, marks)
	h := startWith(t, setup{
		acceptor:      returning(ingest.AcceptResult{}, nil),
		authenticator: byToken,
		gate:          gate,
	})
	refusals := []sent{
		carrying(),
		carrying(otherSchemePrefix + otlptest.Token),
		bearing(unknownTokenID),
		bearing(staleTokenID),
	}
	for _, refused := range refusals {
		refused.export(t, h)
	}
	untouched := gate.Admission.Stats()
	marksBefore := marks.recorded()
	admittedAfter := carrying(bearerPrefix+otlptest.Token).export(t, h).Code()
	type outcome struct {
		Stats        admission.Stats
		Marks        []string
		FirstGranted codes.Code
	}
	want := outcome{
		FirstGranted: codes.OK,
	}
	got := outcome{
		Stats:        untouched,
		Marks:        marksBefore,
		FirstGranted: admittedAfter,
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("four refused requests, then the first granted one (-want +got):\n%s", diff)
	}
}

func TestTheAdmissionLimitsAreTheDesignedOnes(t *testing.T) {
	type pinned struct {
		Limits        admission.Limits
		SlotHoldLimit time.Duration
		Default       int
		Max           int
	}
	want := pinned{
		Limits: admission.Limits{
			Slots:             10,
			MinShare:          2,
			ActiveWindow:      10 * time.Second,
			WaitingWindow:     60 * time.Second,
			RequestsPerSecond: 2000,
		},
		SlotHoldLimit: 15 * time.Second,
		Default:       2000,
		Max:           100_000,
	}
	got := pinned{
		Limits:        otlp.AdmissionLimits(otlp.DefaultRequestsPerSecond),
		SlotHoldLimit: otlp.DefaultOptions(nil, nil).SlotHoldLimit,
		Default:       otlp.DefaultRequestsPerSecond,
		Max:           otlp.MaxRequestsPerSecond,
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("admission limits (-want +got):\n%s", diff)
	}
}

type refusalRound struct {
	t     *testing.T
	h     harness
	gate  admission.Gate
	calls int
}

func (r refusalRound) export() {
	r.t.Helper()
	for range r.calls {
		if _, err := r.h.Client.Export(r.t.Context(), requestOf(1)); status.Code(err) != codes.ResourceExhausted {
			r.t.Fatalf("export in a refusal round: %v, want RESOURCE_EXHAUSTED", err)
		}
	}
}

func (r refusalRound) overShare() {
	r.t.Helper()
	var releases []func()
	for {
		release, err := r.gate.Enter(otlptest.TokenID, nil)
		if err != nil {
			break
		}
		releases = append(releases, release)
	}
	r.export()
	for _, release := range releases {
		release()
	}
}

func (r refusalRound) slotsFull() {
	r.t.Helper()
	var releases []func()
	for index := 0; r.gate.Admission.Stats().InUse < otlp.RequestSlots; index++ {
		release, err := r.gate.Enter(otherToken(index), nil)
		if err != nil {
			r.t.Fatalf("fill slot %d: %v", index, err)
		}
		releases = append(releases, release)
	}
	r.export()
	for _, release := range releases {
		release()
	}
}

func (r refusalRound) rateSpent() {
	r.t.Helper()
	for {
		release, err := r.gate.Enter(otlptest.TokenID, nil)
		if err != nil {
			break
		}
		release()
	}
	r.export()
}

func (r refusalRound) run() {
	r.t.Helper()
	r.overShare()
	r.slotsFull()
	r.rateSpent()
}

func admissionWarn(reason string, holder string, held int, suppressed int) map[string]any {
	record := map[string]any{
		"level":               "WARN",
		"msg":                 eventAdmission,
		"reason":              reason,
		"token_id":            otlptest.TokenID,
		"peer":                bufconnPeerString,
		"suppressed_requests": float64(suppressed),
	}
	if holder != "" {
		record["top_holder_id"] = holder
		record["top_holder_held"] = float64(held)
	}
	return record
}

func TestAdmissionRejectionsWriteOneWarnPerReasonPerInterval(t *testing.T) {
	clock := &fakeClock{
		now: time.Unix(0, 0),
	}
	books, err := admission.New(otlp.AdmissionLimits(otlp.DefaultRequestsPerSecond), clock)
	if err != nil {
		t.Fatalf("admission.New: %v", err)
	}
	gate := &admission.Gate{
		Admission: books,
		Marks:     otlptest.NoMarks{},
	}
	h := startWith(t, setup{
		acceptor:      returning(ingest.AcceptResult{}, nil),
		authenticator: otlptest.Admitting(),
		gate:          gate,
		credentials:   tokenCredentials(),
	})
	refusalRound{
		t:     t,
		h:     h,
		gate:  *gate,
		calls: admissionRefusals,
	}.run()
	clock.Advance(otlp.LogInterval)
	h.Clock.Advance(otlp.LogInterval)
	refusalRound{
		t:     t,
		h:     h,
		gate:  *gate,
		calls: 1,
	}.run()
	want := []map[string]any{
		admissionWarn("share", otlptest.TokenID, 10, 0),
		admissionWarn("slots", string(otherToken(0)), 1, 0),
		admissionWarn("rate", "", 0, 0),
		admissionWarn("share", otlptest.TokenID, 2, admissionRefusals-1),
		admissionWarn("slots", string(otherToken(0)), 1, admissionRefusals-1),
		admissionWarn("rate", "", 0, admissionRefusals-1),
	}
	if diff := cmp.Diff(want, withoutTime(h.Logs.Events(t, eventAdmission))); diff != "" {
		t.Errorf("admission warnings (-want +got):\n%s", diff)
	}
}
