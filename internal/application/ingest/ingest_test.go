package ingest_test

import (
	"context"
	"errors"
	"iter"
	"slices"
	"strings"
	"sync"
	"testing"
	"unicode/utf8"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"

	"github.com/DmytroLysenko1/faultline/internal/application/ingest"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/ids"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/kind"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/operation"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/resource"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/span"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/telemetrytest"
)

const (
	firstSpanID  = 1
	secondSpanID = 2
	thirdSpanID  = 3

	overLimitBytes    = 256
	k8sNamespaceLimit = 63
	k8sPodNameLimit   = 253
	unknownKind       = 9
	rawValueMarker    = "RAW-VALUE-MUST-NOT-LEAK"

	concurrentWorkers   = 8
	lengthHintOvershoot = 1000
	requestsPerWorker   = 50
	spansPerAccepted    = 2
	overloadedBatchLen  = 1
)

var errJournalBroken = errors.New("journal broken")

type spanInput struct {
	Resource resource.Resource
	Fields   span.Fields
}

type sliceSource struct {
	Inputs   []spanInput
	Reported int
	Passes   int
}

func sourceOf(inputs []spanInput) *sliceSource {
	return &sliceSource{
		Inputs:   inputs,
		Reported: len(inputs),
	}
}

func (s *sliceSource) Len() int {
	return s.Reported
}

func (s *sliceSource) All() iter.Seq2[resource.Resource, *ingest.Record] {
	return func(yield func(resource.Resource, *ingest.Record) bool) {
		s.Passes++
		for i := range s.Inputs {
			if !yield(s.Inputs[i].Resource, &ingest.Record{Fields: s.Inputs[i].Fields}) {
				return
			}
		}
	}
}

type fakeJournal struct {
	lock     sync.Mutex
	Batches  [][]span.Span
	Contexts []context.Context
	Err      error
}

func (f *fakeJournal) Append(ctx context.Context, received []span.Span) error {
	f.lock.Lock()
	defer f.lock.Unlock()
	f.Contexts = append(f.Contexts, ctx)
	if f.Err != nil {
		return f.Err
	}
	f.Batches = append(f.Batches, received)
	return nil
}

type harness struct {
	Journal *fakeJournal
	Accept  *ingest.AcceptBatch
}

func newHarness(journalErr error) harness {
	journal := &fakeJournal{
		Err: journalErr,
	}
	return harness{
		Journal: journal,
		Accept:  newAcceptBatch(journal),
	}
}

func validInput(spanID byte) spanInput {
	return spanInput{
		Resource: telemetrytest.ValidResource(),
		Fields: telemetrytest.FieldsWith(func(f *span.Fields) {
			f.SpanID = ids.SpanID{
				spanID,
			}
		}),
	}
}

func inputWithResource(edit func(*resource.Resource)) spanInput {
	return spanInput{
		Resource: telemetrytest.ResourceWith(edit),
		Fields:   telemetrytest.ValidFields(),
	}
}

func inputWithFields(edit func(*span.Fields)) spanInput {
	return spanInput{
		Resource: telemetrytest.ValidResource(),
		Fields:   telemetrytest.FieldsWith(edit),
	}
}

func serviceNameOfLength(length int) spanInput {
	return inputWithResource(func(r *resource.Resource) {
		r.ServiceName = strings.Repeat("s", length)
	})
}

func expectedSpan(t *testing.T, input *spanInput) span.Span {
	t.Helper()
	return telemetrytest.MustSpan(t, input.Resource, &input.Fields)
}

func rejection(t *testing.T, input *spanInput) string {
	t.Helper()
	_, err := span.New(input.Resource, &input.Fields)
	if err == nil {
		t.Fatalf("span.New accepted an input the test expects to be rejected")
	}
	return err.Error()
}

func execute(t *testing.T, h harness, inputs ...spanInput) (ingest.AcceptResult, error) {
	t.Helper()
	return h.Accept.Execute(t.Context(), sourceOf(inputs))
}

func diffResult(want, got ingest.AcceptResult) string {
	return cmp.Diff(want, got, cmpopts.EquateEmpty())
}

func TestExecuteAppendsValidSpansAsOneBatchInInputOrder(t *testing.T) {
	h := newHarness(nil)
	first := validInput(firstSpanID)
	invalid := serviceNameOfLength(0)
	second := validInput(secondSpanID)
	got, err := execute(t, h, first, invalid, second)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	want := ingest.AcceptResult{
		Accepted: spansPerAccepted,
		Rejected: 1,
		Reasons: []string{
			rejection(t, &invalid),
		},
	}
	if diff := diffResult(want, got); diff != "" {
		t.Errorf("result (-want +got):\n%s", diff)
	}
	wantBatches := [][]span.Span{
		{
			expectedSpan(t, &first),
			expectedSpan(t, &second),
		},
	}
	if diff := cmp.Diff(wantBatches, h.Journal.Batches); diff != "" {
		t.Errorf("journal batches (-want +got):\n%s", diff)
	}
}

type validationCase struct {
	Name string
	Args spanInput
	Want bool
}

func identityLimitCases() []validationCase {
	return []validationCase{
		{
			Name: "service.name exactly at the limit",
			Args: serviceNameOfLength(overLimitBytes - 1),
			Want: true,
		},
		{
			Name: "service.name one byte over the limit",
			Args: serviceNameOfLength(overLimitBytes),
		},
		{
			Name: "service.namespace one byte over the limit",
			Args: inputWithResource(func(r *resource.Resource) {
				r.ServiceNamespace = strings.Repeat("n", overLimitBytes)
			}),
		},
		{
			Name: "k8s.namespace.name exactly at the Kubernetes limit",
			Args: inputWithResource(func(r *resource.Resource) {
				r.K8sNamespaceName = strings.Repeat("k", k8sNamespaceLimit)
			}),
			Want: true,
		},
		{
			Name: "k8s.namespace.name one byte over the Kubernetes limit",
			Args: inputWithResource(func(r *resource.Resource) {
				r.K8sNamespaceName = strings.Repeat("k", k8sNamespaceLimit+1)
			}),
		},
		{
			Name: "k8s.pod.name one byte over the Kubernetes limit",
			Args: inputWithResource(func(r *resource.Resource) {
				r.K8sPodName = strings.Repeat("p", k8sPodNameLimit+1)
			}),
		},
	}
}

func identityContentCases() []validationCase {
	return []validationCase{
		{
			Name: "missing service.name",
			Args: serviceNameOfLength(0),
		},
		{
			Name: "invalid UTF-8 in service.name",
			Args: inputWithResource(func(r *resource.Resource) {
				r.ServiceName = "cart\xff"
			}),
		},
		{
			Name: "control character in k8s.namespace.name",
			Args: inputWithResource(func(r *resource.Resource) {
				r.K8sNamespaceName = "shop\n"
			}),
		},
		{
			Name: "SDK fallback service name is accepted as is",
			Args: inputWithResource(func(r *resource.Resource) {
				r.ServiceName = "unknown_service:java"
			}),
			Want: true,
		},
	}
}

func spanFieldCases() []validationCase {
	return []validationCase{
		{
			Name: "zero trace id",
			Args: inputWithFields(func(f *span.Fields) {
				f.TraceID = ids.TraceID{}
			}),
		},
		{
			Name: "zero span id",
			Args: inputWithFields(func(f *span.Fields) {
				f.SpanID = ids.SpanID{}
			}),
		},
		{
			Name: "server without parent is accepted as a root",
			Args: inputWithFields(func(f *span.Fields) {
				f.Kind = kind.Server
			}),
			Want: true,
		},
		{
			Name: "kind outside OTLP is accepted",
			Args: inputWithFields(func(f *span.Fields) {
				f.Kind = unknownKind
			}),
			Want: true,
		},
	}
}

func wantFor(t *testing.T, tc *validationCase) ingest.AcceptResult {
	t.Helper()
	if tc.Want {
		return ingest.AcceptResult{
			Accepted: 1,
		}
	}
	return ingest.AcceptResult{
		Rejected: 1,
		Reasons: []string{
			rejection(t, &tc.Args),
		},
	}
}

func TestExecuteAppliesEveryValidationRule(t *testing.T) {
	cases := append(identityLimitCases(), identityContentCases()...)
	cases = append(cases, spanFieldCases()...)
	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			h := newHarness(nil)
			got, err := execute(t, h, tc.Args)
			if err != nil {
				t.Fatalf("Execute: %v", err)
			}
			if diff := diffResult(wantFor(t, &tc), got); diff != "" {
				t.Errorf("result (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(got.Accepted, journalSpans(h.Journal)); diff != "" {
				t.Errorf("spans in journal (-want +got):\n%s", diff)
			}
		})
	}
}

func journalSpans(journal *fakeJournal) int {
	total := 0
	for _, appended := range journal.Batches {
		total += len(appended)
	}
	return total
}

func TestExecuteWithoutValidSpansDoesNotTouchJournal(t *testing.T) {
	invalid := serviceNameOfLength(0)
	onlyInvalid := []spanInput{
		invalid,
		invalid,
	}
	tests := []struct {
		Name string
		Args []spanInput
		Want ingest.AcceptResult
	}{
		{
			Name: "empty request",
		},
		{
			Name: "only invalid spans",
			Args: onlyInvalid,
			Want: ingest.AcceptResult{
				Rejected: len(onlyInvalid),
				Reasons: []string{
					rejection(t, &invalid),
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			h := newHarness(nil)
			got, err := execute(t, h, tt.Args...)
			if err != nil {
				t.Fatalf("Execute: %v", err)
			}
			if diff := diffResult(tt.Want, got); diff != "" {
				t.Errorf("result (-want +got):\n%s", diff)
			}
			if len(h.Journal.Contexts) != 0 {
				t.Errorf("Append called %d times, want none", len(h.Journal.Contexts))
			}
			wantStats := ingest.Stats{
				RejectedSpans: int64(tt.Want.Rejected),
			}
			if diff := cmp.Diff(wantStats, h.Accept.Stats()); diff != "" {
				t.Errorf("a request without valid spans is still a successful response (-want +got):\n%s", diff)
			}
		})
	}
}

func TestExecuteReasonsAreDistinctInFirstSeenOrder(t *testing.T) {
	tooLong := serviceNameOfLength(overLimitBytes)
	missing := serviceNameOfLength(0)
	inputs := []spanInput{
		tooLong,
		missing,
		tooLong,
		missing,
		tooLong,
	}
	got, err := execute(t, newHarness(nil), inputs...)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	want := ingest.AcceptResult{
		Rejected: len(inputs),
		Reasons: []string{
			rejection(t, &tooLong),
			rejection(t, &missing),
		},
	}
	if diff := diffResult(want, got); diff != "" {
		t.Errorf("result (-want +got):\n%s", diff)
	}
}

func TestExecuteKeepsAtMostMaxReasonsButCountsEveryRejection(t *testing.T) {
	const extraDistinct = 5
	inputs := make([]spanInput, 0, ingest.MaxReasons+extraDistinct)
	for offset := range ingest.MaxReasons + extraDistinct {
		inputs = append(inputs, serviceNameOfLength(overLimitBytes+offset))
	}
	want := make([]string, 0, len(inputs))
	for index := range inputs {
		want = append(want, rejection(t, &inputs[index]))
	}
	if len(slices.Compact(slices.Sorted(slices.Values(want)))) != len(inputs) {
		t.Fatalf("test inputs must produce %d distinct reasons, got %q", len(inputs), want)
	}
	got, err := execute(t, newHarness(nil), inputs...)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if got.Rejected != len(inputs) {
		t.Errorf("Rejected = %d, want %d", got.Rejected, len(inputs))
	}
	if diff := cmp.Diff(want[:ingest.MaxReasons], got.Reasons); diff != "" {
		t.Errorf("reasons (-want +got):\n%s", diff)
	}
}

func TestExecuteReasonsCarryNoRawValues(t *testing.T) {
	tooLong := inputWithResource(func(r *resource.Resource) {
		r.ServiceName = rawValueMarker + strings.Repeat("x", overLimitBytes)
	})
	brokenUTF8 := inputWithResource(func(r *resource.Resource) {
		r.K8sPodName = rawValueMarker + "\xc3\x28"
	})
	inputs := []spanInput{
		tooLong,
		brokenUTF8,
	}
	got, err := execute(t, newHarness(nil), inputs...)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if len(got.Reasons) != len(inputs) {
		t.Fatalf("Reasons = %q, want one per distinct rejection", got.Reasons)
	}
	for _, reason := range got.Reasons {
		if strings.Contains(reason, rawValueMarker) {
			t.Errorf("reason %q leaks the raw attribute value", reason)
		}
		if !utf8.ValidString(reason) {
			t.Errorf("reason %q is not valid UTF-8 and would break the OTLP response", reason)
		}
	}
}

type journalFailureCase struct {
	Name    string
	Args    error
	Want    ingest.Stats
	WantErr error
}

func journalFailureCases() []journalFailureCase {
	return []journalFailureCase{
		{
			Name: "overloaded journal is counted as an overloaded response",
			Args: ingest.ErrOverloaded,
			Want: ingest.Stats{
				OverloadedResponses: 1,
			},
			WantErr: ingest.ErrOverloaded,
		},
		{
			Name: "batch larger than the journal is counted as too large",
			Args: ingest.ErrBatchTooLarge,
			Want: ingest.Stats{
				TooLargeResponses: 1,
			},
			WantErr: ingest.ErrBatchTooLarge,
		},
		{
			Name:    "closed journal stays recognizable",
			Args:    ingest.ErrUnavailable,
			WantErr: ingest.ErrUnavailable,
		},
		{
			Name:    "deadline stays recognizable",
			Args:    context.DeadlineExceeded,
			WantErr: context.DeadlineExceeded,
		},
		{
			Name:    "unexpected journal failure is wrapped",
			Args:    errJournalBroken,
			WantErr: errJournalBroken,
		},
	}
}

func TestExecuteJournalFailureAcceptsNothing(t *testing.T) {
	for _, tt := range journalFailureCases() {
		t.Run(tt.Name, func(t *testing.T) {
			h := newHarness(tt.Args)
			got, err := execute(t, h, validInput(firstSpanID), serviceNameOfLength(0))
			if !errors.Is(err, tt.WantErr) {
				t.Fatalf("err = %v, want %v", err, tt.WantErr)
			}
			if diff := diffResult(ingest.AcceptResult{}, got); diff != "" {
				t.Errorf("result on failure (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(tt.Want, h.Accept.Stats()); diff != "" {
				t.Errorf("stats (-want +got):\n%s", diff)
			}
		})
	}
}

func TestExecuteNeverReusesASliceItGaveTheJournal(t *testing.T) {
	h := newHarness(nil)
	first, second := validInput(firstSpanID), validInput(secondSpanID)

	for _, input := range []spanInput{first, second} {
		if _, err := h.Accept.Execute(t.Context(), sourceOf([]spanInput{input})); err != nil {
			t.Fatalf("Execute: %v", err)
		}
	}

	want := [][]span.Span{{expectedSpan(t, &first)}, {expectedSpan(t, &second)}}
	if diff := cmp.Diff(want, h.Journal.Batches); diff != "" {
		t.Errorf("a later request changed spans the journal already owns (-want +got):\n%s", diff)
	}
}

func TestExecutePassesTheCallerContext(t *testing.T) {
	type ctxKey struct{}
	ctx := context.WithValue(t.Context(), ctxKey{}, "request")
	h := newHarness(nil)
	if _, err := h.Accept.Execute(ctx, sourceOf([]spanInput{
		validInput(firstSpanID),
	})); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if len(h.Journal.Contexts) != 1 {
		t.Fatalf("Append called %d times, want 1", len(h.Journal.Contexts))
	}
	if h.Journal.Contexts[0].Value(ctxKey{}) != "request" {
		t.Errorf("journal did not receive the caller context")
	}
}

type sizeGatedJournal struct {
	fakeJournal
}

func (j *sizeGatedJournal) Append(ctx context.Context, received []span.Span) error {
	if len(received) == overloadedBatchLen {
		return ingest.ErrOverloaded
	}
	return j.fakeJournal.Append(ctx, received)
}

func executeConcurrently(t *testing.T, accept *ingest.AcceptBatch, inputs []spanInput, wantErr error) {
	t.Helper()
	for range requestsPerWorker {
		if _, err := accept.Execute(t.Context(), sourceOf(inputs)); !errors.Is(err, wantErr) {
			t.Errorf("err = %v, want %v", err, wantErr)
		}
	}
}

func TestStatsSumConcurrentRequestsExactly(t *testing.T) {
	accept := newAcceptBatch(&sizeGatedJournal{})
	accepted := []spanInput{
		validInput(firstSpanID),
		validInput(secondSpanID),
		serviceNameOfLength(0),
	}
	overloaded := []spanInput{
		validInput(thirdSpanID),
	}
	var workers sync.WaitGroup
	for range concurrentWorkers {
		workers.Go(func() {
			executeConcurrently(t, accept, accepted, nil)
		})
		workers.Go(func() {
			executeConcurrently(t, accept, overloaded, ingest.ErrOverloaded)
		})
	}
	workers.Wait()
	requests := int64(concurrentWorkers * requestsPerWorker)
	want := ingest.Stats{
		AcceptedSpans:       requests * spansPerAccepted,
		RejectedSpans:       requests,
		OverloadedResponses: requests,
	}
	if diff := cmp.Diff(want, accept.Stats()); diff != "" {
		t.Errorf("stats (-want +got):\n%s", diff)
	}
}

func TestExecuteWalksTheSourceExactlyOnce(t *testing.T) {
	source := sourceOf([]spanInput{
		validInput(firstSpanID),
		serviceNameOfLength(0),
		validInput(secondSpanID),
	})
	if _, err := newHarness(nil).Accept.Execute(t.Context(), source); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if source.Passes != 1 {
		t.Errorf("All() walked %d times, want exactly one synchronous pass", source.Passes)
	}
}

func TestExecuteResultDoesNotDependOnTheLengthHint(t *testing.T) {
	inputs := []spanInput{
		validInput(firstSpanID),
		serviceNameOfLength(0),
		validInput(secondSpanID),
	}
	want, err := execute(t, newHarness(nil), inputs...)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	for _, hint := range []int{
		-1,
		0,
		1,
		len(inputs) * lengthHintOvershoot,
	} {
		h := newHarness(nil)
		source := sourceOf(inputs)
		source.Reported = hint
		got, err := h.Accept.Execute(t.Context(), source)
		if err != nil {
			t.Fatalf("Execute with Len() = %d: %v", hint, err)
		}
		if diff := diffResult(want, got); diff != "" {
			t.Errorf("Len() = %d changed the result (-want +got):\n%s", hint, diff)
		}
		if diff := cmp.Diff(want.Accepted, journalSpans(h.Journal)); diff != "" {
			t.Errorf("Len() = %d changed the journal (-want +got):\n%s", hint, diff)
		}
	}
}

func newAcceptBatch(journal interface {
	Append(ctx context.Context, received []span.Span) error
},
) *ingest.AcceptBatch {
	return ingest.NewAcceptBatch(journal, operation.NewNormalizer(nil))
}
