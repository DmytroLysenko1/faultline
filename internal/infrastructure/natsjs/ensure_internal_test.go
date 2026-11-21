package natsjs

import (
	"cmp"
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	gocmp "github.com/google/go-cmp/cmp"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	neverVisible        = -1
	designedWait        = 5 * time.Second
	shortCallerBudget   = 2 * time.Second
	twoHiddenReads      = 300 * time.Millisecond
	readsInDesignedWait = 8
	readsInShortBudget  = 5
	slowAnswer          = 500 * time.Millisecond
)

var errBroken = errors.New("stream read broken")

type laggingStreams struct {
	jetstream.JetStream
	lock     sync.Mutex
	hidden   int
	hiddenAs error
	hangs    bool
	slowBy   time.Duration
	ownName  bool
	config   jetstream.StreamConfig
	reads    int
	updates  int
}

func (l *laggingStreams) Stream(ctx context.Context, _ string) (jetstream.Stream, error) {
	if l.hangs {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if err := answerAfter(ctx, l.slowBy); err != nil {
		return nil, err
	}
	l.lock.Lock()
	defer l.lock.Unlock()
	l.reads++
	if l.hidden != 0 {
		l.hidden--
		return nil, l.hiddenAs
	}
	return &shownStream{info: &jetstream.StreamInfo{Config: l.config}}, nil
}

func (l *laggingStreams) CreateStream(context.Context, jetstream.StreamConfig) (jetstream.Stream, error) {
	if l.ownName {
		return &shownStream{}, nil
	}
	return nil, jetstream.ErrStreamNameAlreadyInUse
}

func (l *laggingStreams) UpdateStream(context.Context, jetstream.StreamConfig) (jetstream.Stream, error) {
	l.lock.Lock()
	defer l.lock.Unlock()
	l.updates++
	return &shownStream{info: &jetstream.StreamInfo{Config: l.config}}, nil
}

func answerAfter(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

type shownStream struct {
	jetstream.Stream
	info *jetstream.StreamInfo
}

func (s *shownStream) CachedInfo() *jetstream.StreamInfo {
	return s.info
}

type heldLease struct{}

func (heldLease) Valid() bool {
	return true
}

type lagArgs struct {
	create   bool
	ownName  bool
	hangs    bool
	slowBy   time.Duration
	hidden   int
	hiddenAs error
	budget   time.Duration
}

type lagOutcome struct {
	reads   int
	updates int
	took    time.Duration
}

func TestAJustCreatedStreamIsWaitedForOnlyWhereItCanStillAppear(t *testing.T) {
	tests := []struct {
		name    string
		args    lagArgs
		want    lagOutcome
		wantErr error
		notErr  error
	}{
		{name: "the stream of the other processor appears on the third read",
			args: lagArgs{create: true, hidden: 2},
			want: lagOutcome{reads: 4, updates: 1, took: twoHiddenReads}, notErr: context.DeadlineExceeded},
		{name: "the processor's own stream appears on the third read",
			args: lagArgs{create: true, ownName: true, hidden: 2},
			want: lagOutcome{reads: 3, took: twoHiddenReads}, notErr: context.DeadlineExceeded},
		{name: "a stream that never appears is refused after the designed wait",
			args:    lagArgs{create: true, hidden: neverVisible},
			want:    lagOutcome{reads: readsInDesignedWait, took: designedWait},
			wantErr: errStreamNotVisible, notErr: context.DeadlineExceeded},
		{name: "a caller with less time left hears its own deadline",
			args:    lagArgs{create: true, hidden: neverVisible, budget: shortCallerBudget},
			want:    lagOutcome{reads: readsInShortBudget, took: shortCallerBudget},
			wantErr: context.DeadlineExceeded, notErr: errStreamNotVisible},
		{name: "a final failure while waiting is not rewritten",
			args:    lagArgs{create: true, hidden: neverVisible, hiddenAs: errBroken},
			want:    lagOutcome{reads: 1},
			wantErr: errBroken, notErr: errStreamNotVisible},
		{name: "a stream seen at plan time and gone is not waited for",
			args:    lagArgs{hidden: neverVisible},
			want:    lagOutcome{reads: 1},
			wantErr: jetstream.ErrStreamNotFound, notErr: errStreamNotVisible},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				streams, next := lagging(tt.args)
				ctx, cancel := callerContext(t, tt.args.budget)
				defer cancel()
				started := time.Now()

				err := writer{js: streams, lease: heldLease{}}.apply(ctx, next)

				if !errors.Is(err, tt.wantErr) || errors.Is(err, tt.notErr) {
					t.Fatalf("apply = %v, want %v and not %v", err, tt.wantErr, tt.notErr)
				}
				got := lagOutcome{reads: streams.reads, updates: streams.updates, took: time.Since(started)}
				if diff := gocmp.Diff(tt.want, got, gocmp.AllowUnexported(lagOutcome{})); diff != "" {
					t.Errorf("reads, updates, time (-want +got):\n%s", diff)
				}
			})
		})
	}
}

func TestAStreamThatNeverAppearsSaysWhyInOneLine(t *testing.T) {
	tests := []struct {
		name string
		args lagArgs
		want string
	}{
		{name: "NATS keeps answering not found", args: lagArgs{create: true, hidden: neverVisible},
			want: jetstream.ErrStreamNotFound.Error()},
		{name: "NATS answers slowly, the window cuts an answer on its way",
			args: lagArgs{create: true, hidden: neverVisible, slowBy: slowAnswer}, want: jetstream.ErrStreamNotFound.Error()},
		{name: "NATS never answers", args: lagArgs{create: true, hangs: true}, want: errNoAnswer.Error()},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				streams, next := lagging(tt.args)

				err := writer{js: streams, lease: heldLease{}}.apply(t.Context(), next)

				got := err.Error()
				if !errors.Is(err, errStreamNotVisible) || strings.Contains(got, "\n") ||
					!strings.Contains(got, tt.want) || strings.Contains(got, context.DeadlineExceeded.Error()) {
					t.Errorf("apply = %q, want not visible in one line naming %q and no deadline", got, tt.want)
				}
			})
		})
	}
}

func lagging(args lagArgs) (*laggingStreams, plan) {
	want := &jetstream.StreamConfig{
		Name:     "JOURNAL_P0",
		Subjects: []string{"journal.p0"},
		Metadata: map[string]string{metaVersion: "1"},
	}
	streams := &laggingStreams{
		hidden:   args.hidden,
		hiddenAs: cmp.Or[error](args.hiddenAs, jetstream.ErrStreamNotFound),
		ownName:  args.ownName,
		hangs:    args.hangs,
		slowBy:   args.slowBy,
		config:   *want,
	}
	return streams, plan{want: want, create: args.create}
}

func callerContext(t *testing.T, budget time.Duration) (context.Context, context.CancelFunc) {
	t.Helper()
	if budget == 0 {
		return context.WithCancel(t.Context())
	}
	return context.WithTimeout(t.Context(), budget)
}
