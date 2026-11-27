package otlp

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/tap"

	"github.com/DmytroLysenko1/faultline/internal/application/ingestauth"
	"github.com/DmytroLysenko1/faultline/internal/domain/credential"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp/bearer"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp/throttle"
)

const (
	AuthRetryDelay      = 5 * time.Second
	authorizationHeader = "authorization"
)

var errNoVerdict = errors.New("stream reached the handler without a tap verdict")

type verdictKey struct{}

type verdict struct {
	refusal   error
	principal ingestauth.Principal
	cancel    context.CancelCauseFunc
}

func (v verdict) stop() {
	if v.cancel != nil {
		v.cancel(nil)
	}
}

type authenticator interface {
	Authenticate(ctx context.Context, token string) (ingestauth.Principal, error)
}

type TapStats struct {
	Missing uint64
	Header  uint64
	Failed  uint64
}

type Tap struct {
	authenticator authenticator
	logger        *slog.Logger
	rejections    map[string]*throttle.Throttle
	failures      *throttle.Throttle
	missing       atomic.Uint64
	header        atomic.Uint64
	failed        atomic.Uint64
}

func NewTap(authenticator authenticator, options Options) *Tap {
	rejections := make(map[string]*throttle.Throttle, len(rejectionReasons()))
	for _, reason := range rejectionReasons() {
		rejections[reason] = throttle.New(LogInterval, options.Clock)
	}
	return &Tap{
		authenticator: authenticator,
		logger:        options.Logger,
		rejections:    rejections,
		failures:      throttle.New(LogInterval, options.Clock),
	}
}

func (t *Tap) Stats() TapStats {
	return TapStats{
		Missing: t.missing.Load(),
		Header:  t.header.Load(),
		Failed:  t.failed.Load(),
	}
}

func (t *Tap) handle(ctx context.Context, info *tap.Info) (context.Context, error) {
	return t.decide(ctx, info.Header), nil
}

func (t *Tap) decide(ctx context.Context, header metadata.MD) context.Context {
	principal, refusal := t.authenticate(ctx, header)
	held, cancel := context.WithCancelCause(ctx)
	stripped := metadata.NewIncomingContext(held, withoutAuthorization(header))
	return context.WithValue(stripped, verdictKey{}, verdict{
		refusal:   refusal,
		principal: principal,
		cancel:    cancel,
	})
}

func (t *Tap) enforce(ctx context.Context) (verdict, error) {
	found := verdictOf(ctx)
	if found.refusal == nil {
		return found, nil
	}
	outcome := authFailureOf(found.refusal)
	outcome.report(t, ctx, found.refusal)
	return found, outcome.status.Err()
}

func verdictOf(ctx context.Context) verdict {
	found, decided := ctx.Value(verdictKey{}).(verdict)
	if !decided {
		return verdict{
			refusal: errNoVerdict,
		}
	}
	return found
}

func (t *Tap) authenticate(ctx context.Context, header metadata.MD) (ingestauth.Principal, error) {
	token, err := bearer.Credential(header.Get(authorizationHeader))
	if err != nil {
		return ingestauth.Principal{}, err
	}
	return t.authenticator.Authenticate(ctx, token)
}

func withoutAuthorization(header metadata.MD) metadata.MD {
	kept := header.Copy()
	delete(kept, authorizationHeader)
	return kept
}

func (t *Tap) missingCredential(ctx context.Context, _ error) {
	t.missing.Add(1)
	t.warnRejected(ctx, reasonMissing, "")
}

func (t *Tap) malformedHeader(ctx context.Context, _ error) {
	t.header.Add(1)
	t.warnRejected(ctx, reasonHeader, "")
}

func (t *Tap) rejected(ctx context.Context, err error) {
	rejection, classified := errors.AsType[ingestauth.RejectionError](err)
	if !classified {
		t.failure(ctx, err)
		return
	}
	t.warnRejected(ctx, rejection.Reason.String(), rejection.TokenID)
}

func (*Tap) stale(_ context.Context, _ error) {}

func (t *Tap) failure(ctx context.Context, err error) {
	t.failed.Add(1)
	suppressed, admitted := t.failures.Admit(0)
	if !admitted {
		return
	}
	t.logger.LogAttrs(ctx, slog.LevelError, "otlp.auth.failed", withTrace(ctx,
		slog.Any("error", err),
		slog.Int64("suppressed_requests", suppressed.Events),
	)...)
}

func (t *Tap) warnRejected(ctx context.Context, reason string, id credential.ID) {
	suppressed, admitted := t.rejections[reason].Admit(0)
	if !admitted {
		return
	}
	attributes := []slog.Attr{
		slog.String("reason", reason),
		slog.String("peer", peerOf(ctx)),
		slog.Int64("suppressed_requests", suppressed.Events),
	}
	if id != "" {
		attributes = append(attributes, slog.String("token_id", string(id)))
	}
	t.logger.LogAttrs(ctx, slog.LevelWarn, "otlp.auth.rejected", withTrace(ctx, attributes...)...)
}

func peerOf(ctx context.Context) string {
	remote, found := peer.FromContext(ctx)
	if !found || remote.Addr == nil {
		return ""
	}
	return remote.Addr.String()
}
