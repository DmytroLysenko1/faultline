package ingestauth

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/DmytroLysenko1/faultline/internal/domain/credential"
)

const MaxStalenessLimit = 60 * time.Second

var (
	ErrUnauthenticated   = errors.New("ingest token rejected")
	ErrDirectoryStale    = errors.New("ingest token directory is stale")
	ErrInvalidPolicy     = errors.New("invalid ingest auth policy")
	ErrMissingDependency = errors.New("ingest auth dependency missing")
)

type tokenDirectory interface {
	Snapshot(ctx context.Context) Snapshot
}

type clock interface {
	Now() time.Time
}

type Policy struct {
	Tenant       string
	MaxStaleness time.Duration
	CheckHorizon time.Duration
}

func (p Policy) validate() error {
	if p.Tenant == "" {
		return fmt.Errorf("empty tenant: %w", ErrInvalidPolicy)
	}
	if p.CheckHorizon <= 0 ||
		p.CheckHorizon > p.MaxStaleness ||
		p.MaxStaleness > MaxStalenessLimit {
		return fmt.Errorf("check horizon %s, max staleness %s, limit %s: %w",
			p.CheckHorizon,
			p.MaxStaleness,
			MaxStalenessLimit,
			ErrInvalidPolicy,
		)
	}
	return nil
}

type Snapshot struct {
	Grants    credential.Grants
	CheckedAt time.Time
}

type Stats struct {
	Accepted        uint64
	Rejected        [reasonCount]uint64
	StaleRejections uint64
	Age             time.Duration
}

type Authenticator struct {
	directory       tokenDirectory
	clock           clock
	policy          Policy
	accepted        atomic.Uint64
	rejected        [reasonCount]atomic.Uint64
	staleRejections atomic.Uint64
}

func NewAuthenticator(directory tokenDirectory, clock clock, policy Policy) (*Authenticator, error) {
	if directory == nil || clock == nil {
		return nil, ErrMissingDependency
	}
	if err := policy.validate(); err != nil {
		return nil, err
	}
	return &Authenticator{
		directory: directory,
		clock:     clock,
		policy:    policy,
	}, nil
}

func (a *Authenticator) Authenticate(ctx context.Context, token string) error {
	snapshot := a.directory.Snapshot(ctx)
	if a.stale(snapshot) {
		a.staleRejections.Add(1)
		return ErrDirectoryStale
	}
	parsed, err := credential.Parse(token)
	if err != nil {
		return a.reject(ReasonMalformed, "")
	}
	if _, err := snapshot.Grants.Admit(parsed, a.policy.Tenant); err != nil {
		return a.reject(admitReason(err), parsed.ID)
	}
	a.accepted.Add(1)
	return nil
}

func (a *Authenticator) Stats(ctx context.Context) Stats {
	stats := Stats{
		Accepted:        a.accepted.Load(),
		StaleRejections: a.staleRejections.Load(),
		Age:             a.age(a.directory.Snapshot(ctx)),
	}
	for reason := range reasonCount {
		stats.Rejected[reason] = a.rejected[reason].Load()
	}
	return stats
}

func (a *Authenticator) stale(snapshot Snapshot) bool {
	if snapshot.CheckedAt.IsZero() {
		return true
	}
	age := a.age(snapshot)
	return age < 0 || age > a.policy.MaxStaleness
}

func (a *Authenticator) reject(reason Reason, id credential.ID) error {
	a.rejected[reason].Add(1)
	return RejectionError{
		Reason:  reason,
		TokenID: id,
	}
}

func (a *Authenticator) Fresh(ctx context.Context) bool {
	return !a.stale(a.directory.Snapshot(ctx))
}

func (a *Authenticator) age(snapshot Snapshot) time.Duration {
	return a.clock.Now().Sub(snapshot.CheckedAt)
}
