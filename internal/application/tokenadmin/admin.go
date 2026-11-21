package tokenadmin

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/DmytroLysenko1/faultline/internal/domain/credential"
)

var (
	ErrIDTaken           = errors.New("ingest token id already registered")
	ErrTokenLimit        = errors.New("ingest token limit reached")
	ErrUnknownToken      = errors.New("unknown or already revoked ingest token id")
	ErrOutcomeUnknown    = errors.New("ingest token write outcome unknown")
	ErrStoreUnavailable  = errors.New("ingest token store unavailable")
	ErrRevokeBlocked     = errors.New("ingest token store refuses revocation")
	ErrNotShown          = errors.New("issued ingest token could not be shown and was revoked")
	ErrLiveTokenNotShown = errors.New("issued ingest token was not shown and may be live")
	ErrInvalidBudget     = errors.New("invalid token admin budget")
	ErrMissingDependency = errors.New("token admin dependency missing")
	ErrNotAnnounced      = errors.New("ingest token id could not be announced; nothing was stored")
)

type Revocation uint8

const (
	Revoked Revocation = iota + 1
	Absent
	NothingToRevoke
)

type grantStore interface {
	Register(ctx context.Context, grant credential.Grant) error
	Revoke(ctx context.Context, id credential.ID) (Revocation, error)
	List(ctx context.Context) ([]credential.Grant, error)
}

type tokenOutput interface {
	Announce(ctx context.Context, id credential.ID, tenant string) error
	Show(ctx context.Context, token credential.IngestToken) error
}

type clock interface {
	Now() time.Time
}

type Budget struct {
	Settle       time.Duration
	Compensation time.Duration
}

type Admin struct {
	store  grantStore
	random io.Reader
	clock  clock
	output tokenOutput
	budget Budget
}

func NewAdmin(
	store grantStore,
	random io.Reader,
	clock clock,
	output tokenOutput,
	budget Budget,
) (*Admin, error) {
	admin := &Admin{
		store:  store,
		random: random,
		clock:  clock,
		output: output,
		budget: budget,
	}
	if err := admin.missingDependency(); err != nil {
		return nil, err
	}
	if budget.Settle <= 0 || budget.Compensation <= 0 {
		return nil, fmt.Errorf("settle %s, compensation %s: %w", budget.Settle, budget.Compensation, ErrInvalidBudget)
	}
	return admin, nil
}

func (a *Admin) Issue(ctx context.Context, tenant string) (credential.ID, error) {
	token, grant, err := credential.Generate(a.random, tenant, a.clock.Now())
	if err != nil {
		return "", fmt.Errorf("issue an ingest token for %q: %w", tenant, err)
	}
	if err := a.output.Announce(ctx, grant.ID, tenant); err != nil {
		return "", fmt.Errorf("announce ingest token %s: %w", grant.ID, errors.Join(ErrNotAnnounced, err))
	}
	settleCtx, cancel := context.WithTimeout(ctx, a.budget.Settle)
	defer cancel()
	if err := a.store.Register(settleCtx, grant); err != nil {
		return "", a.afterFailedRegister(ctx, grant.ID, fmt.Errorf("register ingest token %s: %w", grant.ID, err))
	}
	if err := a.output.Show(ctx, token); err != nil {
		return "", a.afterFailedShow(ctx, grant.ID, err)
	}
	return grant.ID, nil
}

func (a *Admin) Revoke(ctx context.Context, id credential.ID) (Revocation, error) {
	if _, err := credential.ParseID(string(id)); err != nil {
		return 0, fmt.Errorf("revoke ingest token %q: %w", id, errors.Join(ErrUnknownToken, err))
	}
	ctx, cancel := context.WithTimeout(ctx, a.budget.Settle)
	defer cancel()
	revocation, err := a.store.Revoke(ctx, id)
	if err != nil {
		return 0, fmt.Errorf("revoke ingest token %s: %w", id, err)
	}
	if revocation == NothingToRevoke {
		return revocation, fmt.Errorf("revoke ingest token %s: %w", id, ErrUnknownToken)
	}
	return revocation, nil
}

func (a *Admin) List(ctx context.Context) ([]credential.Grant, error) {
	ctx, cancel := context.WithTimeout(ctx, a.budget.Settle)
	defer cancel()
	grants, err := a.store.List(ctx)
	if err != nil {
		return nil, fmt.Errorf("list ingest tokens: %w", err)
	}
	return grants, nil
}

func (a *Admin) afterFailedRegister(ctx context.Context, id credential.ID, cause error) error {
	if provesNoRecord(cause) {
		return cause
	}
	_, revokeErr := a.compensate(ctx, id)
	return liveNotShown(id, cause, revokeErr)
}

func (a *Admin) afterFailedShow(ctx context.Context, id credential.ID, cause error) error {
	revocation, revokeErr := a.compensate(ctx, id)
	if revokeErr != nil || revocation != Revoked {
		return liveNotShown(id, cause, revokeErr)
	}
	return fmt.Errorf("show ingest token %s: %w", id, errors.Join(ErrNotShown, cause))
}

func (a *Admin) compensate(ctx context.Context, id credential.ID) (Revocation, error) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), a.budget.Compensation)
	defer cancel()
	revocation, err := a.store.Revoke(ctx, id)
	if err != nil {
		return 0, fmt.Errorf("compensate ingest token %s: %w", id, err)
	}
	return revocation, nil
}

func (a *Admin) missingDependency() error {
	dependencies := []struct {
		name    string
		present bool
	}{
		{name: "grant store", present: a.store != nil},
		{name: "random reader", present: a.random != nil},
		{name: "clock", present: a.clock != nil},
		{name: "token output", present: a.output != nil},
	}
	for _, dependency := range dependencies {
		if !dependency.present {
			return fmt.Errorf("%s: %w", dependency.name, ErrMissingDependency)
		}
	}
	return nil
}

func provesNoRecord(err error) bool {
	if errors.Is(err, ErrOutcomeUnknown) {
		return false
	}
	return errors.Is(err, ErrIDTaken) ||
		errors.Is(err, ErrTokenLimit) ||
		errors.Is(err, ErrStoreUnavailable)
}

func liveNotShown(id credential.ID, cause, revokeErr error) error {
	return fmt.Errorf("ingest token %s: %w: %s", id,
		ErrLiveTokenNotShown, errors.Join(cause, revokeErr).Error(),
	)
}
