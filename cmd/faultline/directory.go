package main

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/samber/lo"

	"github.com/DmytroLysenko1/faultline/internal/application/ingestauth"
	"github.com/DmytroLysenko1/faultline/internal/application/selfalert"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/tokens"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp"
)

const (
	layerDirectory       = "directory"
	componentDirectory   = "directory"
	stageAwaitTokens     = "directory/await_tokens"
	stageAuthenticator   = "directory/authenticator"
	eventTokenWaitPrefix = "tokens."

	tokenReload        = 15 * time.Second
	tokenRetry         = 2 * time.Second
	tokenCheck         = 2 * time.Second
	tokenReread        = 5 * time.Second
	tokenMaxStaleness  = 45 * time.Second
	tokenOpenBudget    = 10 * time.Second
	directoryStopGrace = 100 * time.Millisecond
	directoryHeap      = tokens.SnapshotHeap + otlp.MaxConnections*otlp.MaxConcurrentStreams*otlp.MaxHeaderListBytes

	reasonBucketMissing    = "bucket_missing"
	reasonBucketLeaderless = "bucket_leaderless"
	reasonBucketQuorumless = "bucket_quorumless"

	hintBucketDrift = "run faultline tokens init to reconcile the token bucket; " +
		"if it reports that the bucket needs recreating, follow the runbook Tokens / recreate the bucket"
)

var errDirectoryNotStarted = errors.New("token directory has not started")

type tokenGuard struct {
	directory     *tokens.Directory
	authenticator *ingestauth.Authenticator
	tap           *otlp.Tap
}

type tokenWaitRule struct {
	target error
	reason string
}

type directoryUnit struct {
	source  connSource
	tenant  string
	options tokens.DirectoryOptions
	clock   wallClock
	phases  phaseLog
	logger  *slog.Logger
	sup     unitSupervisor
	cancel  context.CancelFunc
	guarded atomic.Pointer[tokenGuard]
}

func directoryLayer(source connSource, tenant string, phases phaseLog, logger *slog.Logger) (layer, *directoryUnit) {
	guard := &directoryUnit{
		source: source,
		tenant: tenant,
		options: tokens.DirectoryOptions{
			ReloadInterval: tokenReload,
			RetryEvery:     tokenRetry,
			CheckTime:      tokenCheck,
			RereadTime:     tokenReread,
			Logger:         logger,
		},
		clock:  systemClock{},
		phases: phases,
		logger: logger,
	}
	return layer{
		name:        layerDirectory,
		unit:        guard,
		components:  []string{componentDirectory},
		stopBudget:  directoryStopGrace + exitGrace,
		statsGroups: guard.statsGroups,
		sample:      guard.sample,
		contributions: []contribution{
			{
				name:  "token directory",
				bytes: directoryHeap,
			},
		},
	}, guard
}

func (u *directoryUnit) start(ctx context.Context, sup unitSupervisor) error {
	directory, err := u.awaitTokens(ctx, u.source.conns())
	if err != nil {
		return &stageError{
			stage: stageAwaitTokens,
			err:   err,
			hint:  tokenStartHint(err),
		}
	}
	guard, err := u.guard(directory)
	if err != nil {
		return &stageError{
			stage: stageAuthenticator,
			err:   err,
		}
	}
	runCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	u.sup = sup
	u.cancel = cancel
	u.guarded.Store(guard)
	sup.spawn(componentDirectory, func() error {
		return directory.Run(runCtx)
	})
	return nil
}

func (u *directoryUnit) guard(directory *tokens.Directory) (*tokenGuard, error) {
	authenticator, err := ingestauth.NewAuthenticator(directory, u.clock, ingestauth.Policy{
		Tenant:       u.tenant,
		MaxStaleness: tokenMaxStaleness,
		CheckHorizon: u.options.Horizon(),
	})
	if err != nil {
		return nil, err
	}
	return &tokenGuard{
		directory:     directory,
		authenticator: authenticator,
		tap:           otlp.NewTap(authenticator, otlp.DefaultOptions(u.logger, u.clock)),
	}, nil
}

func (u *directoryUnit) awaitTokens(ctx context.Context, conn *natsjs.Conn) (*tokens.Directory, error) {
	wait := &streamWait{
		warned: map[string]bool{},
	}
	for {
		directory, err := u.openOnce(ctx, conn, wait)
		if directory != nil || err != nil {
			return directory, err
		}
	}
}

func (u *directoryUnit) openOnce(ctx context.Context, conn *natsjs.Conn, wait *streamWait) (*tokens.Directory, error) {
	directory, err := u.open(ctx, conn)
	if err == nil || !tokens.WaitsFor(err) {
		return directory, err
	}
	u.waits().keep(ctx, wait, tokenWaitReason(err), err)
	return nil, pause(ctx, tokenRetry)
}

func (u *directoryUnit) open(ctx context.Context, conn *natsjs.Conn) (*tokens.Directory, error) {
	openCtx, cancel := context.WithTimeout(ctx, tokenOpenBudget)
	defer cancel()
	options := u.options
	options.Clock = u.clock
	return tokens.OpenDirectory(openCtx, conn, &options)
}

func (u *directoryUnit) waits() waitLog {
	return waitLog{
		state:  stateWaitingTokens,
		prefix: eventTokenWaitPrefix,
		phases: u.phases,
		logger: u.logger,
	}
}

func (u *directoryUnit) stop(ctx context.Context) error {
	if u.cancel == nil {
		return nil
	}
	u.cancel()
	waitCtx, cancel := context.WithTimeout(ctx, directoryStopGrace)
	defer cancel()
	exited, err := u.sup.wait(waitCtx, componentDirectory)
	if !exited {
		return errStepAbandoned
	}
	return err
}

func (u *directoryUnit) tap() (*otlp.Tap, error) {
	guard := u.guarded.Load()
	if guard == nil {
		return nil, errDirectoryNotStarted
	}
	return guard.tap, nil
}

func (u *directoryUnit) statsGroups(ctx context.Context) []slog.Attr {
	guard := u.guarded.Load()
	if guard == nil {
		return nil
	}
	return []slog.Attr{
		authGroup(guard.tap.Stats(), guard.authenticator.Stats(ctx), new(guard.directory.Stats())),
	}
}

func (u *directoryUnit) sample(ctx context.Context, sample *selfalert.Sample) {
	guard := u.guarded.Load()
	if guard == nil {
		return
	}
	fillAuthSample(sample, new(guard.directory.Stats()), guard.authenticator.Fresh(ctx))
}

func fillAuthSample(sample *selfalert.Sample, stats *tokens.DirectoryStats, fresh bool) {
	sample.TokensStale = !fresh
	sample.TokensRegressed = stats.Regressed
	sample.RevokeBlocked = stats.RevokeBlocked
	sample.BucketDrift = stats.ConfigDrift
	sample.EpochChanges = stats.EpochChanges
	sample.MassRevokes = stats.MassRevokes
}

func tokenStartHint(err error) string {
	if !errors.Is(err, tokens.ErrBucketDrift) {
		return ""
	}
	return hintBucketDrift
}

func tokenWaitRules() []tokenWaitRule {
	return []tokenWaitRule{
		{
			target: tokens.ErrBucketMissing,
			reason: reasonBucketMissing,
		},
		{
			target: tokens.ErrNoLeader,
			reason: reasonBucketLeaderless,
		},
		{
			target: tokens.ErrNoQuorum,
			reason: reasonBucketQuorumless,
		},
	}
}

func tokenWaitReason(err error) string {
	rule, found := lo.Find(tokenWaitRules(), func(rule tokenWaitRule) bool {
		return errors.Is(err, rule.target)
	})
	if !found {
		return reasonJetStreamUnavailable
	}
	return rule.reason
}
