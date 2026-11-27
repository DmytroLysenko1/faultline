package tokens

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync/atomic"
	"time"

	"github.com/DmytroLysenko1/faultline/internal/application/ingestauth"
	"github.com/DmytroLysenko1/faultline/internal/domain/credential"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs"
)

const (
	horizonReloads = 2
	SnapshotHeap   = 864 << 10
)

var ErrAlreadyRunning = errors.New("token directory already running")

type readState func(ctx context.Context) (bucketState, configVerdict, error)

type readEntries func(ctx context.Context) (bucketEntries, configVerdict, error)

type clock interface {
	Now() time.Time
}

type systemClock struct{}

func (systemClock) Now() time.Time {
	return time.Now()
}

type DirectoryOptions struct {
	ReloadInterval time.Duration
	RetryEvery     time.Duration
	CheckTime      time.Duration
	RereadTime     time.Duration
	Logger         *slog.Logger
	Clock          clock
}

func (o *DirectoryOptions) validate() error {
	if o == nil {
		return fmt.Errorf("no directory options: %w", ErrInvalidOptions)
	}
	if !allPositive(o.ReloadInterval, o.RetryEvery, o.CheckTime, o.RereadTime) {
		return fmt.Errorf("directory periods must be positive: %w", ErrInvalidOptions)
	}
	if o.RetryEvery > o.ReloadInterval || o.CheckTime+o.RereadTime >= o.ReloadInterval {
		return fmt.Errorf("retry %s and reads %s+%s must fit reload %s: %w",
			o.RetryEvery, o.CheckTime, o.RereadTime, o.ReloadInterval, ErrInvalidOptions)
	}
	return nil
}

func (o *DirectoryOptions) Horizon() time.Duration {
	return horizonReloads*o.ReloadInterval + o.CheckTime + o.RereadTime
}

type DirectoryStats struct {
	Tokens           int
	Checks           uint64
	CheckFailures    uint64
	LeaderlessChecks uint64
	QuorumlessChecks uint64
	RegressedChecks  uint64
	DriftedChecks    uint64
	Rereads          uint64
	RereadFailures   uint64
	RereadDuration   time.Duration
	InvalidEntries   uint64
	MassRevokes      uint64
	EpochChanges     uint64
	LastSequence     uint64
	Regressed        bool
	RevokeBlocked    bool
	ConfigDrift      bool
}

type snapshot struct {
	grants    credential.Grants
	checkedAt time.Time
}

type counters struct {
	checks           atomic.Uint64
	checkFailures    atomic.Uint64
	leaderlessChecks atomic.Uint64
	quorumlessChecks atomic.Uint64
	regressedChecks  atomic.Uint64
	driftedChecks    atomic.Uint64
	rereads          atomic.Uint64
	rereadFailures   atomic.Uint64
	rereadDuration   atomic.Int64
	invalidEntries   atomic.Uint64
	massRevokes      atomic.Uint64
	epochChanges     atomic.Uint64
	lastSequence     atomic.Uint64
	regressed        atomic.Bool
	revokeBlocked    atomic.Bool
	configDrift      atomic.Bool
}

type Directory struct {
	state   readState
	entries readEntries
	options DirectoryOptions
	logger  *slog.Logger
	clock   clock
	current atomic.Pointer[snapshot]
	running atomic.Bool
	stats   counters
	epoch   epoch
	checks  checkLog
}

func newDirectory(state readState, entries readEntries, options *DirectoryOptions) (*Directory, error) {
	if err := options.validate(); err != nil {
		return nil, err
	}
	directory := &Directory{
		state:   state,
		entries: entries,
		options: *options,
		logger:  loggerOf(options.Logger),
		clock:   clockOf(options.Clock),
		checks: checkLog{
			next:       map[string]time.Time{},
			suppressed: map[string]uint64{},
		},
	}
	directory.current.Store(&snapshot{})
	return directory, nil
}

func (d *Directory) Snapshot(context.Context) ingestauth.Snapshot {
	current := d.current.Load()
	return ingestauth.Snapshot{
		Grants:    current.grants,
		CheckedAt: current.checkedAt,
	}
}

func (d *Directory) Stats() DirectoryStats {
	return DirectoryStats{
		Tokens:           len(d.current.Load().grants),
		Checks:           d.stats.checks.Load(),
		CheckFailures:    d.stats.checkFailures.Load(),
		LeaderlessChecks: d.stats.leaderlessChecks.Load(),
		QuorumlessChecks: d.stats.quorumlessChecks.Load(),
		RegressedChecks:  d.stats.regressedChecks.Load(),
		DriftedChecks:    d.stats.driftedChecks.Load(),
		Rereads:          d.stats.rereads.Load(),
		RereadFailures:   d.stats.rereadFailures.Load(),
		RereadDuration:   time.Duration(d.stats.rereadDuration.Load()),
		InvalidEntries:   d.stats.invalidEntries.Load(),
		MassRevokes:      d.stats.massRevokes.Load(),
		EpochChanges:     d.stats.epochChanges.Load(),
		LastSequence:     d.stats.lastSequence.Load(),
		Regressed:        d.stats.regressed.Load(),
		RevokeBlocked:    d.stats.revokeBlocked.Load(),
		ConfigDrift:      d.stats.configDrift.Load(),
	}
}

func OpenDirectory(ctx context.Context, conn *natsjs.Conn, options *DirectoryOptions) (*Directory, error) {
	reader := bucketReader{
		nc:     conn.Control.NATS,
		stream: streamName,
	}
	directory, err := newDirectory(reader.readState, reader.readEntries, options)
	if err != nil {
		return nil, err
	}
	if err := directory.load(ctx); err != nil {
		return nil, err
	}
	return directory, nil
}

func WaitsFor(err error) bool {
	return errors.Is(err, ErrBucketMissing) ||
		errors.Is(err, ErrNoLeader) ||
		errors.Is(err, ErrNoQuorum) ||
		natsjs.Temporary(err)
}

func loggerOf(logger *slog.Logger) *slog.Logger {
	if logger == nil {
		return slog.New(slog.DiscardHandler)
	}
	return logger
}

func clockOf(given clock) clock {
	if given == nil {
		return systemClock{}
	}
	return given
}

func allPositive(periods ...time.Duration) bool {
	return !slices.ContainsFunc(periods, func(period time.Duration) bool {
		return period <= 0
	})
}
