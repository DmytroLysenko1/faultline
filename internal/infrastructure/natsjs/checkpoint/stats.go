package checkpoint

import (
	"sync/atomic"
	"time"

	"github.com/samber/lo"

	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs"
)

type skipReason uint8

const (
	skipBusy skipReason = iota
	skipLeaseSuspended
	skipBucketFull
	skipRetentionSuspended
	skipStopped
	skipNotStarted
	skipFinalSkipped
	skipReasons
)

var skipNames = [skipReasons]string{
	skipBusy:               "busy",
	skipLeaseSuspended:     "lease_suspended",
	skipBucketFull:         "bucket_full",
	skipRetentionSuspended: "retention_suspended",
	skipStopped:            "stopped",
	skipNotStarted:         "not_started",
	skipFinalSkipped:       "final_skipped",
}

type uploadClass uint8

const (
	noUploadClass uploadClass = iota
	classOther
	classBucketFull
	classSealed
	classTooLarge
	classTimeout
	classForeignStream
	uploadClasses
)

var uploadClassNames = [uploadClasses]string{
	noUploadClass:      "",
	classOther:         "other",
	classBucketFull:    "bucket_full",
	classSealed:        "sealed",
	classTooLarge:      "too_large",
	classTimeout:       "timeout",
	classForeignStream: "foreign_stream",
}

var reasonClasses = map[natsjs.Reason]uploadClass{
	natsjs.ReasonUnexpected:            classOther,
	natsjs.ReasonNoResponders:          classTimeout,
	natsjs.ReasonTimeout:               classTimeout,
	natsjs.ReasonDisconnected:          classTimeout,
	natsjs.ReasonStoreFailed:           classOther,
	natsjs.ReasonInProcess:             classTimeout,
	natsjs.ReasonInsufficientResources: classBucketFull,
	natsjs.ReasonStreamMismatch:        classForeignStream,
	natsjs.ReasonSealed:                classSealed,
	natsjs.ReasonAccountLimits:         classBucketFull,
	natsjs.ReasonStreamFull:            classBucketFull,
	natsjs.ReasonTooLarge:              classTooLarge,
	natsjs.ReasonCanceled:              classOther,
	natsjs.ReasonClosed:                classOther,
}

type failReason uint8

const (
	failUpload failReason = iota
	failChunkCount
	failVerify
	failCAS
	failRegressed
	failNameCollision
	failCommit
	failBucketRead
	failBucketUnresolved
	failReasons
)

var failNames = [failReasons]string{
	failUpload:           "upload",
	failChunkCount:       "chunk_count",
	failVerify:           "verify",
	failCAS:              "cas",
	failRegressed:        "position_regressed",
	failNameCollision:    "name_collision",
	failCommit:           "commit",
	failBucketRead:       "bucket_read",
	failBucketUnresolved: "bucket_unresolved",
}

type Stats struct {
	Epoch              uint64
	Committed          uint64
	CommittedThrough   uint64
	CommittedAt        time.Time
	LastBytes          int64
	LastUpload         time.Duration
	LastVerify         time.Duration
	MeasuredUploadRate int64
	MeasuredVerifyRate int64
	BucketBytes        uint64
	BucketMaxBytes     int64
	Skipped            map[string]uint64
	Failed             map[string]uint64
	UploadFailed       map[string]uint64
	LastFailure        error
	SpoolLeft          uint64
	LeftAtStop         uint64
	Pending            uint64
	Fences             uint64
	CASUnknown         uint64
	PurgeLeftover      uint64
	Cycles             uint64
	Finals             uint64
	PreviousLeftAtStop uint64
	SweptObjects       uint64
	SweptOrphans       uint64
	SweepFailures      uint64
	UnreadableMeta     uint64
	LastSweepError     error
}

type commitRecord struct {
	through  uint64
	at       time.Time
	measured *measurement
	final    bool
}

type measurement struct {
	bytes    int64
	upload   time.Duration
	verify   time.Duration
	uploadBs int64
	verifyBs int64
}

type counters struct {
	sweptObjects  atomic.Uint64
	sweptOrphans  atomic.Uint64
	sweepFailures atomic.Uint64
	unreadable    atomic.Uint64
	lastSweep     atomic.Pointer[error]
	committed     atomic.Uint64
	lastCommit    atomic.Pointer[commitRecord]
	lastMeasure   atomic.Pointer[measurement]
	bucketBytes   atomic.Uint64
	skipped       [skipReasons]atomic.Uint64
	failed        [failReasons]atomic.Uint64
	uploadFailed  [uploadClasses]atomic.Uint64
	lastFailure   atomic.Pointer[error]
	spoolLeft     atomic.Uint64
	leftAtStop    atomic.Uint64
	pending       atomic.Uint64
	fences        atomic.Uint64
	casUnknown    atomic.Uint64
	purgeLeftover atomic.Uint64
	cycles        atomic.Uint64
	finals        atomic.Uint64
	previousLeft  atomic.Uint64
}

func (c *counters) snapshot(epoch uint64, maxBytes int64) Stats {
	stats := Stats{
		Epoch:          epoch,
		Committed:      c.committed.Load(),
		BucketBytes:    c.bucketBytes.Load(),
		BucketMaxBytes: maxBytes,
		Skipped:        reasonCounts(skipNames[:], c.skipped[:]),
		Failed:         reasonCounts(failNames[:], c.failed[:]),
		UploadFailed:   reasonCounts(uploadClassNames[classOther:], c.uploadFailed[classOther:]),
		LastFailure:    loaded(c.lastFailure.Load()),
		SpoolLeft:      c.spoolLeft.Load(),
		LeftAtStop:     c.leftAtStop.Load(),
		Pending:        c.pending.Load(),
		Fences:         c.fences.Load(),
		CASUnknown:     c.casUnknown.Load(),
		PurgeLeftover:  c.purgeLeftover.Load(),
		Cycles:         c.cycles.Load(),
		SweptObjects:   c.sweptObjects.Load(),
		SweptOrphans:   c.sweptOrphans.Load(),
		SweepFailures:  c.sweepFailures.Load(),
		UnreadableMeta: c.unreadable.Load(),
		LastSweepError: loaded(c.lastSweep.Load()),
	}
	c.withCommits(&stats)
	return stats
}

func (c *counters) withCommits(stats *Stats) {
	stats.Finals = c.finals.Load()
	stats.PreviousLeftAtStop = c.previousLeft.Load()
	if last := c.lastCommit.Load(); last != nil {
		stats.CommittedThrough = last.through
		stats.CommittedAt = last.at
	}
	if last := c.lastMeasure.Load(); last != nil {
		stats.withMeasure(last)
	}
}

func (s *Stats) withMeasure(last *measurement) {
	s.LastBytes = last.bytes
	s.LastUpload = last.upload
	s.LastVerify = last.verify
	s.MeasuredUploadRate = last.uploadBs
	s.MeasuredVerifyRate = last.verifyBs
}

func reasonCounts(names []string, counts []atomic.Uint64) map[string]uint64 {
	return lo.SliceToMap(lo.Range(len(names)), func(i int) (string, uint64) {
		return names[i], counts[i].Load()
	})
}

func loaded(err *error) error {
	if err == nil {
		return nil
	}
	return *err
}

func (c *counters) sweepFailed(err error) {
	c.sweepFailures.Add(1)
	c.lastSweep.Store(new(translate(err)))
}

func (c *counters) skip(reason skipReason) {
	c.skipped[reason].Add(1)
}

func (c *counters) fail(reason failReason, err error) {
	c.failed[reason].Add(1)
	c.lastFailure.Store(new(translate(err)))
}

func (c *counters) failUpload(class uploadClass) {
	if class != noUploadClass {
		c.uploadFailed[class].Add(1)
	}
}

func (c *counters) commit(record *commitRecord) {
	c.committed.Add(1)
	c.lastCommit.Store(record)
	if record.final {
		c.finals.Add(1)
	}
	if record.measured != nil {
		c.lastMeasure.Store(record.measured)
	}
}

func rate(bytes int64, took time.Duration) int64 {
	if took <= 0 {
		return 0
	}
	return int64(float64(bytes) / took.Seconds())
}
