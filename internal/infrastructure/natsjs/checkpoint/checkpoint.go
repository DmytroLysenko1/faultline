package checkpoint

import (
	"errors"
	"fmt"
	"math/bits"
	"time"

	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs"
)

var (
	ErrInvalidOptions = errors.New("invalid checkpoint options")
	ErrBucketDrift    = errors.New("checkpoint bucket configuration drifted")
)

var (
	errEpochRegressed    = errors.New("checkpoint pointer holds a later epoch")
	errPointerMoved      = errors.New("checkpoint pointer moved since it was read")
	errObjectChanged     = errors.New("checkpoint object changed while it was read")
	errCheckpointMissing = errors.New("checkpoint missing although the journal was trimmed")
	errStreamRecreated   = errors.New("partition stream is not the one the checkpoint pointer was taken from")
	errCorrupt           = errors.New("checkpoint object or pointer corrupt")
	errStoreClosed       = errors.New("checkpoint store closed")
	errStoreState        = errors.New("checkpoint store used out of order")
	errDiscardTarget     = errors.New("discard target does not match the refused checkpoint")
	errLeaseLost         = errors.New("partition lease not valid")
	errPointerForeign    = errors.New("checkpoint pointer was written by another holder")
	errCASUnknown        = errors.New("checkpoint pointer write has no known outcome")
)

const (
	PointerBucket      = "checkpoint_pointers"
	objectBucketFormat = "checkpoints_p%d"
	pointerKeyFormat   = "p%d"
	dedupWindow        = 2 * time.Minute
	uploadSlack        = 10 * time.Second
	clockMargin        = 30 * time.Second
	verifyFloorRate    = 64 << 20
	bucketFactorTenths = 44
	tenths             = 10
)

type Options struct {
	Topology   natsjs.Topology
	Partition  uint16
	Epoch      uint64
	LeaseTTL   time.Duration
	UploadRate int64
	VerifyRate int64
	WorstImage int64
	MaxBytes   int64
	SpoolDir   string
	LimitsHash uint64
	RulesHash  uint32
}

func (o *Options) Validate() error {
	return o.validate(dedupWindow)
}

func (o *Options) validate(window time.Duration) error {
	if o == nil {
		return fmt.Errorf("%w: no options", ErrInvalidOptions)
	}
	if err := o.Topology.Validate(); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidOptions, err)
	}
	checks := []func() error{
		o.validateOwner,
		o.validateRates,
		func() error {
			return o.validateWindow(window)
		},
		o.validateCapacity,
	}
	for _, check := range checks {
		if err := check(); err != nil {
			return err
		}
	}
	return nil
}

func (o *Options) validateWindow(window time.Duration) error {
	if window <= chunkAttempt+uploadSlack {
		return fmt.Errorf("%w: a dedup window of %s does not outlive one chunk attempt and the upload slack",
			ErrInvalidOptions, window)
	}
	if cycle := o.worstCycle(); cycle >= window {
		return fmt.Errorf("%w: worst upload and verify cycle %s is not under the %s dedup window",
			ErrInvalidOptions, cycle, window)
	}
	return nil
}

func (o *Options) validateOwner() error {
	if int(o.Partition) >= o.Topology.Partitions || o.Epoch == 0 || o.LeaseTTL <= 0 || o.SpoolDir == "" {
		return fmt.Errorf("%w: partition %d of %d, epoch %d, lease ttl %s, spool dir %q",
			ErrInvalidOptions, o.Partition, o.Topology.Partitions, o.Epoch, o.LeaseTTL, o.SpoolDir)
	}
	return nil
}

func (o *Options) validateRates() error {
	if o.UploadRate <= 0 || o.VerifyRate < 0 || o.WorstImage <= 0 {
		return fmt.Errorf("%w: upload rate %d, verify rate %d, worst image %d",
			ErrInvalidOptions, o.UploadRate, o.VerifyRate, o.WorstImage)
	}
	return nil
}

func (o *Options) validateCapacity() error {
	if lessProduct(o.MaxBytes, tenths, o.WorstImage, bucketFactorTenths) {
		return fmt.Errorf("%w: bucket of %d bytes is under 4.4 x the worst image of %d bytes",
			ErrInvalidOptions, o.MaxBytes, o.WorstImage)
	}
	return nil
}

func (o *Options) worstCycle() time.Duration {
	return transfer(o.WorstImage, o.UploadRate) + transfer(o.WorstImage, o.verifyRate()) + uploadSlack
}

func (o *Options) verifyRate() int64 {
	if o.VerifyRate > 0 {
		return o.VerifyRate
	}
	return verifyFloorRate
}

func (o *Options) orphanAge() time.Duration {
	return transfer(o.WorstImage, o.UploadRate) + uploadSlack + o.LeaseTTL + clockMargin
}

func (o *Options) objectBucket() string {
	return fmt.Sprintf(objectBucketFormat, o.Partition)
}

func (o *Options) pointerKey() string {
	return fmt.Sprintf(pointerKeyFormat, o.Partition)
}

func lessProduct(a, aFactor, b, bFactor int64) bool {
	aHigh, aLow := bits.Mul64(unsigned(a), unsigned(aFactor))
	bHigh, bLow := bits.Mul64(unsigned(b), unsigned(bFactor))
	return aHigh < bHigh || aHigh == bHigh && aLow < bLow
}

func transfer(size, rate int64) time.Duration {
	return time.Duration(float64(size) / float64(rate) * float64(time.Second))
}
