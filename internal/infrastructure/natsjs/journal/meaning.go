package journal

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/DmytroLysenko1/faultline/internal/application/ingest"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/record"
)

var errPublishTimeout = errors.New("publish ran out of its budget")

type ingestError struct {
	meaning error
	cause   error
}

func (f *ingestError) Error() string {
	return f.cause.Error()
}

func (f *ingestError) Unwrap() []error {
	return []error{f.meaning, f.cause}
}

func toIngest(err error) error {
	meaning := meaningOf(err)
	if errors.Is(err, meaning) || (errors.Is(meaning, ingest.ErrUnavailable) && hasFinalMeaning(err)) {
		return err
	}
	return &ingestError{
		meaning: meaning,
		cause:   err,
	}
}

func meaningOf(err error) error {
	if errors.Is(err, record.ErrTooLarge) {
		return ingest.ErrBatchTooLarge
	}
	failure, classified := errors.AsType[*natsjs.PublishError](err)
	if !classified {
		return ingest.ErrUnavailable
	}
	return meaningOfReason(failure.Reason)
}

func meaningOfReason(reason natsjs.Reason) error {
	switch reason {
	case natsjs.ReasonStreamFull:
		return ingest.ErrOverloaded
	case natsjs.ReasonTooLarge:
		return ingest.ErrBatchTooLarge
	default:
		return ingest.ErrUnavailable
	}
}

func finalMeanings() []error {
	return []error{ingest.ErrBatchTooLarge, ingest.ErrOverloaded}
}

func hasFinalMeaning(err error) bool {
	return slices.ContainsFunc(finalMeanings(), func(meaning error) bool {
		return errors.Is(err, meaning)
	})
}

func finalFailure(errs []error) error {
	for _, meaning := range finalMeanings() {
		if err := firstMatching(errs, meaning); err != nil {
			return err
		}
	}
	return nil
}

func firstFailure(errs []error) error {
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

func firstMatching(errs []error, target error) error {
	for _, err := range errs {
		if errors.Is(err, target) {
			return err
		}
	}
	return nil
}

func notAttempted(cause error) error {
	return ingest.NotAttempted(cause)
}

func withoutDeadline(failure error) error {
	if !errors.Is(failure, context.DeadlineExceeded) {
		return failure
	}
	return &ingestError{
		meaning: ingest.ErrUnavailable,
		cause:   fmt.Errorf("%w: %s", errPublishTimeout, failure.Error()),
	}
}
