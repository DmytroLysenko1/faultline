package otlp

import (
	"context"
	"errors"
	"time"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/DmytroLysenko1/faultline/internal/application/ingest"
	"github.com/DmytroLysenko1/faultline/internal/application/ingestauth"
	"github.com/DmytroLysenko1/faultline/internal/domain/credential"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp/admission"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp/bearer"
)

const (
	overloadedMessage           = "span journal overloaded, retry later"
	tooLargeMessage             = "request exceeds span journal capacity"
	unavailableMessage          = "span journal unavailable"
	internalMessage             = "internal error"
	rejectedMessage             = "ingest token rejected"
	directoryUnavailableMessage = "ingest token directory unavailable"
	shareMessage                = "ingest token uses its share of request slots, retry later"
	slotsMessage                = "ingest request slots are busy, retry later"
	rateMessage                 = "ingest request rate exceeded, retry later"
	tooSlowMessage              = "ingest request body was not received in time, retry"
	reasonMissing               = "missing"
	reasonHeader                = "header"
	headerReasons               = 2
)

type tapReporter func(t *Tap, ctx context.Context, err error)

type reporter func(e *events, ctx context.Context, err error)

type failure struct {
	status *status.Status
	report reporter
}

type failureRule struct {
	target error
	status func(err error) *status.Status
	report reporter
}

func failureOf(err error) failure {
	for _, rule := range failureRules() {
		if errors.Is(err, rule.target) {
			return failure{
				status: rule.status(err),
				report: rule.report,
			}
		}
	}
	return failure{
		status: status.New(codes.Internal, internalMessage),
		report: (*events).internal,
	}
}

func failureRules() []failureRule {
	return append(journalRules(), contextRules()...)
}

func journalRules() []failureRule {
	return []failureRule{
		{
			target: ingest.ErrBatchTooLarge,
			status: tooLargeStatus,
			report: (*events).tooLarge,
		},
		{
			target: ingest.ErrOverloaded,
			status: overloadedStatus,
			report: (*events).overloaded,
		},
		{
			target: ingest.ErrNotAttempted,
			status: unavailableStatus,
			report: (*events).interrupted,
		},
		{
			target: ingest.ErrUnavailable,
			status: unavailableStatus,
			report: (*events).unavailable,
		},
	}
}

func contextRules() []failureRule {
	return []failureRule{
		{
			target: context.Canceled,
			status: status.FromContextError,
			report: (*events).interrupted,
		},
		{
			target: context.DeadlineExceeded,
			status: status.FromContextError,
			report: (*events).interrupted,
		},
	}
}

func overloadedStatus(_ error) *status.Status {
	return retryingStatus(codes.ResourceExhausted, overloadedMessage, RetryDelay)
}

func retryingStatus(code codes.Code, message string, delay time.Duration) *status.Status {
	plain := status.New(code, message)
	detailed, err := plain.WithDetails(&errdetails.RetryInfo{
		RetryDelay: durationpb.New(delay),
	})
	if err != nil {
		return plain
	}
	return detailed
}

func tooLargeStatus(_ error) *status.Status {
	return status.New(codes.InvalidArgument, tooLargeMessage)
}

func unavailableStatus(_ error) *status.Status {
	return status.New(codes.Unavailable, unavailableMessage)
}

type authFailure struct {
	status *status.Status
	report tapReporter
}

type authRule struct {
	target error
	status func() *status.Status
	report tapReporter
}

func authFailureOf(err error) authFailure {
	for _, rule := range authRules() {
		if errors.Is(err, rule.target) {
			return authFailure{
				status: rule.status(),
				report: rule.report,
			}
		}
	}
	return authFailure{
		status: status.New(codes.Internal, internalMessage),
		report: (*Tap).failure,
	}
}

func authRules() []authRule {
	return []authRule{
		{
			target: bearer.ErrMissing,
			status: rejectedStatus,
			report: (*Tap).missingCredential,
		},
		{
			target: bearer.ErrHeader,
			status: rejectedStatus,
			report: (*Tap).malformedHeader,
		},
		{
			target: ingestauth.ErrUnauthenticated,
			status: rejectedStatus,
			report: (*Tap).rejected,
		},
		{
			target: ingestauth.ErrDirectoryStale,
			status: directoryUnavailableStatus,
			report: (*Tap).stale,
		},
	}
}

func rejectionReasons() []string {
	reasons := make([]string, 0, headerReasons+ingestauth.ReasonCount)
	reasons = append(reasons, reasonMissing, reasonHeader)
	for reason := range ingestauth.ReasonCount {
		reasons = append(reasons, ingestauth.Reason(reason).String())
	}
	return reasons
}

func rejectedStatus() *status.Status {
	return status.New(codes.Unauthenticated, rejectedMessage)
}

func directoryUnavailableStatus() *status.Status {
	return retryingStatus(codes.Unavailable, directoryUnavailableMessage, AuthRetryDelay)
}

type admissionReporter func(e *events, ctx context.Context, id credential.ID, err error)

type admissionFailure struct {
	status *status.Status
	report admissionReporter
}

type admissionRule struct {
	target error
	status func() *status.Status
	report admissionReporter
}

func admissionFailureOf(err error) admissionFailure {
	for _, rule := range admissionRules() {
		if errors.Is(err, rule.target) {
			return admissionFailure{
				status: rule.status(),
				report: rule.report,
			}
		}
	}
	return admissionFailure{
		status: status.New(codes.Internal, internalMessage),
		report: (*events).admissionFailed,
	}
}

func admissionRules() []admissionRule {
	return []admissionRule{
		{
			target: admission.ErrShareExceeded,
			status: shareStatus,
			report: (*events).admissionRejected,
		},
		{
			target: admission.ErrSlotsFull,
			status: slotsStatus,
			report: (*events).admissionRejected,
		},
		{
			target: admission.ErrRateExceeded,
			status: rateStatus,
			report: (*events).admissionRejected,
		},
		{
			target: admission.ErrSlotHeldTooLong,
			status: tooSlowStatus,
			report: (*events).bodyTooSlow,
		},
	}
}

func shareStatus() *status.Status {
	return retryingStatus(codes.ResourceExhausted, shareMessage, RetryDelay)
}

func slotsStatus() *status.Status {
	return retryingStatus(codes.ResourceExhausted, slotsMessage, RetryDelay)
}

func rateStatus() *status.Status {
	return retryingStatus(codes.ResourceExhausted, rateMessage, RetryDelay)
}

func tooSlowStatus() *status.Status {
	return status.New(codes.Canceled, tooSlowMessage)
}
