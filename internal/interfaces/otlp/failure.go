package otlp

import (
	"context"
	"errors"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/DmytroLysenko1/faultline/internal/application/ingest"
)

const (
	overloadedMessage  = "span journal overloaded, retry later"
	tooLargeMessage    = "request exceeds span journal capacity"
	unavailableMessage = "span journal unavailable"
	internalMessage    = "internal error"
)

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
	return []failureRule{
		{
			target: ingest.ErrOverloaded,
			status: overloadedStatus,
			report: (*events).overloaded,
		},
		{
			target: ingest.ErrBatchTooLarge,
			status: tooLargeStatus,
			report: (*events).tooLarge,
		},
		{
			target: ingest.ErrUnavailable,
			status: unavailableStatus,
			report: (*events).interrupted,
		},
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

func overloadedStatus(error) *status.Status {
	overloaded := status.New(codes.ResourceExhausted, overloadedMessage)
	detailed, err := overloaded.WithDetails(&errdetails.RetryInfo{
		RetryDelay: durationpb.New(RetryDelay),
	})
	if err != nil {
		return overloaded
	}
	return detailed
}

func tooLargeStatus(error) *status.Status {
	return status.New(codes.InvalidArgument, tooLargeMessage)
}

func unavailableStatus(error) *status.Status {
	return status.New(codes.Unavailable, unavailableMessage)
}
