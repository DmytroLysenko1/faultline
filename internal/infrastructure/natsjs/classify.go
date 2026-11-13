package natsjs

import (
	"context"
	"errors"
	"fmt"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/DmytroLysenko1/faultline/internal/application/ingest"
)

type Reason uint8

const (
	ReasonUnexpected Reason = iota
	ReasonNoResponders
	ReasonTimeout
	ReasonDisconnected
	ReasonStoreFailed
	ReasonInProcess
	ReasonInsufficientResources
	ReasonStreamMismatch
	ReasonSealed
	ReasonAccountLimits
	ReasonStreamFull
	ReasonTooLarge
	ReasonCanceled
	ReasonClosed
	reasonCount
)

const ReasonCount = int(reasonCount)

var reasonNames = [reasonCount]string{
	ReasonUnexpected:            "unexpected",
	ReasonNoResponders:          "no_responders",
	ReasonTimeout:               "timeout",
	ReasonDisconnected:          "disconnected",
	ReasonStoreFailed:           "store_failed",
	ReasonInProcess:             "in_process",
	ReasonInsufficientResources: "insufficient_resources",
	ReasonStreamMismatch:        "stream_mismatch",
	ReasonSealed:                "sealed",
	ReasonAccountLimits:         "account_limits",
	ReasonStreamFull:            "stream_full",
	ReasonTooLarge:              "too_large",
	ReasonCanceled:              "canceled",
	ReasonClosed:                "closed",
}

func (r Reason) String() string {
	if r >= reasonCount {
		return reasonNames[ReasonUnexpected]
	}
	return reasonNames[r]
}

func (r Reason) sentinel() error {
	if r == ReasonStreamFull {
		return ingest.ErrOverloaded
	}
	if r == ReasonTooLarge {
		return ingest.ErrBatchTooLarge
	}
	return ingest.ErrUnavailable
}

const (
	codeAccountLimits        jetstream.ErrorCode = 10002
	codeNoSuitablePeers      jetstream.ErrorCode = 10005
	codeClusterUnavailable   jetstream.ErrorCode = 10008
	codeInsufficientResource jetstream.ErrorCode = 10023
	codeStorageResources     jetstream.ErrorCode = 10047
	codeMessageTooLarge      jetstream.ErrorCode = 10054
	codeStreamMismatch       jetstream.ErrorCode = 10060
	codeStoreFailed          jetstream.ErrorCode = 10077
	codeHeaderTooLarge       jetstream.ErrorCode = 10097
	codeStreamSealed         jetstream.ErrorCode = 10109
	codeNoLimits             jetstream.ErrorCode = 10120
	codeDuplicateInProcess   jetstream.ErrorCode = 10158
)

var codeReasons = map[jetstream.ErrorCode]Reason{
	codeAccountLimits:        ReasonAccountLimits,
	codeClusterUnavailable:   ReasonNoResponders,
	codeInsufficientResource: ReasonInsufficientResources,
	codeStorageResources:     ReasonInsufficientResources,
	codeMessageTooLarge:      ReasonTooLarge,
	codeStreamMismatch:       ReasonStreamMismatch,
	codeHeaderTooLarge:       ReasonTooLarge,
	codeStreamSealed:         ReasonSealed,
	codeNoLimits:             ReasonAccountLimits,
	codeDuplicateInProcess:   ReasonInProcess,
}

const (
	storeFullBytes    = "maximum bytes exceeded"
	storeFullMessages = "maximum messages exceeded"
	storeTooLarge     = "message too large"
)

var storeReasons = map[string]Reason{
	storeFullBytes:    ReasonStreamFull,
	storeFullMessages: ReasonStreamFull,
	storeTooLarge:     ReasonTooLarge,
}

type clientRule struct {
	target error
	reason Reason
}

var clientRules = []clientRule{
	{target: jetstream.ErrNoStreamResponse, reason: ReasonNoResponders},
	{target: nats.ErrNoResponders, reason: ReasonNoResponders},
	{target: context.DeadlineExceeded, reason: ReasonTimeout},
	{target: context.Canceled, reason: ReasonCanceled},
	{target: nats.ErrTimeout, reason: ReasonTimeout},
	{target: nats.ErrMaxPayload, reason: ReasonTooLarge},
	{target: nats.ErrReconnectBufExceeded, reason: ReasonDisconnected},
	{target: nats.ErrConnectionClosed, reason: ReasonClosed},
	{target: nats.ErrConnectionDraining, reason: ReasonClosed},
	{target: nats.ErrConnectionReconnecting, reason: ReasonDisconnected},
	{target: nats.ErrDisconnected, reason: ReasonDisconnected},
}

type PublishError struct {
	Reason Reason
	Code   uint16
	Cause  error
}

func (e *PublishError) Error() string {
	return fmt.Sprintf("journal publish: %s: %v", e.Reason, e.Cause)
}

func (e *PublishError) Unwrap() []error {
	return []error{e.Reason.sentinel(), e.Cause}
}

func classify(err error) *PublishError {
	apiErr, isAPI := errors.AsType[*jetstream.APIError](err)
	if !isAPI {
		return &PublishError{
			Reason: clientReason(err),
			Cause:  err,
		}
	}
	return &PublishError{
		Reason: serverReason(apiErr),
		Code:   uint16(apiErr.ErrorCode),
		Cause:  err,
	}
}

func serverReason(apiErr *jetstream.APIError) Reason {
	if apiErr.ErrorCode == codeStoreFailed {
		return storeReason(apiErr.Description)
	}
	if reason, known := codeReasons[apiErr.ErrorCode]; known {
		return reason
	}
	return ReasonUnexpected
}

func storeReason(description string) Reason {
	if reason, known := storeReasons[description]; known {
		return reason
	}
	return ReasonStoreFailed
}

func clientReason(err error) Reason {
	for _, rule := range clientRules {
		if errors.Is(err, rule.target) {
			return rule.reason
		}
	}
	return ReasonUnexpected
}
