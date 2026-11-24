package natsjs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"slices"
	"syscall"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type DialReason string

const (
	DialDNS            DialReason = "dns"
	DialNetwork        DialReason = "network"
	DialNoNATSGreeting DialReason = "no_nats_greeting"
	DialNoServers      DialReason = "no_servers"
)

const (
	opDial = "dial"
	opRead = "read"
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

func (r Reason) retryable() bool {
	return r == ReasonNoResponders || r == ReasonDisconnected || r == ReasonTimeout
}

const (
	codeAccountLimits        jetstream.ErrorCode = 10002
	codeNoSuitablePeers      jetstream.ErrorCode = 10005
	codeClusterUnavailable   jetstream.ErrorCode = 10008
	codeNotLeader            jetstream.ErrorCode = 10009
	codeTooManyRequests      jetstream.ErrorCode = 10167
	codeInsufficientResource jetstream.ErrorCode = 10023
	codeStorageResources     jetstream.ErrorCode = 10047
	codeMessageTooLarge      jetstream.ErrorCode = 10054
	codeStreamMismatch       jetstream.ErrorCode = 10060
	codeStoreFailed          jetstream.ErrorCode = 10077
	codeHeaderTooLarge       jetstream.ErrorCode = 10097
	codeStreamSealed         jetstream.ErrorCode = 10109
	codeNoLimits             jetstream.ErrorCode = 10120
	codeDuplicateInProcess   jetstream.ErrorCode = 10158
	codePurgeFailed          jetstream.ErrorCode = 10110
)

var codeReasons = map[jetstream.ErrorCode]Reason{
	codeAccountLimits:        ReasonAccountLimits,
	codeNoSuitablePeers:      ReasonInsufficientResources,
	codeClusterUnavailable:   ReasonNoResponders,
	codeNotLeader:            ReasonNoResponders,
	codeTooManyRequests:      ReasonNoResponders,
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

func (e *PublishError) Unwrap() error {
	return e.Cause
}

func Classify(err error) *PublishError {
	if classified, found := errors.AsType[*PublishError](err); found {
		return classified
	}
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

func WrongLastSequence(err error) bool {
	return HasCode(err, jetstream.JSErrCodeStreamWrongLastSequence) ||
		HasCode(err, jetstream.JSErrCodeStreamWrongLastSequenceConstant)
}

func Unsent(err error) bool {
	return errors.Is(err, nats.ErrReconnectBufExceeded) ||
		errors.Is(err, nats.ErrNoResponders) ||
		errors.Is(err, jetstream.ErrNoStreamResponse)
}

func PurgeDenied(err error) bool {
	return HasCode(err, codePurgeFailed)
}

func HasCode(err error, code jetstream.ErrorCode) bool {
	apiErr, isAPI := errors.AsType[*jetstream.APIError](err)
	return isAPI && apiErr.ErrorCode == code
}

type dialRule struct {
	matches func(err error) bool
	reason  DialReason
}

func dialRules() []dialRule {
	return []dialRule{
		{matches: lookupFailed, reason: DialDNS},
		{matches: greetingMissing, reason: DialNoNATSGreeting},
		{matches: networkFailed, reason: DialNetwork},
		{matches: noServers, reason: DialNoServers},
	}
}

func Unreachable(err error) (DialReason, bool) {
	if err == nil || errors.Is(err, context.Canceled) {
		return "", false
	}
	for _, rule := range dialRules() {
		if rule.matches(err) {
			return rule.reason, true
		}
	}
	return "", false
}

func lookupFailed(err error) bool {
	_, found := errors.AsType[*net.DNSError](err)
	return found
}

func greetingMissing(err error) bool {
	return errors.Is(err, io.EOF) || timedOut(err, opRead)
}

func networkFailed(err error) bool {
	return timedOut(err, opDial) || isAny(err, networkErrors())
}

func noServers(err error) bool {
	return errors.Is(err, nats.ErrNoServers)
}

func timedOut(err error, op string) bool {
	failure, found := errors.AsType[*net.OpError](err)
	return found && failure.Op == op && failure.Timeout()
}

func networkErrors() []error {
	return []error{
		syscall.ECONNRESET,
		syscall.EPIPE,
		syscall.EHOSTUNREACH,
		syscall.ENETUNREACH,
		nats.ErrTimeout,
		context.DeadlineExceeded,
	}
}

func isAny(err error, targets []error) bool {
	return slices.ContainsFunc(targets, func(target error) bool {
		return errors.Is(err, target)
	})
}
