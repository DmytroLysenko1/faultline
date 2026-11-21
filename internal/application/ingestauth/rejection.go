package ingestauth

import (
	"errors"

	"github.com/DmytroLysenko1/faultline/internal/domain/credential"
)

type Reason uint8

const (
	ReasonMalformed Reason = iota
	ReasonUnknown
	ReasonMismatch
	ReasonTenant
	reasonCount
)

const ReasonCount = int(reasonCount)

var reasonNames = [reasonCount]string{
	ReasonMalformed: "malformed",
	ReasonUnknown:   "unknown",
	ReasonMismatch:  "mismatch",
	ReasonTenant:    "tenant",
}

func (r Reason) String() string {
	if r >= reasonCount {
		return reasonNames[ReasonMalformed]
	}
	return reasonNames[r]
}

type RejectionError struct {
	Reason  Reason
	TokenID credential.ID
}

func (RejectionError) Error() string {
	return ErrUnauthenticated.Error()
}

func (RejectionError) Unwrap() error {
	return ErrUnauthenticated
}

type admitRule struct {
	target error
	reason Reason
}

var admitRules = []admitRule{
	{target: credential.ErrUnknownGrant, reason: ReasonUnknown},
	{target: credential.ErrDigestMismatch, reason: ReasonMismatch},
	{target: credential.ErrForeignTenant, reason: ReasonTenant},
}

func admitReason(err error) Reason {
	for _, rule := range admitRules {
		if errors.Is(err, rule.target) {
			return rule.reason
		}
	}
	return ReasonUnknown
}
