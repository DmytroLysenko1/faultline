package operation

import (
	"errors"
	"fmt"
	"unicode/utf8"
)

var (
	ErrInvalidOperation = errors.New("invalid operation")
	ErrInvalidRules     = errors.New("invalid route rules")
)

const (
	MaxNameBytes    = 256
	MaxPathSegments = 32
	OverflowName    = "__overflow__"
)

type Protocol uint8

const (
	ProtocolUnspecified Protocol = iota
	ProtocolHTTP
	ProtocolGRPC
	ProtocolRPC
)

type Operation struct {
	Protocol Protocol
	Name     string
}

func (o Operation) Validate() error {
	switch {
	case o.Protocol > ProtocolRPC:
		return fmt.Errorf("%w: protocol %d", ErrInvalidOperation, o.Protocol)
	case (o.Protocol == ProtocolUnspecified) != (o.Name == ""):
		return fmt.Errorf("%w: protocol %d with name %q", ErrInvalidOperation, o.Protocol, o.Name)
	}
	return validateName(o.Name)
}

func validateName(name string) error {
	switch {
	case len(name) > MaxNameBytes || !utf8.ValidString(name):
		return fmt.Errorf("%w: name is not valid UTF-8 of at most %d bytes", ErrInvalidOperation, MaxNameBytes)
	case name == OverflowName:
		return fmt.Errorf("%w: %q is reserved for the overflow series", ErrInvalidOperation, OverflowName)
	}
	return nil
}

type Facts struct {
	HTTPMethod string
	Route      string
	Path       string
	Target     string
	URL        string
	RPCSystem  string
	RPCService string
	RPCMethod  string
}
