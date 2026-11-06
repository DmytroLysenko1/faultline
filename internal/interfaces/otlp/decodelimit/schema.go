package decodelimit

import "google.golang.org/protobuf/encoding/protowire"

type messageKind uint8

const (
	requestMessageKind messageKind = iota
	resourceSpansMessageKind
	resourceMessageKind
	entityRefMessageKind
	scopeSpansMessageKind
	scopeMessageKind
	spanMessageKind
	eventMessageKind
	linkMessageKind
	statusMessageKind
	keyValueMessageKind
	anyValueMessageKind
	arrayValueMessageKind
	keyValueListMessageKind
	messageKindCount
)

type role uint8

const (
	scalar role = iota
	nested
	text
)

const (
	ownRequest       = 32
	ownResourceSpans = 128
	ownScopeSpans    = 112
	ownSpan          = 208
	ownEvent         = 64
	ownLink          = 80
	ownEntityRef     = 80

	pointerSlot  = 8
	keyValueSlot = 40
	anyValueSlot = 16
	stringSlot   = 16

	stringWrapper    = 16
	bytesWrapper     = 24
	scalarWrapper    = 8
	containerWrapper = 32
)

type field struct {
	number  protowire.Number
	role    role
	child   messageKind
	slot    int64
	wrapper int64
}

type message struct {
	own    int64
	fields []field
}

type schema [messageKindCount]message

func (m message) lookup(number protowire.Number) (field, bool) {
	for _, candidate := range m.fields {
		if candidate.number == number {
			return candidate, true
		}
	}
	return field{}, false
}

func child(number protowire.Number, of messageKind, slot int64) field {
	return field{
		number: number,
		role:   nested,
		child:  of,
		slot:   slot,
	}
}

func textField(number protowire.Number, slot int64) field {
	return field{
		number: number,
		role:   text,
		slot:   slot,
	}
}

func oneof(number protowire.Number, of role, nestedKind messageKind, wrapper int64) field {
	return field{
		number:  number,
		role:    of,
		child:   nestedKind,
		wrapper: wrapper,
	}
}
