package decodelimit

import "google.golang.org/protobuf/encoding/protowire"

const (
	fieldRequestResourceSpans protowire.Number = 1

	fieldResourceSpansResource   protowire.Number = 1
	fieldResourceSpansScopeSpans protowire.Number = 2
	fieldResourceSpansSchemaURL  protowire.Number = 3
	fieldResourceSpansDeprecated protowire.Number = 1000

	fieldResourceAttributes protowire.Number = 1
	fieldResourceEntityRefs protowire.Number = 3

	fieldEntityRefSchemaURL       protowire.Number = 1
	fieldEntityRefType            protowire.Number = 2
	fieldEntityRefIDKeys          protowire.Number = 3
	fieldEntityRefDescriptionKeys protowire.Number = 4

	fieldScopeSpansScope     protowire.Number = 1
	fieldScopeSpansSpans     protowire.Number = 2
	fieldScopeSpansSchemaURL protowire.Number = 3

	fieldScopeName       protowire.Number = 1
	fieldScopeVersion    protowire.Number = 2
	fieldScopeAttributes protowire.Number = 3

	fieldSpanTraceState protowire.Number = 3
	fieldSpanName       protowire.Number = 5
	fieldSpanAttributes protowire.Number = 9
	fieldSpanEvents     protowire.Number = 11
	fieldSpanLinks      protowire.Number = 13
	fieldSpanStatus     protowire.Number = 15

	fieldEventName       protowire.Number = 2
	fieldEventAttributes protowire.Number = 3

	fieldLinkTraceState protowire.Number = 3
	fieldLinkAttributes protowire.Number = 4

	fieldStatusMessage protowire.Number = 2

	fieldKeyValueKey   protowire.Number = 1
	fieldKeyValueValue protowire.Number = 2

	fieldAnyValueString    protowire.Number = 1
	fieldAnyValueBool      protowire.Number = 2
	fieldAnyValueInt       protowire.Number = 3
	fieldAnyValueDouble    protowire.Number = 4
	fieldAnyValueArray     protowire.Number = 5
	fieldAnyValueKeyValues protowire.Number = 6
	fieldAnyValueBytes     protowire.Number = 7
	fieldAnyValueStrindex  protowire.Number = 8

	fieldArrayValues        protowire.Number = 1
	fieldKeyValueListValues protowire.Number = 1
)
