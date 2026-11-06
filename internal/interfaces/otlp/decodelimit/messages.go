package decodelimit

func newSchema() *schema {
	return &schema{
		requestMessageKind:       requestMessage(),
		resourceSpansMessageKind: resourceSpansMessage(),
		resourceMessageKind:      resourceMessage(),
		entityRefMessageKind:     entityRefMessage(),
		scopeSpansMessageKind:    scopeSpansMessage(),
		scopeMessageKind:         scopeMessage(),
		spanMessageKind:          spanMessage(),
		eventMessageKind:         eventMessage(),
		linkMessageKind:          linkMessage(),
		statusMessageKind:        statusMessage(),
		keyValueMessageKind:      keyValueMessage(),
		anyValueMessageKind:      anyValueMessage(),
		arrayValueMessageKind:    arrayValueMessage(),
		keyValueListMessageKind:  keyValueListMessage(),
	}
}

func requestMessage() message {
	return message{
		own: ownRequest,
		fields: []field{
			child(fieldRequestResourceSpans, resourceSpansMessageKind, pointerSlot),
		},
	}
}

func resourceSpansMessage() message {
	return message{
		own: ownResourceSpans,
		fields: []field{
			child(fieldResourceSpansResource, resourceMessageKind, 0),
			child(fieldResourceSpansScopeSpans, scopeSpansMessageKind, pointerSlot),
			textField(fieldResourceSpansSchemaURL, 0),
			child(fieldResourceSpansDeprecated, scopeSpansMessageKind, pointerSlot),
		},
	}
}

func resourceMessage() message {
	return message{
		fields: []field{
			child(fieldResourceAttributes, keyValueMessageKind, keyValueSlot),
			child(fieldResourceEntityRefs, entityRefMessageKind, pointerSlot),
		},
	}
}

func entityRefMessage() message {
	return message{
		own: ownEntityRef,
		fields: []field{
			textField(fieldEntityRefSchemaURL, 0),
			textField(fieldEntityRefType, 0),
			textField(fieldEntityRefIDKeys, stringSlot),
			textField(fieldEntityRefDescriptionKeys, stringSlot),
		},
	}
}

func scopeSpansMessage() message {
	return message{
		own: ownScopeSpans,
		fields: []field{
			child(fieldScopeSpansScope, scopeMessageKind, 0),
			child(fieldScopeSpansSpans, spanMessageKind, pointerSlot),
			textField(fieldScopeSpansSchemaURL, 0),
		},
	}
}

func scopeMessage() message {
	return message{
		fields: []field{
			textField(fieldScopeName, 0),
			textField(fieldScopeVersion, 0),
			child(fieldScopeAttributes, keyValueMessageKind, keyValueSlot),
		},
	}
}

func spanMessage() message {
	return message{
		own: ownSpan,
		fields: []field{
			textField(fieldSpanTraceState, 0),
			textField(fieldSpanName, 0),
			child(fieldSpanAttributes, keyValueMessageKind, keyValueSlot),
			child(fieldSpanEvents, eventMessageKind, pointerSlot),
			child(fieldSpanLinks, linkMessageKind, pointerSlot),
			child(fieldSpanStatus, statusMessageKind, 0),
		},
	}
}

func eventMessage() message {
	return message{
		own: ownEvent,
		fields: []field{
			textField(fieldEventName, 0),
			child(fieldEventAttributes, keyValueMessageKind, keyValueSlot),
		},
	}
}

func linkMessage() message {
	return message{
		own: ownLink,
		fields: []field{
			textField(fieldLinkTraceState, 0),
			child(fieldLinkAttributes, keyValueMessageKind, keyValueSlot),
		},
	}
}

func statusMessage() message {
	return message{
		fields: []field{
			textField(fieldStatusMessage, 0),
		},
	}
}

func keyValueMessage() message {
	return message{
		fields: []field{
			textField(fieldKeyValueKey, 0),
			child(fieldKeyValueValue, anyValueMessageKind, 0),
		},
	}
}

func anyValueMessage() message {
	return message{
		fields: []field{
			oneof(fieldAnyValueString, text, 0, stringWrapper),
			oneof(fieldAnyValueBool, scalar, 0, scalarWrapper),
			oneof(fieldAnyValueInt, scalar, 0, scalarWrapper),
			oneof(fieldAnyValueDouble, scalar, 0, scalarWrapper),
			oneof(fieldAnyValueArray, nested, arrayValueMessageKind, containerWrapper),
			oneof(fieldAnyValueKeyValues, nested, keyValueListMessageKind, containerWrapper),
			oneof(fieldAnyValueBytes, text, 0, bytesWrapper),
			oneof(fieldAnyValueStrindex, scalar, 0, scalarWrapper),
		},
	}
}

func arrayValueMessage() message {
	return message{
		fields: []field{
			child(fieldArrayValues, anyValueMessageKind, anyValueSlot),
		},
	}
}

func keyValueListMessage() message {
	return message{
		fields: []field{
			child(fieldKeyValueListValues, keyValueMessageKind, keyValueSlot),
		},
	}
}
