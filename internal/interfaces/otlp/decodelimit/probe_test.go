//go:build !race

package decodelimit_test

import (
	"fmt"
	"testing"

	"google.golang.org/protobuf/encoding/protowire"
)

type probedMessage struct {
	Name    string
	Element func(encoded []byte) []byte
	Wrap    path
}

func within(numbers ...protowire.Number) func(encoded []byte) []byte {
	return func(encoded []byte) []byte {
		for index := len(numbers) - 1; index >= 0; index-- {
			encoded = message(numbers[index], encoded)
		}
		return encoded
	}
}

func inResource(inner []byte) []byte {
	return message(1, message(1, inner))
}

func probedMessages() []probedMessage {
	return []probedMessage{
		{Name: "ResourceSpans", Element: within(1), Wrap: inRequest},
		{Name: "Resource", Element: within(1, 1), Wrap: inRequest},
		{Name: "EntityRef", Element: within(3), Wrap: inResource},
		{Name: "ScopeSpans", Element: within(2), Wrap: inResourceSpans},
		{Name: "InstrumentationScope", Element: within(2, 1), Wrap: inResourceSpans},
		{Name: "Span", Element: within(2), Wrap: inScopeSpans},
		{Name: "Status", Element: within(2, 15), Wrap: inScopeSpans},
		{Name: "Event", Element: within(11), Wrap: inSpan},
		{Name: "Link", Element: within(13), Wrap: inSpan},
		{Name: "KeyValue", Element: within(9), Wrap: inSpan},
		{Name: "AnyValue", Element: within(1), Wrap: inArrayValue},
		{Name: "ArrayValue", Element: within(9, 2, 5), Wrap: inSpan},
		{Name: "KeyValueList", Element: within(9, 2, 6), Wrap: inSpan},
	}
}

func probedFields() []protowire.Number {
	fields := make([]protowire.Number, 0, maxProbedField+1)
	for number := protowire.Number(1); number <= maxProbedField; number++ {
		fields = append(fields, number)
	}
	return append(fields, deprecatedField)
}

func TestWeightBoundsEveryFieldPdataDecodes(t *testing.T) {
	for _, probed := range probedMessages() {
		decoded := 0
		for _, number := range probedFields() {
			for _, encoded := range [][]byte{
				message(number, nil),
				varint(number),
			} {
				wire := probed.Wrap(repeat(probed.Element(encoded), probeCopies))
				if assertBound(t, fmt.Sprintf("%s field %d", probed.Name, number), wire) {
					decoded++
				}
			}
		}
		if decoded == 0 {
			t.Errorf("%s: pdata rejected every probe shape, the probe checked nothing", probed.Name)
		}
	}
}

type spreadShape struct {
	Name   string
	Parent func(children []byte) []byte
	Child  func(length int) []byte
	Wrap   path
}

func keyValue(length int) []byte {
	return message(1, text(length))
}

func spreadShapes() []spreadShape {
	return []spreadShape{
		{
			Name:   "span attributes",
			Parent: func(children []byte) []byte { return message(2, children) },
			Child:  func(length int) []byte { return message(9, keyValue(length)) },
			Wrap:   inScopeSpans,
		},
		{
			Name:   "scope attributes",
			Parent: func(children []byte) []byte { return message(2, message(1, children)) },
			Child:  func(length int) []byte { return message(3, keyValue(length)) },
			Wrap:   inResourceSpans,
		},
		{
			Name:   "resource attributes",
			Parent: func(children []byte) []byte { return message(1, message(1, children)) },
			Child:  func(length int) []byte { return message(1, keyValue(length)) },
			Wrap:   inRequest,
		},
		{
			Name:   "event attributes",
			Parent: func(children []byte) []byte { return message(2, message(11, children)) },
			Child:  func(length int) []byte { return message(3, keyValue(length)) },
			Wrap:   inScopeSpans,
		},
		{
			Name:   "link attributes",
			Parent: func(children []byte) []byte { return message(2, message(13, children)) },
			Child:  func(length int) []byte { return message(4, keyValue(length)) },
			Wrap:   inScopeSpans,
		},
		{
			Name:   "kvlist entries",
			Parent: func(children []byte) []byte { return message(9, message(2, message(6, children))) },
			Child:  func(length int) []byte { return message(1, keyValue(length)) },
			Wrap:   inSpan,
		},
		{
			Name:   "array of bytes",
			Parent: func(children []byte) []byte { return message(9, message(2, message(5, children))) },
			Child:  func(length int) []byte { return message(1, message(7, text(length))) },
			Wrap:   inSpan,
		},
		{
			Name:   "array of strings",
			Parent: func(children []byte) []byte { return message(9, message(2, message(5, children))) },
			Child:  func(length int) []byte { return message(1, message(1, text(length))) },
			Wrap:   inSpan,
		},
		{
			Name:   "entity id keys",
			Parent: func(children []byte) []byte { return message(1, message(1, message(3, children))) },
			Child:  func(length int) []byte { return message(3, text(length)) },
			Wrap:   inRequest,
		},
		{
			Name:   "entity refs",
			Parent: func(children []byte) []byte { return message(1, message(1, children)) },
			Child:  func(length int) []byte { return message(3, message(2, text(length))) },
			Wrap:   inRequest,
		},
		{
			Name:   "status messages",
			Parent: func(children []byte) []byte { return message(2, children) },
			Child:  func(length int) []byte { return message(15, message(2, text(length))) },
			Wrap:   inScopeSpans,
		},
		{
			Name:   "spans per scope",
			Parent: func(children []byte) []byte { return message(2, children) },
			Child:  func(length int) []byte { return message(2, message(5, text(length))) },
			Wrap:   inResourceSpans,
		},
		{
			Name:   "events per span",
			Parent: func(children []byte) []byte { return message(2, children) },
			Child:  func(length int) []byte { return message(11, message(2, text(length))) },
			Wrap:   inScopeSpans,
		},
		{
			Name:   "links per span",
			Parent: func(children []byte) []byte { return message(2, children) },
			Child:  func(length int) []byte { return message(13, message(3, text(length))) },
			Wrap:   inScopeSpans,
		},
		{
			Name:   "scopes per resource",
			Parent: func(children []byte) []byte { return message(1, children) },
			Child:  func(length int) []byte { return message(2, message(3, text(length))) },
			Wrap:   inRequest,
		},
		{
			Name:   "deprecated scopes per resource",
			Parent: func(children []byte) []byte { return message(1, children) },
			Child:  func(length int) []byte { return message(deprecatedField, message(3, text(length))) },
			Wrap:   inRequest,
		},
	}
}

func spreadWire(shape spreadShape, children, length int) []byte {
	parent := shape.Parent(repeat(shape.Child(length), children))
	return shape.Wrap(repeat(parent, max(1, spreadWireTarget/len(parent))))
}

func TestWeightBoundsChildrenSpreadOverManyParents(t *testing.T) {
	for _, shape := range spreadShapes() {
		for _, children := range []int{1, 2, 3, 5, 9, 17, 33, 36, 65, 70} {
			for _, length := range []int{0, 1, 9, 17, 33, 49} {
				label := fmt.Sprintf("%s: %d children of %d bytes", shape.Name, children, length)
				assertDecodes(t, label, spreadWire(shape, children, length))
			}
		}
	}
}

func textLengthsAtSizeClassEdges() []int {
	return []int{
		129, 145, 241, 257, 289, 449, 513, 577, 641, 705, 769, 897,
		1025, 2049, 2305, 4097, 8193, 16385, 32769, 65537, 131073, 262145, 524289, 1048577,
	}
}

func TestWeightBoundsTextAtSizeClassEdges(t *testing.T) {
	for _, length := range textLengthsAtSizeClassEdges() {
		spanName := message(2, message(5, text(length)))
		bytesValue := message(2, message(9, message(2, message(7, text(length)))))
		for label, wire := range map[string][]byte{
			"span name":   inScopeSpans(repeat(spanName, max(1, bombBytes/len(spanName)))),
			"bytes value": inScopeSpans(repeat(bytesValue, max(1, bombBytes/len(bytesValue)))),
		} {
			assertDecodes(t, fmt.Sprintf("%s of %d bytes", label, length), wire)
		}
	}
}
