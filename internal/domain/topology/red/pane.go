package red

import (
	"fmt"

	"github.com/DmytroLysenko1/faultline/pkg/stream"
)

type PaneKind uint8

const (
	PaneEarly PaneKind = iota + 1
	PaneOnTime
	PaneLate
)

func kindOf(kind stream.PaneKind) PaneKind {
	switch kind {
	case stream.PaneEarly:
		return PaneEarly
	case stream.PaneOnTime:
		return PaneOnTime
	case stream.PaneLate:
		return PaneLate
	default:
		panic(fmt.Sprintf("red: unknown pane kind %d", kind))
	}
}
