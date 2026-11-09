package stream

type PaneKind uint8

const (
	PaneEarly PaneKind = iota + 1
	PaneOnTime
	PaneLate
)

type Pane struct {
	Window   Window
	Kind     PaneKind
	Revision uint32
}

type Placement uint8

const (
	PlacementOnTime Placement = iota + 1
	PlacementLate
	PlacementDroppedLate
	PlacementDroppedNoWatermark
)

func (p Placement) Dropped() bool {
	return p == PlacementDroppedLate || p == PlacementDroppedNoWatermark
}

type Stats struct {
	Series       int
	Capacity     int
	Offered      uint64
	OnTime       uint64
	LateAccepted uint64
	LateDropped  uint64
	NoWatermark  uint64
	Reassigned   uint64
	PanesEarly   uint64
	PanesOnTime  uint64
	PanesLate    uint64
	Retired      uint64
}
