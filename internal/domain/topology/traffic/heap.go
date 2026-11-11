package traffic

import (
	"unsafe"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/resource"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/cardinality"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/red"
	"github.com/DmytroLysenko1/faultline/pkg/sketch"
	"github.com/DmytroLysenko1/faultline/pkg/stream"
)

const (
	subjectREDBytes = int64(unsafe.Sizeof(SubjectRED{}))
	nodeBookBytes   = int64(unsafe.Sizeof(resource.ServiceRef{}))
	edgeBookBytes   = int64(unsafe.Sizeof(edgeEnds{}))
)

func (l *Limits) WorstCaseHeapBytes() int64 {
	subjects := l.subjects()
	engines := stream.WorstCaseHeapBytes[subjectKey, red.Cell](l.Windows, subjects) +
		stream.WorstCaseHeapBytes[operationKey, red.Counts](l.Windows, l.MaxSeries) +
		stream.WorstCaseHeapBytes[subjectKey, red.Counts](l.Windows, subjects)
	book := int64(l.MaxNodes)*nodeBookBytes + int64(l.MaxEdges)*edgeBookBytes
	return engines + sketch.WorstCaseHeapBytes(l.SketchEntries) + cardinality.WorstHeapBytes(l.MaxSeries, l.MaxNodes) +
		book + l.ViewHeapBytes()
}

func (l *Limits) ViewHeapBytes() int64 {
	return int64(l.subjects()) * subjectREDBytes
}
