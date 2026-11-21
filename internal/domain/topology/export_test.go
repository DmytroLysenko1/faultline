package topology

import "github.com/DmytroLysenko1/faultline/pkg/checkpoint"

const (
	SectionMatcher = sectionMatcher
	SectionGraph   = sectionGraph
	SectionTraffic = sectionTraffic
)

func (l *Limits) WorstSectionBytes() map[checkpoint.Tag]int64 {
	return l.worstSectionBytes()
}
