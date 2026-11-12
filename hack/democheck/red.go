package main

import (
	"cmp"
	"fmt"
	"math/bits"
	"slices"
	"strings"

	"github.com/DmytroLysenko1/faultline/test/demo/reference"
)

const (
	lateDropDenominator = 100
	missingDetail       = "missing in pipeline.stats"
	mismatchFormat      = "%d != %d"
	overflowDetail      = "a sum overflows uint64"
)

type engine struct {
	Offered      uint64 `json:"offered"`
	OnTime       uint64 `json:"on_time"`
	LateAccepted uint64 `json:"late_accepted"`
	LateDropped  uint64 `json:"late_dropped"`
	NoWatermark  uint64 `json:"no_watermark"`
	Series       *int   `json:"series"`
	Capacity     *int   `json:"capacity"`
}

type windows struct {
	Subjects   *engine `json:"subjects"`
	Operations *engine `json:"operations"`
	Overflow   *engine `json:"overflow"`
}

type cardinality struct {
	Operations        *int `json:"operations"`
	ServicesOverLimit *int `json:"services_over_limit"`
}

type redCheck struct {
	label  string
	holds  bool
	detail string
}

type redVerdict struct {
	missingFinals    []reference.Edge
	missingReceivers []string
	required         int
	receivers        int
	checks           []redCheck
	onlyFinals       bool
}

func redFinals(enabled bool, expected *reference.Reference, shown *shownGraph) *redVerdict {
	if !enabled {
		return nil
	}
	receivers := receiversOf(expected)
	return &redVerdict{
		missingFinals:    untimedEdges(expected, shown),
		missingReceivers: unservedReceivers(receivers, shown),
		required:         len(expected.Required),
		receivers:        len(receivers),
		onlyFinals:       true,
	}
}

func untimedEdges(expected *reference.Reference, shown *shownGraph) []reference.Edge {
	var untimed []reference.Edge
	for edge := range expected.Required {
		if !shown.timedFinals[edge] {
			untimed = append(untimed, edge)
		}
	}
	slices.SortFunc(untimed, func(a, b reference.Edge) int {
		return cmp.Or(strings.Compare(a.Source, b.Source), strings.Compare(a.Target, b.Target))
	})
	return untimed
}

func receiversOf(expected *reference.Reference) []string {
	receivers := make([]string, 0, len(expected.Required))
	for edge := range expected.Required {
		receivers = append(receivers, edge.Target)
	}
	slices.Sort(receivers)
	return slices.Compact(receivers)
}

func unservedReceivers(receivers []string, shown *shownGraph) []string {
	var unserved []string
	for _, receiver := range receivers {
		if !shown.servedFinals[receiver] {
			unserved = append(unserved, receiver)
		}
	}
	return unserved
}

func (r *redVerdict) withStats(stats *statsRecord) *redVerdict {
	if r == nil {
		return nil
	}
	r.onlyFinals = false
	r.checks = slices.Concat(engineChecks(stats.Windows), totalsChecks(stats), cardinalityChecks(stats))
	return r
}

func engineChecks(counted *windows) []redCheck {
	if counted == nil || counted.Subjects == nil || counted.Operations == nil || counted.Overflow == nil {
		return []redCheck{missing("windows.subjects, windows.operations, windows.overflow")}
	}
	subjects, operations, overflow := counted.Subjects, counted.Operations, counted.Overflow
	return []redCheck{
		i6("subjects", subjects),
		i6("operations", operations),
		i6("overflow", overflow),
		equalSums("I11 operations.offered+overflow.offered != subjects.offered",
			[]uint64{operations.Offered, overflow.Offered}, []uint64{subjects.Offered}),
		equalSums("I11 operations.late_dropped != subjects.late_dropped",
			[]uint64{operations.LateDropped}, []uint64{subjects.LateDropped}),
		seriesWithinCapacity(operations),
		{
			label:  "windows.subjects.offered == 0",
			holds:  subjects.Offered > 0,
			detail: fmt.Sprint(subjects.Offered),
		},
		lateDropsBelowLimit(subjects),
	}
}

func i6(name string, counted *engine) redCheck {
	return equalSums("I6 "+name+".offered != on_time+late_accepted+late_dropped+no_watermark",
		[]uint64{counted.Offered},
		[]uint64{counted.OnTime, counted.LateAccepted, counted.LateDropped, counted.NoWatermark})
}

func seriesWithinCapacity(operations *engine) redCheck {
	if operations.Series == nil || operations.Capacity == nil {
		return missing("windows.operations.series, windows.operations.capacity")
	}
	return redCheck{
		label:  "I11 operations.series > operations.capacity",
		holds:  *operations.Series <= *operations.Capacity,
		detail: fmt.Sprintf("%d > %d", *operations.Series, *operations.Capacity),
	}
}

func lateDropsBelowLimit(subjects *engine) redCheck {
	high, low := bits.Mul64(subjects.LateDropped, lateDropDenominator)
	return redCheck{
		label:  "subjects.late_dropped/offered >= 1%",
		holds:  subjects.Offered == 0 || high == 0 && low < subjects.Offered,
		detail: fmt.Sprintf("%d of %d", subjects.LateDropped, subjects.Offered),
	}
}

func totalsChecks(stats *statsRecord) []redCheck {
	if stats.Graph.CallsTotal == nil || stats.Graph.ServedTotal == nil {
		return []redCheck{missing("graph.calls_total, graph.served_total")}
	}
	if stats.Windows == nil || stats.Windows.Subjects == nil {
		return []redCheck{missing("windows.subjects for I10")}
	}
	return []redCheck{
		equalSums("I10 graph.calls_total+graph.served_total != subjects.offered",
			[]uint64{*stats.Graph.CallsTotal, *stats.Graph.ServedTotal}, []uint64{stats.Windows.Subjects.Offered}),
	}
}

func cardinalityChecks(stats *statsRecord) []redCheck {
	counted := stats.Cardinality
	if counted == nil || counted.Operations == nil || counted.ServicesOverLimit == nil {
		return []redCheck{missing("cardinality.operations, cardinality.services_over_limit")}
	}
	return []redCheck{
		{
			label:  "cardinality.services_over_limit != 0",
			holds:  *counted.ServicesOverLimit == 0,
			detail: fmt.Sprint(*counted.ServicesOverLimit),
		},
		{
			label:  "cardinality.operations == 0",
			holds:  *counted.Operations > 0,
			detail: fmt.Sprint(*counted.Operations),
		},
	}
}

func equalSums(label string, left, right []uint64) redCheck {
	leftSum, leftFits := total(left)
	rightSum, rightFits := total(right)
	if !leftFits || !rightFits {
		return redCheck{
			label:  label,
			detail: overflowDetail,
		}
	}
	return redCheck{
		label:  label,
		holds:  leftSum == rightSum,
		detail: fmt.Sprintf(mismatchFormat, leftSum, rightSum),
	}
}

func total(values []uint64) (uint64, bool) {
	var sum, carried uint64
	for _, value := range values {
		var carry uint64
		sum, carry = bits.Add64(sum, value, 0)
		carried |= carry
	}
	return sum, carried == 0
}

func missing(fields string) redCheck {
	return redCheck{
		label:  fields,
		detail: missingDetail,
	}
}

func (r *redVerdict) passed() bool {
	if r == nil {
		return true
	}
	if len(r.missingFinals) > 0 || len(r.missingReceivers) > 0 {
		return false
	}
	return r.holding() == len(r.checks)
}

func (r *redVerdict) writeProblems(out *strings.Builder) {
	if r == nil {
		return
	}
	writeEdges(out, "required edge without a final window with latency", r.missingFinals)
	for _, receiver := range r.missingReceivers {
		fmt.Fprintf(out, "receiver without a final window: %s\n", receiver)
	}
	for _, check := range r.checks {
		if !check.holds {
			fmt.Fprintf(out, "%s: %s\n", check.label, check.detail)
		}
	}
}

func (r *redVerdict) writeSummary(out *strings.Builder) {
	if r == nil {
		return
	}
	fmt.Fprintf(out, "RED: finals on %d/%d required edges, %d/%d receivers",
		r.required-len(r.missingFinals), r.required, r.receivers-len(r.missingReceivers), r.receivers)
	if r.onlyFinals {
		out.WriteString("\n")
		return
	}
	fmt.Fprintf(out, ", %d/%d stats checks\n", r.holding(), len(r.checks))
}

func (r *redVerdict) holding() int {
	held := 0
	for _, check := range r.checks {
		if check.holds {
			held++
		}
	}
	return held
}
