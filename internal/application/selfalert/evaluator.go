package selfalert

import "github.com/DmytroLysenko1/faultline/internal/domain/telemetry/resource"

type Transition struct {
	Event      string
	Raised     bool
	LowTraffic bool
	Reading    Reading
	Owners     []resource.ServiceRef
}

type Alert struct {
	Event  string
	Raised bool
}

type hysteresis struct {
	raised    bool
	highRun   int
	clearRun  int
	escalated float64
}

func (h *hysteresis) rise(rule *rule, value float64) bool {
	h.highRun, h.clearRun = h.highRun+1, 0
	if h.raised {
		return h.escalate(rule, value)
	}
	if h.highRun < rule.raiseAfter {
		return false
	}
	h.raised, h.escalated = true, value
	return true
}

func (h *hysteresis) escalate(rule *rule, value float64) bool {
	if !rule.escalates || value <= h.escalated {
		return false
	}
	h.escalated = value
	return true
}

func (h *hysteresis) hold() {
	h.highRun, h.clearRun = 0, 0
}

func (h *hysteresis) settle(rule *rule) bool {
	h.highRun, h.clearRun = 0, h.clearRun+1
	if !h.raised || h.clearRun < rule.clearAfter {
		return false
	}
	h.raised, h.escalated = false, 0
	return true
}

type Evaluator struct {
	thresholds Thresholds
	rules      []rule
	states     []hysteresis
	previous   Sample
}

func NewEvaluator(thresholds Thresholds) *Evaluator {
	table := rules()
	return &Evaluator{
		thresholds: thresholds,
		rules:      table,
		states:     make([]hysteresis, len(table)),
	}
}

func (e *Evaluator) Evaluate(current *Sample) []Transition {
	var transitions []Transition
	for index := range e.rules {
		if transition, changed := e.observe(index, current); changed {
			transitions = append(transitions, transition)
		}
	}
	e.previous = *current
	return transitions
}

func (e *Evaluator) Alerts() []Alert {
	alerts := make([]Alert, len(e.rules))
	for index := range e.rules {
		alerts[index] = Alert{
			Event:  e.rules[index].event,
			Raised: e.states[index].raised,
		}
	}
	return alerts
}

func (e *Evaluator) observe(index int, current *Sample) (Transition, bool) {
	state, rule := &e.states[index], &e.rules[index]
	reading, verdict := rule.read(&e.previous, current, e.thresholds)
	transition := Transition{
		Event:      rule.event,
		Reading:    reading,
		LowTraffic: verdict == verdictInsufficient,
	}
	if rule.listOwners {
		transition.Owners = current.Owners
	}
	switch verdict {
	case verdictSteady:
		state.hold()
		return transition, false
	case verdictHigh:
		transition.Raised = true
		return transition, state.rise(rule, reading.Value)
	case verdictLow, verdictInsufficient:
		return transition, state.settle(rule)
	}
	return transition, false
}
