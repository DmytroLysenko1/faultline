package matcher

import "github.com/DmytroLysenko1/faultline/internal/domain/topology/role"

type Outcome uint8

const (
	OutcomeIgnored Outcome = iota
	OutcomeUnpairable
	OutcomeWaiting
	OutcomeMatched
	OutcomeDuplicate
)

func (o Outcome) Served(side role.Role) bool {
	switch side {
	case role.Callee:
		return o == OutcomeWaiting || o == OutcomeMatched
	case role.Entry:
		return o == OutcomeUnpairable
	default:
		return false
	}
}

func unpaired(side role.Role) Outcome {
	if side.Participates() {
		return OutcomeUnpairable
	}
	return OutcomeIgnored
}
