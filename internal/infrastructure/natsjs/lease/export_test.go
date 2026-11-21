package lease

import "time"

func WithElectionBudget(options *Options, budget time.Duration) *Options {
	options.electionBudget = budget
	return options
}
