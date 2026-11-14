package lease

import "time"

// WithElectionBudget lets the package's integration tests run with short timings.
func WithElectionBudget(options *Options, budget time.Duration) *Options {
	options.electionBudget = budget
	return options
}
