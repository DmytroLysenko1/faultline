package main

import "context"

type exitsOnTrigger struct {
	trigger chan struct{}
}

func (u *exitsOnTrigger) start(_ context.Context, sup unitSupervisor) error {
	sup.spawn("a/run", func() error {
		<-u.trigger
		return errA
	})
	return nil
}

func (u *exitsOnTrigger) stop(context.Context) error { return nil }

func partitionAttr(outcome exitOutcome) string {
	for _, attr := range outcome.attrs {
		if attr.Key == "partition" {
			return attr.Value.String()
		}
	}
	return ""
}
