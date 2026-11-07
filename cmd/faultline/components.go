package main

import (
	"context"
	"sync/atomic"
)

const (
	componentOTLP     = "otlp"
	componentAPI      = "api"
	componentPipeline = "pipeline"
)

type componentExit struct {
	name       string
	err        error
	unexpected bool
}

type component struct {
	stopRequested atomic.Bool
	done          chan struct{}
}

type components struct {
	byName map[string]*component
	exits  chan componentExit
}

func newComponents(names ...string) *components {
	byName := make(map[string]*component, len(names))
	for _, name := range names {
		byName[name] = &component{
			done: make(chan struct{}),
		}
	}
	return &components{
		byName: byName,
		exits:  make(chan componentExit, len(names)),
	}
}

func (c *components) start(name string, serve func() error) {
	part := c.byName[name]
	go func() {
		err := serve()
		c.exits <- componentExit{
			name:       name,
			err:        err,
			unexpected: !part.stopRequested.Load(),
		}
		close(part.done)
	}()
}

func (c *components) firstExit() <-chan componentExit {
	return c.exits
}

func (c *components) stopping(name string) {
	c.byName[name].stopRequested.Store(true)
}

func (c *components) exited(name string) <-chan struct{} {
	return c.byName[name].done
}

func (c *components) wait(ctx context.Context, name string) bool {
	select {
	case <-c.exited(name):
		return true
	case <-ctx.Done():
		return false
	}
}

func (c *components) hasExited(name string) bool {
	select {
	case <-c.exited(name):
		return true
	default:
		return false
	}
}

func (c *components) remainingExits() []componentExit {
	var remaining []componentExit
	for {
		select {
		case exit := <-c.exits:
			remaining = append(remaining, exit)
		default:
			return remaining
		}
	}
}
