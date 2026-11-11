package topology

import "github.com/DmytroLysenko1/faultline/internal/domain/topology/traffic"

type Option func(*Builder)

func WithFinalSink(sink traffic.FinalSink) Option {
	return func(b *Builder) {
		b.sink = sink
	}
}
