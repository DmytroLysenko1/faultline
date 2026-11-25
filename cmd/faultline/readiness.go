package main

import (
	"net/http"

	"go.opentelemetry.io/otel/trace"

	"github.com/DmytroLysenko1/faultline/internal/interfaces/connectapi"
)

const (
	healthPattern = "GET /healthz"
	readyPattern  = "GET /readyz"
)

type alive struct{}

func (alive) Ready() bool {
	return true
}

type readiness struct {
	life   *lifecycle
	layers func() []layer
}

func (r readiness) Ready() bool {
	if !r.life.Ready() {
		return false
	}
	layers := r.layers()
	for index := range layers {
		if !layerReady(&layers[index]) {
			return false
		}
	}
	return true
}

func layerReady(current *layer) bool {
	return current.ready == nil || current.ready()
}

func roleRoutes(ready readiness) apiRoutes {
	return func(trace.TracerProvider) (http.Handler, error) {
		mux := http.NewServeMux()
		mux.Handle(healthPattern, connectapi.Health(alive{}))
		mux.Handle(readyPattern, connectapi.Health(ready))
		return mux, nil
	}
}
