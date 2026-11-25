package main

import (
	"testing"

	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/natsjstest"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp"
)

func TestTheOTLPHandshakeTimeoutFitsBothStopGraces(t *testing.T) {
	if otlp.ConnectionTimeout >= otlpStopGrace || otlp.ConnectionTimeout >= otlpGrace {
		t.Errorf("otlp.ConnectionTimeout %s, want below the ingester grace %s and the all grace %s: a silent "+
			"connection would outlive GracefulStop and turn SIGTERM into code 1", otlp.ConnectionTimeout, otlpStopGrace,
			otlpGrace)
	}
}

func TestASilentOTLPConnectionDoesNotFailTheIngesterStop(t *testing.T) {
	cluster := natsjstest.Start(t, singleNATSNode)
	ensureStreams(t, cluster.URLs()...)
	logs := &logBuffer{}
	setup, _, otlpAddress := ingesterOn(t, cluster.URLs())
	notify, done, life := serveIngester(t, setup, logs)
	eventually(t, "serving", life.Served)
	silent := dialRawOTLP(t, otlpAddress)
	if !answers(t, silent, answerWithin) {
		t.Fatal("the silent connection was not accepted")
	}

	notify.send()

	assertCode(t, <-done, exitOK, logs)
	assertNoLine(t, logs, eventLayerStopFailed)
	assertNoLine(t, logs, eventForcedStop)
}

func TestASilentOTLPConnectionDoesNotFailTheAllStop(t *testing.T) {
	cfg := e2eConfig(t)
	logs := &logBuffer{}
	sys, err := newSystem(t.Context(), &cfg, testLogger(logs))
	if err != nil {
		t.Fatalf("newSystem: %v", err)
	}
	started := launchSystem(t, &cfg, sys)
	silent := dialRawOTLP(t, started.open.OTLP.Addr().String())
	if !answers(t, silent, answerWithin) {
		t.Fatal("the silent connection was not accepted")
	}

	if err := started.stop(t); err != nil {
		t.Errorf("run = %v, want nil with a silent OTLP connection open", err)
	}
	assertNoLine(t, logs, "lifecycle.forced_stop")
}
