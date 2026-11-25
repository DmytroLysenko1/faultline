package main

import (
	"testing"

	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/natsjstest"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp"
)

func TestTheOTLPHandshakeTimeoutFitsTheStopGrace(t *testing.T) {
	if otlp.ConnectionTimeout >= otlpStopGrace {
		t.Errorf("otlp.ConnectionTimeout %s, want below the otlp layer grace %s: a silent connection would outlive "+
			"GracefulStop and turn SIGTERM into code 1", otlp.ConnectionTimeout, otlpStopGrace)
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
