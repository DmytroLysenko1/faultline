package main

import (
	"testing"

	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/natsjstest"
)

func TestTheConnLayerHandsItsLiveConnectionToTheLayersAbove(t *testing.T) {
	cluster := natsjstest.Start(t, singleNATSNode)
	logs, notify := &logBuffer{}, newFakeNotify()
	life := newLifecycle(testLogger(logs))
	conn, source := connLayer(natsSetup(cluster.URLs()...), life, testLogger(logs))
	done := launchRoleWith(t, &stack{layers: []layer{conn}}, notify.notify, life)

	eventually(t, "serving", life.Served)
	handed := source.conns()
	if handed == nil || !handed.Connected() {
		t.Fatalf("conns() while serving = %v, want a connected connection", handed)
	}
	notify.send()
	assertCode(t, <-done, exitOK, logs)
	if source.conns() != handed {
		t.Errorf("conns() after stop is another connection than the one handed while serving")
	}
	if handed.Connected() {
		t.Errorf("the handed connection is still connected after the layer stopped")
	}
}
