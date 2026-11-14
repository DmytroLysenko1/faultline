package natsjs

import (
	"errors"
	"strings"
	"testing"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"

	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/natsjstest"
)

const smallPayload = 1 << 20

func linkTo(t *testing.T, cluster *natsjstest.Cluster) *Link {
	t.Helper()
	nc, err := nats.Connect(strings.Join(cluster.URLs(), ","))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(nc.Close)
	return &Link{NATS: nc}
}

// TestEveryConnectionMustCarryARecord: the DLQ copy of a record goes out on the feed connection, so a small
// max_payload behind any of the three is a refusal at Dial, not a stop at the first corruption.
func TestEveryConnectionMustCarryARecord(t *testing.T) {
	roomy := linkTo(t, natsjstest.Start(t, 1))
	small := linkTo(t, natsjstest.Start(t, 1, func(options *server.Options) { options.MaxPayload = smallPayload }))
	for index, role := range roles {
		t.Run(role, func(t *testing.T) {
			links := []*Link{roomy, roomy, roomy}
			links[index] = small

			err := payloadsFit(links)

			if !errors.Is(err, ErrPayloadLimit) || !strings.Contains(err.Error(), role) {
				t.Errorf("payloadsFit = %v, want ErrPayloadLimit naming %s", err, role)
			}
		})
	}
	if err := payloadsFit([]*Link{roomy, roomy, roomy}); err != nil {
		t.Errorf("payloadsFit with room everywhere = %v", err)
	}
}
