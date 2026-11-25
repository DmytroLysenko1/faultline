package feed_test

import (
	"testing"

	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/feed"
)

func TestUnreleasedCountsHeartbeats(t *testing.T) {
	tests := []struct {
		name  string
		stats feed.Stats
		want  uint64
	}{
		{"everything released", feed.Stats{
			DeliveredBatches: 3,
			Heartbeats:       2,
			Released:         5,
		}, 0},
		{"a held heartbeat", feed.Stats{
			DeliveredBatches: 3,
			Heartbeats:       2,
			Released:         4,
		}, 1},
		{"only heartbeats delivered", feed.Stats{
			Heartbeats: 2,
		}, 2},
		{"a release read ahead of its hand-out", feed.Stats{
			DeliveredBatches: 2,
			Released:         3,
		}, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.stats.Unreleased(); got != tt.want {
				t.Errorf("Unreleased() = %d, want %d", got, tt.want)
			}
		})
	}
}
