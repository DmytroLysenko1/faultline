package journaltest_test

import (
	"testing"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/span"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/journaltest"
)

const contractBatches = 64

func TestMemoryKeepsTheFeedContract(t *testing.T) {
	journaltest.RunFeedContract(t, func(t *testing.T, credit int64) *journaltest.Journal {
		journal := newJournal(t, journaltest.Limits{
			MaxBytes:   credit,
			MaxBatches: contractBatches,
		})
		return &journaltest.Journal{
			Feed: journal,
			Append: func(t *testing.T, spans []span.Span) {
				t.Helper()
				if err := journal.Append(t.Context(), spans); err != nil {
					t.Fatalf("Append: %v", err)
				}
			},
			Close:    func(*testing.T) { journal.Close() },
			LimitErr: journaltest.ErrInvalidMax,
		}
	})
}
