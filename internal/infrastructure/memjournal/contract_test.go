package memjournal_test

import (
	"testing"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/span"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/journaltest"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/memjournal"
)

const contractBatches = 64

func TestMemjournalKeepsTheFeedContract(t *testing.T) {
	journaltest.RunFeedContract(t, func(t *testing.T, credit int64) *journaltest.Journal {
		journal := newJournal(t, memjournal.Limits{MaxBytes: credit, MaxBatches: contractBatches})
		return &journaltest.Journal{
			Feed: journal,
			Append: func(t *testing.T, spans []span.Span) {
				t.Helper()
				if err := journal.Append(t.Context(), spans); err != nil {
					t.Fatalf("Append: %v", err)
				}
			},
			Close:    func(*testing.T) { journal.Close() },
			LimitErr: memjournal.ErrInvalidMax,
		}
	})
}
