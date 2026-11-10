package spanmap_test

import (
	"github.com/DmytroLysenko1/faultline/internal/application/ingest"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/operation"
)

func newAcceptBatch() *ingest.AcceptBatch {
	return ingest.NewAcceptBatch(discardJournal{}, operation.NewNormalizer(nil))
}
