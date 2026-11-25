package lease

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"

	"github.com/nats-io/nats.go/jetstream"
)

func TestAnExpiredClaimNamesTheHolderOrTheLastFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		held := newFakeStore(productionTTL)
		held.takeOver([]byte(foreignInstance))
		ctx, cancel := context.WithTimeout(t.Context(), productionLost)
		defer cancel()
		_, err := claim(ctx, held, held.Delete, productionTTL, productionOptions())
		holder, found := errors.AsType[*HeldError](err)
		if !found || holder.Holder != foreignInstance {
			t.Errorf("claim of a held key = %v, want the holder %q", err, foreignInstance)
		}

		silent := newFakeStore(productionTTL)
		silent.steady = noStream
		ctx, cancel = context.WithTimeout(t.Context(), productionLost)
		defer cancel()
		_, err = claim(ctx, silent, silent.Delete, productionTTL, productionOptions())
		if !errors.Is(err, jetstream.ErrNoStreamResponse) {
			t.Errorf("claim against a silent bucket = %v, want its last failure", err)
		}
	})
}
