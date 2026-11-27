package otlptest

import (
	"net"
	"testing"
	"time"

	"github.com/DmytroLysenko1/faultline/internal/domain/credential"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp/admission"
)

type NoMarks struct{}

func (NoMarks) Authenticated(_ net.Addr, _ credential.ID) {}

type wallClock struct{}

func (wallClock) Now() time.Time {
	return time.Now()
}

func Gate(tb testing.TB, limits admission.Limits) *admission.Gate {
	tb.Helper()
	books, err := admission.New(limits, wallClock{})
	if err != nil {
		tb.Fatalf("admission.New: %v", err)
	}
	return &admission.Gate{
		Admission: books,
		Marks:     NoMarks{},
	}
}
