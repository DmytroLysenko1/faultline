//go:build !race

package credential_test

import (
	"testing"

	"github.com/DmytroLysenko1/faultline/internal/domain/credential"
)

const allocationRuns = 100

func TestParseDoesNotAllocate(t *testing.T) {
	_, grant := generated(t, sequentialID(), make([]byte, credential.SecretBytes), testTenant)
	grants := credential.Grants{grant.ID: grant}
	rejected := validWith(last, "B")
	authenticate := func() {
		token, err := credential.Parse(validZeroToken)
		if err != nil {
			t.Fatalf("Parse: %v", err)
		}
		if _, err := grants.Admit(token, testTenant); err != nil {
			t.Fatalf("Admit: %v", err)
		}
		if _, err := credential.Parse(rejected); err == nil {
			t.Fatal("Parse accepted non-zero trailing bits")
		}
	}

	if got := testing.AllocsPerRun(allocationRuns, authenticate); got != 0 {
		t.Errorf("Parse and Admit allocate %.1f times per token, want 0", got)
	}
}
