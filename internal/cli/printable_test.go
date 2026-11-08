package cli_test

import (
	"testing"

	"github.com/DmytroLysenko1/faultline/internal/cli"
)

func TestPrintableShowsInvisibleCharactersAsText(t *testing.T) {
	tests := []struct {
		Name string
		Args string
		Want string
	}{
		{Name: "plain text stays", Args: "unavailable: пробел ✓", Want: "unavailable: пробел ✓"},
		{Name: "terminal escape", Args: "\x1b[2J", Want: `\x1B[2J`},
		{Name: "newline and carriage return", Args: "a\nb\rc", Want: `a\x0Ab\x0Dc`},
		{Name: "right-to-left override", Args: "rtl\u202Eevil", Want: `rtl\u202Eevil`},
		{Name: "line and paragraph separators", Args: "a\u2028b\u2029c", Want: `a\u2028b\u2029c`},
		{Name: "zero-width joiner", Args: "a\u200Db", Want: `a\u200Db`},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			if got := cli.Printable(tt.Args); got != tt.Want {
				t.Errorf("Printable(%q) = %q, want %q", tt.Args, got, tt.Want)
			}
		})
	}
}
