package cli

import (
	"fmt"
	"strings"
	"unicode"
)

const (
	controlText = `\x%02X`
	formatText  = `\u%04X`
)

func Printable(text string) string {
	var shown strings.Builder
	shown.Grow(len(text))
	for _, char := range text {
		shown.WriteString(printable(char))
	}
	return shown.String()
}

func printable(char rune) string {
	switch {
	case unicode.IsControl(char):
		return fmt.Sprintf(controlText, char)
	case unicode.In(char, unicode.Cf, unicode.Zl, unicode.Zp):
		return fmt.Sprintf(formatText, char)
	default:
		return string(char)
	}
}
