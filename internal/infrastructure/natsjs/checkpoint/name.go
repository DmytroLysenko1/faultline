package checkpoint

import "strings"

const (
	nameBytes    = 26
	nameAlphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567"
)

func ValidName(name string) bool {
	return len(name) == nameBytes && strings.Trim(name, nameAlphabet) == ""
}
