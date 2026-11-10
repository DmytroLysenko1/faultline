package operation

import (
	"unicode"
	"unicode/utf8"
)

const (
	ellipsis      = "…"
	replacement   = string(utf8.RuneError)
	truncatedRoom = MaxNameBytes - len(ellipsis)
)

type name struct {
	buf       [MaxNameBytes]byte
	length    int
	truncated bool
}

func (n *name) writeString(s string) {
	for i := 0; i < len(s) && !n.truncated; {
		if run := printableASCII(s[i:]); run > 0 {
			n.writeBytes(s[i : i+run])
			i += run
			continue
		}
		i += n.writeRune(s[i:])
	}
}

func printableASCII(s string) int {
	for i := range len(s) {
		if s[i] < ' ' || s[i] > '~' {
			return i
		}
	}
	return len(s)
}

func (n *name) writeRune(s string) int {
	r, size := utf8.DecodeRuneInString(s)
	if r == utf8.RuneError || unicode.IsControl(r) {
		n.writeBytes(replacement)
		return size
	}
	n.writeBytes(s[:size])
	return size
}

func (n *name) writeBytes(s string) {
	if n.truncated {
		return
	}
	copied := copy(n.buf[n.length:], s)
	n.length += copied
	if copied < len(s) {
		n.truncate()
	}
}

func (n *name) truncate() {
	n.truncated = true
	n.length = min(n.length, truncatedRoom)
	for n.length > 0 && !utf8.RuneStart(n.buf[n.length]) {
		n.length--
	}
	n.length += copy(n.buf[n.length:], ellipsis)
}

func (n *name) String() string {
	return string(n.buf[:n.length])
}

type Names map[string]string

func (names Names) share(built *name) string {
	if shared, seen := names[string(built.buf[:built.length])]; seen {
		return shared
	}
	text := built.String()
	if names != nil {
		names[text] = text
	}
	return text
}
