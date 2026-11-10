package operation

import "strings"

const (
	digits         = "0123456789"
	hexDigits      = digits + "abcdefABCDEF"
	letters        = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	tokenAlphabet  = letters + digits + "+=_-."
	uuidShape      = "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
	encodedAt      = "%40"
	shortHexLength = 7
	longHexLength  = 16
	dateDigits     = 4
	tokenLength    = 20
	tokenSwitches  = 5
)

func isIdentifier(segment string) bool {
	return isNumericIdentifier(segment) || isOpaqueIdentifier(segment)
}

func isNumericIdentifier(segment string) bool {
	switch {
	case isNumber(segment):
		return true
	case hasDigitRun(segment, dateDigits):
		return true
	}
	return endsWithNumber(segment)
}

func isOpaqueIdentifier(segment string) bool {
	switch {
	case len(segment) > MaxNameBytes:
		return true
	case isEmail(segment):
		return true
	case isHexIdentifier(segment) || containsUUID(segment):
		return true
	}
	return isToken(segment)
}

func isNumber(segment string) bool {
	number := strings.TrimPrefix(segment, "-")
	return startsWithDigit(number) && endsWithDigit(number) && onlyOf(number, digits+".")
}

func hasDigitRun(segment string, length int) bool {
	run := 0
	for i := range len(segment) {
		if !isDigit(segment[i]) {
			run = 0
			continue
		}
		run++
		if run == length {
			return true
		}
	}
	return false
}

func endsWithNumber(segment string) bool {
	word := strings.TrimRight(segment, digits)
	return word != segment && len(word) > 1 && strings.ContainsAny(word[len(word)-1:], "-_")
}

func isEmail(segment string) bool {
	return strings.Contains(segment, "@") || strings.Contains(segment, encodedAt)
}

func isHexIdentifier(segment string) bool {
	if !onlyOf(segment, hexDigits) {
		return false
	}
	if len(segment) >= longHexLength {
		return true
	}
	return len(segment) >= shortHexLength && strings.ContainsAny(segment, digits) && !onlyOf(segment, digits)
}

func containsUUID(segment string) bool {
	for start := 0; start+len(uuidShape) <= len(segment); start++ {
		if isUUID(segment[start : start+len(uuidShape)]) {
			return true
		}
	}
	return false
}

func isUUID(candidate string) bool {
	for i := range len(uuidShape) {
		if !matchesUUIDShape(uuidShape[i], candidate[i]) {
			return false
		}
	}
	return true
}

func matchesUUIDShape(shape, b byte) bool {
	if shape == '-' {
		return b == '-'
	}
	return strings.IndexByte(hexDigits, b) >= 0
}

func isToken(segment string) bool {
	return len(segment) >= tokenLength && onlyOf(segment, tokenAlphabet) && characterSwitches(segment) >= tokenSwitches
}

func characterSwitches(segment string) int {
	switches := 0
	for i := 1; i < len(segment); i++ {
		if isCharacterSwitch(segment[i-1], segment[i]) {
			switches++
		}
	}
	return switches
}

func isCharacterSwitch(previous, current byte) bool {
	return isLetterDigitBoundary(previous, current) || (isLower(previous) && isUpper(current))
}

func isLetterDigitBoundary(previous, current byte) bool {
	return (isLetter(previous) && isDigit(current)) || (isDigit(previous) && isLetter(current))
}

func onlyOf(segment, alphabet string) bool {
	return strings.Trim(segment, alphabet) == ""
}

func startsWithDigit(segment string) bool {
	return segment != "" && isDigit(segment[0])
}

func endsWithDigit(segment string) bool {
	return segment != "" && isDigit(segment[len(segment)-1])
}

func isDigit(b byte) bool {
	return '0' <= b && b <= '9'
}

func isLetter(b byte) bool {
	return isLower(b) || isUpper(b)
}

func isLower(b byte) bool {
	return 'a' <= b && b <= 'z'
}

func isUpper(b byte) bool {
	return 'A' <= b && b <= 'Z'
}
