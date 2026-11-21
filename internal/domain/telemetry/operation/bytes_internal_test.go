package operation

import (
	"strings"
	"testing"
)

const (
	digitAlphabet  = "0123456789"
	letterAlphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	byteValues     = 256
)

func TestEachBytePredicateAllowsExactlyItsAlphabet(t *testing.T) {
	tests := []struct {
		name     string
		alphabet string
		allowed  func(b byte) bool
	}{
		{name: "digit", alphabet: digitAlphabet, allowed: isDigit},
		{name: "hex digit", alphabet: digitAlphabet + "abcdefABCDEF", allowed: isHexDigit},
		{name: "number", alphabet: digitAlphabet + ".", allowed: isNumberByte},
		{name: "token", alphabet: letterAlphabet + digitAlphabet + "+=_-.", allowed: isTokenByte},
		{name: "scheme", alphabet: letterAlphabet + digitAlphabet + "+.-", allowed: isSchemeByte},
		{name: "parameter", alphabet: letterAlphabet + digitAlphabet + "_", allowed: isParameterByte},
		{name: "word separator", alphabet: "-_", allowed: isWordSeparator},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for value := range byteValues {
				b := byte(value)
				if want := strings.IndexByte(tt.alphabet, b) >= 0; tt.allowed(b) != want {
					t.Errorf("byte %#x allowed = %t, want %t", b, tt.allowed(b), want)
				}
			}
		})
	}
}

func TestOnlyOfAgreesWithTrimmingTheAlphabet(t *testing.T) {
	alphabet := letterAlphabet + digitAlphabet + "+=_-."
	for _, segment := range []string{"", "abc", "a b", "Zz09+=_-.", "é", "abc\x00", "-", "{id}", "a\xffb"} {
		want := strings.Trim(segment, alphabet) == ""
		if got := onlyOf(segment, isTokenByte); got != want {
			t.Errorf("onlyOf(%q) = %t, want %t", segment, got, want)
		}
	}
}

func TestEndsWithNumberMatchesTrimmingDigits(t *testing.T) {
	for _, segment := range []string{"", "12", "v-12", "a_1", "ab12", "-1", "x-", "é-9", "a--1"} {
		word := strings.TrimRight(segment, digitAlphabet)
		want := word != segment && len(word) > 1 && strings.ContainsAny(word[len(word)-1:], "-_")
		if got := endsWithNumber(segment); got != want {
			t.Errorf("endsWithNumber(%q) = %t, want %t", segment, got, want)
		}
	}
}
