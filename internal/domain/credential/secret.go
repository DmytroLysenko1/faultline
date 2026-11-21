package credential

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
)

const (
	redacted   = "[redacted]"
	bytesMatch = 1
)

type Secret struct {
	_       [0]func()
	encoded *string
}

func (Secret) String() string {
	return redacted
}

func (Secret) GoString() string {
	return redacted
}

func (Secret) Format(state fmt.State, _ rune) {
	_, _ = io.WriteString(state, redacted)
}

func (Secret) LogValue() slog.Value {
	return slog.StringValue(redacted)
}

func (Secret) MarshalText() ([]byte, error) {
	return []byte(redacted), nil
}

func (s Secret) text() string {
	if s.encoded == nil {
		return ""
	}
	return *s.encoded
}

type Digest struct {
	_     [0]func()
	value [sha256.Size]byte
}

func ParseDigest(text string) (Digest, error) {
	var digest Digest
	decoded, err := hex.DecodeString(text)
	if err != nil || len(decoded) != sha256.Size {
		return Digest{}, fmt.Errorf("digest of %d chars: %w", len(text), ErrInvalidGrant)
	}
	copy(digest.value[:], decoded)
	if digest.isZero() {
		return Digest{}, fmt.Errorf("zero digest: %w", ErrInvalidGrant)
	}
	return digest, nil
}

func (d Digest) Equal(other Digest) bool {
	same := subtle.ConstantTimeCompare(d.value[:], other.value[:]) == bytesMatch
	return same && !d.isZero()
}

func (d Digest) MarshalText() ([]byte, error) {
	return []byte(hex.EncodeToString(d.value[:])), nil
}

func (d Digest) isZero() bool {
	return d.value == [sha256.Size]byte{}
}
