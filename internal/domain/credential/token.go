package credential

import (
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"log/slog"
	"strings"
)

const (
	Prefix       = "flt_"
	IDLength     = 12
	SecretBytes  = 32
	SecretLength = 43
	TokenLength  = len(Prefix) + IDLength + len(separator) + SecretLength
)

const (
	idAlphabet  = "abcdefghijklmnopqrstuvwxyz234567"
	separator   = "_"
	idStart     = len(Prefix)
	idEnd       = idStart + IDLength
	secretStart = idEnd + len(separator)
)

var (
	ErrMalformed      = errors.New("malformed ingest token")
	ErrInvalidGrant   = errors.New("invalid ingest token grant")
	ErrNoRandomness   = errors.New("ingest token randomness unavailable")
	ErrUnknownGrant   = errors.New("no grant for the ingest token id")
	ErrDigestMismatch = errors.New("ingest token secret does not match its grant")
	ErrForeignTenant  = errors.New("ingest token belongs to another tenant")
)

type ID string

type IngestToken struct {
	ID     ID
	Secret Secret
}

func Parse(raw string) (IngestToken, error) {
	return parseInto(raw, new(string))
}

func ParseID(raw string) (ID, error) {
	if len(raw) != IDLength || strings.ContainsFunc(raw, outsideIDAlphabet) {
		return "", ErrMalformed
	}
	return ID(raw), nil
}

func (t IngestToken) Digest() Digest {
	secret, err := decodeSecret(t.Secret.text())
	if err != nil {
		return Digest{}
	}
	return Digest{
		value: sha256.Sum256(secret[:]),
	}
}

func Reveal(token IngestToken) string {
	return Prefix + string(token.ID) + separator + token.Secret.text()
}

func (t IngestToken) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("id", string(t.ID)),
		slog.Any("secret", t.Secret),
	)
}

func hasTokenShape(raw string) bool {
	return len(raw) == TokenLength &&
		raw[:idStart] == Prefix &&
		raw[idEnd:secretStart] == separator
}

func outsideIDAlphabet(r rune) bool {
	return !strings.ContainsRune(idAlphabet, r)
}

func parseInto(raw string, encoded *string) (IngestToken, error) {
	if !hasTokenShape(raw) {
		return IngestToken{}, ErrMalformed
	}
	id, err := ParseID(raw[idStart:idEnd])
	if err != nil {
		return IngestToken{}, err
	}
	if _, err := decodeSecret(raw[secretStart:]); err != nil {
		return IngestToken{}, err
	}
	*encoded = raw[secretStart:]
	return IngestToken{
		ID: id,
		Secret: Secret{
			encoded: encoded,
		},
	}, nil
}

func decodeSecret(encoded string) ([SecretBytes]byte, error) {
	var text [SecretLength]byte
	var secret [SecretBytes]byte
	if len(encoded) != SecretLength {
		return secret, ErrMalformed
	}
	copy(text[:], encoded)
	decoded, err := base64.RawURLEncoding.Strict().Decode(secret[:], text[:])
	if err != nil || decoded != SecretBytes {
		return secret, ErrMalformed
	}
	return secret, nil
}
