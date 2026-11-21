package credential

import (
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"time"
)

func Generate(random io.Reader, tenant string, issuedAt time.Time) (IngestToken, Grant, error) {
	id, err := generateID(random)
	if err != nil {
		return IngestToken{}, Grant{}, err
	}
	var secret [SecretBytes]byte
	if _, err := io.ReadFull(random, secret[:]); err != nil {
		return IngestToken{}, Grant{}, fmt.Errorf("read the ingest token secret: %w", errors.Join(ErrNoRandomness, err))
	}
	token := IngestToken{
		ID: id,
		Secret: Secret{
			encoded: new(base64.RawURLEncoding.EncodeToString(secret[:])),
		},
	}
	grant, err := NewGrant(id, tenant, token.Digest(), issuedAt)
	if err != nil {
		return IngestToken{}, Grant{}, err
	}
	return token, grant, nil
}

func generateID(random io.Reader) (ID, error) {
	var symbols [IDLength]byte
	if _, err := io.ReadFull(random, symbols[:]); err != nil {
		return "", fmt.Errorf("read the ingest token id: %w", errors.Join(ErrNoRandomness, err))
	}
	for index, value := range symbols {
		symbols[index] = idAlphabet[int(value)%len(idAlphabet)]
	}
	return ID(symbols[:]), nil
}
