package tokens

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/DmytroLysenko1/faultline/internal/domain/credential"
)

const (
	valueVersion    = 1
	operationHeader = "KV-Operation"
	operationDelete = "DEL"
	operationPurge  = "PURGE"
	issuedAtLayout  = time.RFC3339Nano
)

var (
	errInvalidEntry    = errors.New("invalid token entry")
	errMalformedAnswer = errors.New("malformed jetstream answer")
)

type entryValue struct {
	Version  int    `json:"v"`
	Tenant   string `json:"tenant"`
	Digest   string `json:"digest"`
	IssuedAt string `json:"issued_at"`
}

func idOf(subject string) (credential.ID, error) {
	raw, found := strings.CutPrefix(subject, subjectPrefix)
	id, err := credential.ParseID(raw)
	if !found || err != nil {
		return "", fmt.Errorf("subject %q: %w", subject, errInvalidEntry)
	}
	return id, nil
}

func encodeEntry(grant *credential.Grant) ([]byte, error) {
	digest, err := grant.Digest.MarshalText()
	if err != nil {
		return nil, fmt.Errorf("encode grant %s: %w", grant.ID, err)
	}
	value, err := json.Marshal(entryValue{
		Version:  valueVersion,
		Tenant:   grant.Tenant,
		Digest:   string(digest),
		IssuedAt: grant.IssuedAt.UTC().Format(issuedAtLayout),
	})
	if err != nil {
		return nil, fmt.Errorf("encode grant %s: %w", grant.ID, err)
	}
	return value, nil
}

func decodeEntry(id credential.ID, data []byte) (credential.Grant, error) {
	if len(data) > MaxValueBytes {
		return credential.Grant{}, fmt.Errorf("grant %s of %d bytes: %w", id, len(data), errInvalidEntry)
	}
	value, err := strictValue(data)
	if err != nil {
		return credential.Grant{}, fmt.Errorf("grant %s: %w", id, errors.Join(errInvalidEntry, err))
	}
	return grantOf(id, value)
}

func strictValue(data []byte) (entryValue, error) {
	var value entryValue
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&value); err != nil {
		return entryValue{}, fmt.Errorf("decode value: %w", err)
	}
	if decoder.More() {
		return entryValue{}, errInvalidEntry
	}
	if value.Version != valueVersion {
		return entryValue{}, fmt.Errorf("value version %d: %w", value.Version, errInvalidEntry)
	}
	return value, nil
}

func grantOf(id credential.ID, value entryValue) (credential.Grant, error) {
	digest, err := credential.ParseDigest(value.Digest)
	if err != nil {
		return credential.Grant{}, fmt.Errorf("grant %s: %w", id, errors.Join(errInvalidEntry, err))
	}
	issuedAt, err := time.Parse(issuedAtLayout, value.IssuedAt)
	if err != nil {
		return credential.Grant{}, fmt.Errorf("grant %s issued_at: %w", id, errInvalidEntry)
	}
	grant, err := credential.NewGrant(id, value.Tenant, digest, issuedAt)
	if err != nil {
		return credential.Grant{}, fmt.Errorf("grant %s: %w", id, errors.Join(errInvalidEntry, err))
	}
	return grant, nil
}

func removed(entry rawEntry) bool {
	operation := entry.Header.Get(operationHeader)
	return operation == operationDelete || operation == operationPurge
}

func keyOf(id credential.ID) string {
	return subjectPrefix + string(id)
}
