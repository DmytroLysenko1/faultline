package checkpoint

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

const (
	pointerVersion   = 1
	digestPrefix     = "SHA-256="
	digestBytes      = 32
	nameSeparator    = "-"
	decimal          = 10
	uintBits         = 64
	createdLayout    = time.RFC3339Nano
	noStreamCreation = ""
)

type objectRef struct {
	Name     string `json:"name"`
	NUID     string `json:"nuid"`
	Digest   string `json:"digest"`
	Size     uint64 `json:"size"`
	Position uint64 `json:"position"`
}

type pointerValue struct {
	Version       int        `json:"v"`
	Epoch         uint64     `json:"epoch"`
	StreamCreated string     `json:"stream_created"`
	Current       *objectRef `json:"current"`
	Previous      *objectRef `json:"previous"`
	DiscardedAt   uint64     `json:"discarded_at_first_seq,omitempty"`
}

type pointer struct {
	value    pointerValue
	created  time.Time
	revision uint64
	corrupt  bool
}

func (p *pointer) currentNUID() string {
	if p == nil || p.value.Current == nil {
		return ""
	}
	return p.value.Current.NUID
}

func (p *pointer) holds(name string) bool {
	return sameName(p.value.Current, name) || sameName(p.value.Previous, name)
}

func sameName(ref *objectRef, name string) bool {
	return ref != nil && ref.Name == name
}

func encodePointer(value *pointerValue) ([]byte, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("encode checkpoint pointer: %w", err)
	}
	return data, nil
}

func decodePointer(data []byte, revision uint64, maxBytes int64) (pointer, error) {
	var value pointerValue
	if err := json.Unmarshal(data, &value); err != nil {
		return pointer{}, fmt.Errorf("%w: pointer does not parse: %w", errCorrupt, err)
	}
	created, err := parseCreated(value.StreamCreated)
	if err != nil {
		return pointer{}, err
	}
	if err := value.check(maxBytes); err != nil {
		return pointer{}, err
	}
	return pointer{
		value:    value,
		created:  created,
		revision: revision,
	}, nil
}

func parseCreated(text string) (time.Time, error) {
	if text == noStreamCreation {
		return time.Time{}, nil
	}
	created, err := time.Parse(createdLayout, text)
	if err != nil {
		return time.Time{}, fmt.Errorf("%w: pointer stream_created %q: %w", errCorrupt, text, err)
	}
	return created, nil
}

func (v *pointerValue) check(maxBytes int64) error {
	if v.Version != pointerVersion {
		return fmt.Errorf("%w: pointer version %d, want %d", errCorrupt, v.Version, pointerVersion)
	}
	for _, ref := range []*objectRef{v.Current, v.Previous} {
		if err := ref.check(maxBytes); err != nil {
			return err
		}
	}
	return nil
}

func (r *objectRef) check(maxBytes int64) error {
	if r == nil {
		return nil
	}
	if err := r.checkName(); err != nil {
		return err
	}
	if err := r.checkContent(maxBytes); err != nil {
		return err
	}
	return checkDigest(r.Digest)
}

func (r *objectRef) checkContent(maxBytes int64) error {
	if r.NUID == "" || r.Size == 0 || r.Size > unsigned(maxBytes) {
		return fmt.Errorf("%w: pointer object %q of %d bytes, nuid %q", errCorrupt, r.Name, r.Size, r.NUID)
	}
	return nil
}

func (r *objectRef) checkName() error {
	epoch, position, named := parseName(r.Name)
	if !named || epoch == 0 || position != r.Position {
		return fmt.Errorf("%w: pointer object name %q at position %d", errCorrupt, r.Name, r.Position)
	}
	return nil
}

func unsigned(value int64) uint64 {
	if value < 0 {
		return 0
	}
	return uint64(value)
}

func checkDigest(digest string) error {
	encoded, prefixed := strings.CutPrefix(digest, digestPrefix)
	sum, err := base64.URLEncoding.DecodeString(encoded)
	if !prefixed || err != nil || len(sum) != digestBytes {
		return fmt.Errorf("%w: pointer digest %q", errCorrupt, digest)
	}
	return nil
}

func parseName(name string) (uint64, uint64, bool) {
	epochText, positionText, cut := strings.Cut(name, nameSeparator)
	epoch, epochErr := strconv.ParseUint(epochText, decimal, uintBits)
	position, positionErr := strconv.ParseUint(positionText, decimal, uintBits)
	return epoch, position, cut && epochErr == nil && positionErr == nil
}
