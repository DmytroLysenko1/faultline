package bearer

import (
	"errors"
	"strings"

	"github.com/DmytroLysenko1/faultline/internal/domain/credential"
)

const Scheme = "Bearer "

const headerLength = len(Scheme) + credential.TokenLength

var (
	ErrMissing = errors.New("authorization header missing")
	ErrHeader  = errors.New("authorization header malformed")
)

func Credential(values []string) (string, error) {
	if len(values) == 0 {
		return "", ErrMissing
	}
	if len(values) > 1 {
		return "", ErrHeader
	}
	value := values[0]
	if len(value) != headerLength {
		return "", ErrHeader
	}
	if !strings.EqualFold(value[:len(Scheme)], Scheme) {
		return "", ErrHeader
	}
	return value[len(Scheme):], nil
}
