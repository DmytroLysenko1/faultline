package resource

import (
	"cmp"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"
)

var ErrInvalid = errors.New("invalid resource")

const (
	maxServiceNameBytes      = 255
	maxServiceNamespaceBytes = 255
	maxK8sNamespaceBytes     = 63
	maxK8sPodNameBytes       = 253
	identityFieldCount       = 4

	asciiLastControl = 0x1F
	asciiDelete      = 0x7F
)

type Resource struct {
	ServiceName      string
	ServiceNamespace string
	K8sNamespaceName string
	K8sPodName       string
}

type ServiceRef struct {
	Namespace string
	Name      string
}

type identity struct {
	Field    string
	Value    string
	MaxBytes int
}

func (r Resource) Validate() error {
	if r.ServiceName == "" {
		return fmt.Errorf("%w: service.name: missing", ErrInvalid)
	}
	for _, attribute := range r.identities() {
		if err := attribute.validate(); err != nil {
			return err
		}
	}
	return nil
}

func (r Resource) ServiceRef() ServiceRef {
	return ServiceRef{
		Namespace: cmp.Or(r.K8sNamespaceName, r.ServiceNamespace),
		Name:      r.ServiceName,
	}
}

func (r Resource) identities() [identityFieldCount]identity {
	return [...]identity{
		{
			Field:    "service.name",
			Value:    r.ServiceName,
			MaxBytes: maxServiceNameBytes,
		},
		{
			Field:    "service.namespace",
			Value:    r.ServiceNamespace,
			MaxBytes: maxServiceNamespaceBytes,
		},
		{
			Field:    "k8s.namespace.name",
			Value:    r.K8sNamespaceName,
			MaxBytes: maxK8sNamespaceBytes,
		},
		{
			Field:    "k8s.pod.name",
			Value:    r.K8sPodName,
			MaxBytes: maxK8sPodNameBytes,
		},
	}
}

func (i identity) validate() error {
	if len(i.Value) > i.MaxBytes {
		return fmt.Errorf("%w: %s: %d bytes > %d", ErrInvalid, i.Field, len(i.Value), i.MaxBytes)
	}
	if !utf8.ValidString(i.Value) {
		return fmt.Errorf("%w: %s: invalid UTF-8", ErrInvalid, i.Field)
	}
	if strings.ContainsFunc(i.Value, isControlCharacter) {
		return fmt.Errorf("%w: %s: control character", ErrInvalid, i.Field)
	}
	return nil
}

func isControlCharacter(r rune) bool {
	return r <= asciiLastControl || r == asciiDelete
}
