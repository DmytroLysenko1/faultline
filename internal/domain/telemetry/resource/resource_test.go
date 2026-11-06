package resource_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/resource"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/telemetrytest"
)

func TestResourceValidateRejects(t *testing.T) {
	tests := []struct {
		Name    string
		Args    resource.Resource
		Want    string
		WantErr error
	}{
		{
			Name: "missing service.name",
			Args: telemetrytest.ResourceWith(func(r *resource.Resource) {
				r.ServiceName = ""
			}),
			Want:    "invalid resource: service.name: missing",
			WantErr: resource.ErrInvalid,
		},
		{
			Name: "service.name one byte over limit",
			Args: telemetrytest.ResourceWith(func(r *resource.Resource) {
				r.ServiceName = strings.Repeat("a", 256)
			}),
			Want:    "invalid resource: service.name: 256 bytes > 255",
			WantErr: resource.ErrInvalid,
		},
		{
			Name: "service.namespace one byte over limit",
			Args: telemetrytest.ResourceWith(func(r *resource.Resource) {
				r.ServiceNamespace = strings.Repeat("a", 256)
			}),
			Want:    "invalid resource: service.namespace: 256 bytes > 255",
			WantErr: resource.ErrInvalid,
		},
		{
			Name: "k8s.namespace.name one byte over limit",
			Args: telemetrytest.ResourceWith(func(r *resource.Resource) {
				r.K8sNamespaceName = strings.Repeat("a", 64)
			}),
			Want:    "invalid resource: k8s.namespace.name: 64 bytes > 63",
			WantErr: resource.ErrInvalid,
		},
		{
			Name: "k8s.pod.name one byte over limit",
			Args: telemetrytest.ResourceWith(func(r *resource.Resource) {
				r.K8sPodName = strings.Repeat("a", 254)
			}),
			Want:    "invalid resource: k8s.pod.name: 254 bytes > 253",
			WantErr: resource.ErrInvalid,
		},
		{
			Name: "invalid UTF-8",
			Args: telemetrytest.ResourceWith(func(r *resource.Resource) {
				r.ServiceName = "check\xffout"
			}),
			Want:    "invalid resource: service.name: invalid UTF-8",
			WantErr: resource.ErrInvalid,
		},
		{
			Name: "newline",
			Args: telemetrytest.ResourceWith(func(r *resource.Resource) {
				r.ServiceName = "check\nout"
			}),
			Want:    "invalid resource: service.name: control character",
			WantErr: resource.ErrInvalid,
		},
		{
			Name: "NUL",
			Args: telemetrytest.ResourceWith(func(r *resource.Resource) {
				r.K8sPodName = "pod\x00"
			}),
			Want:    "invalid resource: k8s.pod.name: control character",
			WantErr: resource.ErrInvalid,
		},
		{
			Name: "unit separator, the last control character",
			Args: telemetrytest.ResourceWith(func(r *resource.Resource) {
				r.ServiceName = "check\x1fout"
			}),
			Want:    "invalid resource: service.name: control character",
			WantErr: resource.ErrInvalid,
		},
		{
			Name: "DEL",
			Args: telemetrytest.ResourceWith(func(r *resource.Resource) {
				r.ServiceNamespace = "ns\x7f"
			}),
			Want:    "invalid resource: service.namespace: control character",
			WantErr: resource.ErrInvalid,
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			err := tt.Args.Validate()
			if !errors.Is(err, tt.WantErr) {
				t.Fatalf("err = %v, want %v", err, tt.WantErr)
			}
			if diff := cmp.Diff(tt.Want, err.Error()); diff != "" {
				t.Errorf("reason mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestResourceServiceRef(t *testing.T) {
	tests := []struct {
		Name    string
		Args    resource.Resource
		Want    resource.ServiceRef
		WantErr error
	}{
		{
			Name: "identity strings exactly at the limit",
			Args: telemetrytest.ResourceWith(func(r *resource.Resource) {
				r.ServiceName = strings.Repeat("s", 255)
				r.K8sNamespaceName = strings.Repeat("n", 63)
				r.K8sPodName = strings.Repeat("p", 253)
			}),
			Want: resource.ServiceRef{
				Namespace: strings.Repeat("n", 63),
				Name:      strings.Repeat("s", 255),
			},
		},
		{
			Name: "SDK fallback service name is kept as is",
			Args: telemetrytest.ResourceWith(func(r *resource.Resource) {
				r.ServiceName = "unknown_service:java"
			}),
			Want: resource.ServiceRef{
				Namespace: "otel-demo",
				Name:      "unknown_service:java",
			},
		},
		{
			Name: "k8s.namespace.name wins over service.namespace",
			Args: telemetrytest.ResourceWith(func(r *resource.Resource) {
				r.ServiceNamespace = "shop"
			}),
			Want: resource.ServiceRef{
				Namespace: "otel-demo",
				Name:      "checkout",
			},
		},
		{
			Name: "service.namespace when k8s.namespace.name is absent",
			Args: telemetrytest.ResourceWith(func(r *resource.Resource) {
				r.K8sNamespaceName = ""
				r.ServiceNamespace = "shop"
			}),
			Want: resource.ServiceRef{
				Namespace: "shop",
				Name:      "checkout",
			},
		},
		{
			Name: "empty namespace when both are absent",
			Args: telemetrytest.ResourceWith(func(r *resource.Resource) {
				r.K8sNamespaceName = ""
			}),
			Want: resource.ServiceRef{
				Name: "checkout",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			err := tt.Args.Validate()
			if !errors.Is(err, tt.WantErr) {
				t.Fatalf("err = %v, want %v", err, tt.WantErr)
			}
			if diff := cmp.Diff(tt.Want, tt.Args.ServiceRef()); diff != "" {
				t.Errorf("service mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
