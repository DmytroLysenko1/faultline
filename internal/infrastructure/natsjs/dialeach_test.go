package natsjs_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"

	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/natsjstest"
)

const (
	certificateLife = time.Hour
	passRuns        = 20
	caFileName      = "ca.pem"
	pemCertificate  = "CERTIFICATE"
	caFileMode      = 0o600
)

type probed struct {
	Host   string
	Reason natsjs.DialReason
}

func hostOf(t *testing.T, raw string) string {
	t.Helper()
	parsed, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("parse %s: %v", raw, err)
	}
	return parsed.Host
}

func probedOf(failures []natsjs.URLFailure) []probed {
	got := make([]probed, 0, len(failures))
	for _, failure := range failures {
		got = append(got, probed{
			Host:   failure.Host,
			Reason: failure.Reason,
		})
	}
	return got
}

func TestDialEachNamesEveryFailingURLByItsHost(t *testing.T) {
	cluster := natsjstest.Start(t, singleNode)
	refused := natsjstest.RefusedURL(t)
	silent := natsjstest.SilentEndpoint(t).URL
	auth := authServer(t)
	options := quickOptions(refused, cluster.URLs()[0], silent, auth)

	failures, err := natsjs.DialEach(t.Context(), options)
	if err != nil {
		t.Fatalf("DialEach: %v", err)
	}

	want := []probed{
		{
			Host:   hostOf(t, refused),
			Reason: natsjs.DialNoServers,
		},
		{
			Host:   hostOf(t, silent),
			Reason: natsjs.DialNoNATSGreeting,
		},
		{
			Host: hostOf(t, auth),
		},
	}
	if diff := cmp.Diff(want, probedOf(failures)); diff != "" {
		t.Errorf("failures (-want +got):\n%s", diff)
	}
	if !errors.Is(failures[2].Err, nats.ErrAuthorization) {
		t.Errorf("the auth failure carries %v, want the authorization violation", failures[2].Err)
	}
}

func TestDialEachSeesTheAuthFailureThatDialHides(t *testing.T) {
	options := quickOptions(authServer(t), natsjstest.RefusedURL(t))
	hidden := 0
	for range passRuns {
		_, err := natsjs.Dial(t.Context(), options)
		if _, unreachable := natsjs.Unreachable(err); unreachable {
			hidden++
		}

		failures, err := natsjs.DialEach(t.Context(), options)
		if err != nil {
			t.Fatalf("DialEach: %v", err)
		}

		if len(failures) != len(options.URLs) || failures[0].Reason != "" {
			t.Fatalf("DialEach = %+v, want the auth failure on the first url and the refused second", failures)
		}
	}
	t.Logf("Dial hid the auth failure behind a refused url in %d of %d runs", hidden, passRuns)
}

func TestDialEachIgnoresTheServersANeighbourAdvertises(t *testing.T) {
	cluster := natsjstest.Start(t, threeNodes, func(options *server.Options) {
		options.Username = authUser
		options.Password = authPassword
		options.ClientAdvertise = net.JoinHostPort("127.0.0.1", "1")
	})
	options := quickOptions(cluster.URLs()[0])
	_, dialErr := natsjs.Dial(t.Context(), options)
	if _, hidden := natsjs.Unreachable(dialErr); !hidden {
		t.Fatalf("Dial = %v, want the advertised neighbour to hide the auth failure behind a wait class", dialErr)
	}

	failures, err := natsjs.DialEach(t.Context(), options)
	if err != nil {
		t.Fatalf("DialEach: %v", err)
	}

	if len(failures) != 1 || failures[0].Reason != "" || !errors.Is(failures[0].Err, nats.ErrAuthorization) {
		t.Errorf("DialEach = %+v, want only the auth failure of the one url", failures)
	}
}

func TestDialEachUsesTheSameCredentials(t *testing.T) {
	address, caFile := tlsServer(t)
	tests := []struct {
		name string
		args string
		want []probed
	}{
		{
			name: "with the operator's CA the TLS url connects",
			args: caFile,
			want: []probed{},
		},
		{
			name: "without it the url fails closed, not as a wait",
			want: []probed{
				{
					Host: address,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			options := quickOptions("tls://" + address)
			options.TLSCAFile = tt.args

			failures, err := natsjs.DialEach(t.Context(), options)
			if err != nil {
				t.Fatalf("DialEach: %v", err)
			}

			if diff := cmp.Diff(tt.want, probedOf(failures)); diff != "" {
				t.Errorf("failures (-want +got):\n%s", diff)
			}
		})
	}
}

func TestDialEachStopsOnCancel(t *testing.T) {
	silent := natsjstest.SilentEndpoint(t)
	options := optionsFor([]string{silent.URL, silent.URL, silent.URL})
	ctx, cancel := context.WithCancel(t.Context())
	stop := time.AfterFunc(drainTimeout, cancel)
	defer stop.Stop()
	started := time.Now()

	failures, err := natsjs.DialEach(ctx, options)

	if !errors.Is(err, ctx.Err()) || failures != nil {
		t.Fatalf("DialEach = %v, %v; want the cancel and no failures", failures, err)
	}
	if took := time.Since(started); took > connectTimeout {
		t.Errorf("DialEach returned %s after a cancel at %s", took, drainTimeout)
	}
}

func TestDialEachRefusesOptionsItCannotKeep(t *testing.T) {
	if _, err := natsjs.DialEach(t.Context(), nil); !errors.Is(err, natsjs.ErrInvalidConnOptions) {
		t.Errorf("DialEach(nil) = %v, want ErrInvalidConnOptions", err)
	}
}

func tlsServer(t *testing.T) (string, string) {
	t.Helper()
	certificate, authority := selfSigned(t)
	caFile := filepath.Join(t.TempDir(), caFileName)
	if err := os.WriteFile(caFile, authority, caFileMode); err != nil {
		t.Fatalf("write the CA: %v", err)
	}
	cluster := natsjstest.Start(t, singleNode, func(options *server.Options) {
		options.TLS = true
		options.TLSConfig = &tls.Config{
			Certificates: []tls.Certificate{certificate},
			MinVersion:   tls.VersionTLS12,
		}
	})
	return hostOf(t, cluster.URLs()[0]), caFile
}

func selfSigned(t *testing.T) (tls.Certificate, []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("key: %v", err)
	}
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "faultline test"},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(certificateLife),
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("certificate: %v", err)
	}
	certificate := tls.Certificate{
		Certificate: [][]byte{der},
		PrivateKey:  key,
	}
	return certificate, pem.EncodeToMemory(&pem.Block{
		Type:  pemCertificate,
		Bytes: der,
	})
}
