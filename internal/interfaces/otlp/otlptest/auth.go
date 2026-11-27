package otlptest

import (
	"context"
	"strings"

	"google.golang.org/grpc"

	"github.com/DmytroLysenko1/faultline/internal/application/ingestauth"
	"github.com/DmytroLysenko1/faultline/internal/domain/credential"
)

const (
	TokenID             = "testtokenid2"
	authorizationHeader = "authorization"
	secretFiller        = "A"
)

var Token = credential.Prefix + TokenID + "_" + strings.Repeat(secretFiller, credential.SecretLength)

type AuthenticatorFunc func(ctx context.Context, token string) (ingestauth.Principal, error)

func (f AuthenticatorFunc) Authenticate(ctx context.Context, token string) (ingestauth.Principal, error) {
	return f(ctx, token)
}

func AdmitToken(_ context.Context, token string) (ingestauth.Principal, error) {
	if token == Token {
		return ingestauth.Principal{
			TokenID: TokenID,
		}, nil
	}
	parsed, err := credential.Parse(token)
	if err != nil {
		return ingestauth.Principal{}, ingestauth.RejectionError{
			Reason: ingestauth.ReasonMalformed,
		}
	}
	return ingestauth.Principal{}, ingestauth.RejectionError{
		Reason:  ingestauth.ReasonUnknown,
		TokenID: parsed.ID,
	}
}

func Admitting() AuthenticatorFunc {
	return AdmitToken
}

type bearerToken struct {
	token string
}

func (b bearerToken) GetRequestMetadata(_ context.Context, _ ...string) (map[string]string, error) {
	return map[string]string{
		authorizationHeader: "Bearer " + b.token,
	}, nil
}

func (bearerToken) RequireTransportSecurity() bool {
	return false
}

func WithToken(token string) grpc.DialOption {
	return grpc.WithPerRPCCredentials(bearerToken{
		token: token,
	})
}

func WithTestToken() grpc.DialOption {
	return WithToken(Token)
}
