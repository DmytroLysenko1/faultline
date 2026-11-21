package credential_test

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/DmytroLysenko1/faultline/internal/domain/credential"
)

const redactedText = "[redacted]"

var errWrapped = errors.New("wrapped")

type holder struct {
	Token  credential.IngestToken
	Secret credential.Secret
}

type hiddenHolder struct {
	token  credential.IngestToken
	secret credential.Secret
}

func secretForms(secret []byte) []string {
	return []string{
		string(secret),
		base64.RawURLEncoding.EncodeToString(secret),
		base64.StdEncoding.EncodeToString(secret),
		hex.EncodeToString(secret),
		strings.ToUpper(hex.EncodeToString(secret)),
		fmt.Sprintf("%v", secret),
		fmt.Sprintf("%#v", secret),
		strings.Trim(fmt.Sprintf("%v", secret), "[]"),
	}
}

var verbs = []string{"%v", "%+v", "%#v", "%s", "%q", "%x", "%X", "%d"}

func formatted(token credential.IngestToken) []string {
	var printed []string
	for _, verb := range verbs {
		printed = append(printed,
			fmt.Sprintf(verb, token),
			fmt.Sprintf(verb, &token),
			fmt.Sprintf(verb, token.Secret),
			fmt.Sprintf(verb, holder{Token: token, Secret: token.Secret}),
		)
	}
	return append(printed,
		token.Secret.String(),
		token.Secret.GoString(),
		token.Secret.LogValue().String(),
		token.LogValue().Resolve().String(),
	)
}

func formattedHidden(token credential.IngestToken) []string {
	hidden := hiddenHolder{token: token, secret: token.Secret}
	var printed []string
	for _, verb := range verbs {
		printed = append(printed,
			fmt.Sprintf(verb, hidden),
			fmt.Sprintf(verb, &hidden),
			fmt.Sprintf(verb, []hiddenHolder{hidden}),
		)
	}
	return printed
}

func logged(token credential.IngestToken) []string {
	hidden := hiddenHolder{token: token, secret: token.Secret}
	var text, structured bytes.Buffer
	for _, logger := range []*slog.Logger{
		slog.New(slog.NewTextHandler(&text, nil)),
		slog.New(slog.NewJSONHandler(&structured, nil)),
	} {
		logger.Info("issued", "token", token, "secret", token.Secret, "holder", holder{Token: token, Secret: token.Secret})
		logger.Info("issued", slog.Any("pointer", &token), slog.Any("hidden", hidden), slog.Any("hidden pointer", &hidden))
	}
	return []string{text.String(), structured.String()}
}

func marshaled(t *testing.T, token credential.IngestToken) []string {
	t.Helper()
	var encoded []string
	for _, value := range []any{holder{Token: token, Secret: token.Secret}, hiddenHolder{token: token, secret: token.Secret}} {
		text, err := json.Marshal(value)
		if err != nil {
			t.Fatalf("json.Marshal: %v", err)
		}
		encoded = append(encoded, string(text))
	}
	return encoded
}

func TestTheSecretPrintsRedactedEverywhere(t *testing.T) {
	token, _ := generated(t, sequentialID(), markerSecret(), testTenant)
	outputs := append(formatted(token), logged(token)...)
	outputs = append(outputs, marshaled(t, token)[0],
		fmt.Errorf("token %v: %w", token, errWrapped).Error(),
		fmt.Errorf("secret %+v: %w", token.Secret, errWrapped).Error(),
	)

	for _, output := range outputs {
		expectNoSecret(t, output)
		if !strings.Contains(output, redactedText) {
			t.Errorf("output %q does not say %s", output, redactedText)
		}
	}
}

func TestASecretInAnUnexportedFieldStaysHidden(t *testing.T) {
	token, _ := generated(t, sequentialID(), markerSecret(), testTenant)
	outputs := append(formattedHidden(token), marshaled(t, token)[1],
		fmt.Errorf("holder %+v: %w", hiddenHolder{token: token, secret: token.Secret}, errWrapped).Error(),
	)
	outputs = append(outputs, logged(token)...)

	for _, output := range outputs {
		expectNoSecret(t, output)
	}
}

func expectNoSecret(t *testing.T, output string) {
	t.Helper()
	for _, form := range secretForms(markerSecret()) {
		if strings.Contains(output, form) {
			t.Errorf("output %q carries the secret as %q", output, form)
		}
	}
}

func TestOnlyRevealPrintsTheSecret(t *testing.T) {
	token, _ := generated(t, sequentialID(), markerSecret(), testTenant)

	revealed := credential.Reveal(token)

	encoded := base64.RawURLEncoding.EncodeToString(markerSecret())
	if want := credential.Prefix + string(token.ID) + "_" + encoded; revealed != want {
		t.Errorf("Reveal() = %q, want %q", revealed, want)
	}
}
