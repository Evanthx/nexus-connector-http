package httpconnector

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDescribe(t *testing.T) {
	a := newWithWindow(testWindow)
	a.domains = []string{"api.stripe.com", "googleapis.com", "10.0.0.5"}
	a.secretDomains = map[string][]string{
		"HTTP_CONNECTOR_SECRET_STRIPE": {"api.stripe.com"},
		"HTTP_CONNECTOR_SECRET_GOOGLE": {"google.com", "googleapis.com"},
		"HTTP_CONNECTOR_SECRET_PROXY":  nil,
	}
	require.Equal(t, []string{
		"Only calls to these domains are allowed (HTTP_CONNECTOR_ALLOWED_DOMAINS): api.stripe.com, googleapis.com, 10.0.0.5",
		"Secret HTTP_CONNECTOR_SECRET_GOOGLE may be sent to: google.com, googleapis.com",
		"Secret HTTP_CONNECTOR_SECRET_PROXY may be sent to: any allowed domain (DISABLE ALLOWLIST)",
		"Secret HTTP_CONNECTOR_SECRET_STRIPE may be sent to: api.stripe.com",
	}, a.Describe())

	a.domains = nil
	a.secretDomains = map[string][]string{"HTTP_CONNECTOR_SECRET_PROXY": nil}
	require.Equal(t, []string{
		`Calls to any domain are allowed: the allowlist is disabled (HTTP_CONNECTOR_ALLOWED_DOMAINS is "DISABLE ALLOWLIST")`,
		"Secret HTTP_CONNECTOR_SECRET_PROXY may be sent to: any host (DISABLE ALLOWLIST)",
	}, a.Describe())

	a.secretDomains = map[string][]string{}
	require.Equal(t, "Secrets: none", a.Describe()[1])
}

// Describe names secrets and domains, never a secret's value.
func TestDescribe_NoSecretValues(t *testing.T) {
	t.Setenv(AllowedDomainsEnvVar, "api.stripe.com")
	t.Setenv("HTTP_CONNECTOR_SECRET_DESCRIBE", "sk_live_never_logged")
	t.Setenv("HTTP_CONNECTOR_SECRET_DESCRIBE_DOMAINS", "api.stripe.com")
	a, err := New()
	require.NoError(t, err)
	for _, line := range a.Describe() {
		require.NotContains(t, line, "sk_live_never_logged")
	}
	require.Contains(t, a.Describe(), "Secret HTTP_CONNECTOR_SECRET_DESCRIBE may be sent to: api.stripe.com")
}
