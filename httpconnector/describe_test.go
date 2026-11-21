package httpconnector

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/temporalio/nexus-connector-http/nexus-connector-shared/settings"
	"github.com/temporalio/nexus-connector-http/nexus-connector-shared/temporalclient"
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

// The startup report lists the settings, marking defaults, the Temporal connection, and what the
// Outbound Connector will do, naming secrets but never their values.
func TestReport(t *testing.T) {
	env := settings.Map{
		"TEMPORAL_CONFIG_FILE":                 filepath.Join(t.TempDir(), "none.toml"),
		AllowedDomainsEnvVar:                   "api.stripe.com",
		AttemptTimeoutEnvVar:                   "30",
		"HTTP_CONNECTOR_SECRET_STRIPE":         "sk_live_never_logged",
		"HTTP_CONNECTOR_SECRET_STRIPE_DOMAINS": "api.stripe.com",
	}
	a, err := NewWithEnv(env)
	require.NoError(t, err)
	opts, err := temporalclient.Options(env)
	require.NoError(t, err)
	require.Equal(t, `Outbound Connector configuration (values marked "default" come from variables that aren't set):
  Allowed hosts
    HTTP_CONNECTOR_ALLOWED_DOMAINS          api.stripe.com
  Requests
    HTTP_CONNECTOR_ALLOW_REDIRECTS          false (default)
    HTTP_CONNECTOR_ATTEMPT_TIMEOUT_SECONDS  30s
    HTTP_CONNECTOR_RATE_LIMIT_MINUTES       5m0s (default)
    HTTP_CONNECTOR_MAX_DURATION_MINUTES     25h0m0s (default)
  Temporal
    namespace                               connectors (default)
    address                                 localhost:7233 (default)
    payload codec                           none: payloads go to Temporal as they are, unencrypted (set TEMPORAL_CODEC_ENDPOINT to encrypt them)
What the Outbound Connector will do:
  - Serve the HTTPService Nexus service, with the operation request, and run requests on the task queue "nexus-connector-http": point the Outbound Connector's Nexus endpoint at it.
  - Only calls to these domains are allowed (HTTP_CONNECTOR_ALLOWED_DOMAINS): api.stripe.com
  - Secret HTTP_CONNECTOR_SECRET_STRIPE may be sent to: api.stripe.com`, a.Report(opts))
	require.NotContains(t, a.Report(opts), "sk_live_never_logged")
}
