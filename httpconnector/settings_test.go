package httpconnector

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/temporalio/nexus-connector-http/generated/httpapi"
	"github.com/temporalio/nexus-connector-http/settings"
)

// NewWithEnv reads its configuration and the requests' secrets from the env it is given, not the
// process's.
func TestNewWithEnv_ReadsOnlyItsEnv(t *testing.T) {
	t.Setenv(AllowedDomainsEnvVar, "") // would fail New
	t.Setenv(SecretEnvPrefix+"PAGE", "from_the_process")
	env := settings.Map{
		AllowedDomainsEnvVar:                           "example.com",
		RateLimitMinutesEnvVar:                         "7",
		MaxDurationEnvVar:                              "8",
		AllowRedirectsEnvVar:                           "true",
		SecretEnvPrefix + "PAGE":                       "from_the_env",
		SecretEnvPrefix + "PAGE" + SecretDomainsSuffix: "api.example.com",
	}
	a, err := NewWithEnv(env)
	require.NoError(t, err)
	require.Equal(t, []string{"example.com"}, a.domains)
	require.Equal(t, 7*time.Minute, a.lim.ttl)
	require.Equal(t, 8*time.Minute, a.maxTotal)
	require.True(t, a.allowRedirects)
	require.Equal(t, map[string][]string{SecretEnvPrefix + "PAGE": {"api.example.com"}}, a.secretDomains)

	req := httpapi.SendRequestInput{URL: "https://api.example.com", APIKeyEnvVar: new(SecretEnvPrefix + "PAGE")}
	httpReq, err := build(t.Context(), a.env, req)
	require.NoError(t, err)
	require.Equal(t, "Bearer from_the_env", httpReq.Header.Get("Authorization"))
	require.Equal(t, []string{"from_the_env"}, secrets(a.env, req))
}

func TestCheckSecretsAllowed(t *testing.T) {
	a := newWithWindow(testWindow)
	a.domains = []string{"stripe.com", "10.0.0.5"}
	a.secretDomains = map[string][]string{
		SecretEnvPrefix + "STRIPE": {"api.stripe.com", "stripe.com"},
		SecretEnvPrefix + "ANY":    nil,
		SecretEnvPrefix + "LAN":    {"10.0.0.5", "10.0.0.6"},
		SecretEnvPrefix + "OTHER":  {"example.com"},
	}
	require.EqualError(t, a.CheckSecretsAllowed(),
		`HTTP_CONNECTOR_SECRET_LAN_DOMAINS lists "10.0.0.6", which isn't within HTTP_CONNECTOR_ALLOWED_DOMAINS, so the connector won't call it`+"\n"+
			`HTTP_CONNECTOR_SECRET_OTHER_DOMAINS lists "example.com", which isn't within HTTP_CONNECTOR_ALLOWED_DOMAINS, so the connector won't call it`)

	a.domains = nil
	require.NoError(t, a.CheckSecretsAllowed(), "the allowlist is disabled")
}

func TestClientOptions(t *testing.T) {
	none := filepath.Join(t.TempDir(), "none.toml")
	opts, err := ClientOptions(settings.Map{"TEMPORAL_CONFIG_FILE": none, "TEMPORAL_ADDRESS": "temporal.example:7233"})
	require.NoError(t, err)
	require.Equal(t, "temporal.example:7233", opts.HostPort)
	require.Equal(t, "connectors", opts.Namespace)

	opts, err = ClientOptions(settings.Map{"TEMPORAL_CONFIG_FILE": none, "TEMPORAL_NAMESPACE": "mine"})
	require.NoError(t, err)
	require.Equal(t, "mine", opts.Namespace)
}

// Every variable the connector names has an entry in Settings, with help.
func TestSettings_ListsEveryVariable(t *testing.T) {
	listed := map[string]string{}
	for _, v := range Settings.Variables() {
		listed[v.Name] = v.Help
	}
	for _, name := range []string{AllowedDomainsEnvVar, AllowRedirectsEnvVar, RateLimitMinutesEnvVar, MaxDurationEnvVar,
		"TEMPORAL_ADDRESS", "TEMPORAL_NAMESPACE", "TEMPORAL_API_KEY", "HTTPS_PROXY", "HTTP_PROXY", "NO_PROXY"} {
		require.NotEmpty(t, listed[name], name)
	}
	for _, name := range []string{"HTTPS_PROXY", "https_proxy", "HTTP_PROXY", "http_proxy", "NO_PROXY", "no_proxy"} {
		require.NotNil(t, Settings.External(name), "%s is read by Go, not the connector", name)
	}
	require.True(t, Settings.IsSecret(SecretEnvPrefix+"STRIPE"))
	require.False(t, Settings.IsSecret(SecretEnvPrefix+"STRIPE"+SecretDomainsSuffix))
	require.True(t, Settings.IsSecret("TEMPORAL_API_KEY"))
}
