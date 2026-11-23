package setup

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/temporalio/nexus-connector-http/httpconnector"
	"github.com/temporalio/nexus-connector-http/nexus-connector-shared/settings"
)

const (
	allowedDomains = httpconnector.AllowedDomainsEnvVar
	allowRedirects = httpconnector.AllowRedirectsEnvVar
	attemptTimeout = httpconnector.AttemptTimeoutEnvVar
	rateLimit      = httpconnector.RateLimitMinutesEnvVar
	maxDuration    = httpconnector.MaxDurationEnvVar
	secretPrefix   = httpconnector.SecretEnvPrefix
	domainsSuffix  = httpconnector.SecretDomainsSuffix
)

// load is the Outbound Connector's configuration in env, as the program loads it.
func load(env settings.Map) (httpconnector.Config, error) {
	vals, err := Outbound.Load(env)
	if err != nil {
		return httpconnector.Config{}, err
	}
	return Config(vals), nil
}

// With only the required variable set, every other setting is its default.
func TestConfig_Defaults(t *testing.T) {
	cfg, err := load(settings.Map{allowedDomains: "example.com"})
	require.NoError(t, err)
	require.Equal(t, httpconnector.Config{
		AllowedDomains:  []string{"example.com"},
		AttemptTimeout:  60 * time.Second,
		RateLimitWindow: 5 * time.Minute,
		MaxDuration:     25 * time.Hour,
		SecretDomains:   map[string][]string{},
	}, cfg)
}

// Config reads every value from the env it is given, not the process's.
func TestConfig_ReadsOnlyItsEnv(t *testing.T) {
	t.Setenv(allowedDomains, "") // would fail the process's
	t.Setenv(secretPrefix+"PAGE", "from_the_process")
	cfg, err := load(settings.Map{
		allowedDomains:                        "example.com",
		rateLimit:                             "7",
		maxDuration:                           "8",
		attemptTimeout:                        "30",
		allowRedirects:                        "true",
		secretPrefix + "PAGE":                 "from_the_env",
		secretPrefix + "PAGE" + domainsSuffix: "api.example.com",
	})
	require.NoError(t, err)
	require.Equal(t, httpconnector.Config{
		AllowedDomains:  []string{"example.com"},
		AllowRedirects:  true,
		AttemptTimeout:  30 * time.Second,
		RateLimitWindow: 7 * time.Minute,
		MaxDuration:     8 * time.Minute,
		SecretDomains:   map[string][]string{secretPrefix + "PAGE": {"api.example.com"}},
	}, cfg)
}

// Load checks every variable before failing, so one error names every invalid value.
func TestLoad_ReportsEveryInvalidValue(t *testing.T) {
	_, err := load(settings.Map{rateLimit: "0", allowedDomains: "", allowRedirects: "yes", maxDuration: "1h"})
	for _, name := range []string{rateLimit, allowedDomains, allowRedirects, maxDuration} {
		require.ErrorContains(t, err, name)
	}
}

func TestAllowedDomains(t *testing.T) {
	for _, unset := range []string{"", "  "} {
		_, err := allowedDomainsSetting.Get(settings.Map{allowedDomains: unset})
		require.ErrorContains(t, err, "set "+allowedDomains+": a comma-separated list", "value %q", unset)
	}

	for _, disable := range []string{httpconnector.DisableAllowlist, " disable allowlist "} {
		domains, err := allowedDomainsSetting.Get(settings.Map{allowedDomains: disable})
		require.NoError(t, err)
		require.Nil(t, domains, "value %q allows any host", disable)
	}

	domains, err := allowedDomainsSetting.Get(settings.Map{allowedDomains: " temporal.io,Google.com ,  stripe.com.,10.0.0.5"})
	require.NoError(t, err)
	require.Equal(t, []string{"temporal.io", "google.com", "stripe.com", "10.0.0.5"}, domains)

	for _, bad := range []string{
		"temporal.io,", "a.com,,b.com", "https://temporal.io", "temporal.io:443", "temporal.io/docs",
		"*.temporal.io", ".temporal.io", "temporal..io", "temporal io", "temporal.io," + httpconnector.DisableAllowlist,
		"com", "temporal.io,io", "localhost", "*", "",
		// A last label of digits makes an IPv4 address, which must be complete and valid.
		"0.5", "10.0", "168.0.5", "300.1.1.1", "1.2.3.4.5", "example.123",
	} {
		_, err := allowedDomainsSetting.Get(settings.Map{allowedDomains: bad})
		require.ErrorContains(t, err, allowedDomains, "value %q", bad)
	}
}

func TestAllowRedirects(t *testing.T) {
	for v, want := range map[string]bool{"": false, " ": false, "false": false, "0": false, "true": true, "TRUE": true, " 1 ": true} {
		got, err := allowRedirectsSetting.Get(settings.Map{allowRedirects: v})
		require.NoError(t, err, "value %q", v)
		require.Equal(t, want, got, "value %q", v)
	}
	for _, bad := range []string{"yes", "on", "WHITELISTED"} {
		_, err := allowRedirectsSetting.Get(settings.Map{allowRedirects: bad})
		require.ErrorContains(t, err, allowRedirects, "value %q", bad)
	}
}

func TestAttemptTimeout(t *testing.T) {
	d, err := attemptTimeoutSetting.Get(settings.Map{attemptTimeout: "10"})
	require.NoError(t, err)
	require.Equal(t, 10*time.Second, d)

	for _, bad := range []string{"9", "0", "-1", "1.5", "30s", " 30"} {
		_, err := attemptTimeoutSetting.Get(settings.Map{attemptTimeout: bad})
		require.EqualError(t, err, attemptTimeout+` must be a whole number of seconds, at least 10; got "`+bad+`"`)
	}
}

func TestRateLimitWindow(t *testing.T) {
	window, err := rateLimitSetting.Get(settings.Map{rateLimit: "2"})
	require.NoError(t, err)
	require.Equal(t, 2*time.Minute, window)

	for _, bad := range []string{"0", "-1", "1.5", "five", " 2"} {
		_, err := rateLimitSetting.Get(settings.Map{rateLimit: bad})
		require.ErrorContains(t, err, rateLimit, "value %q", bad)
	}
}

func TestMaxDuration(t *testing.T) {
	d, err := maxDurationSetting.Get(settings.Map{maxDuration: "90"})
	require.NoError(t, err)
	require.Equal(t, 90*time.Minute, d)

	for _, bad := range []string{"0", "-1", "1.5", "1h", " 2"} {
		_, err := maxDurationSetting.Get(settings.Map{maxDuration: bad})
		require.ErrorContains(t, err, maxDuration, "value %q", bad)
	}
}

// Every secret needs its list.
func TestSecretDomains(t *testing.T) {
	_, err := load(settings.Map{allowedDomains: httpconnector.DisableAllowlist, secretPrefix + "NEWTEST": "v"})
	require.ErrorContains(t, err, secretPrefix+"NEWTEST needs "+secretPrefix+"NEWTEST_DOMAINS")
}

// Every variable the Outbound Connector names has an entry in Outbound, with help.
func TestOutbound_ListsEveryVariable(t *testing.T) {
	listed := map[string]string{}
	for _, v := range Outbound.Variables() {
		listed[v.Name] = v.Help
	}
	for _, name := range []string{allowedDomains, allowRedirects, attemptTimeout, rateLimit, maxDuration,
		"TEMPORAL_ADDRESS", "TEMPORAL_NAMESPACE", "TEMPORAL_API_KEY", "HTTPS_PROXY", "HTTP_PROXY", "NO_PROXY"} {
		require.NotEmpty(t, listed[name], name)
	}
	for _, name := range []string{"HTTPS_PROXY", "https_proxy", "HTTP_PROXY", "http_proxy", "NO_PROXY", "no_proxy"} {
		require.NotNil(t, Outbound.External(name), "%s is read by Go, not the Outbound Connector", name)
	}
	require.True(t, Outbound.IsSecret(secretPrefix+"STRIPE"))
	require.False(t, Outbound.IsSecret(secretPrefix+"STRIPE"+domainsSuffix))
	require.True(t, Outbound.IsSecret("TEMPORAL_API_KEY"))
}
