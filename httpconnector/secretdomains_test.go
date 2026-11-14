package httpconnector

import (
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/temporalio/nexus-connector-http/generated/httpapi"
)

func TestReadSecretDomains(t *testing.T) {
	domains, err := readSecretDomains([]string{
		"HOME=/home/x",
		"HTTP_CONNECTOR_ALLOWED_DOMAINS=example.com",
		"HTTP_CONNECTOR_SECRET_STRIPE=sk_test",
		"HTTP_CONNECTOR_SECRET_STRIPE_DOMAINS=api.stripe.com",
		"HTTP_CONNECTOR_SECRET_GOOGLE=AIza",
		"HTTP_CONNECTOR_SECRET_GOOGLE_DOMAINS= Google.com , googleapis.com. ",
		"HTTP_CONNECTOR_SECRET_LAN=t0ken",
		"HTTP_CONNECTOR_SECRET_LAN_DOMAINS=10.0.0.5",
		"HTTP_CONNECTOR_SECRET_PROXY=p",
		"HTTP_CONNECTOR_SECRET_PROXY_DOMAINS=disable allowlist",
		"HTTP_CONNECTOR_SECRET_BLANK=", // empty: not a secret, and needs no list
	})
	require.NoError(t, err)
	require.Equal(t, map[string][]string{
		"HTTP_CONNECTOR_SECRET_STRIPE": {"api.stripe.com"},
		"HTTP_CONNECTOR_SECRET_GOOGLE": {"google.com", "googleapis.com"},
		"HTTP_CONNECTOR_SECRET_LAN":    {"10.0.0.5"},
		"HTTP_CONNECTOR_SECRET_PROXY":  nil,
	}, domains)
}

// Every problem is reported at once, naming variables but never a value, even a secret put in a
// list's variable by mistake.
func TestReadSecretDomains_ReportsEveryProblem(t *testing.T) {
	_, err := readSecretDomains([]string{
		"HTTP_CONNECTOR_SECRET_NOLIST=a",
		"HTTP_CONNECTOR_SECRET_EMPTYLIST=b",
		"HTTP_CONNECTOR_SECRET_EMPTYLIST_DOMAINS=  ",
		"HTTP_CONNECTOR_SECRET_SWAPPED=api.example.com",
		"HTTP_CONNECTOR_SECRET_SWAPPED_DOMAINS=sk_live_abc123",
		"HTTP_CONNECTOR_SECRET_PARTIAL=c",
		"HTTP_CONNECTOR_SECRET_PARTIAL_DOMAINS=10.0",
		"HTTP_CONNECTOR_SECRET_ORPHAN_DOMAINS=example.com",
		"HTTP_CONNECTOR_SECRET_UNSET_DOMAINS=example.com",
		"HTTP_CONNECTOR_SECRET_UNSET=",
		"HTTP_CONNECTOR_SECRET_X_DOMAINS_DOMAINS=example.com", // X_DOMAINS is a list, never a secret
		"HTTP_CONNECTOR_SECRET_X=d",
		"HTTP_CONNECTOR_SECRET_X_DOMAINS=example.com",
	})
	require.Error(t, err)
	for _, want := range []string{
		"HTTP_CONNECTOR_SECRET_NOLIST needs HTTP_CONNECTOR_SECRET_NOLIST_DOMAINS",
		"HTTP_CONNECTOR_SECRET_EMPTYLIST needs HTTP_CONNECTOR_SECRET_EMPTYLIST_DOMAINS",
		"HTTP_CONNECTOR_SECRET_SWAPPED_DOMAINS must be a comma-separated list of domains",
		"HTTP_CONNECTOR_SECRET_PARTIAL_DOMAINS must be a comma-separated list of domains",
		"HTTP_CONNECTOR_SECRET_ORPHAN_DOMAINS is set, but HTTP_CONNECTOR_SECRET_ORPHAN",
		"HTTP_CONNECTOR_SECRET_UNSET_DOMAINS is set, but HTTP_CONNECTOR_SECRET_UNSET",
		"HTTP_CONNECTOR_SECRET_X_DOMAINS_DOMAINS is set, but HTTP_CONNECTOR_SECRET_X_DOMAINS",
	} {
		require.ErrorContains(t, err, want)
	}
	require.Len(t, strings.Split(err.Error(), "\n"), 7, "one line per problem: %v", err)
	require.NotContains(t, err.Error(), "sk_live_abc123")
	require.NotContains(t, err.Error(), "HTTP_CONNECTOR_SECRET_X needs", "X has its list")
}

// New reads every secret's list, and refuses to start without one.
func TestNew_SecretDomains(t *testing.T) {
	t.Setenv(AllowedDomainsEnvVar, DisableAllowlist)
	t.Setenv(SecretEnvPrefix+"NEWTEST", "v")
	_, err := New()
	require.ErrorContains(t, err, SecretEnvPrefix+"NEWTEST needs "+SecretEnvPrefix+"NEWTEST_DOMAINS")

	t.Setenv(SecretEnvPrefix+"NEWTEST_DOMAINS", "api.example.com")
	a, err := New()
	require.NoError(t, err)
	require.Equal(t, []string{"api.example.com"}, a.secretDomains[SecretEnvPrefix+"NEWTEST"])
}

func TestCheckSecretDomains(t *testing.T) {
	a := newWithWindow(testWindow)
	a.secretDomains = map[string][]string{
		"HTTP_CONNECTOR_SECRET_STRIPE": {"api.stripe.com"},
		"HTTP_CONNECTOR_SECRET_GOOGLE": {"google.com", "googleapis.com"},
		"HTTP_CONNECTOR_SECRET_LAN":    {"127.0.0.1"},
		"HTTP_CONNECTOR_SECRET_ANY":    nil,
	}
	key := func(url, envVar string) httpapi.SendRequestInput {
		return httpapi.SendRequestInput{URL: url, APIKeyEnvVar: &envVar}
	}
	header := func(url, envVar string) httpapi.SendRequestInput {
		return httpapi.SendRequestInput{URL: url, Headers: &httpapi.SendRequestInputHeaders{
			AdditionalProperties: map[string]string{"X-Api-Key": envVar},
		}}
	}

	for _, ok := range []httpapi.SendRequestInput{
		{URL: "https://anything.example/"}, // no secret
		key("https://api.stripe.com/v1/charges", "HTTP_CONNECTOR_SECRET_STRIPE"),
		key("https://API.Stripe.COM.:443/v1", "HTTP_CONNECTOR_SECRET_STRIPE"),
		key("https://eu.api.stripe.com/", "HTTP_CONNECTOR_SECRET_STRIPE"),
		header("https://www.googleapis.com/x", "HTTP_CONNECTOR_SECRET_GOOGLE"),
		header("https://google.com/x", "HTTP_CONNECTOR_SECRET_GOOGLE"),
		key("http://127.0.0.1:8080/", "HTTP_CONNECTOR_SECRET_LAN"),
		key("https://anywhere.example/", "HTTP_CONNECTOR_SECRET_ANY"),
	} {
		require.NoError(t, a.CheckSecretDomains(ok), ok.URL)
	}

	for _, tc := range []struct {
		req  httpapi.SendRequestInput
		host string
	}{
		{key("https://stripe.com/", "HTTP_CONNECTOR_SECRET_STRIPE"), "stripe.com"}, // a parent isn't in the list
		{key("https://evil.com/", "HTTP_CONNECTOR_SECRET_STRIPE"), "evil.com"},
		{key("https://api.stripe.com.evil.com/", "HTTP_CONNECTOR_SECRET_STRIPE"), "api.stripe.com.evil.com"},
		{key("https://api.stripe.com@evil.com/", "HTTP_CONNECTOR_SECRET_STRIPE"), "evil.com"}, // userinfo
		{key("https://evil.com/api.stripe.com", "HTTP_CONNECTOR_SECRET_STRIPE"), "evil.com"},
		{key("https://evilapi.stripe.com.co/", "HTTP_CONNECTOR_SECRET_STRIPE"), "evilapi.stripe.com.co"},
		{header("https://api.stripe.com/", "HTTP_CONNECTOR_SECRET_GOOGLE"), "api.stripe.com"}, // another allowed host's secret
		{key("http://localhost:8080/", "HTTP_CONNECTOR_SECRET_LAN"), "localhost"},
		{key("http://x.127.0.0.1/", "HTTP_CONNECTOR_SECRET_LAN"), "x.127.0.0.1"},
		{key("http://127.0.0.2/", "HTTP_CONNECTOR_SECRET_LAN"), "127.0.0.2"},
		{key("http://[::ffff:127.0.0.1]/", "HTTP_CONNECTOR_SECRET_LAN"), "::ffff:127.0.0.1"},
	} {
		err := a.CheckSecretDomains(tc.req)
		require.Error(t, err, tc.req.URL)
		require.Contains(t, err.Error(), `may not be sent to "`+tc.host+`"`, tc.req.URL)
	}

	// A variable without a list, such as one set after the worker started, isn't sent anywhere.
	err := a.CheckSecretDomains(key("https://api.stripe.com/", "HTTP_CONNECTOR_SECRET_LATER"))
	require.ErrorContains(t, err, "HTTP_CONNECTOR_SECRET_LATER has no HTTP_CONNECTOR_SECRET_LATER_DOMAINS")

	// Both a key and a header are checked, and each one not allowed is named.
	req := key("https://api.stripe.com/", "HTTP_CONNECTOR_SECRET_STRIPE")
	req.Headers = &httpapi.SendRequestInputHeaders{AdditionalProperties: map[string]string{
		"X-Goog-Api-Key": "HTTP_CONNECTOR_SECRET_GOOGLE",
		"X-Lan":          "HTTP_CONNECTOR_SECRET_LAN",
	}}
	err = a.CheckSecretDomains(req)
	require.ErrorContains(t, err, "HTTP_CONNECTOR_SECRET_GOOGLE may not be sent")
	require.ErrorContains(t, err, "HTTP_CONNECTOR_SECRET_LAN may not be sent")
	require.NotContains(t, err.Error(), "HTTP_CONNECTOR_SECRET_STRIPE")
}

// A list's variable can't be sent as a secret, as apiKeyEnvVar or as a header's value.
func TestValidate_ListIsNotASecret(t *testing.T) {
	t.Setenv("HTTP_CONNECTOR_SECRET_STRIPE_DOMAINS", "api.stripe.com")
	for _, req := range []httpapi.SendRequestInput{
		{URL: "https://api.stripe.com/", APIKeyEnvVar: new("HTTP_CONNECTOR_SECRET_STRIPE_DOMAINS")},
		{URL: "https://api.stripe.com/", Headers: &httpapi.SendRequestInputHeaders{
			AdditionalProperties: map[string]string{"X-Api-Key": "HTTP_CONNECTOR_SECRET_STRIPE_DOMAINS"},
		}},
	} {
		require.ErrorContains(t, Validate(req), "lists the domains a secret may be sent to; it isn't a secret")
	}
}

// SendRequest itself refuses a secret its host may not be sent, without sending anything, as a
// non-retryable InvalidRequest: the path a library caller such as nexus-connector-openapi takes.
func TestSendRequest_SecretNotSentToOtherHost(t *testing.T) {
	t.Setenv(testSecretVar, "s3cr3t")
	srv, hits := countingServer(t, http.StatusOK, "ok")
	a := newWithWindow(testWindow)
	a.secretDomains[testSecretVar] = []string{"api.example.com"}
	_, err := runSendRequest(t, a, httpapi.SendRequestInput{URL: srv.URL, APIKeyEnvVar: new(testSecretVar)})
	ae := appErr(t, err)
	require.Equal(t, ErrorTypeInvalidRequest, ae.Type())
	require.True(t, ae.NonRetryable())
	require.Contains(t, ae.Message(), testSecretVar+` may not be sent to "127.0.0.1"`)
	require.NotContains(t, ae.Message(), "s3cr3t")
	require.Zero(t, hits.Load(), "nothing sent")

	a.secretDomains[testSecretVar] = []string{"127.0.0.1"}
	_, err = runSendRequest(t, a, httpapi.SendRequestInput{URL: srv.URL, APIKeyEnvVar: new(testSecretVar)})
	require.NoError(t, err)
	require.EqualValues(t, 1, hits.Load())
}

// Header names that differ only in case are one header. A request with two is refused before
// anything is sent: otherwise the check could pass one secret while the other, chosen at random,
// is sent and left unredacted.
func TestSendRequest_HeaderNamesDifferingInCaseRefused(t *testing.T) {
	t.Setenv(SecretEnvPrefix+"PERMISSIVE", "permissive_value")
	t.Setenv(SecretEnvPrefix+"RESTRICTED", "restricted_value")
	srv, hits := countingServer(t, http.StatusOK, "ok")
	a := newWithWindow(testWindow)
	a.secretDomains = map[string][]string{
		SecretEnvPrefix + "PERMISSIVE": nil,
		SecretEnvPrefix + "RESTRICTED": {"api.stripe.com"},
	}
	req := httpapi.SendRequestInput{URL: srv.URL, Headers: &httpapi.SendRequestInputHeaders{
		AdditionalProperties: map[string]string{
			"X-Api-Key": SecretEnvPrefix + "PERMISSIVE",
			"x-api-key": SecretEnvPrefix + "RESTRICTED",
		},
	}}
	require.ErrorContains(t, Validate(req), `headers "X-Api-Key" and "x-api-key" are the same header`)
	// Map order is random, so try enough times that each order comes up.
	for range 50 {
		_, err := runSendRequest(t, a, req)
		require.Equal(t, ErrorTypeInvalidRequest, appErr(t, err).Type())
	}
	require.Zero(t, hits.Load(), "nothing sent")

	// Literal values collide the same way.
	req.Headers.AdditionalProperties = map[string]string{"X-Mode": "a", "x-mode": "b"}
	require.Error(t, Validate(req))
}

// A bad entry in a secret's list is explained without being quoted, since it can be a secret.
func TestParseDomains_SaysWhatIsWrong(t *testing.T) {
	for entry, want := range map[string]string{
		"https://api.example.com": "has a scheme",
		"api.example.com/v1":      "has a path",
		"api.example.com:443":     "has a port",
		"*.example.com":           "has a wildcard",
		"api example.com":         "has a space",
		"intranet":                "is a single label",
		"192.168":                 "isn't a complete one",
		".example.com":            "has an empty label",
		"sk_live_abc123":          "is a secret in this variable by mistake?",
		"api.example.com,":        "entry 2 isn't a domain: it is empty",
	} {
		_, err := parseDomains("HTTP_CONNECTOR_SECRET_X_DOMAINS", entry, false)
		require.ErrorContains(t, err, want, entry)
		require.NotContains(t, err.Error(), strings.TrimSuffix(entry, ","), "the entry isn't quoted")
	}
	// The allowlist holds no secrets, so it quotes the entry, with the same reason.
	_, err := parseDomains(AllowedDomainsEnvVar, "https://api.example.com", true)
	require.ErrorContains(t, err, `"https://api.example.com" isn't a domain: it has a scheme`)
}

// _DOMAIN for _DOMAINS is reported as a misspelling, not as a secret needing a list of its own.
func TestReadSecretDomains_SingularSuffixIsAMisspelling(t *testing.T) {
	_, err := readSecretDomains([]string{
		"HTTP_CONNECTOR_SECRET_GOOGLE=AIza",
		"HTTP_CONNECTOR_SECRET_GOOGLE_DOMAIN=googleapis.com",
	})
	require.ErrorContains(t, err, "HTTP_CONNECTOR_SECRET_GOOGLE_DOMAIN looks like a misspelling of HTTP_CONNECTOR_SECRET_GOOGLE_DOMAINS")
	require.NotContains(t, err.Error(), "_DOMAIN_DOMAINS")

	// A secret whose own name ends in _DOMAIN, with its list, is fine.
	_, err = readSecretDomains([]string{
		"HTTP_CONNECTOR_SECRET_CUSTOM_DOMAIN=v",
		"HTTP_CONNECTOR_SECRET_CUSTOM_DOMAIN_DOMAINS=api.example.com",
	})
	require.NoError(t, err)
}
