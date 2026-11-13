package httpconnector

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/temporalio/nexus-connector-http/generated/httpapi"
)

func TestAllowedDomains(t *testing.T) {
	for _, unset := range []string{"", "  "} {
		t.Setenv(AllowedDomainsEnvVar, unset)
		_, err := allowedDomains()
		require.ErrorContains(t, err, AllowedDomainsEnvVar+" is required", "value %q", unset)
	}

	for _, disable := range []string{DisableAllowlist, " disable allowlist "} {
		t.Setenv(AllowedDomainsEnvVar, disable)
		domains, err := allowedDomains()
		require.NoError(t, err)
		require.Nil(t, domains, "value %q allows any host", disable)
	}

	t.Setenv(AllowedDomainsEnvVar, " temporal.io,Google.com ,  stripe.com.,10.0.0.5")
	domains, err := allowedDomains()
	require.NoError(t, err)
	require.Equal(t, []string{"temporal.io", "google.com", "stripe.com", "10.0.0.5"}, domains)

	for _, bad := range []string{
		"temporal.io,", "a.com,,b.com", "https://temporal.io", "temporal.io:443", "temporal.io/docs",
		"*.temporal.io", ".temporal.io", "temporal..io", "temporal io", "temporal.io," + DisableAllowlist,
	} {
		t.Setenv(AllowedDomainsEnvVar, bad)
		_, err := allowedDomains()
		require.ErrorContains(t, err, AllowedDomainsEnvVar, "value %q", bad)
	}
}

func TestNew_AllowedDomains(t *testing.T) {
	t.Setenv(AllowedDomainsEnvVar, "temporal.io")
	a, err := New()
	require.NoError(t, err)
	require.Equal(t, []string{"temporal.io"}, a.domains)

	t.Setenv(AllowedDomainsEnvVar, "https://temporal.io")
	_, err = New()
	require.Error(t, err)

	t.Setenv(AllowedDomainsEnvVar, "")
	_, err = New()
	require.ErrorContains(t, err, AllowedDomainsEnvVar+" is required")
}

func TestCheckDomain(t *testing.T) {
	a := newWithWindow(testWindow)
	require.NoError(t, a.CheckDomain("https://anything.example/"), "no list allows any host")

	a.domains = []string{"temporal.io", "127.0.0.1"}
	for _, ok := range []string{
		"https://temporal.io",
		"https://temporal.io:8443/x?y=z",
		"http://docs.temporal.io/develop/java/nexus/quickstart",
		"https://a.b.TEMPORAL.IO./",
		"http://127.0.0.1:1234/",
	} {
		require.NoError(t, a.CheckDomain(ok), ok)
	}
	for _, blocked := range []string{
		"https://eviltemporal.io/",
		"https://temporal.io.evil.com/",
		"https://evil.com/temporal.io",
		"https://evil.com/?u=https://temporal.io",
		"https://temporal.io@evil.com/",
		"http://127.0.0.2/",
	} {
		err := a.CheckDomain(blocked)
		require.ErrorContains(t, err, "is not on the connector's list of allowed domains", blocked)
	}
}

// A request to a host that isn't allowed fails with InvalidRequest without being sent, and one
// that is allowed is sent.
func TestSendRequest_DomainNotAllowed(t *testing.T) {
	srv, hits := countingServer(t, http.StatusOK, "ok")
	a := newWithWindow(testWindow)

	a.domains = []string{"example.com"}
	_, err := runSendRequest(t, a, httpapi.SendRequestInput{URL: srv.URL})
	ae := appErr(t, err)
	require.Equal(t, ErrorTypeInvalidRequest, ae.Type())
	require.True(t, ae.NonRetryable())
	require.Contains(t, ae.Message(), `domain "127.0.0.1" is not on the connector's list of allowed domains (`+AllowedDomainsEnvVar+")")
	require.Zero(t, hits.Load())

	a.domains = []string{"example.com", strings.Split(strings.TrimPrefix(srv.URL, "http://"), ":")[0]}
	resp, err := runSendRequest(t, a, httpapi.SendRequestInput{URL: srv.URL})
	require.NoError(t, err)
	require.Equal(t, int64(http.StatusOK), resp.StatusCode)
	require.Equal(t, int32(1), hits.Load())
}

// redirectingServer redirects every request to target, with a 302.
func redirectingServer(t *testing.T, target string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target, http.StatusFound)
	}))
	t.Cleanup(srv.Close)
	return srv
}

// A redirect to a host that isn't an allowed domain isn't followed: the call fails, without a
// retry, with a ClientError that names the host, and the redirect's status and Location in its
// details. The target never gets the request.
func TestSendRequest_RedirectOffAllowlistIsNotFollowed(t *testing.T) {
	target, hits := countingServer(t, http.StatusOK, "ok")
	// target listens on 127.0.0.1; "localhost" reaches it under a name that isn't allowed.
	targetURL := strings.Replace(target.URL, "127.0.0.1", "localhost", 1) + "/landing"
	srv := redirectingServer(t, targetURL)
	a := newWithWindow(testWindow)
	a.domains = []string{"127.0.0.1"}
	a.allowRedirects = true

	_, err := runSendRequest(t, a, httpapi.SendRequestInput{URL: srv.URL})
	ae := appErr(t, err)
	require.Equal(t, ErrorTypeClientError, ae.Type())
	require.True(t, ae.NonRetryable())
	require.Equal(t, srv.URL+` redirected to "localhost", which is not on the connector's list of allowed domains (`+
		AllowedDomainsEnvVar+"), so the redirect wasn't followed", ae.Message())
	var resp httpapi.SendRequestOutput
	require.NoError(t, ae.Details(&resp))
	require.Equal(t, int64(http.StatusFound), resp.StatusCode)
	require.Equal(t, []string{targetURL}, resp.Headers.AdditionalProperties["Location"])
	require.Zero(t, hits.Load(), "the redirect's target never got the request")
}

// A redirect within the allowed domains is followed, and with no allowed domains any redirect is.
func TestSendRequest_RedirectOnAllowlistIsFollowed(t *testing.T) {
	target, hits := countingServer(t, http.StatusOK, "ok")
	for _, tc := range []struct {
		name    string
		domains []string
		target  string
	}{
		{"to an allowed domain", []string{"127.0.0.1"}, target.URL + "/landing"},
		{"with no allowed domains", nil, strings.Replace(target.URL, "127.0.0.1", "localhost", 1) + "/landing"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := hits.Load()
			a := newWithWindow(testWindow)
			a.domains = tc.domains
			a.allowRedirects = true
			resp, err := runSendRequest(t, a, httpapi.SendRequestInput{URL: redirectingServer(t, tc.target).URL})
			require.NoError(t, err)
			require.Equal(t, int64(http.StatusOK), resp.StatusCode)
			require.Equal(t, before+1, hits.Load())
		})
	}
}
