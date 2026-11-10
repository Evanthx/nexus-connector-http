package httpconnector

import (
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/temporalio/nexus-connector-http/generated/httpapi"
)

func TestAllowedDomains(t *testing.T) {
	for _, unset := range []string{"", "  "} {
		t.Setenv(AllowedDomainsEnvVar, unset)
		domains, err := allowedDomains()
		require.NoError(t, err)
		require.Nil(t, domains, "value %q allows any host", unset)
	}

	t.Setenv(AllowedDomainsEnvVar, " temporal.io,Google.com ,  stripe.com.,10.0.0.5")
	domains, err := allowedDomains()
	require.NoError(t, err)
	require.Equal(t, []string{"temporal.io", "google.com", "stripe.com", "10.0.0.5"}, domains)

	for _, bad := range []string{
		"temporal.io,", "a.com,,b.com", "https://temporal.io", "temporal.io:443", "temporal.io/docs",
		"*.temporal.io", ".temporal.io", "temporal..io", "temporal io",
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
