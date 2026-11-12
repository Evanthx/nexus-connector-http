package httpconnector

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/temporalio/nexus-connector-http/generated/httpapi"
)

func TestAllowRedirects(t *testing.T) {
	for v, want := range map[string]bool{"": false, " ": false, "false": false, "0": false, "true": true, "TRUE": true, " 1 ": true} {
		t.Setenv(AllowRedirectsEnvVar, v)
		got, err := allowRedirects()
		require.NoError(t, err, "value %q", v)
		require.Equal(t, want, got, "value %q", v)
	}
	for _, bad := range []string{"yes", "on", "WHITELISTED"} {
		t.Setenv(AllowRedirectsEnvVar, bad)
		_, err := allowRedirects()
		require.ErrorContains(t, err, AllowRedirectsEnvVar, "value %q", bad)
		_, err = New()
		require.Error(t, err, "New with value %q", bad)
	}
	t.Setenv(AllowRedirectsEnvVar, "true")
	a, err := New()
	require.NoError(t, err)
	require.True(t, a.allowRedirects)
}

// By default no redirect is followed, even to the same host: the 3xx fails as a ClientError with
// its response, and its target never gets the request.
func TestSendRequest_RedirectsOffByDefault(t *testing.T) {
	target, hits := countingServer(t, http.StatusOK, "ok")
	srv := redirectingServer(t, target.URL+"/landing")

	_, err := runSendRequest(t, newWithWindow(testWindow), httpapi.SendRequestInput{URL: srv.URL})
	ae := appErr(t, err)
	require.Equal(t, ErrorTypeClientError, ae.Type())
	require.True(t, ae.NonRetryable())
	var out httpapi.SendRequestOutput
	require.NoError(t, ae.Details(&out))
	require.Equal(t, int64(http.StatusFound), out.StatusCode)
	require.Equal(t, []string{target.URL + "/landing"}, out.Headers.AdditionalProperties["Location"])
	require.Zero(t, hits.Load())
}

// Any header, not only Authorization, can name a secret variable: the variable's value is sent,
// and an echo of it is redacted. An unset variable fails with InvalidRequest without sending.
func TestSendRequest_HeaderNamesSecret(t *testing.T) {
	t.Setenv(testSecretVar, "s3cr3t")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = io.WriteString(w, "got "+r.Header.Get("X-Api-Key"))
	}))
	defer srv.Close()

	_, err := runSendRequest(t, newWithWindow(testWindow), httpapi.SendRequestInput{
		URL:     srv.URL,
		Headers: &httpapi.SendRequestInputHeaders{AdditionalProperties: map[string]string{"x-api-key": testSecretVar}},
	})
	var out httpapi.SendRequestOutput
	require.NoError(t, appErr(t, err).Details(&out))
	require.Equal(t, "got [REDACTED]", out.Body, "the secret was sent, and its echo redacted")

	_, err = runSendRequest(t, newWithWindow(testWindow), httpapi.SendRequestInput{
		URL:     srv.URL,
		Headers: &httpapi.SendRequestInputHeaders{AdditionalProperties: map[string]string{"X-Api-Key": SecretEnvPrefix + "UNSET"}},
	})
	ae := appErr(t, err)
	require.Equal(t, ErrorTypeInvalidRequest, ae.Type())
	require.Contains(t, ae.Message(), SecretEnvPrefix+"UNSET")
}

// sent is what a server saw of one request.
type sent struct {
	path, apiKey, authorization, referer, literal string
}

// recordingServer records every request it gets on ch and answers 200.
func recordingServer(t *testing.T, ch chan<- sent) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ch <- sent{r.URL.Path, r.Header.Get("X-Api-Key"), r.Header.Get("Authorization"), r.Header.Get("Referer"), r.Header.Get("X-Literal")}
	}))
	t.Cleanup(srv.Close)
	return srv
}

// With redirects on, a redirect within the original host keeps the secret headers, and one that
// leaves it drops them, as Go drops Authorization. Referer, which would carry a query API key, is
// never sent; a header with a literal value is.
func TestSendRequest_RedirectDropsSecretsOffSite(t *testing.T) {
	t.Setenv(testSecretVar, "s3cr3t")
	t.Setenv(SecretEnvPrefix+"AUTH", "Bearer t0ken")
	got := make(chan sent, 1)
	target := recordingServer(t, got)
	onSite := target.URL + "/landing"
	// target listens on 127.0.0.1; "localhost" reaches it as a different host.
	offSite := strings.Replace(target.URL, "127.0.0.1", "localhost", 1) + "/landing"

	for _, tc := range []struct {
		name   string
		target string
		keep   bool
	}{{"same host", onSite, true}, {"other host", offSite, false}} {
		t.Run(tc.name, func(t *testing.T) {
			a := newWithWindow(testWindow)
			a.allowRedirects = true
			_, err := runSendRequest(t, a, httpapi.SendRequestInput{
				URL:          redirectingServer(t, tc.target).URL,
				APIKeyEnvVar: new(testSecretVar),
				APIKeyParam:  new("key"),
				Headers: &httpapi.SendRequestInputHeaders{AdditionalProperties: map[string]string{
					"X-Api-Key":     testSecretVar,
					"Authorization": SecretEnvPrefix + "AUTH",
					"X-Literal":     "plain",
				}},
			})
			require.NoError(t, err)
			s := <-got
			require.Equal(t, "/landing", s.path)
			require.Empty(t, s.referer)
			require.Equal(t, "plain", s.literal)
			if tc.keep {
				require.Equal(t, sent{"/landing", "s3cr3t", "Bearer t0ken", "", "plain"}, s)
			} else {
				require.Equal(t, sent{"/landing", "", "", "", "plain"}, s)
			}
		})
	}
}

// Once a redirect has left the original host, a later redirect back doesn't restore the secret
// headers, as with Go's own Authorization.
func TestSendRequest_RedirectBackDoesNotRestoreSecrets(t *testing.T) {
	t.Setenv(testSecretVar, "s3cr3t")
	got := make(chan sent, 1)
	target := recordingServer(t, got)
	bounce := redirectingServer(t, target.URL+"/landing") // on 127.0.0.1
	offSite := strings.Replace(bounce.URL, "127.0.0.1", "localhost", 1)
	a := newWithWindow(testWindow)
	a.allowRedirects = true

	_, err := runSendRequest(t, a, httpapi.SendRequestInput{
		URL:     redirectingServer(t, offSite).URL,
		Headers: &httpapi.SendRequestInputHeaders{AdditionalProperties: map[string]string{"X-Api-Key": testSecretVar}},
	})
	require.NoError(t, err)
	require.Equal(t, sent{path: "/landing"}, <-got)
}
