package httpconnector

import (
	"fmt"
	"net/http"
)

// AllowRedirectsEnvVar names the environment variable on the connector worker that decides whether
// requests follow redirects: true or false (any value strconv.ParseBool accepts). Unset or empty,
// they don't, even within the same host, and a 3xx fails as a ClientError with its response, so
// the caller can see its Location. When true, see checkRedirect.
const AllowRedirectsEnvVar = "HTTP_CONNECTOR_ALLOW_REDIRECTS"

// checkRedirect returns the http.Client CheckRedirect policy for a request whose secretHeaders,
// from secretHeaderVars, were filled from the connector's secret variables.
//
// Unless AllowRedirectsEnvVar is true, no redirect is followed: SendRequest gets the 3xx itself as
// the response, which fails as a ClientError. Otherwise it is http.Client's default policy (at
// most 10 redirects, enforced here because setting CheckRedirect replaces it), except:
//
//   - A redirect from https to http isn't followed: http.Client alone would follow it and send
//     the Authorization header in plaintext. The 3xx fails as a ClientError, as above.
//   - A redirect to a host that isn't an allowed domain (AllowedDomainsEnvVar) isn't followed, so
//     an allowed site can't send the request elsewhere. It fails with a redirectNotAllowedError,
//     which SendRequest turns into a ClientError naming the host.
//   - Referer is never sent: http.Client sets it to the previous URL, query included, which would
//     carry an apiKeyParam key to the next host.
//   - Once a redirect leaves the original host and its subdomains, the secret headers are dropped
//     too, as http.Client itself drops Authorization and Cookie (see leftSite). Headers the caller
//     set to a literal value are sent on, as http.Client does.
func (a *HTTPActivities) checkRedirect(secretHeaders map[string]string) func(*http.Request, []*http.Request) error {
	return func(req *http.Request, via []*http.Request) error {
		if !a.allowRedirects {
			return http.ErrUseLastResponse
		}
		if len(via) >= maxRedirects {
			return fmt.Errorf("stopped after %d redirects", maxRedirects)
		}
		if via[len(via)-1].URL.Scheme == "https" && req.URL.Scheme != "https" {
			return http.ErrUseLastResponse
		}
		if host := normalHost(req.URL.Hostname()); !a.allowedHost(host) {
			return &redirectNotAllowedError{host: host}
		}
		req.Header.Del("Referer")
		if leftSite(req, via) {
			for name := range secretHeaders {
				req.Header.Del(name)
			}
		}
		return nil
	}
}

// leftSite reports whether any request in the chain so far, ending in req, went to a host other
// than the original request's host or a subdomain of it. That is when http.Client stops sending
// Authorization and Cookie, and as there, a later redirect back doesn't restore them. Hosts are
// compared as they are in the URLs, so any host http.Client counts as different is different here
// too.
func leftSite(req *http.Request, via []*http.Request) bool {
	original := via[0].URL.Hostname()
	if !isDomainOrSubdomain(req.URL.Hostname(), original) {
		return true
	}
	for _, r := range via[1:] {
		if !isDomainOrSubdomain(r.URL.Hostname(), original) {
			return true
		}
	}
	return false
}

// redirectNotAllowedError is checkRedirect's error for a redirect to a host that isn't an allowed
// domain. SendRequest turns it into a ClientError.
type redirectNotAllowedError struct {
	host string
}

func (e *redirectNotAllowedError) Error() string {
	return fmt.Sprintf("redirected to %q, which is not on the connector's list of allowed domains (%s), so the redirect wasn't followed",
		e.host, AllowedDomainsEnvVar)
}
