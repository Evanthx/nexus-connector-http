package httpconnector

import (
	"fmt"
	"os"
	"strings"
)

// AllowedDomainsEnvVar names the environment variable on the connector worker that limits which
// hosts requests can go to: a comma-separated list of domains, such as
// "temporal.io, api.stripe.com". A domain allows itself and its subdomains. Unset or empty, any
// host is allowed. When redirects are followed (AllowRedirectsEnvVar), every redirect is checked
// too: one to a host that isn't allowed isn't followed.
const AllowedDomainsEnvVar = "HTTP_CONNECTOR_ALLOWED_DOMAINS"

// allowedDomains reads the domains from AllowedDomainsEnvVar, lower case and without spaces. nil
// means any host is allowed. An entry that isn't a bare domain, such as one with a scheme, port,
// path or wildcard, or an empty entry, is an error.
func allowedDomains() ([]string, error) {
	v := os.Getenv(AllowedDomainsEnvVar)
	if strings.TrimSpace(v) == "" {
		return nil, nil
	}
	var domains []string
	for entry := range strings.SplitSeq(v, ",") {
		d := strings.TrimSuffix(strings.ToLower(strings.TrimSpace(entry)), ".")
		if !isDomain(d) {
			return nil, fmt.Errorf("%s must be a comma-separated list of domains, such as \"example.com, api.example.org\"; %q isn't a domain",
				AllowedDomainsEnvVar, strings.TrimSpace(entry))
		}
		domains = append(domains, d)
	}
	return domains, nil
}

// isDomain reports whether d, in lower case, is a domain name or IPv4 address: dot-separated
// labels of letters, digits and hyphens.
func isDomain(d string) bool {
	if d == "" {
		return false
	}
	for label := range strings.SplitSeq(d, ".") {
		if label == "" {
			return false
		}
		for _, c := range label {
			if (c < 'a' || c > 'z') && (c < '0' || c > '9') && c != '-' {
				return false
			}
		}
	}
	return true
}

// CheckDomain reports an error if rawURL's host isn't allowed by AllowedDomainsEnvVar: it must be
// one of the domains or a subdomain of one. With no domains set, every host is allowed. The
// sendRequest operation calls it before starting the activity, and SendRequest before sending.
func (a *HTTPActivities) CheckDomain(rawURL string) error {
	if a.domains == nil {
		return nil
	}
	u, err := parseURL(rawURL)
	if err != nil {
		return err
	}
	host := normalHost(u.Hostname())
	if a.allowedHost(host) {
		return nil
	}
	return fmt.Errorf("domain %q is not on the connector's list of allowed domains (%s)", host, AllowedDomainsEnvVar)
}

// allowedHost reports whether host, from normalHost, is one of the allowed domains or a subdomain
// of one. With no domains set, every host is allowed.
func (a *HTTPActivities) allowedHost(host string) bool {
	if a.domains == nil {
		return true
	}
	for _, d := range a.domains {
		if isDomainOrSubdomain(host, d) {
			return true
		}
	}
	return false
}

// isDomainOrSubdomain reports whether host is domain or a subdomain of it, at a dot boundary. Both
// must be in the same form, such as lower case. A host with a colon or percent sign is an IPv6
// address, which only matches exactly, as in http.Client.
func isDomainOrSubdomain(host, domain string) bool {
	if host == domain {
		return true
	}
	return !strings.ContainsAny(host, ":%") && strings.HasSuffix(host, "."+domain)
}

// normalHost is a URL's host name as the allowed domains are matched against it: lower case,
// without a port or trailing dot.
func normalHost(hostname string) string {
	return strings.TrimSuffix(strings.ToLower(hostname), ".")
}
