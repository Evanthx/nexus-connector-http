package httpconnector

import (
	"fmt"
	"net/netip"
	"os"
	"strings"
)

// AllowedDomainsEnvVar names the environment variable on the connector worker that limits which
// hosts requests can go to: a comma-separated list of domains, such as
// "temporal.io, api.stripe.com". A domain allows itself and its subdomains. It is required: unset
// or empty, the worker doesn't start. Set to DisableAllowlist, any host is allowed. When redirects
// are followed (AllowRedirectsEnvVar), every redirect is checked too: one to a host that isn't
// allowed isn't followed.
const AllowedDomainsEnvVar = "HTTP_CONNECTOR_ALLOWED_DOMAINS"

// DisableAllowlist, as the whole value of AllowedDomainsEnvVar (case and surrounding spaces
// ignored), turns the allowlist off, so the connector calls any host.
const DisableAllowlist = "DISABLE ALLOWLIST"

// allowedDomains reads the domains from AllowedDomainsEnvVar, lower case and without spaces. nil
// means any host is allowed, which only DisableAllowlist gives. An unset or empty value, an entry
// that isn't a bare domain, such as one with a scheme, port, path or wildcard, or an empty entry,
// is an error.
func allowedDomains() ([]string, error) {
	v := strings.TrimSpace(os.Getenv(AllowedDomainsEnvVar))
	if v == "" {
		return nil, fmt.Errorf("%s is required: set it to a comma-separated list of the domains the connector may call, such as \"example.com, api.example.org\", or to %q to allow any host",
			AllowedDomainsEnvVar, DisableAllowlist)
	}
	return parseDomains(AllowedDomainsEnvVar, v, true)
}

// parseDomains reads v, the value of the variable name, as a list of domains: DisableAllowlist,
// which gives nil, or comma-separated domains, lower case and without spaces. An entry that isn't
// a bare domain (see isDomain), or an empty entry, is an error, which quotes the entry only if
// showEntries is set.
func parseDomains(name, v string, showEntries bool) ([]string, error) {
	v = strings.TrimSpace(v)
	if strings.EqualFold(v, DisableAllowlist) {
		return nil, nil
	}
	var domains []string
	for i, entry := range strings.Split(v, ",") {
		d := strings.TrimSuffix(strings.ToLower(strings.TrimSpace(entry)), ".")
		if !isDomain(d) {
			bad := fmt.Sprintf("entry %d", i+1)
			if showEntries {
				bad = fmt.Sprintf("%q", strings.TrimSpace(entry))
			}
			return nil, fmt.Errorf("%s must be a comma-separated list of domains, such as \"example.com, api.example.org\"; %s isn't a domain",
				name, bad)
		}
		domains = append(domains, d)
	}
	return domains, nil
}

// isDomain reports whether d, in lower case, is a domain name or IPv4 address: at least two
// dot-separated labels of letters, digits and hyphens. A single label, such as a top-level domain
// like "com", isn't one: it would allow every host under it. A last label of only digits makes d
// an IPv4 address, as in a URL, so it must be a complete one: "10.0" or "0.5" isn't a domain.
func isDomain(d string) bool {
	if !strings.Contains(d, ".") {
		return false
	}
	if last := d[strings.LastIndexByte(d, '.')+1:]; last != "" && strings.Trim(last, "0123456789") == "" {
		ip, err := netip.ParseAddr(d)
		return err == nil && ip.Is4()
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
// one of the domains or a subdomain of one. With the allowlist disabled, every host is allowed. The
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
// of one. With the allowlist disabled (nil domains), every host is allowed.
func (a *HTTPActivities) allowedHost(host string) bool {
	return hostIn(host, a.domains)
}

// hostIn reports whether host, from normalHost, is one of domains or a subdomain of one. nil, from
// DisableAllowlist, holds every host.
func hostIn(host string, domains []string) bool {
	if domains == nil {
		return true
	}
	for _, d := range domains {
		if isDomainOrSubdomain(host, d) {
			return true
		}
	}
	return false
}

// isDomainOrSubdomain reports whether host is domain or a subdomain of it, at a dot boundary. Both
// must be in the same form, such as lower case. An IP address, as host or as domain, only matches
// exactly: "x.10.0.0.5" isn't under "10.0.0.5", and "10.0.0.5" isn't under "0.0.5". A host with a
// colon or percent sign is an IPv6 address, possibly with a zone, which also only matches exactly.
func isDomainOrSubdomain(host, domain string) bool {
	if host == domain {
		return true
	}
	if strings.ContainsAny(host, ":%") || isIP(host) || isIP(domain) {
		return false
	}
	return strings.HasSuffix(host, "."+domain)
}

// isIP reports whether s is an IP address.
func isIP(s string) bool {
	_, err := netip.ParseAddr(s)
	return err == nil
}

// normalHost is a URL's host name as the allowed domains are matched against it: lower case,
// without a port or trailing dot.
func normalHost(hostname string) string {
	return strings.TrimSuffix(strings.ToLower(hostname), ".")
}
