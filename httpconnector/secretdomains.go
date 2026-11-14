package httpconnector

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/temporalio/nexus-connector-http/generated/httpapi"
)

// SecretDomainsSuffix ends the name of the variable that lists the hosts a secret may be sent to:
// HTTP_CONNECTOR_SECRET_STRIPE_KEY may be sent only to the hosts in
// HTTP_CONNECTOR_SECRET_STRIPE_KEY_DOMAINS, a list like AllowedDomainsEnvVar's, such as
// "api.stripe.com" or "google.com, googleapis.com", or DisableAllowlist for any allowed host.
// Every secret variable must have one, or the worker doesn't start. A variable with this suffix is
// a list, never a secret.
const SecretDomainsSuffix = "_DOMAINS"

// readSecretDomains reads, for each secret variable in environ (name=value pairs, as
// settings.Env.Environ returns them), the domains in its SecretDomainsSuffix variable. It is
// secretsFamily's check, so New runs it through Settings. A secret variable is one that
// starts with SecretEnvPrefix, doesn't end with SecretDomainsSuffix, and isn't empty. A nil list,
// from DisableAllowlist, allows any host the allowlist allows. It reports every secret without a
// valid list, and every list whose secret is unset or empty, in one error. The error names
// variables, never values, in case a secret was put in a list's variable by mistake.
func readSecretDomains(environ []string) (map[string][]string, error) {
	secretVars := make(map[string]bool)
	lists := make(map[string]string) // secret variable → its list's value
	for _, kv := range environ {
		name, v, _ := strings.Cut(kv, "=")
		if !strings.HasPrefix(name, SecretEnvPrefix) {
			continue
		}
		if secretVar, ok := strings.CutSuffix(name, SecretDomainsSuffix); ok {
			lists[secretVar] = v
		} else if v != "" {
			secretVars[name] = true
		}
	}
	domains := make(map[string][]string, len(secretVars))
	var errs []error
	for _, name := range slices.Sorted(maps.Keys(secretVars)) {
		listVar := name + SecretDomainsSuffix
		v, ok := lists[name]
		if base, typo := strings.CutSuffix(name, "_DOMAIN"); typo && !ok && secretVars[base] {
			errs = append(errs, fmt.Errorf("%s looks like a misspelling of %s: a list's name ends in %s",
				name, base+SecretDomainsSuffix, SecretDomainsSuffix))
			continue
		}
		if !ok || strings.TrimSpace(v) == "" {
			errs = append(errs, fmt.Errorf("%s needs %s: set it to the domains the secret may be sent to, such as \"api.example.com\", or to %q to allow any host the connector may call",
				name, listVar, DisableAllowlist))
			continue
		}
		d, err := parseDomains(listVar, v, false)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		domains[name] = d
	}
	for _, name := range slices.Sorted(maps.Keys(lists)) {
		if !secretVars[name] {
			errs = append(errs, fmt.Errorf("%s is set, but %s, the secret it is for, is unset or empty", name+SecretDomainsSuffix, name))
		}
	}
	return domains, errors.Join(errs...)
}

// CheckSecretDomains reports an error if req names a secret variable, as apiKeyEnvVar or as a
// header's whole value, that may not be sent to req's host: the host must be one of the domains in
// the variable's SecretDomainsSuffix list or a subdomain of one, matched as for
// AllowedDomainsEnvVar. The sendRequest operation calls it before starting the activity, and
// SendRequest before sending. The error names variables and the host, never a value.
//
// Redirects need no check of their own. A redirect is sent a secret only while it stays on the
// original host or its subdomains, all of which are in the list since the original host is: the
// connector drops secret headers once a redirect leaves (see checkRedirect), http.Client drops the
// Authorization header an apiKeyEnvVar key goes in, and a key in the query isn't carried over.
func (a *HTTPActivities) CheckSecretDomains(req httpapi.SendRequestInput) error {
	vars := requestSecretVars(req)
	if len(vars) == 0 {
		return nil
	}
	u, err := parseURL(req.URL)
	if err != nil {
		return err
	}
	host := normalHost(u.Hostname())
	var errs []error
	for _, name := range vars {
		domains, ok := a.secretDomains[name]
		switch {
		case !ok:
			errs = append(errs, fmt.Errorf("%s has no %s on the connector worker, so it isn't sent", name, name+SecretDomainsSuffix))
		case !hostIn(host, domains):
			errs = append(errs, fmt.Errorf("%s may not be sent to %q: it isn't in %s", name, host, name+SecretDomainsSuffix))
		default: // the host is on the secret's list
		}
	}
	return errors.Join(errs...)
}

// CheckSecretsAllowed reports each domain in a secret's SecretDomainsSuffix list that the allowed
// domains (AllowedDomainsEnvVar) don't hold: the connector never calls it, so the secret is never
// sent there, which usually means one of the two lists is wrong. With the allowlist disabled, or a
// secret's list DisableAllowlist, there is nothing to report. The worker starts either way; the
// setup page's Test reports it. The error names variables and domains, never a value.
func (a *HTTPActivities) CheckSecretsAllowed() error {
	if a.domains == nil {
		return nil
	}
	var errs []error
	for _, name := range slices.Sorted(maps.Keys(a.secretDomains)) {
		for _, d := range a.secretDomains[name] {
			if !hostIn(d, a.domains) {
				errs = append(errs, fmt.Errorf("%s lists %q, which isn't within %s, so the connector won't call it",
					name+SecretDomainsSuffix, d, AllowedDomainsEnvVar))
			}
		}
	}
	return errors.Join(errs...)
}

// requestSecretVars are the secret variables req names, sorted: its apiKeyEnvVar, and each
// variable a header's whole value names (see secretHeaderVars).
func requestSecretVars(req httpapi.SendRequestInput) []string {
	set := make(map[string]bool)
	if v := value(req.APIKeyEnvVar); strings.HasPrefix(v, SecretEnvPrefix) {
		set[v] = true
	}
	for _, v := range secretHeaderVars(req) {
		set[v] = true
	}
	return slices.Sorted(maps.Keys(set))
}
