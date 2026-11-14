package httpconnector

import (
	"fmt"
	"maps"
	"slices"
	"strings"
)

// Describe is the worker's setup as lines to log at startup: the allowed domains, or that the
// allowlist is disabled, then each secret variable and the hosts it may be sent to. It names
// variables and domains, never a secret's value.
func (a *HTTPActivities) Describe() []string {
	lines := []string{allowlistLine(a.domains)}
	if len(a.secretDomains) == 0 {
		return append(lines, "Secrets: none")
	}
	for _, name := range slices.Sorted(maps.Keys(a.secretDomains)) {
		lines = append(lines, fmt.Sprintf("Secret %s may be sent to: %s", name, secretHostsLine(a.secretDomains[name], a.domains)))
	}
	return lines
}

func allowlistLine(domains []string) string {
	if domains == nil {
		return fmt.Sprintf("Calls to any domain are allowed: the allowlist is disabled (%s is %q)", AllowedDomainsEnvVar, DisableAllowlist)
	}
	return fmt.Sprintf("Only calls to these domains are allowed (%s): %s", AllowedDomainsEnvVar, strings.Join(domains, ", "))
}

func secretHostsLine(secretDomains, allowed []string) string {
	switch {
	case secretDomains != nil:
		return strings.Join(secretDomains, ", ")
	case allowed == nil:
		return fmt.Sprintf("any host (%s)", DisableAllowlist)
	default:
		return fmt.Sprintf("any allowed domain (%s)", DisableAllowlist)
	}
}
