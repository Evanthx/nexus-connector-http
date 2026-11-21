package httpconnector

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/temporalio/nexus-connector-http/nexus-connector-shared/report"
	"github.com/temporalio/nexus-connector-http/nexus-connector-shared/temporalclient"
	"go.temporal.io/sdk/client"
)

// Report is the Outbound Connector's configuration and what it will do with it, for the log when
// it starts: its settings, defaults marked, the Temporal connection (temporal, the client options
// it connects with), then what it serves and Describe's lines. It names secret variables, never
// their values.
func (a *HTTPActivities) Report(temporal client.Options) string {
	domains := DisableAllowlist
	if a.domains != nil {
		domains = strings.Join(a.domains, ", ")
	}
	sections := []report.Section{
		{Title: "Allowed hosts", Settings: []report.Setting{{Name: AllowedDomainsEnvVar, Value: domains}}},
		{Title: "Requests", Settings: []report.Setting{
			report.Env(a.env, AllowRedirectsEnvVar, a.allowRedirects),
			report.Env(a.env, AttemptTimeoutEnvVar, a.attemptTimeout),
			report.Env(a.env, RateLimitMinutesEnvVar, a.lim.ttl),
			report.Env(a.env, MaxDurationEnvVar, a.maxTotal),
		}},
		temporalclient.Section(a.env, temporal),
	}
	behavior := append([]string{fmt.Sprintf("Serve the HTTPService Nexus service, with the operation request, and run requests on "+
		"the task queue %q: point the Outbound Connector's Nexus endpoint at it.", TaskQueue)}, a.Describe()...)
	return report.Format("Outbound Connector", sections, behavior)
}

// Describe is the Outbound Connector's setup as lines to log at startup: the allowed domains, or that the
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
