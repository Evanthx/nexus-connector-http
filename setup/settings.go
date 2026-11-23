// Package setup lists every environment variable the Outbound Connector reads, as one
// settings.Program, Outbound.
//
// The Outbound Connector is a main package, which nothing can import, so its variables are declared
// here for it, the setup page (configui) and the docs (configui/gendocs) to share. httpconnector
// never imports this package: a program loads Outbound and passes Config's result to
// httpconnector.New. Add a new variable here only, with its help text and check, read it in Config,
// then run `make docs`.
package setup

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/temporalio/nexus-connector-http/httpconnector"
	"github.com/temporalio/nexus-connector-http/nexus-connector-shared/settings"
	"github.com/temporalio/nexus-connector-http/nexus-connector-shared/temporalclient"
)

// Outbound is the Outbound Connector's process and every environment variable it reads, in the
// order the setup page and docs/setup.md show them.
var Outbound = &settings.Program{
	ID:      "outbound",
	Name:    "Outbound Connector",
	Command: "go run ./cmd/outbound",
	Sections: []*settings.Section{
		{Title: "Allowed hosts", Variables: []*settings.Variable{allowedDomainsSetting.Variable}},
		{Title: "Secrets", Families: []*settings.Family{secretsFamily.Family}},
		{Title: "Requests", Variables: []*settings.Variable{
			allowRedirectsSetting.Variable, attemptTimeoutSetting.Variable, rateLimitSetting.Variable,
			maxDurationSetting.Variable,
		}},
		temporalSection,
		networkSection,
	},
}

// Config is the Outbound Connector's configuration in vals, which Outbound.Load made.
func Config(vals *settings.Values) httpconnector.Config {
	return httpconnector.Config{
		AllowedDomains:  allowedDomainsSetting.From(vals),
		AllowRedirects:  allowRedirectsSetting.From(vals),
		AttemptTimeout:  attemptTimeoutSetting.From(vals),
		RateLimitWindow: rateLimitSetting.From(vals),
		MaxDuration:     maxDurationSetting.From(vals),
		SecretDomains:   secretsFamily.From(vals),
	}
}

// temporalSection is the Temporal variables. The Temporal SDK's envconfig reads them, so they have
// no check here.
var temporalSection = settings.Temporal{
	Namespace: temporalclient.DefaultNamespace,
	NamespaceHelp: "The namespace the Outbound Connector runs in. The Nexus endpoint must target it and the task queue " +
		"`" + httpconnector.TaskQueue + "`. All connectors default to `" + temporalclient.DefaultNamespace + "` and are kept apart by task queue.",
	Docs: "See [Connecting to Temporal Cloud or another server](#connecting-to-temporal-cloud-or-another-server).",
}.Section()

var allowedDomainsSetting = settings.Define(settings.Variable{
	Name:     httpconnector.AllowedDomainsEnvVar,
	Required: "yes",
	Default:  "none",
	Help: "The hosts the Outbound Connector may call: a comma-separated list of domains, such as `temporal.io, api.stripe.com`; " +
		"spaces are ignored. Each domain allows itself and its subdomains; an IPv4 address allows only itself. " +
		"A redirect to a host not on the list isn't followed. `" + httpconnector.DisableAllowlist + "` as the whole value allows " +
		"any host, including ones on the Outbound Connector's own network. Unset or empty, or an entry with a scheme, port, " +
		"path or `*`, a single label such as `com` or `localhost`, or a partial IPv4 address such as `10.0`, " +
		"stops the Outbound Connector at startup.",
	Docs: "See [Usage — allowed domains](usage.md#allowed-domains).",
}, parseAllowedDomains)

var secretsFamily = settings.DefineFamily(settings.Family{
	Prefix: httpconnector.SecretEnvPrefix,
	Members: []settings.Member{
		{
			Label:  "Secret",
			Secret: true,
			Help: "The secret, such as an API key. Requests name the variable, never the value, in `apiKeyEnvVar` " +
				"or as the whole value of any header, so the value stays out of workflow history; it is redacted " +
				"from responses and errors. As a header's value it holds the scheme too, such as `Bearer sk_live_...`.",
		},
		{
			Suffix:      httpconnector.SecretDomainsSuffix,
			Label:       "Allowed domains",
			Required:    "for each secret",
			Placeholder: "api.example.com",
			Help: "Where the secret may be sent: a list like `" + httpconnector.AllowedDomainsEnvVar + "`, such as `api.stripe.com`, " +
				"or `" + httpconnector.DisableAllowlist + "` for any host the Outbound Connector may call. A request that would send the " +
				"secret anywhere else fails without being sent. A secret without its list, or a list without its " +
				"secret, stops the Outbound Connector at startup. Never a secret itself.",
		},
	},
	Help: "Secrets, such as API keys, that requests name rather than carry, each with the hosts it may be sent to. " +
		"Name a secret after the API it is for, such as `" + httpconnector.SecretEnvPrefix + "STRIPE`.",
	AddButton: "Add secret and allowed domains",
	AddHelp: "Name the secret after the API it is for, such as STRIPE, in upper-case letters, digits and underscores. " +
		"This adds " + httpconnector.SecretEnvPrefix + "<NAME>, which holds the secret, and " + httpconnector.SecretEnvPrefix + "<NAME>" +
		httpconnector.SecretDomainsSuffix + ", the hosts it may be sent to. Requests then name " + httpconnector.SecretEnvPrefix +
		"<NAME> in apiKeyEnvVar, or as a header's whole value.",
	Example: "STRIPE",
	Docs:    "See [Usage — API keys and secrets](usage.md#api-keys-and-secrets).",
}, func(env settings.Env) (map[string][]string, error) {
	return httpconnector.ReadSecretDomains(env.Environ())
})

var allowRedirectsSetting = settings.Define(settings.Variable{
	Name:    httpconnector.AllowRedirectsEnvVar,
	Default: "false",
	Help: "`true` to follow redirects, up to 10, except from https to http and to a host not in `" +
		httpconnector.AllowedDomainsEnvVar + "`; headers filled from a secret are dropped once a redirect leaves the original " +
		"host. `false` follows none, even on the same host: the 3xx fails as a `ClientError` with its response. " +
		"Takes any value Go's `strconv.ParseBool` does.",
	Docs: "See [Usage — redirects](usage.md#redirects).",
}, func(v string) (bool, error) {
	return settings.Bool(httpconnector.AllowRedirectsEnvVar)(strings.TrimSpace(v))
})

var attemptTimeoutSetting = settings.Define(settings.Variable{
	Name:    httpconnector.AttemptTimeoutEnvVar,
	Default: "60",
	Help: "How long one attempt may take, in whole seconds, at least 10. Each HTTP request times out 5 seconds " +
		"sooner, so it fails with the Outbound Connector's own `Transient` error before the attempt times out. A caller's " +
		"Nexus operation needs at least this plus 10 seconds.",
}, parseAttemptTimeout)

var rateLimitSetting = settings.Define(settings.Variable{
	Name:    httpconnector.RateLimitMinutesEnvVar,
	Default: "5",
	Help: "How long a rate-limit key (the URL plus who is calling) is held back after a 429 response, in whole " +
		"minutes, at least 1. Until then, requests with that key fail as `RateLimited` without being sent. A " +
		"`Retry-After` header on the 429 that asks for less time is used instead.",
}, settings.Minutes(httpconnector.RateLimitMinutesEnvVar, defaultRateLimitWindow))

// defaultRateLimitWindow is the window when RateLimitMinutesEnvVar is unset or empty. A live entry
// blocks every caller with that key for the window, so the endpoint is probed about once per
// window: the waiting requests all retry when it ends, and if the first one's 429 comes back before
// the others check, they wait another window. Requests that check at the same moment all go out.
const defaultRateLimitWindow = 5 * time.Minute

var maxDurationSetting = settings.Define(settings.Variable{
	Name:    httpconnector.MaxDurationEnvVar,
	Default: "1500 (25 hours)",
	Help: "The longest a call keeps retrying, in whole minutes, at least 1, when the caller's Nexus operation " +
		"has a longer timeout or none.",
}, settings.Minutes(httpconnector.MaxDurationEnvVar, 25*time.Hour))

// networkSection is the proxy settings Go's net/http reads (http.ProxyFromEnvironment, which the
// Outbound Connector's requests use through http.DefaultTransport) and gRPC reads for the connection to
// Temporal (its delegating resolver calls http.ProxyFromEnvironment for an https URL of the
// address, so it uses HTTPS_PROXY and NO_PROXY only).
var networkSection = &settings.Section{Title: "Network", Variables: []*settings.Variable{
	{
		Name: "HTTPS_PROXY", Default: "none", External: "Go's HTTP client and gRPC", Aliases: []string{"https_proxy"},
		Mask: settings.MaskURLPassword,
		Help: "The proxy for the Outbound Connector's requests to `https` URLs and for its connection to Temporal, such as " +
			"`http://proxy.example:8080`, or `http://user:password@proxy.example:8080` for a proxy that asks for a " +
			"password. `" + httpconnector.AllowedDomainsEnvVar + "` still checks the host requested, not the proxy. Requests to " +
			"`localhost` and loopback addresses never use a proxy.",
	},
	{
		Name: "HTTP_PROXY", Default: "none", External: "Go's HTTP client", Aliases: []string{"http_proxy"},
		Mask: settings.MaskURLPassword,
		Help: "The proxy for the Outbound Connector's requests to `http` URLs, in the same form as `HTTPS_PROXY`. The " +
			"connection to Temporal doesn't use it.",
	},
	{
		Name: "NO_PROXY", Default: "none", External: "Go's HTTP client and gRPC", Aliases: []string{"no_proxy"},
		Help: "Hosts reached without the proxies, for requests and the connection to Temporal: a comma-separated " +
			"list of domains (each with its subdomains; with a leading `.`, subdomains only), IP addresses or CIDR " +
			"ranges, each optionally with a port, or `*` for every host.",
	},
}}

// parseAllowedDomains reads AllowedDomainsEnvVar's value, v: the domains, lower case and without
// spaces, or nil for DisableAllowlist, which allows any host. Unset, empty or only spaces is an
// error, and so is an entry that isn't a bare domain (see httpconnector.ParseDomains).
func parseAllowedDomains(v string) ([]string, error) {
	v = strings.TrimSpace(v)
	if v == "" {
		return nil, settings.Required(httpconnector.AllowedDomainsEnvVar, fmt.Sprintf(
			"a comma-separated list of the domains the Outbound Connector may call, such as \"example.com, api.example.org\", or %q to allow any host",
			httpconnector.DisableAllowlist))
	}
	return httpconnector.ParseDomains(httpconnector.AllowedDomainsEnvVar, v, true)
}

// parseAttemptTimeout reads AttemptTimeoutEnvVar's value, v, as whole seconds, at least
// httpconnector.MinAttemptTimeout, or httpconnector.DefaultAttemptTimeout if it is unset or empty.
func parseAttemptTimeout(v string) (time.Duration, error) {
	if v == "" {
		return httpconnector.DefaultAttemptTimeout, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil || time.Duration(n)*time.Second < httpconnector.MinAttemptTimeout {
		return 0, fmt.Errorf("%s must be a whole number of seconds, at least %d; got %q",
			httpconnector.AttemptTimeoutEnvVar, int(httpconnector.MinAttemptTimeout/time.Second), v)
	}
	return time.Duration(n) * time.Second, nil
}
