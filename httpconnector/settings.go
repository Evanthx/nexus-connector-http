package httpconnector

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/temporalio/nexus-connector-http/settings"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/contrib/envconfig"
)

// This file lists every environment variable the connector reads, in the order the setup page
// (configui) and docs/setup.md show them. New loads the connector's configuration through
// Settings, the setup page shows it, and `make docs` generates docs/setup.md's table from it. Add
// a new variable here only, with its help text and check, then run `make docs`.

// Settings is the connector's process and every environment variable it reads.
var Settings = &settings.Program{
	ID:      "connector",
	Name:    "HTTP connector",
	Command: "go run ./cmd/worker",
	Sections: []*settings.Section{
		{Title: "Allowed hosts", Variables: []*settings.Variable{allowedDomainsSetting.Variable}},
		{Title: "Secrets", Families: []*settings.Family{secretsFamily.Family}},
		{Title: "Requests", Variables: []*settings.Variable{
			allowRedirectsSetting.Variable, rateLimitSetting.Variable, maxDurationSetting.Variable,
		}},
		settings.Temporal{
			Namespace: defaultNamespace,
			NamespaceHelp: "The namespace the connector runs in. The Nexus endpoint must target it and the task queue " +
				"`" + TaskQueue + "`. All connectors default to `" + defaultNamespace + "` and are kept apart by task queue.",
			Docs: "See [Connecting to Temporal Cloud or another server](#connecting-to-temporal-cloud-or-another-server).",
		}.Section(),
		{Title: "Network", Variables: proxyVariables},
	},
}

var allowedDomainsSetting = settings.Define(settings.Variable{
	Name:     AllowedDomainsEnvVar,
	Required: "yes",
	Default:  "none",
	Help: "The hosts the connector may call: a comma-separated list of domains, such as `temporal.io, api.stripe.com`; " +
		"spaces are ignored. Each domain allows itself and its subdomains; an IPv4 address allows only itself. " +
		"A redirect to a host not on the list isn't followed. `" + DisableAllowlist + "` as the whole value allows " +
		"any host, including ones on the connector's own network. Unset or empty, or an entry with a scheme, port, " +
		"path or `*`, a single label such as `com` or `localhost`, or a partial IPv4 address such as `10.0`, " +
		"stops the worker at startup.",
	Docs: "See [Usage — allowed domains](usage.md#allowed-domains).",
}, parseAllowedDomains)

var secretsFamily = settings.DefineFamily(settings.Family{
	Prefix: SecretEnvPrefix,
	Members: []settings.Member{
		{
			Label:  "Secret",
			Secret: true,
			Help: "The secret, such as an API key. Requests name the variable, never the value, in `apiKeyEnvVar` " +
				"or as the whole value of any header, so the value stays out of workflow history; it is redacted " +
				"from responses and errors. As a header's value it holds the scheme too, such as `Bearer sk_live_...`.",
		},
		{
			Suffix:      SecretDomainsSuffix,
			Label:       "Allowed domains",
			Required:    "for each secret",
			Placeholder: "api.example.com",
			Help: "Where the secret may be sent: a list like `" + AllowedDomainsEnvVar + "`, such as `api.stripe.com`, " +
				"or `" + DisableAllowlist + "` for any host the connector may call. A request that would send the " +
				"secret anywhere else fails without being sent. A secret without its list, or a list without its " +
				"secret, stops the worker at startup. Never a secret itself.",
		},
	},
	Help: "Secrets, such as API keys, that requests name rather than carry, each with the hosts it may be sent to. " +
		"Name a secret after the API it is for, such as `" + SecretEnvPrefix + "STRIPE`.",
	AddButton: "Add secret and allowed domains",
	AddHelp: "Name the secret after the API it is for, such as STRIPE, in upper-case letters, digits and underscores. " +
		"This adds " + SecretEnvPrefix + "<NAME>, which holds the secret, and " + SecretEnvPrefix + "<NAME>" +
		SecretDomainsSuffix + ", the hosts it may be sent to. Requests then name " + SecretEnvPrefix +
		"<NAME> in apiKeyEnvVar, or as a header's whole value.",
	Example: "STRIPE",
	Docs:    "See [Usage — API keys and secrets](usage.md#api-keys-and-secrets).",
}, func(env settings.Env) (map[string][]string, error) { return readSecretDomains(env.Environ()) })

var allowRedirectsSetting = settings.Define(settings.Variable{
	Name:    AllowRedirectsEnvVar,
	Default: "false",
	Help: "`true` to follow redirects, up to 10, except from https to http and to a host not in `" +
		AllowedDomainsEnvVar + "`; headers filled from a secret are dropped once a redirect leaves the original " +
		"host. `false` follows none, even on the same host: the 3xx fails as a `ClientError` with its response. " +
		"Takes any value Go's `strconv.ParseBool` does.",
	Docs: "See [Usage — redirects](usage.md#redirects).",
}, parseAllowRedirects)

var rateLimitSetting = settings.Define(settings.Variable{
	Name:    RateLimitMinutesEnvVar,
	Default: "5",
	Help: "How long a rate-limit key (the URL plus who is calling) is held back after a 429 response, in whole " +
		"minutes, at least 1. Until then, requests with that key fail as `RateLimited` without being sent.",
}, minutes(RateLimitMinutesEnvVar, defaultRateLimitWindow))

var maxDurationSetting = settings.Define(settings.Variable{
	Name:    MaxDurationEnvVar,
	Default: "1500 (25 hours)",
	Help: "The longest a call keeps retrying, in whole minutes, at least 1, when the caller's Nexus operation " +
		"has a longer timeout or none.",
}, minutes(MaxDurationEnvVar, defaultMaxDuration))

// proxyVariables are the proxy settings Go's net/http reads (http.ProxyFromEnvironment, which the
// connector's requests use through http.DefaultTransport) and gRPC reads for the connection to
// Temporal (its delegating resolver calls http.ProxyFromEnvironment for an https URL of the
// address, so it uses HTTPS_PROXY and NO_PROXY only).
var proxyVariables = []*settings.Variable{
	{
		Name: "HTTPS_PROXY", Default: "none", External: "Go's HTTP client and gRPC", Aliases: []string{"https_proxy"},
		Mask: settings.MaskURLPassword,
		Help: "The proxy for the connector's requests to `https` URLs and for its connection to Temporal, such as " +
			"`http://proxy.example:8080`, or `http://user:password@proxy.example:8080` for a proxy that asks for a " +
			"password. `" + AllowedDomainsEnvVar + "` still checks the host requested, not the proxy. Requests to " +
			"`localhost` and loopback addresses never use a proxy.",
	},
	{
		Name: "HTTP_PROXY", Default: "none", External: "Go's HTTP client", Aliases: []string{"http_proxy"},
		Mask: settings.MaskURLPassword,
		Help: "The proxy for the connector's requests to `http` URLs, in the same form as `HTTPS_PROXY`. The " +
			"connection to Temporal doesn't use it.",
	},
	{
		Name: "NO_PROXY", Default: "none", External: "Go's HTTP client and gRPC", Aliases: []string{"no_proxy"},
		Help: "Hosts reached without the proxies, for requests and the connection to Temporal: a comma-separated " +
			"list of domains (each with its subdomains; with a leading `.`, subdomains only), IP addresses or CIDR " +
			"ranges, each optionally with a port, or `*` for every host.",
	},
}

// defaultNamespace is the connector's namespace when neither TEMPORAL_NAMESPACE nor the profile
// file sets one.
const defaultNamespace = "connectors"

// ClientOptions loads the Temporal client options from env with the SDK's envconfig, as the
// temporal CLI does (TEMPORAL_* variables and the CLI's profile file), with the namespace
// defaulting to connectors.
func ClientOptions(env settings.Env) (client.Options, error) {
	opts, err := envconfig.LoadClientOptions(envconfig.LoadClientOptionsRequest{EnvLookup: env})
	if err != nil {
		return client.Options{}, err
	}
	if opts.Namespace == "" {
		opts.Namespace = defaultNamespace
	}
	return opts, nil
}

// parseAllowedDomains reads AllowedDomainsEnvVar's value, v: the domains, lower case and without
// spaces, or nil for DisableAllowlist, which allows any host. Unset or empty, an entry that isn't a
// bare domain, such as one with a scheme, port, path or wildcard, or an empty entry, is an error.
func parseAllowedDomains(v string) ([]string, error) {
	v = strings.TrimSpace(v)
	if v == "" {
		return nil, fmt.Errorf("%s is required: set it to a comma-separated list of the domains the connector may call, such as \"example.com, api.example.org\", or to %q to allow any host",
			AllowedDomainsEnvVar, DisableAllowlist)
	}
	return parseDomains(AllowedDomainsEnvVar, v, true)
}

// parseAllowRedirects reads AllowRedirectsEnvVar's value, v. Unset or empty is false; a value that
// isn't a boolean is an error.
func parseAllowRedirects(v string) (bool, error) {
	v = strings.TrimSpace(v)
	if v == "" {
		return false, nil
	}
	allow, err := strconv.ParseBool(v)
	if err != nil {
		return false, fmt.Errorf("%s must be true or false; got %q", AllowRedirectsEnvVar, v)
	}
	return allow, nil
}

// minutes reads the variable name's value as a whole number of minutes, at least 1, or def if it
// is unset or empty.
func minutes(name string, def time.Duration) func(v string) (time.Duration, error) {
	return func(v string) (time.Duration, error) {
		if v == "" {
			return def, nil
		}
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			return 0, fmt.Errorf("%s must be a whole number of minutes, at least 1; got %q", name, v)
		}
		return time.Duration(n) * time.Minute, nil
	}
}
