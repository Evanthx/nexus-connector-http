# Setup

How to install prerequisites, start Temporal, create the Nexus endpoint, and run the Outbound Connector, locally and on Temporal Cloud.

## Prerequisites

- **Go 1.26** or later (`go version`).
- **Temporal CLI 1.9.1** or later (`temporal --version`). It includes Temporal Server 1.32.
- Network access from the Outbound Connector to the HTTP URLs callers will request. The test workflow calls `https://httpbin.org`.

Run every command below from the repository root. Steps 1 and 3 keep running; give each its own terminal.

## 1. Start Temporal

```bash
temporal server start-dev \
  --dynamic-config-value activity.enableCallbacks=true \
  --namespace connectors \
  --namespace http-testcaller
```

This creates two namespaces: `connectors` for the Outbound Connector and `http-testcaller` for the test workflow. `activity.enableCallbacks` lets a standalone activity complete a Nexus operation, which the Outbound Connector needs in its default activity mode. Without it every call fails with `handler error (BAD_REQUEST)` and the Outbound Connector logs `completion callbacks are not enabled for this namespace`. With `HTTP_CONNECTOR_MODE=sync` (see [Errors — activity or sync mode](errors.md#activity-or-sync-mode)) the Outbound Connector uses no activity and doesn't need it.

Add `--http-port 7243` if you want to call the endpoint directly over HTTP (see [Usage — call over HTTP](usage.md#call-over-http)).

The Web UI is at <http://localhost:8233>. The dev server keeps endpoints in memory, so you need to repeat step 2 each time you restart it.

## 2. Create the Nexus endpoint

```bash
temporal operator nexus endpoint create \
  --name http-endpoint \
  --target-namespace connectors \
  --target-task-queue nexus-connector-http \
  --description-file ./endpoint-description.md
```

Callers in any namespace reach the Outbound Connector through this endpoint. On Temporal Cloud, create the endpoint in the Cloud UI or with `tcld`, and allow each caller namespace to call it.

## 3. Start the Outbound Connector

Export the domains the Outbound Connector may call and any API keys it should read, then run the Outbound Connector in the same terminal. To work them out in a form and test them first, use the [setup page](#setup-page).

```bash
export HTTP_CONNECTOR_ALLOWED_DOMAINS="httpbin.org"   # required; see below
export HTTP_CONNECTOR_SECRET_WEATHER=...   # any secrets callers will name,
export HTTP_CONNECTOR_SECRET_WEATHER_DOMAINS=api.weatherapi.com   # each with the hosts it may go to
go run ./cmd/outbound
```

The Outbound Connector logs its configuration and what it will do, then `Starting worker on
namespace "connectors", task queue "nexus-connector-http"`:

```
Outbound Connector configuration (values marked "default" come from variables that aren't set):
  Allowed hosts
    HTTP_CONNECTOR_ALLOWED_DOMAINS          api.weatherapi.com, httpbin.org
  Requests
    HTTP_CONNECTOR_MODE                     activity (default)
    HTTP_CONNECTOR_ALLOW_REDIRECTS          false (default)
    HTTP_CONNECTOR_ATTEMPT_TIMEOUT_SECONDS  1m (default)
    HTTP_CONNECTOR_RATE_LIMIT_MINUTES       5m (default)
    HTTP_CONNECTOR_MAX_DURATION_MINUTES     25h (default)
  Temporal
    namespace                               connectors (default)
    address                                 localhost:7233 (default)
    payload codec                           none: payloads go to Temporal as they are, unencrypted (set TEMPORAL_CODEC_ENDPOINT to encrypt them)
What the Outbound Connector will do:
  - Serve the HTTPService Nexus service, with the operation request, and run requests on the task queue "nexus-connector-http": point the Outbound Connector's Nexus endpoint at it.
  - Run each call as a standalone activity (HTTP_CONNECTOR_MODE=activity), retried with this connector's policy until 10 seconds before the caller's Nexus operation times out, or HTTP_CONNECTOR_MAX_DURATION_MINUTES.
  - Only calls to these domains are allowed (HTTP_CONNECTOR_ALLOWED_DOMAINS): api.weatherapi.com, httpbin.org
  - Secret HTTP_CONNECTOR_SECRET_WEATHER may be sent to: api.weatherapi.com
Starting worker on namespace "connectors", task queue "nexus-connector-http"
```

With `HTTP_CONNECTOR_MODE=sync`, the second line is `Run each call within its Nexus request, with no
activity (HTTP_CONNECTOR_MODE=sync): one attempt per request, which must finish within it (about 10
seconds). ...`. With the allowlist disabled, the allowlist line is `Calls to any domain are allowed: the allowlist is
disabled (HTTP_CONNECTOR_ALLOWED_DOMAINS is "DISABLE ALLOWLIST")`. A secret whose list is `DISABLE
ALLOWLIST` may be sent to `any allowed domain`, or to `any host` when the allowlist is disabled too.
With no secrets, the last line is `Secrets: none`. Only names and domains are logged, never a
secret's value. Before the report, the Outbound Connector checks every `HTTP_CONNECTOR_*` variable
and tries to connect to Temporal. If anything fails (such as a non-integer
`HTTP_CONNECTOR_RATE_LIMIT_MINUTES` and an unreachable server), it exits with one report listing
every problem, so you can fix them all at once:

```
The Outbound Connector can't start:
Invalid Outbound Connector configuration:
  - HTTP_CONNECTOR_RATE_LIMIT_MINUTES must be a whole number of minutes, at least 1, not "1.5"
Failed to connect to Temporal:
  - failed reaching server: ... connection refused
```

## Environment variables

Set these on the Outbound Connector (`cmd/outbound`). The [setup page](#setup-page) shows the same list and tests your values. The tables below are generated from `setup/settings.go` with `make docs`: change a variable there, not here.

<!-- settings:start -->
### Allowed hosts

| Variable | Required | Default | Meaning |
|---|---|---|---|
| `HTTP_CONNECTOR_ALLOWED_DOMAINS` | yes | none | The hosts the Outbound Connector may call: a comma-separated list of domains, such as `temporal.io, api.stripe.com`; spaces are ignored. Each domain allows itself and its subdomains; an IPv4 address allows only itself. A redirect to a host not on the list isn't followed. `DISABLE ALLOWLIST` as the whole value allows any host, including ones on the Outbound Connector's own network. Unset or empty, or an entry with a scheme, port, path or `*`, a single label such as `com` or `localhost`, or a partial IPv4 address such as `10.0`, stops the Outbound Connector at startup. See [Usage — allowed domains](usage.md#allowed-domains). |

### Secrets

Secrets, such as API keys, that requests name rather than carry, each with the hosts it may be sent to. Name a secret after the API it is for, such as `HTTP_CONNECTOR_SECRET_STRIPE`. See [Usage — API keys and secrets](usage.md#api-keys-and-secrets).

| Variable | Required | Default | Meaning |
|---|---|---|---|
| `HTTP_CONNECTOR_SECRET_<NAME>` (secret) |  |  | The secret, such as an API key. Requests name the variable, never the value, in `apiKeyEnvVar` or as the whole value of any header, so the value stays out of workflow history; it is redacted from responses and errors. As a header's value it holds the scheme too, such as `Bearer sk_live_...`. |
| `HTTP_CONNECTOR_SECRET_<NAME>_DOMAINS` | for each secret |  | Where the secret may be sent: a list like `HTTP_CONNECTOR_ALLOWED_DOMAINS`, such as `api.stripe.com`, or `DISABLE ALLOWLIST` for any host the Outbound Connector may call. A request that would send the secret anywhere else fails without being sent. A secret without its list, or a list without its secret, stops the Outbound Connector at startup. Never a secret itself. |

### Requests

| Variable | Required | Default | Meaning |
|---|---|---|---|
| `HTTP_CONNECTOR_MODE` |  | activity | How each call runs. `activity`: as a standalone activity, with this connector's retry policy, rate-limit waits, attempts of up to `HTTP_CONNECTOR_ATTEMPT_TIMEOUT_SECONDS`, and the last real error reaching the caller before its timeout. `sync`: within the Nexus request, with no activity, which costs less, but each attempt must finish within the request (about 10 seconds), the caller's Temporal server retries failures with its own backoff, and a caller whose retries never succeed gets only a timeout. `HTTP_CONNECTOR_ATTEMPT_TIMEOUT_SECONDS` then only caps each request, and `HTTP_CONNECTOR_MAX_DURATION_MINUTES` doesn't apply. Any case. See [Errors — activity or sync mode](errors.md#activity-or-sync-mode). |
| `HTTP_CONNECTOR_ALLOW_REDIRECTS` |  | false | `true` to follow redirects, up to 10, except from https to http and to a host not in `HTTP_CONNECTOR_ALLOWED_DOMAINS`; headers filled from a secret are dropped once a redirect leaves the original host. `false` follows none, even on the same host: the 3xx fails as a `ClientError` with its response. Takes any value Go's `strconv.ParseBool` does. See [Usage — redirects](usage.md#redirects). |
| `HTTP_CONNECTOR_ATTEMPT_TIMEOUT_SECONDS` |  | 60 | How long one attempt may take, in whole seconds, at least 10. Each HTTP request times out 5 seconds sooner, so it fails with the Outbound Connector's own `Transient` error before the attempt times out. In activity mode, a caller's Nexus operation needs at least this plus 10 seconds; in sync mode it only caps each request. |
| `HTTP_CONNECTOR_RATE_LIMIT_MINUTES` |  | 5 | How long a rate-limit key (the URL plus who is calling) is held back after a 429 response, in whole minutes, at least 1. Until then, requests with that key fail as `RateLimited` without being sent. A `Retry-After` header on the 429 that asks for less time is used instead. |
| `HTTP_CONNECTOR_MAX_DURATION_MINUTES` |  | 1500 (25 hours) | The longest a call keeps retrying, in whole minutes, at least 1, when the caller's Nexus operation has a longer timeout or none. |

### Temporal

gRPC headers sent with every call to Temporal: `TEMPORAL_GRPC_META_<NAME>` sends header `<name>`, lower case with `-` for `_`. Shown as secrets, since a header can hold credentials.

| Variable | Required | Default | Meaning |
|---|---|---|---|
| `TEMPORAL_ADDRESS` |  | localhost:7233 | The Temporal frontend's `host:port`; for Temporal Cloud, the namespace's gRPC endpoint. The Temporal SDK's `envconfig` reads this and every variable below as the `temporal` CLI does: each one that is set overrides the profile file's value. See [Connecting to Temporal Cloud or another server](#connecting-to-temporal-cloud-or-another-server). |
| `TEMPORAL_NAMESPACE` |  | connectors | The namespace the Outbound Connector runs in. The Nexus endpoint must target it and the task queue `nexus-connector-http`. All connectors default to `connectors` and are kept apart by task queue. |
| `TEMPORAL_API_KEY` (secret) |  | none | An API key, for a Temporal Cloud namespace that uses API key authentication. Turns on TLS unless `TEMPORAL_TLS` is `false`. |
| `TEMPORAL_PROFILE` |  | default | The profile to read from the profile file. If it is set and the file has no such profile, the program can't connect; `default` may be missing. |
| `TEMPORAL_CONFIG_FILE` |  | temporalio/temporal.toml in the user's config directory | The profile file, which `temporal config set` writes. A missing file is fine: only the variables apply. The user's config directory is `~/.config` on Linux (or `$XDG_CONFIG_HOME`) and `~/Library/Application Support` on macOS. |
| `TEMPORAL_TLS` |  | on with an API key or any TLS setting, else off | `true` or `1` to connect with TLS, `false` or `0` not to. Any other value is ignored. |
| `TEMPORAL_TLS_CLIENT_CERT_PATH` |  | none | The path to the client certificate for mTLS, with `TEMPORAL_TLS_CLIENT_KEY_PATH`. |
| `TEMPORAL_TLS_CLIENT_CERT_DATA` |  | none | The client certificate for mTLS itself, in PEM, instead of a path, with `TEMPORAL_TLS_CLIENT_KEY_DATA`. |
| `TEMPORAL_TLS_CLIENT_KEY_PATH` |  | none | The path to the client certificate's private key, with `TEMPORAL_TLS_CLIENT_CERT_PATH`. |
| `TEMPORAL_TLS_CLIENT_KEY_DATA` (secret) |  | none | The client certificate's private key itself, in PEM, instead of a path, with `TEMPORAL_TLS_CLIENT_CERT_DATA`. |
| `TEMPORAL_TLS_SERVER_CA_CERT_PATH` |  | the system's | The path to the CA certificate to verify the server with, for a server whose certificate the system doesn't trust. |
| `TEMPORAL_TLS_SERVER_CA_CERT_DATA` |  | the system's | The CA certificate itself, in PEM, instead of a path. |
| `TEMPORAL_TLS_SERVER_NAME` |  | the address's host | The name to expect on the server's certificate, and to send as SNI. |
| `TEMPORAL_TLS_DISABLE_HOST_VERIFICATION` |  | false | `true` or `1` to accept a server certificate that doesn't name the host. For testing only. |
| `TEMPORAL_CLIENT_AUTHORITY` |  | the address | The `:authority` header to send, for a proxy or load balancer that routes on it. |
| `TEMPORAL_CODEC_ENDPOINT` |  | none: payloads are not encoded | A payload codec server, such as `https://codec.example.com`, to encrypt every payload the program sends to Temporal and decrypt every payload it reads, so data reaches Temporal only encrypted. The same codec server the Temporal UI uses works. The program checks it at startup and waits for it. See [Encryption](encryption.md). |
| `TEMPORAL_CODEC_AUTH` (secret) |  | none | The `Authorization` header value to send to `TEMPORAL_CODEC_ENDPOINT`, such as `Bearer <token>`. |
| `TEMPORAL_GRPC_META_<NAME>` (secret) |  |  | The header's value. |

### Network

| Variable | Required | Default | Meaning |
|---|---|---|---|
| `HTTPS_PROXY` |  | none | *Read by Go's HTTP client and gRPC from the process's environment; also as `https_proxy` when unset or empty.* The proxy for the Outbound Connector's requests to `https` URLs and for its connection to Temporal, such as `http://proxy.example:8080`, or `http://user:password@proxy.example:8080` for a proxy that asks for a password. `HTTP_CONNECTOR_ALLOWED_DOMAINS` still checks the host requested, not the proxy. Requests to `localhost` and loopback addresses never use a proxy. |
| `HTTP_PROXY` |  | none | *Read by Go's HTTP client from the process's environment; also as `http_proxy` when unset or empty.* The proxy for the Outbound Connector's requests to `http` URLs, in the same form as `HTTPS_PROXY`. The connection to Temporal doesn't use it. |
| `NO_PROXY` |  | none | *Read by Go's HTTP client and gRPC from the process's environment; also as `no_proxy` when unset or empty.* Hosts reached without the proxies, for requests and the connection to Temporal: a comma-separated list of domains (each with its subdomains; with a leading `.`, subdomains only), IP addresses or CIDR ranges, each optionally with a port, or `*` for every host. |

<!-- settings:end -->

Caller workers need only their own `TEMPORAL_*` settings.

## Setup page

To work out the variables in a form and test them before starting the Outbound Connector, run:

```bash
make configui    # or: go run ./configui --addr localhost:8090
```

and open <http://localhost:8090>. The page shows every variable above with its help and default, and the value set in the terminal you started it from. Secrets show only as set or not set; a secret you type is used for the test only, never shown again or saved. A `TEMPORAL_*` value the profile file sets is shown under its variable, with the file and profile (a secret only as set there); a value typed on the page overrides it. The proxy variables under **Network** are read by Go itself, not the Outbound Connector's code, so the page shows them read-only, as set where you started it, with a proxy's password as `****`; the Test uses them as they are. To change one, set it in your shell and start the page again. It lists each secret already set with its `_DOMAINS` list, and **Add secret and allowed domains** adds a new pair. **Test** checks:

- **Settings**: every variable, as the Outbound Connector checks them at startup, listing every problem at once.
- **Temporal**: it connects as the Outbound Connector would, then checks that the namespace exists and that a Nexus endpoint targets it and the task queue `nexus-connector-http`. If none does, it shows the command to create one.
- **Outbound Connector**: what the Outbound Connector would allow, that each secret's domains are within `HTTP_CONNECTOR_ALLOWED_DOMAINS`, and, if you give a test request URL, the result of one `GET` sent through the Outbound Connector's own request code, optionally with a secret as a Bearer token.

The page ends with the `export` lines to set, with a `<set NAME>` placeholder for each secret. It never changes the Outbound Connector's environment, and it only answers on localhost.

## Connecting to Temporal Cloud or another server

The Outbound Connector, the test caller's worker, and the starter all connect the same way the `temporal` CLI does, using the SDK's `envconfig` package. They read the `TEMPORAL_*` environment variables and the CLI's profile file (`temporal config set` writes it). With neither set, they connect to `localhost:7233`.

The namespace defaults to `connectors` for the Outbound Connector and `http-testcaller` for the test caller. To use another, set `TEMPORAL_NAMESPACE` or the profile file's `namespace`; the environment variable wins. Any namespace works, including `default`.

All Nexus connectors default to the same `connectors` namespace and are kept apart by task queue. This Outbound Connector's task queue is `nexus-connector-http`, and it can't be changed. Copies of this Outbound Connector in one namespace share that queue, so they share the same work: run them with the same configuration, or put them in different namespaces.

Every variable `envconfig` reads is in [Temporal](#temporal) above. A variable that is set overrides the profile file's value. Behind a proxy, the connection to Temporal uses `HTTPS_PROXY` and `NO_PROXY` (see [Network](#network)).

In activity mode (the default), the Outbound Connector's namespace requires standalone activities with completion callbacks (`activity.enableCallbacks`). On a self-hosted server, set that dynamic config value. On Temporal Cloud, confirm with Temporal support that it is enabled for your namespace before running the Outbound Connector. Sync mode (`HTTP_CONNECTOR_MODE=sync`) doesn't need it.
