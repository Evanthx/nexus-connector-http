# Setup

How to install prerequisites, start Temporal, create the Nexus endpoint, and run the connector worker, locally and on Temporal Cloud.

## Prerequisites

- **Go 1.26** or later (`go version`).
- **Temporal CLI 1.9.1** or later (`temporal --version`). It includes Temporal Server 1.32.
- Network access from the connector worker to the HTTP URLs callers will request. The test workflow calls `https://httpbin.org`.

Run every command below from the repository root. Steps 1 and 3 keep running; give each its own terminal.

## 1. Start Temporal

```bash
temporal server start-dev \
  --dynamic-config-value activity.enableCallbacks=true \
  --namespace connectors \
  --namespace http-testcaller
```

This creates two namespaces: `connectors` for the connector and `http-testcaller` for the test workflow. `activity.enableCallbacks` lets a standalone activity complete a Nexus operation. Without it every call fails with `handler error (BAD_REQUEST)` and the worker logs `completion callbacks are not enabled for this namespace`.

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

Callers in any namespace reach the connector through this endpoint. On Temporal Cloud, create the endpoint in the Cloud UI or with `tcld`, and allow each caller namespace to call it.

## 3. Start the connector worker

Export the domains the connector may call and any API keys it should read, then run the worker in the same terminal:

```bash
export HTTP_CONNECTOR_ALLOWED_DOMAINS="httpbin.org"   # required; see below
export HTTP_CONNECTOR_SECRET_WEATHER=...   # any secrets callers will name
go run ./cmd/worker
```

The worker logs `Starting worker on namespace "connectors", task queue "nexus-connector-http"`. Before that line, the worker checks every `HTTP_CONNECTOR_*` variable and tries to connect to Temporal. If anything fails (such as a non-integer `HTTP_CONNECTOR_RATE_LIMIT_MINUTES` and an unreachable server), it exits with one report listing every problem, so you can fix them all at once.

## Environment variables

Set these on the connector worker (`cmd/worker`).

| Variable | Meaning |
|---|---|
| `HTTP_CONNECTOR_SECRET_*` | API keys that requests can name in `apiKeyEnvVar`, or as the whole value of any header. See [Usage — API keys and secrets](usage.md#api-keys-and-secrets). |
| `HTTP_CONNECTOR_RATE_LIMIT_MINUTES` | How long a key stays rate-limited after a 429 response, in whole minutes. Must be a whole number ≥ 1. Default 5. |
| `HTTP_CONNECTOR_MAX_DURATION_MINUTES` | The longest the connector keeps retrying, in whole minutes, when the caller's timeout is longer or unset. Must be a whole number ≥ 1. Default 1500 (25 hours). |
| `HTTP_CONNECTOR_ALLOW_REDIRECTS` | `true` to follow redirects; default `false`, which follows none. See [Usage — redirects](usage.md#redirects). |
| `HTTP_CONNECTOR_ALLOWED_DOMAINS` | Comma-separated list of domains the connector may call, e.g. `temporal.io, api.stripe.com`. Spaces are ignored. Each allows itself and its subdomains. **Required**: unset or empty, the worker exits at startup. Set to `DISABLE ALLOWLIST` to allow any host. See [Usage — allowed domains](usage.md#allowed-domains). |
| `TEMPORAL_*` | How to connect to Temporal. See below. |

Caller workers need only their own `TEMPORAL_*` settings.

## Connecting to Temporal Cloud or another server

The connector worker, the test caller's worker, and the starter all connect the same way the `temporal` CLI does, using the SDK's `envconfig` package. They read the `TEMPORAL_*` environment variables and the CLI's profile file (`temporal config set` writes it). With neither set, they connect to `localhost:7233`.

The namespace defaults to `connectors` for the connector and `http-testcaller` for the test caller. To use another, set `TEMPORAL_NAMESPACE` or the profile file's `namespace`; the environment variable wins. Any namespace works, including `default`.

All Nexus connectors default to the same `connectors` namespace and are kept apart by task queue. This connector's task queue is `nexus-connector-http`, and it can't be changed. Copies of this connector in one namespace share that queue, so they share the same work: run them with the same configuration, or put them in different namespaces.

| Variable | Meaning |
|---|---|
| `TEMPORAL_ADDRESS` | `host:port` of the Temporal frontend |
| `TEMPORAL_NAMESPACE` | Namespace |
| `TEMPORAL_API_KEY` | API key |
| `TEMPORAL_TLS_CLIENT_CERT_PATH`, `TEMPORAL_TLS_CLIENT_KEY_PATH` | mTLS client certificate and key |
| `TEMPORAL_TLS_SERVER_CA_CERT_PATH`, `TEMPORAL_TLS_SERVER_NAME` | TLS verification |
| `TEMPORAL_PROFILE` | Profile to use from the profile file |

The connector's namespace requires standalone activities with completion callbacks (`activity.enableCallbacks`). On a self-hosted server, set that dynamic config value. On Temporal Cloud, confirm with Temporal support that it is enabled for your namespace before running the connector.
