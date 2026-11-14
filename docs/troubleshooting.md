# Troubleshooting

Common errors, their causes, and fixes.

## Worker fails to start

**`invalid value for HTTP_CONNECTOR_RATE_LIMIT_MINUTES`** (or `MAX_DURATION_MINUTES`): the value must be a whole number of minutes, at least 1. Any other value (a float, a string, 0) causes the worker to exit before logging `Starting worker`.

**`HTTP_CONNECTOR_ALLOWED_DOMAINS is required`**: the variable is unset or empty. Set it to the comma-separated domains the connector may call, or to `DISABLE ALLOWLIST` to allow any host. See [Usage — allowed domains](usage.md#allowed-domains).

**`HTTP_CONNECTOR_SECRET_X needs HTTP_CONNECTOR_SECRET_X_DOMAINS`**: every secret variable needs a list of the hosts it may be sent to. Set `HTTP_CONNECTOR_SECRET_X_DOMAINS`, such as `api.example.com`. See [Usage — API keys and secrets](usage.md#api-keys-and-secrets).

**`HTTP_CONNECTOR_SECRET_X_DOMAINS is set, but HTTP_CONNECTOR_SECRET_X, the secret it is for, is unset or empty`**: the list's name doesn't match a secret. Check the spelling of both, and that the secret is exported in the worker's environment.

**`invalid domain in HTTP_CONNECTOR_ALLOWED_DOMAINS`**: each entry must be a bare domain of at least two labels, with no scheme, port, path, or wildcard — for example `api.stripe.com`, not `https://api.stripe.com`, `*.stripe.com`, or `com`. An empty entry (a trailing comma) also fails.

## Every call fails at once

**`handler error (BAD_REQUEST)`** with the worker logging `completion callbacks are not enabled for this namespace`: the connector's namespace was not started with `activity.enableCallbacks=true`. Restart the dev server with that flag and recreate the endpoint. On Temporal Cloud, confirm with Temporal support that this feature is enabled for your namespace.

**`unable to decode: Payload validation failed`**: the request JSON does not match the contract. Missing `url`, an unknown field, or a method that is not in the allowed list (see [Usage — options](usage.md#options)). A Go caller using `generated/httpapi` gets a `PayloadValidationError` with field-level details.

**`scheme must be http or https`**: the URL uses a scheme the connector does not support, such as `ftp://`.

**`HTTP_CONNECTOR_SECRET_X may not be sent to "..."`**: the request names a secret, as `apiKeyEnvVar` or a header's value, whose `_DOMAINS` list doesn't include the URL's host. Add the host to `HTTP_CONNECTOR_SECRET_X_DOMAINS` if the secret belongs there, or use the right secret for that host.

**`domain "..." is not on the connector's list of allowed domains`**: `HTTP_CONNECTOR_ALLOWED_DOMAINS` is set and the request URL's host is not on it. Add the domain to the list, or set the variable to `DISABLE ALLOWLIST`. See [Usage — allowed domains](usage.md#allowed-domains).

## Calls never make progress

A workflow that never advances past the Nexus operation usually means one of:

- **No connector worker is polling.** Check that `go run ./cmd/worker` is still running and connected to the right namespace and task queue (`nexus-connector-http`).
- **The endpoint does not exist.** The dev server keeps endpoints in memory. If you restarted it, recreate the endpoint (step 2 of [Setup](setup.md)).
- **The caller's worker is not polling.** The caller workflow is stuck, not the connector. Check that the caller's worker is running.

## Calls fail after retrying

**`Transient: ... returned status 503`** (or similar 5xx): the target server is returning errors. The connector retried until the caller's timeout expired. See [Errors — timeouts](errors.md#timeouts) to understand when retries stop.

**`RateLimited`**: the target server returned 429. The connector waited for the rate-limit window (`HTTP_CONNECTOR_RATE_LIMIT_MINUTES`, default 5 minutes) but the caller's timeout ran out before another window fit. Increase the caller's `ScheduleToCloseTimeout` or reduce the rate-limit window.

**`ClientError: ... returned status 301`** (or 302, 303, 307, 308): redirects are off by default. Request the URL in the error details' `Location` directly, or set `HTTP_CONNECTOR_ALLOW_REDIRECTS=true` on the connector worker. See [Usage — redirects](usage.md#redirects).

**`ClientError: ... redirected to "...", which is not on the connector's list of allowed domains`**: a redirect took the request to a host not in `HTTP_CONNECTOR_ALLOWED_DOMAINS`. The redirect target host must be added to the list if the redirect is expected. See [Usage — allowed domains](usage.md#allowed-domains).

**`ClientError: ... TLS certificate`**: the target server's certificate did not verify. The connector treats this as a non-retryable error. Check that the URL is correct and the server's certificate is valid.

## Response body is `[REDACTED]`

The connector redacts secrets from responses to prevent them from reaching workflow history. If the target server echoes a secret value back (some APIs do, including httpbin), every occurrence is replaced with `[REDACTED]`. This is intentional. Base64-encoded secrets are not caught. See [Usage — API keys and secrets](usage.md#api-keys-and-secrets).

## Relevant documentation

- [Temporal Nexus](https://docs.temporal.io/nexus) — Nexus concepts, endpoints, and security.
- [Temporal dynamic config](https://docs.temporal.io/references/dynamic-configuration) — `activity.enableCallbacks` and other server config values.
- [Temporal SDK envconfig](https://pkg.go.dev/go.temporal.io/sdk/contrib/envconfig) — `TEMPORAL_*` environment variables.
- [Typed clients](clients.md) — generating clients for this connector with nexgen.
