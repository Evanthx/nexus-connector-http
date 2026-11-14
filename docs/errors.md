# Errors, timeouts, and rate limiting

## Error types

Every failure is a `temporal.ApplicationError` whose `Type()` is one of these constants from the `httpconnector` package. Callers find it with `errors.As` wherever in the error chain it appears.

| Type | When | Retried |
|---|---|---|
| `InvalidRequest` | The request cannot be made: a bad URL, `timeoutSeconds` outside 1–60, the caller's operation timeout too short for one attempt plus 10 seconds, a header name or value that is not valid HTTP, two header names that differ only in case (such as `X-Api-Key` and `x-api-key`, which are one header), a bad or missing API key variable, a host not in the allowed domains list, or a secret whose `_DOMAINS` list doesn't include the host. The request is never sent. Surfaces as a `BAD_REQUEST` handler error with this error as its cause; no activity is created. | No |
| `ClientError` | The endpoint answered with a status that resending the same request will not change: any 4xx not listed below, 501, 505, 511, or a 3xx that was not followed (any redirect unless `HTTP_CONNECTOR_ALLOW_REDIRECTS` is true, a 304, a redirect from `https` to `http`, or a redirect to a host not in the allowed domains list). Also: a response body over 2 MiB, or a TLS certificate that does not verify (expired, wrong host, or unknown authority). | No |
| `Transient` | The endpoint answered 408, 409, 420, 421, 423, 424, 425, or any 5xx except 501, 505, and 511; or the request timed out or failed on the network. | With backoff: 1 s, doubling up to 30 s. |
| `RateLimited` | The endpoint answered 429, or another request with the same rate-limit key already hit 429 within the current window. The request may not have been sent if the window was still open. | Once per rate-limit window. |

Retries stop shortly before the caller's operation times out (see [Timeouts](#timeouts)). The caller then receives the last failure. `InvalidRequest` is detected when the operation starts, before any activity is created.

### Reading error details in Go

For errors that come from an HTTP response, the error's details hold a `SendRequestOutput` with the status code, headers, and body:

```go
var appErr *temporal.ApplicationError
if errors.As(err, &appErr) && appErr.Type() == httpconnector.ErrorTypeClientError {
    var resp httpapi.SendRequestOutput
    _ = appErr.Details(&resp)
    // resp.StatusCode, resp.Body, resp.Headers ...
}
```

A request that does not match the contract (unknown field, unlisted method, missing `url`) fails before the operation runs. A Go caller using `generated/httpapi` gets a `PayloadValidationError` with the list of violations when encoding the input. A caller that sends raw JSON gets a `BAD_REQUEST` handler error `unable to decode: Payload validation failed`, which does not name the failing field.

## Timeouts

| Setting | Set by | Bounds | Default |
|---|---|---|---|
| `timeoutSeconds` | the request | one HTTP attempt, 1 to 60 seconds | 30 |
| Nexus operation timeout | the caller | how long the caller waits; the connector retries until 10 seconds before it | none |
| `HTTP_CONNECTOR_MAX_DURATION_MINUTES` | the connector worker | the longest the connector keeps retrying, regardless of the caller's timeout | 1500 (25 hours) |
| `HTTP_CONNECTOR_RATE_LIMIT_MINUTES` | the connector worker | how long a 429 holds back requests with the same key | 5 minutes |

The caller's server sends the remaining operation time in the `operation-timeout` header (the smaller of the remaining `ScheduleToCloseTimeout` and `StartToCloseTimeout`). The connector retries until 10 seconds before that value, so the last error reaches the caller before it gives up. If the header is absent or the remaining time exceeds `HTTP_CONNECTOR_MAX_DURATION_MINUTES`, the maximum applies.

A caller timeout that is too short for one attempt plus 10 seconds fails immediately with `InvalidRequest`:

```
the caller's Nexus operation times out in 20s, which leaves no time for an attempt: it needs at
least timeoutSeconds (30s) plus 10s for the result to reach the caller; give the operation a
longer timeout or lower timeoutSeconds
```

Cancelling a caller workflow cancels the connector activity. Any attempt already in flight finishes, but no further attempts are made.

## Rate limiting

The connector keeps an in-memory cache of rate-limited keys. The key for a request is its URL plus the identity of the caller: the basic auth username, else the API key variable name, else the `Authorization` header's variable name, else `anon`.

- When an endpoint answers **429**, the key is cached for the rate-limit window and the request fails with `RateLimited`.
- While a key is cached, every request with the same key fails with `RateLimited` **without being sent**, and retries when the window ends.
- A **2xx** response clears the key.

The window is `HTTP_CONNECTOR_RATE_LIMIT_MINUTES` on the connector worker (a whole number of minutes, at least 1, default 5).

However many callers are waiting, one connector worker tries a rate-limited endpoint about once per window. The timing is not strict: when the window ends, all waiting requests retry at once; if the first 429 arrives before the others check, they wait another window.

The connector does not schedule a retry that would start after the caller's operation timeout, so the window and the timeout together bound the number of attempts. With the default 25-hour maximum and a 5-minute window, an endpoint that keeps answering 429 is tried about every 5 minutes for 25 hours. With a 10-minute caller timeout, it is tried twice and fails after about 5 minutes.

## Known limitations

- **The rate-limit cache is in memory.** It is lost when the worker restarts, and each worker instance has its own. A production deployment would use a shared store such as Redis with a TTL.
- **No early wake-up.** When a rate-limited key clears, waiting requests still wait until their next scheduled retry rather than waking immediately.
- **Text bodies only.** Bodies are JSON strings, so binary content would need base64 encoding.
- **Response size limit.** The whole response body is returned in the activity result. A body over 2 MiB fails with `ClientError`. A smaller body near Temporal's default payload limit (2 MB) may also fail to be returned.
- **Any URL, if the allowlist is disabled.** With `HTTP_CONNECTOR_ALLOWED_DOMAINS=DISABLE ALLOWLIST`, the connector calls any `http` or `https` URL, including addresses on its own internal network. List the allowed domains instead in environments where that matters.
