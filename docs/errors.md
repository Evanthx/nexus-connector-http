# Errors, timeouts, and rate limiting

The Outbound Connector runs each call in one of two modes, set by `HTTP_CONNECTOR_MODE`: `activity` (the default) or `sync`. The error types are the same in both, but who retries and what the caller gets when retries never succeed differ. Everything below describes activity mode unless it says otherwise; see [Activity or sync mode](#activity-or-sync-mode) for sync mode.

## Error types

Every failure is a `temporal.ApplicationError` whose `Type()` is one of these constants from the `httpconnector` package. Callers find it with `errors.As` wherever in the error chain it appears.

| Type | When | Retried (activity mode) |
|---|---|---|
| `InvalidRequest` | The request cannot be made: a bad URL, the caller's operation timeout too short for one attempt plus 10 seconds, a header name or value that is not valid HTTP, two header names that differ only in case (such as `X-Api-Key` and `x-api-key`, which are one header), a bad or missing API key variable, a host not in the allowed domains list, or a secret whose `_DOMAINS` list doesn't include the host. The request is never sent. Surfaces as a `BAD_REQUEST` handler error with this error as its cause; no activity is created. | No |
| `ClientError` | The endpoint answered with a status that resending the same request will not change: any 4xx not listed below, 501, 505, 511, or a 3xx that was not followed (any redirect unless `HTTP_CONNECTOR_ALLOW_REDIRECTS` is true, a 304, a redirect from `https` to `http`, or a redirect to a host not in the allowed domains list). Also: a response body over 2 MiB, or a TLS certificate that does not verify (expired, wrong host, or unknown authority). | No |
| `Transient` | The endpoint answered 408, 409, 420, 421, 423, 424, 425, or any 5xx except 501, 505, and 511; or the request timed out or failed on the network. | With backoff: 5 s, then three times longer each time, at most 5 minutes apart (5 s, 15 s, 45 s, 2 min 15 s, then every 5 min). |
| `RateLimited` | The endpoint answered 429, or another request with the same rate-limit key already hit 429 within the current window. The request may not have been sent if the window was still open. | Once per rate-limit window, or sooner if the 429's `Retry-After` asks for less. |

In activity mode, retries stop shortly before the caller's operation times out (see [Timeouts](#timeouts)). The caller then receives the last failure. In sync mode, the caller's Temporal server retries instead, and a caller whose retries never succeed gets a timeout; see [Activity or sync mode](#activity-or-sync-mode). `InvalidRequest` is detected when the operation starts, before any activity is created.

### Reading error details in Go

For errors that come from an HTTP response, the error's details hold a `RequestOutput` with the status code, headers, and body:

```go
var appErr *temporal.ApplicationError
if errors.As(err, &appErr) && appErr.Type() == httpconnector.ErrorTypeClientError {
    var resp httpapi.RequestOutput
    _ = appErr.Details(&resp)
    // resp.StatusCode, resp.Body, resp.Headers ...
}
```

A request that does not match the contract (unknown field, unlisted method, missing `url`) fails before the operation runs. A Go caller using `generated/httpapi` gets a `PayloadValidationError` with the list of violations when encoding the input. A caller that sends raw JSON gets a `BAD_REQUEST` handler error `unable to decode: Payload validation failed`, which does not name the failing field.

## Timeouts

This section describes activity mode. In sync mode, only `HTTP_CONNECTOR_ATTEMPT_TIMEOUT_SECONDS` (as a cap on each request) and `HTTP_CONNECTOR_RATE_LIMIT_MINUTES` apply; see [Activity or sync mode](#activity-or-sync-mode).

| Setting | Set by | Bounds | Default |
|---|---|---|---|
| `HTTP_CONNECTOR_ATTEMPT_TIMEOUT_SECONDS` | the Outbound Connector | one attempt, at least 10 seconds; each HTTP request times out 5 seconds sooner | 60 |
| Nexus operation timeout | the caller | how long the caller waits; the Outbound Connector retries until 10 seconds before it | none |
| `HTTP_CONNECTOR_MAX_DURATION_MINUTES` | the Outbound Connector | the longest the Outbound Connector keeps retrying, regardless of the caller's timeout | 1500 (25 hours) |
| `HTTP_CONNECTOR_RATE_LIMIT_MINUTES` | the Outbound Connector | how long a 429 holds back requests with the same key | 5 minutes |

The caller's server sends the remaining operation time in the `operation-timeout` header (the smaller of the remaining `ScheduleToCloseTimeout` and `StartToCloseTimeout`). In activity mode, the Outbound Connector retries until 10 seconds before that value, so the last error reaches the caller before it gives up. If the header is absent or the remaining time exceeds `HTTP_CONNECTOR_MAX_DURATION_MINUTES`, the maximum applies.

A caller timeout that is too short for one attempt plus 10 seconds fails immediately with `InvalidRequest`:

```
invalid operation timeout: the caller's Nexus operation times out in 1m0s, which leaves no time
for an attempt: it needs at least one attempt (1m0s) plus 10s for the result to reach the caller;
give the operation a longer timeout (one attempt's time is HTTP_CONNECTOR_ATTEMPT_TIMEOUT_SECONDS)
```

Cancelling a caller workflow cancels the Outbound Connector activity. Any attempt already in flight finishes, but no further attempts are made.

## Rate limiting

The Outbound Connector keeps an in-memory cache of rate-limited keys. The key for a request is its URL plus the identity of the caller: the basic auth username, else the API key variable name, else the `Authorization` header's variable name, else `anon`.

- When an endpoint answers **429**, the key is cached for the rate-limit window and the request fails with `RateLimited`. If the 429 has a `Retry-After` header (seconds or an HTTP date) asking for less time than the window, the key is cached for that long instead. A longer, zero, past or unreadable `Retry-After` is ignored.
- While a key is cached, every request with the same key fails with `RateLimited` **without being sent**, and retries when the key expires.
- A **2xx** response clears the key.

The window is `HTTP_CONNECTOR_RATE_LIMIT_MINUTES` on the Outbound Connector (a whole number of minutes, at least 1, default 5).

However many callers are waiting, one Outbound Connector tries a rate-limited endpoint about once per window. The timing is not strict: when the window ends, all waiting requests retry at once; if the first 429 arrives before the others check, they wait another window.

In activity mode, the Outbound Connector does not schedule a retry that would start after the caller's operation timeout, so the window and the timeout together bound the number of attempts. With the default 25-hour maximum and a 5-minute window, an endpoint that keeps answering 429 is tried about every 5 minutes for 25 hours. With a 10-minute caller timeout, it is tried twice and fails after about 5 minutes.

## Activity or sync mode

`HTTP_CONNECTOR_MODE` on the Outbound Connector chooses how the `HTTPService.request` operation runs each call. Callers don't change: the operation, its input and output, and the error types are the same in both modes.

- **`activity`** (the default): the operation starts a standalone activity, `Request`, and the server completes the caller's Nexus operation through a completion callback when it finishes. The activity retries with the Outbound Connector's own policy, as described in the rest of this page.
- **`sync`**: the operation is a synchronous Nexus operation. It makes one attempt within the Nexus request and replies with the response or the error. There is no activity.

| | `activity` | `sync` |
|---|---|---|
| Cost | One Nexus operation plus the activity's actions (its start and each attempt). | One Nexus operation; no activity actions. |
| Time per attempt | Up to `HTTP_CONNECTOR_ATTEMPT_TIMEOUT_SECONDS` (60 seconds by default). | Must finish within one Nexus request, whose deadline the caller's server sets: 10 seconds by default (dynamic config `component.nexusoperations.request.timeout`). `HTTP_CONNECTOR_ATTEMPT_TIMEOUT_SECONDS` still caps each request. |
| Who retries, and how | The Outbound Connector's activity retry policy: 5 s, three times longer each time, at most 5 minutes apart. Retries stop 10 seconds before the caller's timeout, or after `HTTP_CONNECTOR_MAX_DURATION_MINUTES`. | The caller's Temporal server, with its own backoff for all Nexus operations: 1 s at first, growing exponentially, up to 1 hour apart (dynamic config `component.nexusoperations.retryPolicy.*`). Until the caller's operation times out. |
| What the caller gets when retries never succeed | The Outbound Connector's last error, such as `Transient` with the 503 response in its details. | A `TimeoutError`, `operation timed out (type: ScheduleToClose)`. The last error is only on the caller workflow's pending Nexus operation while it retries. |
| Rate limits | A `RateLimited` attempt waits out the window (or a shorter `Retry-After`) before the next. | The caller's server retries on its own backoff, not after the window. The rate-limit cache refuses those retries without sending them until the window ends. |
| Caller timeout too short | Refused at start with `InvalidRequest`. | Not checked. |
| Server feature needed | Standalone activities with completion callbacks (`activity.enableCallbacks=true`). | None beyond Nexus. |
| Where to see a call in progress | `temporal activity list` / `describe` in the Outbound Connector's namespace. | `temporal workflow describe` on the caller's workflow: its pending Nexus operation. |

### When to choose which

Choose **`sync`** for fast calls, well under 10 seconds, that are safe to repeat, when a caller can accept a timeout error, rather than the endpoint's last error, if the endpoint keeps failing. It costs less, and it works on a namespace without `activity.enableCallbacks`.

Choose **`activity`** (the default) for slow endpoints, when rate limits must be waited out properly rather than probed by the caller's server, or when callers need the real error after retries run out.

Either way, callers should always give the Nexus operation a timeout. In sync mode it is the only limit on retries of a failing endpoint, since `HTTP_CONNECTOR_MAX_DURATION_MINUTES` doesn't apply.

### Errors in sync mode

`Send` returns the same `ApplicationError`s as in activity mode, with the same types and the `RequestOutput` in their details, so a caller's `errors.As` code works unchanged. The synchronous operation wraps each in a Nexus handler error that tells the caller's server whether to retry:

| Error | Handler error | Caller's server |
|---|---|---|
| `InvalidRequest` | `BAD_REQUEST` | Doesn't retry; the request was never sent. |
| `ClientError` (any other non-retryable error) | `INTERNAL`, non-retryable, with the error's message, such as `... returned status 404` | Doesn't retry. |
| `RateLimited` | `RESOURCE_EXHAUSTED`, retryable | Retries with its own backoff. |
| `Transient` | `UNAVAILABLE`, retryable | Retries with its own backoff. |

The Outbound Connector stops waiting for the endpoint 1 second before the Nexus request's deadline, so it can still reply with why the attempt failed. That reply is a retryable `Transient` error:

```
calling <url>: no response before the Nexus request's deadline. With HTTP_CONNECTOR_MODE=sync, a
call must finish within one Nexus request (about 10 seconds); set HTTP_CONNECTOR_MODE=activity for
slower endpoints
```

An endpoint that never answers in time is retried this way until the caller's operation times out, and the caller gets a `TimeoutError`. To see why, run `temporal workflow describe` on the caller's workflow: the pending Nexus operation's last attempt failure holds the error. See [Troubleshooting](troubleshooting.md#a-sync-mode-call-times-out-with-no-http-error).

## Known limitations

- **The rate-limit cache is in memory.** It is lost when the Outbound Connector restarts, and each Outbound Connector process has its own. A production deployment would use a shared store such as Redis with a TTL.
- **No early wake-up.** When a rate-limited key clears, waiting requests still wait until their next scheduled retry rather than waking immediately.
- **Text bodies only.** Bodies are JSON strings, so binary content would need base64 encoding.
- **Response size limit.** The whole response body is returned in the operation's result. A body over 2 MiB fails with `ClientError`. A smaller body near Temporal's default payload limit (2 MB) may also fail to be returned.
- **Any URL, if the allowlist is disabled.** With `HTTP_CONNECTOR_ALLOWED_DOMAINS=DISABLE ALLOWLIST`, the Outbound Connector calls any `http` or `https` URL, including addresses on its own internal network. List the allowed domains instead in environments where that matters.
