# How it works

Architecture overview, package guide, the path of a request through the code, changing the contract, and writing connectors for APIs with non-standard HTTP status codes.

## Architecture

```
 namespace: http-testcaller              namespace: http-connector
┌─────────────────────────┐   Nexus    ┌────────────────────────────────────┐
│ CallerWorkflow          │──────────▶ │ HTTPService.sendRequest            │
│  ExecuteOperation(      │ http-      │   └─ starts standalone activity    │
│   sendRequest,          │ endpoint   │      SendRequest ───▶ HTTP request │
│   SendRequestInput)     │ ◀───────── │      (retries, rate-limit backoff) │
│  → SendRequestOutput    │ completion └────────────────────────────────────┘
└─────────────────────────┘  callback
```

There is no workflow on the connector's side. The Nexus operation validates the request, then starts a **standalone activity** (`SendRequest`) with `temporalnexus.StartActivity`. The server retries the activity on failure. When the activity finishes (success or permanent failure), the server completes the caller's Nexus operation through a completion callback (`activity.enableCallbacks`).

## Packages

| Package | Contents |
|---|---|
| `httpconnector/` | All connector logic. `http.go`: `Register`, the Nexus operation, the `SendRequest` activity, `ApplyStatus`, status classification, redaction. `request.go`: building an `*http.Request`, validation, API keys, the rate-limit key. `errors.go`: error types and retry delays. `ratelimiter.go`: the in-memory rate-limit cache. `allowlist.go`: domain allow-list. `redirect.go`: the redirect policy. `timeout.go`: schedule-to-close calculation. |
| `cmd/worker/` | The connector's process: connects to Temporal, calls `httpconnector.Register`, runs the worker. |
| `generated/httpapi/` | Go types for the contract, generated from `http.nexusrpc.yaml` with [nexgen](https://github.com/temporalio/nexgen). Do not edit directly. |
| `testcaller/` | Test only: `CallerWorkflow`, its worker (`worker/`), a starter (`starter/`), and the end-to-end test. |

At the top level, `http.nexusrpc.yaml` is the contract and `endpoint-description.md` is the endpoint description shown in the Temporal UI.

## Path of a request

1. **Registration.** `cmd/worker` creates a worker on the `nexus-connector-http` task queue and calls `httpconnector.Register`. `Register` creates one `HTTPActivities` value (which owns the rate-limit cache) and registers both the `HTTPService` Nexus service and its `SendRequest` activity with it, so every call on the worker shares one cache. It reads `HTTP_CONNECTOR_RATE_LIMIT_MINUTES`, `HTTP_CONNECTOR_ALLOWED_DOMAINS`, and `HTTP_CONNECTOR_MAX_DURATION_MINUTES`, and exits on an invalid value.

2. **Decoding.** The caller's request arrives through the endpoint. The generated `UnmarshalJSON` decodes it into a `SendRequestInput`, rejecting anything the contract does not allow.

3. **The operation starts an activity.** The `sendRequest` operation does no HTTP work. It calls `Validate` (builds the request without sending it), `CheckDomain` (checks the host against the allow list), and `ScheduleToClose` (calculates how long the call may take from the `operation-timeout` header). Any failure returns a `BAD_REQUEST` handler error with an `InvalidRequest` cause. Otherwise it starts `SendRequest` as a standalone activity with `temporalnexus.StartActivity`, using activity options: ID `http-<Nexus request ID>` (so a retried Nexus start reaches the same activity), `StartToCloseTimeout` 60 seconds, `ScheduleToCloseTimeout` from `ScheduleToClose`, and no attempt limit. The operation returns immediately.

4. **Each attempt.** The server runs `SendRequest` once per attempt. It:
   - builds the request (`build`): `parseURL`, `checkTimeouts`, `setQuery` (query parameters and a query API key), then the `*http.Request` and `setHeaders` (headers, with any header variable's value, the API key as basic auth or Bearer token, then header validation). A failure here is a non-retryable `InvalidRequest`.
   - checks the rate-limit cache for the request's key. If cached, fails with `RateLimited` without sending.
   - sends the request with an `http.Client` whose timeout is `timeoutSeconds` and whose `checkRedirect` follows no redirect unless `HTTP_CONNECTOR_ALLOW_REDIRECTS` is true; then it refuses `https` → `http` redirects and, with the allow list set, redirects to any other host, never sends `Referer`, and drops the headers filled from secrets once a redirect leaves the original site. A transport error is `Transient` (message passed through `redact`), except a TLS certificate failure, which is a `ClientError`.
   - reads up to 2 MiB of body and passes the response through `redactResponse`.
   - returns the response for a 2xx, clearing the rate-limit key. Otherwise `categorizeError` picks the type: a 429 marks the key in the cache, and `ClientError` is non-retryable. The error carries the response in its details.

5. **Retries.** The activity has no retry loop; the server retries it. Each retryable error sets its own next delay with `NextRetryDelay` in `errors.go`: `rateLimitedError` waits the rest of the window; `transientError` backs off from 1 s. The count of `Transient` failures in a row passes between attempts as heartbeat details; a `RateLimited` failure resets it to 0. The retry policy's own backoff applies only when an attempt times out.

6. **Completion.** When the activity succeeds or fails permanently, the server completes the caller's Nexus operation through the completion callback.

## Changing the contract

Edit `http.nexusrpc.yaml`, then run `make generate` and commit the regenerated `generated/httpapi`. CI fails if the generated code is out of date.

```bash
make generate
```

The service's and operation's `fqn` values are the names callers use on the wire (`HTTPService` and `sendRequest`). Keep them: without them, nexgen would derive `SendRequest` and existing callers would stop working.

## APIs that don't follow the standard

The connector decides what to do from the HTTP status code alone. Most APIs follow the standard (a 429 is a rate limit, a 503 is worth retrying, a 404 is not), but some answer `200 OK` and put the real outcome — an error or a rate limit — in the body.

A connector for such an API is a thin shell that uses this connector's code as a library. The shell has its own Nexus service and operation, its activities call `SendRequest` as a function inside their own activity (not a cross-service Nexus hop), and after each response they inspect the body:

```go
resp, err := a.SendRequest(ctx, req)
if err == nil && bodySaysRateLimited(resp) {
    return nil, a.ApplyStatus(ctx, req, http.StatusTooManyRequests, resp)
}
```

`ApplyStatus` is what `SendRequest` itself does with every response: it marks or clears the rate-limit key, sets the retry delay, and returns the typed error. So the remapped response is handled exactly as if the endpoint had sent that status. The error message says both (`returned status 200, treated as 429`). The returned error must be returned from the activity unchanged — it carries the retry delay.

Everything a shell needs is exported: `New` (creates the activities and rate-limit cache), `Validate`, `CheckDomain`, `ScheduleToClose`, and `ActivityOptions`. The [nexus-connector-openapi](https://github.com/Evanthx/nexus-connector-openapi) project is a generated shell of this kind: it generates a typed Nexus service from an OpenAPI spec, with one activity per API operation, each calling `SendRequest` in-process.

This approach works only for in-process callers. A workflow calling this connector's Nexus operation from another namespace cannot reach the connector's internal rate-limit cache.
