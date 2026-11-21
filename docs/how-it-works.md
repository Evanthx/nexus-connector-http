# How it works

Architecture overview, package guide, the path of a request through the code, changing the contract, and writing connectors for APIs with non-standard HTTP status codes.

## Architecture

```
 namespace: http-testcaller              namespace: connectors
┌─────────────────────────┐   Nexus    ┌────────────────────────────────────┐
│ CallerWorkflow          │──────────▶ │ HTTPService.request                │
│  ExecuteOperation(      │ http-      │   └─ starts standalone activity    │
│   Request,              │ endpoint   │      Request ───▶ HTTP request     │
│   RequestInput)         │ ◀───────── │      (retries, rate-limit backoff) │
│  → RequestOutput        │ completion └────────────────────────────────────┘
└─────────────────────────┘  callback
```

There is no workflow on the Outbound Connector's side. The Nexus operation validates the request, then starts a **standalone activity** (`Request`) with `temporalnexus.StartActivity`. The server retries the activity on failure. When the activity finishes (success or permanent failure), the server completes the caller's Nexus operation through a completion callback (`activity.enableCallbacks`).

## Packages

| Package | Contents |
|---|---|
| `httpconnector/` | All connector logic. `http.go`: `Register`, the Nexus operation, the `Request` activity, `ApplyStatus`, status classification, redaction. `request.go`: building an `*http.Request`, validation, API keys, the rate-limit key. `errors.go`: error types and retry delays. `ratelimiter.go`: the in-memory rate-limit cache. `allowlist.go`: domain allow-list. `redirect.go`: the redirect policy. `timeout.go`: schedule-to-close calculation, through `deadline`. `settings.go`: every environment variable the Outbound Connector reads, with its help, default and check (`Settings`), which `New` loads through. |
| `cmd/outbound/` | The Outbound Connector's process: checks its settings and connects to Temporal (`startup`), calls `httpconnector.Register`, logs the startup report (`HTTPActivities.Report`), waits for the payload codec, runs the worker. |
| `configui/` | The setup page (`make configui`): `connector.go` lists the Outbound Connector and its checks; the page itself is in `nexus-connector-shared/configui`. It shows `httpconnector.Settings`, tests values without changing any environment, and lists the `export` lines. Not part of the Outbound Connector. `configui/gendocs` writes `docs/setup.md`'s tables (`make docs`). |
| `generated/httpapi/` | Go types for the contract, generated from `http.nexusrpc.yaml` with [nexgen](https://github.com/temporalio/nexgen). Do not edit directly. |
| `testcaller/` | Test only: `CallerWorkflow`, its worker (`worker/`), a starter (`starter/`), and the end-to-end tests. |
| `nexus-connector-shared/` | Code every Go connector shares, the same in every Go connector repo; see its [README](../nexus-connector-shared/README.md). `settings` (declaring and reading environment variables, such as each secret with its `_DOMAINS` list; the only code that reads the environment), `temporalclient` (the Temporal client options), `codec` ([encryption](encryption.md)), `startup` (how the program starts), `report` (the startup report), `deadline` (the Outbound Connector's time limit), `nexussend` (for Inbound Connectors; unused here), `configui` (the setup page), and test helpers with the demo codec (`test/encryption`). |

At the top level, `http.nexusrpc.yaml` is the contract and `endpoint-description.md` is the endpoint description shown in the Temporal UI.

## Path of a request

1. **Registration.** `cmd/outbound` creates a worker on the `nexus-connector-http` task queue and calls `httpconnector.Register`. `Register` creates one `HTTPActivities` value (which owns the rate-limit cache) and registers both the `HTTPService` Nexus service and its `Request` activity with it, so every call on the worker shares one cache. It reads `HTTP_CONNECTOR_RATE_LIMIT_MINUTES`, `HTTP_CONNECTOR_ALLOWED_DOMAINS`, `HTTP_CONNECTOR_MAX_DURATION_MINUTES`, and each secret's `_DOMAINS` list through `httpconnector.Settings`, and fails on a secret without a list, a list without a secret, or on an invalid value or an unset `HTTP_CONNECTOR_ALLOWED_DOMAINS`. `cmd/outbound` first calls `httpconnector.New` and dials Temporal, and reports every failure from both before exiting.

2. **Decoding.** The caller's request arrives through the endpoint. The generated `UnmarshalJSON` decodes it into a `RequestInput`, rejecting anything the contract does not allow.

3. **The operation starts an activity.** The `request` operation does no HTTP work. It builds the request without sending it (as `Validate` does), then calls `CheckDomain` (checks the host against the allow list), `CheckSecretDomains` (checks the host against the `_DOMAINS` list of each secret the request names), and `ScheduleToClose` (calculates how long the call may take from the `operation-timeout` header). Any failure returns a `BAD_REQUEST` handler error with an `InvalidRequest` cause. Otherwise it starts `Request` as a standalone activity with `temporalnexus.StartActivity`, using activity options: ID `http-<Nexus request ID>` (so a retried Nexus start reaches the same activity), `StartToCloseTimeout` from `HTTP_CONNECTOR_ATTEMPT_TIMEOUT_SECONDS` (60 seconds by default), `ScheduleToCloseTimeout` from `ScheduleToClose`, and no attempt limit. The operation returns immediately.

4. **Each attempt.** The server runs `Request` once per attempt. It:
   - builds the request (`build`): `parseURL`, `setQuery` (query parameters and a query API key), then the `*http.Request` and `setHeaders` (headers, with any header variable's value, the API key as basic auth or Bearer token, then header validation). A failure here is a non-retryable `InvalidRequest`.
   - checks the host against the allow list and each named secret's `_DOMAINS` list again, so a library caller that skips the operation (such as nexus-connector-openapi) gets the same checks. A failure is a non-retryable `InvalidRequest`.
   - checks the rate-limit cache for the request's key. If cached, fails with `RateLimited` without sending.
   - sends the request with an `http.Client` whose timeout is 5 seconds less than the attempt timeout and whose `checkRedirect` follows no redirect unless `HTTP_CONNECTOR_ALLOW_REDIRECTS` is true; then it refuses `https` → `http` redirects and, with the allow list set, redirects to any other host, never sends `Referer`, and drops the headers filled from secrets once a redirect leaves the original site. A transport error is `Transient` (message passed through `redact`), except a TLS certificate failure, which is a `ClientError`.
   - reads up to 2 MiB of body and passes the response through `redactResponse`.
   - returns the response for a 2xx, clearing the rate-limit key. Otherwise `categorizeError` picks the type: a 429 marks the key in the cache, and `ClientError` is non-retryable. The error carries the response in its details.

5. **Retries.** The activity has no retry loop; the server retries it. `Transient` errors and attempt timeouts wait the retry policy's backoff: 5 s, then three times longer each attempt, at most 5 minutes apart. `RateLimited` errors set their own delay with `NextRetryDelay` (`rateLimitedError` in `errors.go`): the rest of the window, or a 429's `Retry-After` if shorter. The policy counts every attempt, so a `Transient` error after a few rate-limited attempts waits the full 5 minutes.

6. **Completion.** When the activity succeeds or fails permanently, the server completes the caller's Nexus operation through the completion callback.

## Changing the contract

Edit `http.nexusrpc.yaml`, then run `make generate` and commit the regenerated `generated/httpapi`. CI fails if the generated code is out of date.

```bash
make generate
```

The service's and operation's `fqn` values are the names callers use on the wire (`HTTPService` and `request`). Keep them: without them, nexgen would derive `Request` and existing callers would stop working.

## APIs that don't follow the standard

The Outbound Connector decides what to do from the HTTP status code alone. Most APIs follow the standard (a 429 is a rate limit, a 503 is worth retrying, a 404 is not), but some answer `200 OK` and put the real outcome — an error or a rate limit — in the body.

A connector for such an API is a thin shell that uses the HTTP connector's code as a library. The shell has its own Nexus service and operation, its activities call `Request` as a function inside their own activity (not a cross-service Nexus hop), and after each response they inspect the body:

```go
resp, err := a.Request(ctx, req)
if err == nil && bodySaysRateLimited(resp) {
    return nil, a.ApplyStatus(ctx, req, http.StatusTooManyRequests, resp)
}
```

`ApplyStatus` is what `Request` itself does with every response: it marks or clears the rate-limit key, sets the retry delay, and returns the typed error. So the remapped response is handled exactly as if the endpoint had sent that status. The error message says both (`returned status 200, treated as 429`). The returned error must be returned from the activity unchanged — it carries the retry delay.

Everything a shell needs is exported: `New` (creates the activities and rate-limit cache), `Validate`, `CheckDomain`, `ScheduleToClose`, and `ActivityOptions`. The [nexus-connector-openapi](https://github.com/Evanthx/nexus-connector-openapi) project is a generated shell of this kind: it generates a typed Nexus service from an OpenAPI spec, with one activity per API operation, each calling `Request` in-process.

This approach works only for in-process callers. A workflow calling this Outbound Connector's Nexus operation from another namespace cannot reach the Outbound Connector's internal rate-limit cache.
