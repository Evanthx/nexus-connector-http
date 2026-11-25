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

There is no workflow on the Outbound Connector's side. This is the default, activity mode (`HTTP_CONNECTOR_MODE=activity`): the Nexus operation validates the request, then starts a **standalone activity** (`Request`) with `temporalnexus.StartActivity`. The server retries the activity on failure. When the activity finishes (success or permanent failure), the server completes the caller's Nexus operation through a completion callback (`activity.enableCallbacks`).

In sync mode (`HTTP_CONNECTOR_MODE=sync`) there is no activity either. The operation is a synchronous Nexus operation: it makes one attempt within the Nexus request and replies with the response or an error. The caller's Temporal server retries a retryable error with its own backoff. See [Sync mode](#sync-mode) below, and [Errors — activity or sync mode](errors.md#activity-or-sync-mode) for when to use which.

```
 namespace: http-testcaller              namespace: connectors
┌─────────────────────────┐   Nexus    ┌────────────────────────────────────┐
│ CallerWorkflow          │──────────▶ │ HTTPService.request (sync)         │
│  ExecuteOperation(...)  │ http-      │   RequestSync: one attempt ───▶    │
│  → RequestOutput        │ endpoint   │   HTTP request, within the Nexus   │
│  (its server retries)   │ ◀───────── │   request's deadline               │
└─────────────────────────┘  reply     └────────────────────────────────────┘
```

## Packages

| Package | Contents |
|---|---|
| `httpconnector/` | All connector logic. `http.go`: `Config`, `New`, `Register`, the activity-mode Nexus operation, `Send` (one HTTP attempt), the `Request` activity, `ApplyStatus`, status classification. `sync.go`: `HTTP_CONNECTOR_MODE` (`Mode`, `ParseMode`), `RequestSync` (the sync-mode operation's body) and `HandlerError` (errors to Nexus handler errors). `redact.go`: redacting secrets from responses and errors. `request.go`: building an `*http.Request`, validation, API keys, the rate-limit key. `errors.go`: error types and retry delays. `ratelimiter.go`: the in-memory rate-limit cache. `allowlist.go`: domain allow-list. `redirect.go`: the redirect policy. `timeout.go`: the attempt timeout's constants and schedule-to-close, through `deadline`. Never imports `setup`. |
| `setup/` | `settings.go`: every environment variable the Outbound Connector reads, with its help, default and check (`setup.Outbound`), and `Config`, which turns the loaded values into an `httpconnector.Config`. |
| `cmd/outbound/` | The Outbound Connector's process: `main.go` checks its settings (`configFrom`, in `config.go`) and connects to Temporal (`startup`), builds the activities once with `httpconnector.New` and passes them to `httpconnector.Register`, logs the startup report (`HTTPActivities.Report`), waits for the payload codec, runs the worker. |
| `configui/` | The setup page (`make configui`): `connector.go` lists the Outbound Connector and its checks; the page itself is in `nexus-connector-shared/configui`. It shows `setup.Outbound`, tests values without changing any environment, and lists the `export` lines. Not part of the Outbound Connector. `configui/gendocs` writes `docs/setup.md`'s tables (`make docs`). |
| `generated/httpapi/` | Go types for the contract, generated from `http.nexusrpc.yaml` with [nexgen](https://github.com/temporalio/nexgen). Do not edit directly. |
| `testcaller/` | Test only: `CallerWorkflow`, its worker (`worker/`), a starter (`starter/`), and the end-to-end tests. |
| `nexus-connector-shared/` | Code every Go connector shares, the same in every Go connector repo; see its [README](../nexus-connector-shared/README.md). `settings` (declaring and reading environment variables, such as each secret with its `_DOMAINS` list; the only code that reads the environment), `temporalclient` (the Temporal client options), `codec` ([encryption](encryption.md)), `startup` (how the program starts), `report` (the startup report), `deadline` (the Outbound Connector's time limit), `nexussend` (for Inbound Connectors; unused here), `configui` (the setup page), and test helpers with the demo codec (`test/encryption`). |

At the top level, `http.nexusrpc.yaml` is the contract and `endpoint-description.md` is the endpoint description shown in the Temporal UI.

## Path of a request

1. **Registration.** `cmd/outbound` loads every variable through `setup.Outbound` (`HTTP_CONNECTOR_ALLOWED_DOMAINS`, `HTTP_CONNECTOR_MODE`, `HTTP_CONNECTOR_RATE_LIMIT_MINUTES`, `HTTP_CONNECTOR_ATTEMPT_TIMEOUT_SECONDS`, `HTTP_CONNECTOR_MAX_DURATION_MINUTES`, `HTTP_CONNECTOR_ALLOW_REDIRECTS`, and each secret's `_DOMAINS` list), which fails on a secret without a list, a list without a secret, or on an invalid value or an unset `HTTP_CONNECTOR_ALLOWED_DOMAINS`, and dials Temporal, reporting every failure from both before exiting. It then creates one `HTTPActivities` value (which owns the rate-limit cache) with `httpconnector.New(setup.Config(vals), env)`, a worker on the `nexus-connector-http` task queue, and calls `httpconnector.Register` with both, which registers the `HTTPService` Nexus service and, in activity mode, its `Request` activity, with that one value, so every call on the worker shares one cache. In sync mode it registers only the Nexus service. The startup report comes from the same value.

2. **Decoding.** The caller's request arrives through the endpoint. The generated `UnmarshalJSON` decodes it into a `RequestInput`, rejecting anything the contract does not allow.

Steps 3 to 6 are activity mode; sync mode is [below](#sync-mode).

3. **The operation starts an activity.** The `request` operation does no HTTP work. It builds the request without sending it (as `Validate` does), then calls `CheckDomain` (checks the host against the allow list), `CheckSecretDomains` (checks the host against the `_DOMAINS` list of each secret the request names), and `ScheduleToClose` (calculates how long the call may take from the `operation-timeout` header). Any failure returns a `BAD_REQUEST` handler error with an `InvalidRequest` cause. Otherwise it starts `Request` as a standalone activity with `temporalnexus.StartActivity`, using activity options: ID `http-<Nexus request ID>` (so a retried Nexus start reaches the same activity), `StartToCloseTimeout` from `HTTP_CONNECTOR_ATTEMPT_TIMEOUT_SECONDS` (60 seconds by default), `ScheduleToCloseTimeout` from `ScheduleToClose`, and no attempt limit. The operation returns immediately.

4. **Each attempt.** The server runs `Request` once per attempt. `Request` is just `Send`, which makes one attempt and needs no Temporal context. It:
   - builds the request (`build`): `parseURL`, `setQuery` (query parameters and a query API key), then the `*http.Request` and `setHeaders` (headers, with any header variable's value, the API key as basic auth or Bearer token, then header validation). A failure here is a non-retryable `InvalidRequest`.
   - checks the host against the allow list and each named secret's `_DOMAINS` list again, so a library caller that skips the operation (such as nexus-connector-openapi) gets the same checks. A failure is a non-retryable `InvalidRequest`.
   - checks the rate-limit cache for the request's key. If cached, fails with `RateLimited` without sending.
   - sends the request with an `http.Client` whose timeout is 5 seconds less than the attempt timeout and whose `checkRedirect` follows no redirect unless `HTTP_CONNECTOR_ALLOW_REDIRECTS` is true; then it refuses `https` → `http` redirects and, with the allow list set, redirects to any other host, never sends `Referer`, and drops the headers filled from secrets once a redirect leaves the original site. A transport error is `Transient` (message passed through `redact`), except a TLS certificate failure, which is a `ClientError`.
   - reads up to 2 MiB of body and passes the response through `redactResponse`.
   - returns the response for a 2xx, clearing the rate-limit key. Otherwise `categorizeError` picks the type: a 429 marks the key in the cache, and `ClientError` is non-retryable. The error carries the response in its details.

5. **Retries.** The activity has no retry loop; the server retries it. `Transient` errors and attempt timeouts wait the retry policy's backoff: 5 s, then three times longer each attempt, at most 5 minutes apart. `RateLimited` errors set their own delay with `NextRetryDelay` (`rateLimitedError` in `errors.go`): the rest of the window, or a 429's `Retry-After` if shorter. The policy counts every attempt, so a `Transient` error after a few rate-limited attempts waits the full 5 minutes.

6. **Completion.** When the activity succeeds or fails permanently, the server completes the caller's Nexus operation through the completion callback.

### Sync mode

With `HTTP_CONNECTOR_MODE=sync`, `Register` registers the `request` operation as a synchronous Nexus operation (`nexus.NewSyncOperation`) whose body is `RequestSync`:

1. **One attempt.** `RequestSync` calls `Send`, the same code as an activity attempt (step 4 above), with a context that ends 1 second (`replyMargin`) before the Nexus request's deadline. The caller's server sets that deadline: 10 seconds by default (dynamic config `component.nexusoperations.request.timeout`). `HTTP_CONNECTOR_ATTEMPT_TIMEOUT_SECONDS` still caps each HTTP request. If the context ends first, the error is a retryable `Transient`: `no response before the Nexus request's deadline`, saying to set `HTTP_CONNECTOR_MODE=activity` for slower endpoints.
2. **The reply.** A 2xx is the operation's result. Any error goes through `HandlerError`, which keeps the `ApplicationError` as the cause and picks the handler error: `InvalidRequest` → `BAD_REQUEST`; any other non-retryable (`ClientError`) → non-retryable `INTERNAL`; `RateLimited` → retryable `RESOURCE_EXHAUSTED`; `Transient` → retryable `UNAVAILABLE`.
3. **Retries.** The caller's Temporal server retries a retryable handler error with its own backoff for Nexus operations (1 second at first, exponential, up to 1 hour; `component.nexusoperations.retryPolicy.*`), until the caller's operation times out. It doesn't wait for the rate-limit window, so the rate-limit cache refuses its retries without sending them until the window ends. A caller whose retries never succeed gets a `TimeoutError`; the last error is on its pending Nexus operation.

The checks at the start of an activity-mode operation (`CheckDomain`, `CheckSecretDomains`, the caller's timeout) are not run separately: `Send` checks the domains itself, and the caller's timeout is not checked. `HTTP_CONNECTOR_MAX_DURATION_MINUTES` doesn't apply.

## Changing the contract

Edit `http.nexusrpc.yaml`, then run `make generate` and commit the regenerated `generated/httpapi`. CI fails if the generated code is out of date.

```bash
make generate
```

The service's and operation's `fqn` values are the names callers use on the wire (`HTTPService` and `request`). Keep them: without them, nexgen would derive `Request` and existing callers would stop working.

## APIs that don't follow the standard

The Outbound Connector decides what to do from the HTTP status code alone. Most APIs follow the standard (a 429 is a rate limit, a 503 is worth retrying, a 404 is not), but some answer `200 OK` and put the real outcome — an error or a rate limit — in the body.

A connector for such an API is a thin shell that uses the HTTP connector's code as a library. The shell has its own Nexus service and operation, its activities call `Send` (or `Request`, which is the same) as a function inside their own activity (not a cross-service Nexus hop), and after each response they inspect the body:

```go
resp, err := a.Send(ctx, req)
if err == nil && bodySaysRateLimited(resp) {
    return nil, a.ApplyStatus(ctx, req, http.StatusTooManyRequests, resp)
}
```

`ApplyStatus` is what `Send` itself does with every response, and works in any context: it marks or clears the rate-limit key, sets the retry delay, and returns the typed error. So the remapped response is handled exactly as if the endpoint had sent that status. The error message says both (`returned status 200, treated as 429`). The returned error must be returned from the activity unchanged — it carries the retry delay.

A shell can also run a call in a synchronous Nexus operation, with no activity, as sync mode does: call `RequestSync(ctx, req)` from the handler, or call `Send` with your own deadline and return `HandlerError(err)` for any error, so the caller's server retries the retryable ones.

Everything a shell needs is exported: `New` (creates the activities and rate-limit cache), `Send`, `Request`, `ApplyStatus`, `RequestSync`, `HandlerError`, `Mode`, `Validate`, `CheckDomain`, `ScheduleToClose`, and `ActivityOptions`. The [nexus-connector-openapi](https://github.com/Evanthx/nexus-connector-openapi) project is a generated shell of this kind: it generates a typed Nexus service from an OpenAPI spec, with one activity per API operation, each calling `Request` in-process.

This approach works only for in-process callers. A workflow calling this Outbound Connector's Nexus operation from another namespace cannot reach the Outbound Connector's internal rate-limit cache.
