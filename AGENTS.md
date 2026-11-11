# AGENTS.md

Instructions for AI agents that set up, run, call, and change this connector. README.md has the
full explanations; this file has what you need to act, and the mistakes to avoid.

## What this is

A Temporal connector that makes **outbound HTTP requests** for workflows in other namespaces (Go,
module `github.com/temporalio/nexus-connector-http`). It serves one Nexus operation,
`HTTPService.sendRequest`, behind a Nexus endpoint. A caller sends a `SendRequestInput` (URL,
method, headers, query parameters, body, credentials) and gets a `SendRequestOutput` (status,
headers, body) back. It is write-only: it makes the request it is given and returns the response,
and never polls.

The operation is backed by a **standalone activity**, not a workflow: it calls
`temporalnexus.StartActivity` to start `SendRequest`, and the server completes the Nexus operation
through a completion callback when the activity finishes. The server's activity retry policy does
all retrying.

## Requirements

- Go 1.26 or later, and Go SDK v1.50 or later (`temporalnexus.StartActivity`).
- Temporal Server 1.32 or later (Temporal CLI 1.9.1 or later for a local dev server), or Temporal
  Cloud. The connector's namespace needs **standalone activities with completion callbacks**:
  - Dev server: `--dynamic-config-value activity.enableCallbacks=true`.
  - Without it, every call fails with `handler error (BAD_REQUEST)` and the worker logs
    `completion callbacks are not enabled for this namespace`.
  - On Temporal Cloud, ask the user to confirm with Temporal that it is enabled for the namespace.
    Don't assume.
- Network access from the connector to the URLs callers will request.

The worker, the test caller's worker, and the starter connect to Temporal like the `temporal` CLI,
with `envconfig`: `TEMPORAL_ADDRESS`, `TEMPORAL_NAMESPACE`, `TEMPORAL_API_KEY`, `TEMPORAL_TLS_*`,
`TEMPORAL_PROFILE`, and the CLI's profile file. With none, `localhost:7233`. The namespace defaults
to `http-connector` for the connector and `http-testcaller` for the test caller; give each process
its own.

## Run the connector

1. **Start Temporal** (local):

   ```bash
   temporal server start-dev \
     --dynamic-config-value activity.enableCallbacks=true \
     --namespace http-connector --namespace http-testcaller
   ```

   Add `--http-port 7243` if anything will call the endpoint over plain HTTP.
2. **Create the Nexus endpoint** targeting the connector's namespace and task queue
   `nexus-connector-http` (`httpconnector.TaskQueue`):

   ```bash
   temporal operator nexus endpoint create --name http-endpoint \
     --target-namespace http-connector --target-task-queue nexus-connector-http \
     --description-file ./endpoint-description.md
   ```

   The dev server keeps endpoints in memory: create it again after every restart. On Temporal
   Cloud, create it in the Cloud UI or with `tcld`, and allow the callers' namespaces to call it.
3. **Export secrets, then run the worker** in the same terminal:

   ```bash
   export HTTP_CONNECTOR_SECRET_WEATHER=...   # any API keys callers will name
   go run ./cmd/worker
   ```

   | Variable | Value |
   |---|---|
   | `HTTP_CONNECTOR_SECRET_*` | API keys callers can name in `apiKeyEnvVar`, or as an `Authorization` header's whole value. |
   | `HTTP_CONNECTOR_RATE_LIMIT_MINUTES` | Optional. Whole minutes ≥ 1, default 5. Any other value makes the worker exit at startup. |
   | `HTTP_CONNECTOR_MAX_DURATION_MINUTES` | Optional. Whole minutes ≥ 1, default 1500 (25h). The longest a call retries, when the caller's timeout is longer or unset. Any other value makes the worker exit at startup. |
   | `HTTP_CONNECTOR_ALLOWED_DOMAINS` | Optional. Comma-separated domains, e.g. `temporal.io, api.stripe.com`; spaces ignored. Each allows itself and its subdomains. Unset: any host. An entry that isn't a bare domain (scheme, port, path, `*`, empty) makes the worker exit at startup. |
   | `TEMPORAL_*` | As above. |

## Call it

From a workflow, with a Nexus client for `HTTPService` on the endpoint. Go callers use
`generated/httpapi`; `testcaller/workflow.go` (`CallerWorkflow`) is a working example to copy.
Other languages generate types from `http.nexusrpc.yaml` with nexgen.

```go
c := workflow.NewNexusClient("http-endpoint", httpapi.HTTPService.ServiceName)
req := httpapi.SendRequestInput{URL: "https://api.example.com/items"}
var resp httpapi.SendRequestOutput
err := c.ExecuteOperation(ctx, httpapi.HTTPService.SendRequest, req, workflow.NexusOperationOptions{
    ScheduleToCloseTimeout: time.Hour, // the connector retries until 10s before this
}).Get(ctx, &resp)
```

The input as JSON:

```json
{"url": "https://api.example.com/items", "method": "POST",
 "headers": {"Content-Type": "application/json"}, "queryParams": {"source": "workflow"},
 "body": "{\"name\":\"widget\"}", "username": "reporting-bot",
 "apiKeyEnvVar": "HTTP_CONNECTOR_SECRET_ITEMS", "timeoutSeconds": 10}
```

| Field | Default | Notes |
|---|---|---|
| `url` | required | Absolute `http` or `https`. |
| `method` | `GET` | `GET`, `HEAD`, `POST`, `PUT`, `PATCH`, `DELETE`, `OPTIONS`, upper case. |
| `headers`, `queryParams` | none | String maps. Query parameters are added to any in the URL. |
| `body` | none | A string; text only, so a JSON body is escaped inside it. |
| `apiKeyEnvVar` | none | Name of a variable on the connector, starting `HTTP_CONNECTOR_SECRET_`. |
| `apiKeyParam` | none | Query parameter for the key. Unset: the key goes in `Authorization`, as `Bearer <key>`, or as the basic auth password with `username`. |
| `username` | none | Basic auth user. Needs `apiKeyEnvVar`; can't be used with `apiKeyParam`. |
| `timeoutSeconds` | 30 | One attempt, 1–60. |

Only a **2xx** is a result. Redirects are followed with Go's default policy (up to 10), except
https → http, and except to a host outside `HTTP_CONNECTOR_ALLOWED_DOMAINS` when it is set.

## Rules that break things when ignored

- **The caller's Nexus timeout sets how long the connector retries**: until `callbackMargin` (10s)
  before it, then the caller gets the connector's last error, not a timeout. The connector reads it
  from the `operation-timeout` header, which the caller's server sets to the smaller of the
  operation's remaining `ScheduleToCloseTimeout` and its `StartToCloseTimeout`. No header, or more
  time than `HTTP_CONNECTOR_MAX_DURATION_MINUTES`, means the maximum. A timeout under
  `timeoutSeconds` + 10s fails at once with `InvalidRequest`. Cancelling the caller's workflow
  cancels the activity.
- **Never put a secret in the request.** Name a `HTTP_CONNECTOR_SECRET_*` variable in
  `apiKeyEnvVar`, or as the `Authorization` header's whole value (the variable then holds the whole
  header, scheme included, e.g. `Bearer ...` or `Basic <base64>` for Stripe-style auth). Any other
  `Authorization` value is sent and stored in workflow history as is. Export the variable in the
  worker's environment, not the caller's.
- **A caller with no Nexus timeout retries a failing endpoint for 25 hours** (the default
  maximum). Give the operation a short timeout when trying things out.
- **The rate-limit cache is per worker and in memory.** A 429 holds back every request with the
  same key (URL plus username, else key variable name, else `Authorization` variable name, else
  `anon`) for the window, without sending them. Restarting the worker clears it; several workers
  each probe on their own.
- **Without `HTTP_CONNECTOR_ALLOWED_DOMAINS`, the connector calls any URL**, including ones on its
  own network. Tell the user if that matters where it runs. The list checks the request's URL and
  every redirect: a redirect to a host not on it isn't followed, and fails as a `ClientError`
  naming the host. It checks names, not the addresses they resolve to.
- **Body limit 2 MiB**, and the response must fit Temporal's payload limit (2 MB by default).

## Check it is working

- The worker logs `Starting worker on namespace "http-connector", task queue
  "nexus-connector-http"`. A config error (such as a bad `HTTP_CONNECTOR_RATE_LIMIT_MINUTES`) exits
  before that.
- Run a request end to end with the test caller (namespace `http-testcaller`), in two more
  terminals:

  ```bash
  go run ./testcaller/worker
  go run ./testcaller/starter                     # GET https://httpbin.org/get, prints the response
  go run ./testcaller/starter -url https://httpbin.org/status/503 -timeout 10m
  ```

  Flags: `-url`, `-method`, `-body`, `-timeout` (the Nexus operation's timeout; none by default).
- A request being retried is a standalone activity in the connector's namespace, with ID
  `http-<Nexus request ID>`:

  ```bash
  temporal activity list --namespace http-connector
  temporal activity describe --namespace http-connector --activity-id <id>
  ```

  `describe` shows the attempt, the last failure with its type and HTTP response, and the next
  attempt time.
- A caller that never progresses usually means no worker is polling the caller's own task queue,
  or the endpoint doesn't exist (dev server restarted).

## Errors

Every failure is an `ApplicationError` whose `Type` is an `httpconnector.ErrorType*` constant;
callers find it with `errors.As`. For an HTTP response, `Details` holds the `SendRequestOutput`.

| Type | Cause | Retried |
|---|---|---|
| `InvalidRequest` | Can't be made: bad URL, `timeoutSeconds` outside 1–60, a caller timeout under `timeoutSeconds` + 10s, a host not in the allowed domains, invalid header name or value, bad or missing key variable. Caught at start: `BAD_REQUEST` handler error with this as its cause, no activity. | No |
| `ClientError` | A status resending won't change: 4xx not listed below, 501, 505, 511, an unfollowed 3xx (including https → http), any other non-2xx; a redirect to a host not in the allowed domains (message `redirected to "<host>", which is not on the connector's list of allowed domains`, details the 3xx with its `Location`); a body over 2 MiB; or a TLS certificate that doesn't verify. | No |
| `Transient` | 408, 409, 420, 421, 423, 424, 425, other 5xx, network errors, timeouts. | Backoff 1s×2 up to 30s |
| `RateLimited` | 429, or the key is in the rate-limit cache. | After the window |

A request that breaks the contract (unknown field, unlisted method, no `url`) fails before the
operation runs: a `PayloadValidationError` when a Go caller encodes it, or a `BAD_REQUEST`
`unable to decode: Payload validation failed` handler error for raw JSON, which doesn't name the
field.

## Changing the code

### Commands

```bash
make generate   # regenerate generated/httpapi from http.nexusrpc.yaml (downloads the pinned nexgen into .build/)
make test       # go test -race; includes testcaller's end-to-end test, which needs the temporal CLI
make lint       # golangci-lint (Slack connector's config)
make ci         # vet, workflowcheck, lint, test
go test ./httpconnector -run TestSendRequest_429CachesKey -v   # single test
go test -short ./...                                         # skip the end-to-end test
```

### Rules for changes

- The contract is `http.nexusrpc.yaml`. After editing it, run `make generate` and commit
  `generated/httpapi`; CI fails if it is stale. Never edit generated code. The service and
  operation `fqn`s pin the wire names `HTTPService`/`sendRequest`; without them nexgen would derive
  `SendRequest`. Keep them. Optional fields are pointers; `method` and `timeoutSeconds` have
  `…OrDefault()` accessors. The generated `MarshalJSON`/`UnmarshalJSON` validate against the
  contract.
- Outside the package, `cmd/worker` and `testcaller` use only `TaskQueue`, `Register`,
  the `ErrorType*` constants and `AllowedDomainsEnvVar`. `New`, `HTTPActivities`, `ApplyStatus`,
  `Validate`, `CheckDomain`, `ScheduleToClose`, `MaxDuration`, `ActivityOptions`, `RateLimitKey`,
  `InvalidRequestError`, `SecretEnvPrefix`, `RateLimitMinutesEnvVar` and `MaxDurationEnvVar` are
  exported on purpose, though only the package uses them: they are
  the connector's API for code that registers or calls it differently (such as
  nexus-connector-openapi). Don't unexport them.
- `Register` makes one `HTTPActivities` for both the service and the activity registration, so one
  rate-limit cache is shared. Keep that.
- A secret value must never enter the request, history, the rate-limit key, error messages, or
  logs. Any new place a response or error text reaches the caller goes through `redact` or
  `redactResponse`.
- For a test that guards a fix, revert the fix and confirm the test fails before calling it done.

### How the code works

- `http.nexusrpc.yaml` — the contract; `generated/httpapi` is generated from it.
- `httpconnector/` — nearly all logic.
  - `http.go`: `TaskQueue`; `Register`, which builds the Nexus operation inline; the `SendRequest`
    activity; `ApplyStatus`, which turns a response's status into the result (clears or marks the
    rate-limit key, builds the typed error) and which a library caller also calls to remap a 2xx
    whose body says otherwise, so keep every status decision in it; `categorizeError()` (status →
    error type); `checkRedirect`; redaction.
  - The operation validates first (`Validate`, then `CheckDomain`, then `ScheduleToClose` from
    the `operation-timeout` header) and returns a `BadRequest` handler error (cause: an
    `InvalidRequest` ApplicationError) without starting an activity. Otherwise it starts
    `SendRequest` with `ActivityOptions(id, total)`. Activity ID is `"http-" + Nexus request ID`, so a
    retried Nexus start reaches the same activity.
  - `ActivityOptions`: StartToClose `attemptTimeout` 60s, which also caps `timeoutSeconds`;
    ScheduleToClose from `ScheduleToClose`; no attempt limit; policy
    backoff 1s×2 up to 30s, which now applies only to attempt timeouts.
  - `request.go`: `build`, in steps `parseURL`, `checkTimeouts`, `setQuery` (query parameters, a
    query API key, and the `username`/`apiKeyParam` check), `setHeaders` (headers, the
    `Authorization` variable via `secretEnv`, basic or Bearer auth, `httpguts` validation). Also
    `Validate`, `apiKey`, `RateLimitKey`, `secrets(req)`.
  - `errors.go`: the `ErrorType*` constants and the retry delays. `transientError` sets
    `NextRetryDelay` 1s×2 up to 30s, counting only Transient failures in a row; the count passes
    between attempts as heartbeat details, and a RateLimited error resets it to 0.
    `rateLimitedError` waits the rest of the window. There is no loop in the activity.
  - `allowlist.go`: `AllowedDomainsEnvVar`, read by `New()` into `HTTPActivities.domains` (nil =
    any host); `CheckDomain`, a method because the list lives on `HTTPActivities`, called by the
    operation and again by `SendRequest` before sending. Matches the lower-cased host without port
    or trailing dot, exactly or as a subdomain at a dot boundary.
  - `timeout.go`: `MaxDurationEnvVar` (default 25h, read by `New()` into
    `HTTPActivities.maxTotal`, exported as `MaxDuration()`; there is always a maximum), `callbackMargin` (10s), and
    `ScheduleToClose(req, header)`: no header → the maximum; else header − margin, capped at the
    maximum, or an error if under `timeoutSeconds`. The server rewrites the header at dispatch, so
    queue time is already subtracted.
  - `ratelimiter.go`: the `limiter`, owned by `HTTPActivities`. A 429 marks the key; while marked,
    `SendRequest` fails with `RateLimited` without sending; a 2xx clears it; `mark` evicts expired
    entries. It takes an explicit `now`. The window is read by `httpconnector.New()`, through
    `envMinutes`, which `MaxDurationEnvVar` shares.
- Secrets: `redact` replaces each secret from `secrets(req)` (including an `Authorization`
  variable's credentials without the scheme, and the base64 `user:key` for basic auth) raw,
  URL-escaped, Go-quoted and JSON-escaped, in transport and body-read errors and, through
  `redactResponse`, in the response's status, headers and body. `checkRedirect` is Go's
  default policy (10 redirects, re-enforced since setting `CheckRedirect` drops it) except that
  it doesn't follow https → http (`http.ErrUseLastResponse`, so the 3xx becomes a `ClientError`
  with its response); Go alone would send `Authorization` there in plaintext. It is a method on
  `HTTPActivities`, so it also refuses a hop to a host `allowedHost` rejects, with a
  `redirectNotAllowedError` that `SendRequest` turns into a non-retryable `ClientError` naming the
  host, with the 3xx's status and headers (body closed by the client) in its details. Otherwise Go keeps
  `Authorization` for the same host and subdomains, and sends `Referer` with any query key.
- Non-2xx errors carry the `SendRequestOutput` in `Details`. A body over `maxResponseBytes`
  (2 MiB) is a `ClientError` without its response, whatever the status.
- `cmd/worker` — the connector's process: `envconfig`, `worker.New` on `TaskQueue`, `Register`.
- `testcaller/` — test only: `CallerWorkflow(req, timeout)` (the Nexus operation timeout; 0 for
  none), a worker, a starter, and the end-to-end test.

### Tests

- `httpconnector/` — `SendRequest` in the SDK's activity test environment against `httptest`
  servers (helpers `runSendRequest`, `appErr`, `countingServer`), plus `Validate` and
  `categorizeError`. The limiter takes an explicit `now` (`ratelimiter_test.go`). Tests use
  `newWithWindow(testWindow)` rather than the environment variable.
- `testcaller/e2e_test.go` — starts a dev server with the `temporal` CLI (skipped if it isn't on
  the PATH, or with `-short`), creates both namespaces and the endpoint, runs both workers
  in-process, and runs `CallerWorkflow` against `httptest` servers: success, a retried 503,
  retries stopping before a 15s caller timeout with the connector's error, a caller timeout too
  short for one attempt rejected at start, a 404 `ClientError` with its response, an
  `InvalidRequest` and a domain not allowed, both rejected at start. It sets the allowed domains
  to `127.0.0.1`.
