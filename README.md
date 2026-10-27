# nexus-connector-http

A Temporal Nexus connector that makes HTTP requests. A workflow calls the connector's
`sendRequest` operation with a description of a request. The connector makes the request, retries
what is worth retrying, backs off from rate-limited endpoints, and returns the response. It only
sends requests; it never polls anything.

The connector is deliberately generic, since every HTTP integration is different. It is written in
Go, but callers reach it through Nexus, so they can be written in any language Temporal supports.
It is useful in three ways:

1. **Durable HTTP calls, ready to use.** Any HTTP request a workflow makes through the connector
   gets durable execution: errors worth retrying are retried with backoff, rate-limited endpoints
   are backed off from, timeouts are enforced, and API keys stay out of workflow history. Each
   integration gets this without writing it again.
2. **A drop-in HTTP client for Go.** A thin adapter with `*http.Client`'s
   `Do(*http.Request) (*http.Response, error)` method, the `HttpRequestDoer` interface, can stand
   in for `http.Client` wherever code sends requests through that method. Used as a library this
   way, the connector is a very easy way to make existing HTTP calls durable: the calling code
   stays the same, and each request goes through the connector.
3. **Generated connectors for any API.** Done as a proof of concept in
   [nexus-connector-openapi](https://github.com/Evanthx/nexus-connector-openapi). From an API's OpenAPI spec, it generates
   a Nexus service with one typed operation per API operation, each backed by a standalone
   activity that sends its request through this connector's code, used as a library. nexgen turns
   the service's contract into typed callers in Go, Java, Python and TypeScript, so a workflow calls
   an operation such as `WeatherService.getWeatherByZip` instead of building HTTP requests, and
   nobody writes HTTP code. It also shows two ways to wrap several calls in one operation: an
   activity that starts a job and polls for its result, and a workflow that makes several calls.

All three assume the API answers with standard HTTP status codes. See [Next step](#next-step) for
the plan to handle an API that doesn't, such as one that reports errors or rate limits in the body of
a `200 OK`.

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

- [How to use](#how-to-use): calling the connector, its options, what it does, and running it.
- [See it in action](#see-it-in-action): the test workflow.
- [How the code works](#how-the-code-works): the packages, and the path of a request through them.
- [Next step](#next-step): connectors for APIs that don't follow the HTTP standard.

AI agents: see [AGENTS.md](AGENTS.md) for how to set up, run and call this connector, and how the
code works.

## How to use

A workflow in any namespace calls the connector through its Nexus endpoint: service `HTTPService`,
operation `sendRequest`, with a `SendRequestInput`, and gets a `SendRequestOutput` back. The call
is asynchronous: the workflow waits while the connector makes the request and retries it. The
connector must be running and reachable through an endpoint; see
[Run the connector](#run-the-connector).

### Call it from a Go workflow

Use the generated types in `generated/httpapi`. [`testcaller/workflow.go`](testcaller/workflow.go)
is a complete workflow.

**This example sets every option** except `apiKeyParam`, which can't be used with `username` (see
[Options](#options)). The input has a field for each part of an HTTP request: the
method, URL, query parameters, headers, body, and credentials. So a workflow can make almost any API call,
not just a fixed set. Likewise, the output has each part of the response:
the status code and text, every header with all its values, and the body. So the workflow gets
back whatever the API returns. Only `URL` is required; every other field is optional and has a
default. The limits are the seven methods in [Options](#options), 60 seconds per attempt, and
text bodies of up to 2 MiB (see [Known limitations](#known-limitations)).

```go
c := workflow.NewNexusClient("http-endpoint", httpapi.HTTPService.ServiceName)

req := httpapi.SendRequestInput{
    URL:    "https://api.example.com/items", // required
    Method: new(httpapi.SendRequestInputMethodPost), // optional; GET if unset
    Headers: &httpapi.SendRequestInputHeaders{AdditionalProperties: map[string]string{ // optional
        "Content-Type": "application/json",
    }},
    QueryParams: &httpapi.SendRequestInputQueryParams{AdditionalProperties: map[string]string{ // optional
        "source": "workflow",
    }},
    Body:                   new(`{"name":"widget"}`),          // optional
    Username:               new("reporting-bot"),              // optional; basic auth, with the API key as password
    APIKeyEnvVar:           new("HTTP_CONNECTOR_SECRET_ITEMS"), // optional; read on the connector
    // APIKeyParam:         new("api_key"),                    // optional; puts the key in the query instead
    TimeoutSeconds:         new(int64(10)),                    // optional; 30 if unset
    ScheduleToCloseSeconds: new(int64(3600)),                  // optional; 90000 (25 hours) if unset
}

var resp httpapi.SendRequestOutput
err := c.ExecuteOperation(ctx, httpapi.HTTPService.SendRequest, req, workflow.NexusOperationOptions{
    // How long this workflow waits. It must be longer than scheduleToCloseSeconds.
    ScheduleToCloseTimeout: httpconnector.ScheduleToClose(req) + 5*time.Minute,
}).Get(ctx, &resp)
// On success: resp.StatusCode, resp.Status, resp.Headers, resp.Body, resp.DurationMillis.
// On failure: see Errors below.
```

This sends `POST https://api.example.com/items?source=workflow` with the JSON body and basic auth:
user `reporting-bot`, password the API key read from `HTTP_CONNECTOR_SECRET_ITEMS` on the
connector. Each attempt can take 10 seconds, and failures are retried for up to an hour.

### Call it from another language, or over HTTP

Generate types from [`http.nexusrpc.yaml`](http.nexusrpc.yaml) in your language and call the
operation the same way. The same request as JSON:

```json
{
  "url": "https://api.example.com/items",
  "method": "POST",
  "headers": {"Content-Type": "application/json"},
  "queryParams": {"source": "workflow"},
  "body": "{\"name\":\"widget\"}",
  "username": "reporting-bot",
  "apiKeyEnvVar": "HTTP_CONNECTOR_SECRET_ITEMS",
  "timeoutSeconds": 10,
  "scheduleToCloseSeconds": 3600
}
```

`body` is a string, so a JSON body is escaped inside it.

You can also start the operation with an HTTP POST to the server's Nexus endpoint. The URL takes
the endpoint's ID, not its name. The response is `201 Created` with an operation token; the
request's result is the activity's, whose ID is `http-` plus the request ID you send:

```bash
ID=$(temporal operator nexus endpoint get --name http-endpoint -o json | jq -r .id)
curl -i -X POST "http://localhost:7243/nexus/endpoints/$ID/services/HTTPService/sendRequest" \
  -H 'Content-Type: application/json' -H 'Nexus-Request-Id: my-request-1' \
  -d '{"url": "https://httpbin.org/get"}'
temporal activity result --namespace http-connector --activity-id http-my-request-1
```

This needs the dev server's HTTP port; start it with `--http-port 7243`, since it is otherwise
random.

### Options

Every field of `SendRequestInput`. A request with a field the contract doesn't define, or a
method it doesn't list, is rejected; see [Errors](#errors).

| Field | Required | Default | What it does |
|---|---|---|---|
| `url` | **Yes** | | Absolute `http` or `https` URL. |
| `method` | No | `GET` | `GET`, `HEAD`, `POST`, `PUT`, `PATCH`, `DELETE`, or `OPTIONS`, in upper case. |
| `headers` | No | none | Request headers. An `Authorization` value that names a secret variable is replaced by its value; see [API keys and secrets](#api-keys-and-secrets). |
| `queryParams` | No | none | Query parameters, added to any already in the URL. |
| `body` | No | none | Request body, as a string. |
| `username` | No | none | Basic auth username. The password is the API key from `apiKeyEnvVar`, so it never enters history. Needs `apiKeyEnvVar`; can't be used with `apiKeyParam`. |
| `apiKeyEnvVar` | No | none | Name of an environment variable on the connector that holds an API key. Must start with `HTTP_CONNECTOR_SECRET_`. |
| `apiKeyParam` | No | none | Query parameter to put the API key in. If unset, the key goes in the `Authorization` header: as the basic auth password with a `username`, else as `Bearer <key>`. Needs `apiKeyEnvVar`. |
| `timeoutSeconds` | No | 30 | Timeout for one attempt, 1 to 60 seconds. |
| `scheduleToCloseSeconds` | No | 90000 (25 hours) | Time for the whole call, every attempt and every wait between them: from `timeoutSeconds` to 90000. Must be shorter than the caller's operation timeout. |

`SendRequestOutput` has `statusCode`, `status` (such as `200 OK`), `headers` (each name with a list
of values), `body`, and `durationMillis`.

### What the connector does

#### The request

The connector sends one HTTP request per attempt, with the URL, method, headers, query parameters
and body given. Redirects are followed, up to 10, except from `https` to a URL that isn't `https`. It returns a
**2xx** answer as the operation's result. Any other answer is an error that carries the response,
and is retried or not depending on its status, below.

#### Errors

Every failure is an `ApplicationError` whose type is one of these:

| Type | When | Retried |
|---|---|---|
| `InvalidRequest` | The request can't be made: a bad URL, a `timeoutSeconds` outside 1–60, a `scheduleToCloseSeconds` out of range or not shorter than the caller's operation timeout, a header name or value that isn't valid HTTP (such as one with a newline), or a bad or missing API key variable. It is never sent. | No |
| `ClientError` | The endpoint answered with a status that sending the same request again won't change: any 4xx not listed under `Transient` or `RateLimited`, 501, 505, 511, or a 3xx that wasn't followed (such as a 304, or a redirect from `https` to `http`). Also a response body over 2 MiB; the connector stops reading after 2 MiB. | No |
| `Transient` | The endpoint answered 408, 409, 420, 421, 423, 424, 425, or any 5xx except 501, 505 and 511, or the request timed out or failed on the network. | With backoff: 1s, doubling, at most 30s apart |
| `RateLimited` | The endpoint answered 429, or another request with the same rate-limit key did within the [rate-limit window](#rate-limiting). | After the rest of that window |

Retries stop after the request's `scheduleToCloseSeconds`; the caller then gets the last failure.
`InvalidRequest` is caught when the operation starts, so no activity is created: the caller's
operation fails with a `BAD_REQUEST` handler error whose cause is the `InvalidRequest` error.

A caller finds the type with `errors.As`, wherever in the error chain it is. For an error from an
HTTP response, the error's details hold the `SendRequestOutput`, so the caller can read the status
and body:

```go
var appErr *temporal.ApplicationError
if errors.As(err, &appErr) && appErr.Type() == httpconnector.ErrorTypeClientError {
    var resp httpapi.SendRequestOutput
    _ = appErr.Details(&resp)
    // resp.StatusCode, resp.Body ...
}
```

A request that doesn't match the contract fails before the operation runs, so it isn't one of
these types. A Go caller using `generated/httpapi` can't send one: encoding it fails with a
`PayloadValidationError`, whose details list each violation. A caller that sends its own JSON gets
a `BAD_REQUEST` handler error saying `unable to decode: Payload validation failed`, which doesn't
say which field was wrong.

#### Timeouts

| Setting | Set by | Bounds | Default |
|---|---|---|---|
| `timeoutSeconds` | the request | one HTTP attempt, 1 to 60 seconds | 30 |
| `scheduleToCloseSeconds` | the request | every attempt and every wait between them, up to 25 hours | 90000 (25 hours) |
| The Nexus operation's `ScheduleToCloseTimeout` | the caller | how long the caller waits; must be longer than `scheduleToCloseSeconds` | none |
| `HTTP_CONNECTOR_RATE_LIMIT_MINUTES` | the connector worker | how long a 429 holds back requests with the same key | 5 minutes |

`timeoutSeconds` can't be over 60 because an activity attempt can't take longer than that. When
`scheduleToCloseSeconds` is up, the call fails with its last error.

Until then, failures are retried by type: `Transient` with backoff (1s, doubling, then every 30
seconds), `RateLimited` once per rate-limit window. The backoff counts only `Transient` failures in
a row, so the first `Transient` failure after a rate limit waits 1s again. For example, 503, 503,
429, 503 waits 1s, 2s, the window, then 1s. With the 25-hour default, an endpoint that keeps
answering 503 is tried about every 30 seconds for 25 hours.

The caller's own timeout must be longer than `scheduleToCloseSeconds`. The caller's server tells
the connector how long the operation has left, and if `scheduleToCloseSeconds` isn't shorter, the
operation fails at once with `InvalidRequest`:

```
scheduleToCloseSeconds is 90000 (25h0m0s), but the caller's Nexus operation times out in 1h0m0s:
set scheduleToCloseSeconds lower than that, or give the operation a longer timeout
```

Without that check, the caller would time out first and get a timeout rather than the connector's
error, while the connector kept trying, since a caller's timeout doesn't cancel the activity. An
operation with no timeout is always accepted. Cancelling the caller's workflow does cancel the
operation and its activity: an attempt already in flight finishes, but no more are made.

#### Rate limiting

The connector keeps an in-memory cache of rate-limited keys. A key is the URL plus who is calling:
the basic auth username, else the API key variable's name, else the `Authorization` header's
variable name, else `anon`.

- When an endpoint answers **429**, the key is cached for the rate-limit window, and the request
  fails with `RateLimited`, to be tried again when the window ends.
- While a key is cached, every request for it fails with `RateLimited` **without being sent**,
  and is tried again when the window ends.
- A **2xx** answer clears the key.

The window is `HTTP_CONNECTOR_RATE_LIMIT_MINUTES` on the connector worker: a whole number of
minutes, at least 1, 5 by default. Any other value stops the worker at startup.

So however many callers are waiting, one connector worker tries a rate-limited endpoint about once
per window. This isn't strict: the waiting requests all retry when the window ends, and if they
check at the same moment, they all go out. If the first one's 429 comes back before the others
check, they wait another window.

The server doesn't schedule a retry that would start after `scheduleToCloseSeconds`, so the window
and `scheduleToCloseSeconds` together bound how many times a request can try. With both defaults, a
request that keeps getting 429s is sent about every 5 minutes for 25 hours. With
`scheduleToCloseSeconds` at 600, it is sent twice and fails after about 5 minutes. A window as long
as `scheduleToCloseSeconds` or longer means it is sent once.

#### API keys and secrets

API keys never go in the request, so they never reach workflow history. A request names an
environment variable on the connector worker (`apiKeyEnvVar`), and the connector reads it when it
makes the request. `apiKeyParam` says where the key goes: the named query parameter, or, if unset,
the `Authorization` header. There it is the basic auth password if the request has a `username`
(for APIs such as Twilio and Mailgun, which pair an account name with a secret), and otherwise a
`Bearer` token.

The variable's name must start with `HTTP_CONNECTOR_SECRET_`. A caller chooses both the URL and the
variable, so without that rule any caller could send any of the worker's environment, such as its
Temporal API key, to a server of its own. For example, with this in the connector's terminal:

```bash
export HTTP_CONNECTOR_SECRET_WEATHER=...
```

a request can use it like this:

```json
{"url": "https://api.weatherapi.com/v1/forecast.json",
 "queryParams": {"q": "Seattle", "days": "3"},
 "apiKeyEnvVar": "HTTP_CONNECTOR_SECRET_WEATHER", "apiKeyParam": "key"}
```

An `Authorization` header can also come from the connector's environment: if its whole value
starts with `HTTP_CONNECTOR_SECRET_`, it is the name of a variable, and the connector sends that
variable's value as the header instead. The variable holds the entire header value, scheme
included:

```bash
export HTTP_CONNECTOR_SECRET_ITEMS_AUTH='Bearer ...'
```

```json
{"url": "https://api.example.com/items",
 "headers": {"Authorization": "HTTP_CONNECTOR_SECRET_ITEMS_AUTH"}}
```

This covers any other layout too. For basic auth where the key is the username and the password
is empty, as with Stripe, put the whole header in the variable:

```bash
export HTTP_CONNECTOR_SECRET_STRIPE_AUTH="Basic $(printf 'sk_live_...:' | base64)"
```

Any other `Authorization` value is sent as it is, and so is stored in workflow history. If both
set the header, `apiKeyEnvVar` without `apiKeyParam` wins over the `headers` value. A variable
without the prefix, or one that is unset or empty, fails the request with `InvalidRequest`.

The connector also keeps keys from leaking on the way back:

- **Echoes are redacted.** An endpoint can send back what it was sent (httpbin does). Every
  secret value, and the credentials of an `Authorization` variable without their scheme, is
  replaced with `[REDACTED]` in the response's status, headers and body, and in error messages:
  as is, escaped in a URL, quoted, or escaped in a JSON string. A key encoded any other way, such
  as in base64, isn't caught.
- **Redirects keep credentials where the caller sent them.** A redirect from `https` to a URL that
  isn't `https` (such as `http`) isn't followed; the redirect itself is the response, and fails as a `ClientError`. The
  `Authorization` header (an API key, basic auth, or any other) is sent only to the first URL's
  scheme, host and port, and no redirect sends a `Referer`.

#### Known limitations

- **The rate-limit cache is in memory.** It is lost when the worker restarts, and each worker has
  its own, so each probes a rate-limited endpoint on its own. A production version would use a
  shared store, such as Redis with a TTL.
- **No early wake-up.** When a rate-limited key clears, waiting requests still wait for their next
  scheduled attempt.
- **Text bodies only.** Bodies are strings carried in JSON, so binary content would need base64.
- **Response size.** The whole body is returned in the activity's result, so a body over 2 MiB
  fails with `ClientError`, and a smaller one near Temporal's payload size limit (2 MB by
  default) can't be returned.
- **Any URL.** The connector will call any `http` or `https` URL, including addresses on its own
  network. Run it where that is acceptable, or add an allowlist.

### Run the connector

#### Prerequisites

- **Go 1.26** or later.
- **Temporal CLI 1.9.1** or later (it includes Temporal Server 1.32). Check with
  `temporal --version`.
- Network access from the connector to the URLs you call. The test workflow calls
  `https://httpbin.org`.

Run every command from the repository root. Steps 1 and 3 keep running; give each its own
terminal.

#### 1. Start Temporal

```bash
temporal server start-dev \
  --dynamic-config-value activity.enableCallbacks=true \
  --namespace http-connector \
  --namespace http-testcaller
```

This creates two namespaces: `http-connector` for the connector, and `http-testcaller` for the
test workflow that calls it. `activity.enableCallbacks` lets a standalone activity complete a Nexus
operation, which is how every call through the connector finishes. Without it, every call fails
with `handler error (BAD_REQUEST)`, and the connector logs `completion callbacks are not enabled
for this namespace`. The Web UI is at <http://localhost:8233>.

#### 2. Create the Nexus endpoint

```bash
temporal operator nexus endpoint create \
  --name http-endpoint \
  --target-namespace http-connector \
  --target-task-queue nexus-connector-http \
  --description-file ./endpoint-description.md
```

Callers in any namespace reach the connector through this endpoint. The dev server keeps endpoints
in memory, so create it again whenever you restart the server.

#### 3. Start the connector

```bash
go run ./cmd/worker
```

It logs `Starting worker on namespace "http-connector", task queue "nexus-connector-http"`. Export
any API keys in this terminal first (see [API keys and secrets](#api-keys-and-secrets)).

#### Environment variables

On the connector worker (`cmd/worker`):

| Variable | Meaning |
|---|---|
| `HTTP_CONNECTOR_SECRET_*` | API keys that requests can name in `apiKeyEnvVar`, or as an `Authorization` header's value. |
| `HTTP_CONNECTOR_RATE_LIMIT_MINUTES` | How long a key stays rate-limited after a 429, in whole minutes. Defaults to 5. |
| `TEMPORAL_*` | How to connect to Temporal; see below. |

Callers' workers need only their own `TEMPORAL_*` settings.

#### Connecting to Temporal Cloud or another server

The connector, the test caller's worker, and the starter all connect the same way the `temporal`
CLI does, with the SDK's `envconfig` package. They read the `TEMPORAL_*` environment variables and
the CLI's profile file (`temporal config set` writes it). With neither, they connect to
`localhost:7233`. The namespace defaults to `http-connector` for the connector and
`http-testcaller` for the test caller.

| Variable | Meaning |
|---|---|
| `TEMPORAL_ADDRESS` | `host:port` of the Temporal frontend |
| `TEMPORAL_NAMESPACE` | Namespace |
| `TEMPORAL_API_KEY` | API key |
| `TEMPORAL_TLS_CLIENT_CERT_PATH`, `TEMPORAL_TLS_CLIENT_KEY_PATH` | mTLS client certificate and key |
| `TEMPORAL_TLS_SERVER_CA_CERT_PATH`, `TEMPORAL_TLS_SERVER_NAME` | TLS verification |
| `TEMPORAL_PROFILE` | Profile to use from the profile file |

Give each process its own `TEMPORAL_NAMESPACE`: the connector's namespace for `cmd/worker`, the
caller's for the test caller.

The connector's namespace needs standalone activities with completion callbacks
(`activity.enableCallbacks`). On a self-hosted server, set that dynamic config value. On Temporal
Cloud, check with Temporal that it is available for your namespace, and create the namespaces and
the endpoint in the Cloud UI or with `tcld`.

## See it in action

The `testcaller` directory holds a workflow, `CallerWorkflow`, that runs in the `http-testcaller`
namespace and makes a request through the endpoint, the way a real caller would. It is for testing
only. First do the three steps of [Run the connector](#run-the-connector).

### 1. Start the test caller's worker

In another terminal:

```bash
go run ./testcaller/worker
```

### 2. Make a request

```bash
go run ./testcaller/starter
```

It starts `CallerWorkflow` with a GET to `https://httpbin.org/get`, with the query parameter
`source=temporal-nexus` and the header `X-Demo-Header: nexus-connector-http`, and prints the
response. httpbin echoes both back:

```
2026/10/16 16:35:58 Started workflow http-testcaller-20261016163558
2026/10/16 16:35:59 Response: 200 OK (664 ms)
{
  "args": {
    "source": "temporal-nexus"
  },
  "headers": {
    ...
    "X-Demo-Header": "nexus-connector-http"
  },
  ...
  "url": "https://httpbin.org/get?source=temporal-nexus"
}
```

A failed request prints its error type, such as
`Request failed with ClientError: https://httpbin.org/status/404 returned status 404`.

The starter's flags (`-url`, `-method`, `-body`, `-schedule-to-close-seconds`) choose another
request:

```bash
go run ./testcaller/starter -url https://httpbin.org/post -method POST -body '{"hello":"world"}'
```

You can also start the workflow with any request from the CLI. Its input is a `SendRequestInput`
as JSON (see [Options](#options)):

```bash
temporal workflow start \
  --namespace http-testcaller \
  --task-queue http-testcaller \
  --type CallerWorkflow \
  --workflow-id my-request \
  --input '{"url":"https://httpbin.org/post","method":"POST","headers":{"Content-Type":"application/json"},"body":"{\"hello\":\"world\"}"}'

temporal workflow result --namespace http-testcaller --workflow-id my-request
```

### 3. See each kind of failure

| Run | What happens |
|---|---|
| `go run ./testcaller/starter -url https://httpbin.org/status/404` | Fails at once with `ClientError`. |
| `go run ./testcaller/starter -url ftp://example.com` | Fails at once with `InvalidRequest`. No activity is started. |
| `go run ./testcaller/starter -url https://httpbin.org/status/503 -schedule-to-close-seconds 600` | Retried with backoff for 10 minutes, then fails with `Transient`. Without the flag it would keep trying for 25 hours. |
| `go run ./testcaller/starter -url https://httpbin.org/status/429 -schedule-to-close-seconds 600` | Tried again after the rate-limit window (5 minutes by default), then fails with `RateLimited` once another window wouldn't fit in the 10 minutes: after about 5 minutes. |

While a request is being retried, the connector's namespace shows it as a standalone activity:

```bash
temporal activity list --namespace http-connector
temporal activity describe --namespace http-connector --activity-id <activity ID from the list>
```

`describe` shows the attempt number, the last failure with its type and the HTTP response, and
when the next attempt is due.

## How the code works

### Packages

| Package | What is in it |
|---|---|
| `httpconnector/` | The connector. `http.go`: `Register`, which registers the Nexus service and the activity; the `SendRequest` activity; its activity options; status classification; redirects and redaction. `request.go`: turning a `SendRequestInput` into an `*http.Request`, validation, API keys, and the rate-limit key. `errors.go`: the error types and the retry delays they set. `ratelimiter.go`: the rate-limit cache. |
| `cmd/worker/` | The connector's process: connects to Temporal, calls `httpconnector.Register`, and runs the worker. |
| `generated/httpapi/` | Go types for the contract, generated from `http.nexusrpc.yaml` with [nexgen](https://github.com/temporalio/nexgen). Don't edit it. |
| `testcaller/` | Testing only: `CallerWorkflow`, its worker (`worker/`), a starter (`starter/`), and the end-to-end test. |

At the top level, `http.nexusrpc.yaml` is the contract and `endpoint-description.md` is the
endpoint's description.

### The path of a request

1. **Registration.** `cmd/worker` creates a worker on the `nexus-connector-http` task queue and
   calls `httpconnector.Register`. `Register` makes one `HTTPActivities`, which owns the
   rate-limit cache, and registers both the `HTTPService` Nexus service and its `SendRequest`
   activity with it, so every call on the worker shares one cache. It reads
   `HTTP_CONNECTOR_RATE_LIMIT_MINUTES`, and fails on an invalid value.

2. **Decoding.** The caller's request arrives through the endpoint. The generated `UnmarshalJSON`
   decodes it into a `SendRequestInput`, rejecting anything the contract doesn't allow.

3. **The operation starts an activity.** The `sendRequest` operation, defined inside `Register`,
   does no HTTP work. It calls `Validate`, which builds the request without sending it, and
   `CheckCallerTimeout`, which compares `scheduleToCloseSeconds` with the caller's remaining
   timeout from the `operation-timeout` header. Either failing returns a `BAD_REQUEST` handler
   error with an `InvalidRequest` cause. Otherwise it starts `SendRequest` as a **standalone
   activity** with `temporalnexus.StartActivity`, using `ActivityOptions`: the ID
   `http-<Nexus request ID>`, so a retried start reaches the same activity;
   `StartToCloseTimeout` 60 seconds; `ScheduleToCloseTimeout` from `scheduleToCloseSeconds`; and a
   retry policy with no attempt limit. The operation returns at once. There is no workflow on the
   connector's side.

4. **Each attempt.** The server runs `SendRequest` once per attempt. It:
   - builds the request (`build`): `parseURL`, `checkTimeouts`, `setQuery` (query parameters and
     a query API key), then the `*http.Request` and `setHeaders` (headers, the `Authorization`
     variable, the API key as basic auth or a Bearer token, then header validation). A failure here is a
     non-retryable `InvalidRequest`;
   - checks the rate-limit cache for the request's key (`RateLimitKey`). If it is cached, it fails
     with `RateLimited` without sending;
   - sends the request with an `http.Client` whose timeout is `timeoutSeconds` and whose
     `checkRedirect` enforces the redirect rules. A transport error is `Transient`, with its
     message passed through `redact`;
   - reads up to 2 MiB of body, and passes the response through `redactResponse`;
   - returns the response for a 2xx, clearing the key. Otherwise `categorizeError` picks the
     type: a 429 marks the key in the cache, and `ClientError` is non-retryable. The error
     carries the response in its details.

5. **Retries.** The activity has no retry loop; the server retries it. Each retryable error sets
   its own next delay with `NextRetryDelay` (`errors.go`): `rateLimitedError` waits the rest of the
   window, and `transientError` backs off from 1s. To count `Transient` failures in a row, each
   attempt records the count as heartbeat details, which the server hands to the next attempt; a
   rate limit resets it to 0. The retry policy's own backoff applies only when an attempt times
   out.

6. **Completion.** When the activity succeeds, or fails for good, the server completes the
   caller's Nexus operation with its result or failure through a completion callback, which
   `activity.enableCallbacks` turns on.

### Tests

```bash
make test        # all tests, with the race detector
go test -short ./...   # skip the end-to-end test
make ci          # vet, workflowcheck, lint (golangci-lint) and the tests, as CI does
```

- `httpconnector/` tests run `SendRequest` in the SDK's activity test environment against local
  `httptest` servers: GET and POST responses, classification, retry delays, timeouts, the
  rate-limit cache, API keys, redirects, redaction, and validation.
- `testcaller/e2e_test.go` starts a dev server with the `temporal` CLI, creates both namespaces and
  the endpoint, runs the connector and the test caller, and makes requests across the namespaces:
  a success, a retried 503, three 503s backing off 1s, 2s, then 4s, retries stopping at
  `scheduleToCloseSeconds`, a 404 with its response, a request that outlasts the caller's timeout,
  and an invalid request. It is skipped if `temporal` isn't on the `PATH`.

### Changing the contract

Edit `http.nexusrpc.yaml`, then run `make generate` and commit the regenerated `generated/httpapi`.
`make generate` downloads the pinned nexgen release into `.build/` the first time. CI fails if the
generated code is out of date.

The service's and operation's `fqn` values are the names callers use on the wire. Keep them:
without them, nexgen would name the operation `SendRequest`, and existing callers would no longer
reach it.

## Next step

### APIs that don't follow the standard

The connector decides what to do from the status code alone, assuming each code means what the
HTTP standard says: a 429 is a rate limit, a 503 is worth retrying, a 404 isn't (see
[Errors](#errors)). Most APIs follow this, but not all. Some answer `200 OK` and put the real
outcome in the body, such as an error or a rate limit.

The plan is to add a hook to the connector that runs on each response before the connector
decides what the response means. That makes a connector for such an API simple to write: a thin
shell that uses this connector's code as a library. The shell has its own Nexus service and
operation, and its operation starts the `SendRequest` activity itself, on the shell's own worker.
It doesn't call this connector's Nexus service, so a call makes one Nexus hop, not two. The shell:

- takes inputs specific to the API, for a better experience for its callers, and turns them into
  a `SendRequestInput`; and
- passes in a function that reads each response and can return an error type for it, such as
  `RateLimited` for a 200 whose body says the request was rate limited.

`SendRequest` then treats the response as if the status code had said so: retrying with backoff,
backing off from the rate limit, or failing without a retry. When the function returns nothing,
the status code decides, as now, so the function only has to recognize what is different about
the API.

The hook isn't there yet. Everything else a shell needs is already exported: `New` (the
activities, with their rate-limit cache), `Validate` and `CheckCallerTimeout` (the checks the
operation makes before starting the activity), `ActivityOptions`, and the `SendRequest` activity.
The OpenAPI proof of concept ([nexus-connector-openapi](https://github.com/Evanthx/nexus-connector-openapi)) is a shell of
this kind, generated rather than written by hand. Its activities call `SendRequest` as a function
inside their own activity, rather than starting it, so they can decode the response into the
operation's output type.

That gives two connectors exposed over Nexus: this generic one, and the API-specific one. The
API-specific connector offers an experience tailored to its API, and because it uses this
connector's code as a library, it gets the same error handling and the same behavior around
retries and rate limits without duplicating that code. Its rate-limit cache is its own, though,
since each worker has its own (see [Known limitations](#known-limitations)).
