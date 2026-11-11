# Testing

Running the test workflow end to end, and running the unit and integration test suites.

## Test workflow

The `testcaller/` directory holds `CallerWorkflow`, which runs in the `http-testcaller` namespace and calls the connector through the Nexus endpoint, the way a real caller would. Complete [Setup](setup.md) first (all three steps).

### 1. Start the test caller's worker

In a new terminal:

```bash
go run ./testcaller/worker
```

### 2. Make a request

```bash
go run ./testcaller/starter
```

This starts `CallerWorkflow` with a GET to `https://httpbin.org/get`, adding the query parameter `source=temporal-nexus` and the header `X-Demo-Header: nexus-connector-http`. httpbin echoes both back:

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

A failed request prints its error type, such as `Request failed with ClientError: https://httpbin.org/status/404 returned status 404`.

Starter flags choose a different request:

| Flag | Default | Meaning |
|---|---|---|
| `-url` | `https://httpbin.org/get` | URL to request |
| `-method` | `GET` | HTTP method |
| `-body` | (none) | Request body |
| `-timeout` | (none) | Nexus operation timeout, e.g. `10m`; the connector retries until shortly before it. |

```bash
go run ./testcaller/starter -url https://httpbin.org/post -method POST -body '{"hello":"world"}'
```

You can also start the workflow directly from the CLI. Its inputs are a `SendRequestInput` as JSON and the Nexus operation timeout in nanoseconds (0 for none):

```bash
temporal workflow start \
  --namespace http-testcaller \
  --task-queue http-testcaller \
  --type CallerWorkflow \
  --workflow-id my-request \
  --input '{"url":"https://httpbin.org/post","method":"POST","headers":{"Content-Type":"application/json"},"body":"{\"hello\":\"world\"}"}' \
  --input 0

temporal workflow result --namespace http-testcaller --workflow-id my-request
```

### 3. See each kind of failure

| Command | What happens |
|---|---|
| `go run ./testcaller/starter -url https://httpbin.org/status/404` | Fails at once with `ClientError`. |
| `go run ./testcaller/starter -url ftp://example.com` | Fails at once with `InvalidRequest`. No activity is started. |
| `go run ./testcaller/starter -url https://httpbin.org/status/503 -timeout 10m` | Retried with backoff until 10 seconds before the 10 minutes are up, then fails with `Transient`. Without the flag it would retry for up to 25 hours. |
| `go run ./testcaller/starter -url https://httpbin.org/status/429 -timeout 10m` | Retried after the rate-limit window (5 minutes by default), then fails with `RateLimited` once another window wouldn't fit in the 10 minutes — after about 5 minutes. |

While a request is being retried you can inspect it as a standalone activity:

```bash
temporal activity list --namespace http-connector
temporal activity describe --namespace http-connector --activity-id <activity ID from the list>
```

`describe` shows the attempt number, the last failure with its error type and HTTP response, and when the next attempt is scheduled.

## Test suites

```bash
make test                                                    # all tests with the race detector
go test -short ./...                                         # skip the end-to-end test
make ci                                                      # vet, workflowcheck, lint, and tests (as CI does)
go test ./httpconnector -run TestSendRequest_429CachesKey -v # single test
```

The test suites are:

- **`httpconnector/`** runs `SendRequest` in the SDK's activity test environment against local `httptest` servers. It covers GET and POST responses, status classification, retry delays, timeouts, the rate-limit cache, API keys, redirects, redaction, and input validation.

- **`testcaller/e2e_test.go`** starts a dev server with the `temporal` CLI, creates both namespaces and the endpoint, runs the connector and the test caller in-process, and makes requests across the namespaces: a successful request, a retried 503, three 503s backing off 1 s / 2 s / 4 s, retries stopping before the caller's timeout, a 404 `ClientError` with its response in the error details, a caller timeout too short for one attempt rejected at start, an invalid request, and a domain not in the allowed list. It is skipped automatically if `temporal` is not on `PATH`, or when run with `-short`.
