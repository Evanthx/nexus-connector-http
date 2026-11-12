# Usage

Calling the connector from Go and other languages, the full options table, API keys and secrets, and allowed domains.

## Call from a Go workflow

Use the generated types in `generated/httpapi`. [`testcaller/workflow.go`](../testcaller/workflow.go) is a complete working example.

The input has a field for each part of an HTTP request (method, URL, headers, query parameters, body, credentials). Only `url` is required; every other field has a default. The output has each part of the response: status code and text, every header with all its values, and the body.

```go
c := workflow.NewNexusClient("http-endpoint", httpapi.HTTPService.ServiceName)

req := httpapi.SendRequestInput{
    URL:    "https://api.example.com/items",           // required
    Method: new(httpapi.SendRequestInputMethodPost),   // optional; GET if unset
    Headers: &httpapi.SendRequestInputHeaders{AdditionalProperties: map[string]string{
        "Content-Type": "application/json",
    }},
    QueryParams: &httpapi.SendRequestInputQueryParams{AdditionalProperties: map[string]string{
        "source": "workflow",
    }},
    Body:         new(`{"name":"widget"}`),               // optional
    Username:     new("reporting-bot"),                   // optional; basic auth
    APIKeyEnvVar: new("HTTP_CONNECTOR_SECRET_ITEMS"),     // optional
    // APIKeyParam: new("api_key"),                       // optional; puts the key in the query instead
    TimeoutSeconds: new(int64(10)),                       // optional; 30 if unset
}

var resp httpapi.SendRequestOutput
err := c.ExecuteOperation(ctx, httpapi.HTTPService.SendRequest, req, workflow.NexusOperationOptions{
    ScheduleToCloseTimeout: time.Hour, // the connector retries until 10 seconds before this
}).Get(ctx, &resp)
// On success: resp.StatusCode, resp.Status, resp.Headers, resp.Body, resp.DurationMillis.
// On failure: see docs/errors.md.
```

This sends `POST https://api.example.com/items?source=workflow` with the JSON body and basic auth: user `reporting-bot`, password the value of `HTTP_CONNECTOR_SECRET_ITEMS` read from the connector's environment.

## Call from another language

Use a client generated for your language from the connector's contract; see
[Typed clients](clients.md). Or send the input as JSON from any Nexus client. The same request as
JSON:

```json
{
  "url": "https://api.example.com/items",
  "method": "POST",
  "headers": {"Content-Type": "application/json"},
  "queryParams": {"source": "workflow"},
  "body": "{\"name\":\"widget\"}",
  "username": "reporting-bot",
  "apiKeyEnvVar": "HTTP_CONNECTOR_SECRET_ITEMS",
  "timeoutSeconds": 10
}
```

`body` is a string, so a JSON body is a string with the JSON escaped inside it.

## Call over HTTP

You can also start the operation directly with an HTTP POST to the server's Nexus endpoint. The URL takes the endpoint's ID (not its name). Start the dev server with `--http-port 7243` to fix the HTTP port:

```bash
ID=$(temporal operator nexus endpoint get --name http-endpoint --output json | jq -r .id)
curl --include \
  --request POST "http://localhost:7243/nexus/endpoints/$ID/services/HTTPService/sendRequest" \
  --header 'Content-Type: application/json' \
  --header 'Nexus-Request-Id: my-request-1' \
  --data '{"url": "https://httpbin.org/get"}'
temporal activity result --namespace http-connector --activity-id http-my-request-1
```

The response is `201 Created` with an operation token. The activity ID is `http-` plus the request ID you send.

## Options

Every field of `SendRequestInput`. A request with an unknown field or an unlisted method is rejected before the operation runs.

| Field | Required | Default | What it does |
|---|---|---|---|
| `url` | **Yes** | | Absolute `http` or `https` URL. |
| `method` | No | `GET` | `GET`, `HEAD`, `POST`, `PUT`, `PATCH`, `DELETE`, or `OPTIONS`, in upper case. |
| `headers` | No | none | Request headers. A value that starts with `HTTP_CONNECTOR_SECRET_` names an environment variable on the connector; the connector sends its value instead. See [API keys and secrets](#api-keys-and-secrets). |
| `queryParams` | No | none | Query parameters, added to any already in the URL. |
| `body` | No | none | Request body, as a string. |
| `username` | No | none | Basic auth username. The password is the API key from `apiKeyEnvVar`. Needs `apiKeyEnvVar`; cannot be combined with `apiKeyParam`. |
| `apiKeyEnvVar` | No | none | Name of an environment variable on the connector that holds an API key. Must start with `HTTP_CONNECTOR_SECRET_`. |
| `apiKeyParam` | No | none | Query parameter to put the API key in. If unset, the key goes in the `Authorization` header: as the basic auth password if `username` is set, otherwise as `Bearer <key>`. Needs `apiKeyEnvVar`. |
| `timeoutSeconds` | No | 30 | Timeout for one attempt, 1 to 60 seconds. |

`SendRequestOutput` has `statusCode`, `status` (such as `200 OK`), `headers` (each name with a list of values), `body`, and `durationMillis`.

## API keys and secrets

API keys never enter the request input or workflow history. A request names an environment variable on the connector worker (`apiKeyEnvVar`), and the connector reads it at request time. `apiKeyParam` says where the key goes in the request: a named query parameter, or, if unset, the `Authorization` header. In the header it is the basic auth password when the request has a `username` (for APIs such as Twilio and Mailgun that pair an account name with a secret), and otherwise a `Bearer` token.

The variable's name must start with `HTTP_CONNECTOR_SECRET_`. Without that prefix, any caller could cause the connector to forward any environment variable (such as its Temporal API key) to a URL the caller controls.

For example, with this in the connector's terminal:

```bash
export HTTP_CONNECTOR_SECRET_WEATHER=...
```

a request can use it:

```json
{"url": "https://api.weatherapi.com/v1/forecast.json",
 "queryParams": {"q": "Seattle", "days": "3"},
 "apiKeyEnvVar": "HTTP_CONNECTOR_SECRET_WEATHER", "apiKeyParam": "key"}
```

Any header value can also reference a secret: if the whole value starts with `HTTP_CONNECTOR_SECRET_`, it is the name of a variable, and the connector sends that variable's value as the header. The variable holds the entire header value, scheme included:

```bash
export HTTP_CONNECTOR_SECRET_ITEMS_AUTH='Bearer ...'
```

```json
{"url": "https://api.example.com/items",
 "headers": {"Authorization": "HTTP_CONNECTOR_SECRET_ITEMS_AUTH"}}
```

For an API that takes its key in its own header:

```json
{"url": "https://api.example.com/items",
 "headers": {"X-Api-Key": "HTTP_CONNECTOR_SECRET_ITEMS_KEY"}}
```

For Stripe-style auth where the key is the username and the password is empty, put the whole encoded header in the variable:

```bash
export HTTP_CONNECTOR_SECRET_STRIPE_AUTH="Basic $(printf 'sk_live_...:' | base64)"
```

Any other header value is sent as-is and stored in workflow history.

The connector also prevents keys from leaking in responses:

- **Echoes are redacted.** Some endpoints echo back what they received (httpbin does). Every secret value, and the credentials of a header variable without the scheme, is replaced with `[REDACTED]` in the response's status, headers, and body, and in error messages — raw, URL-escaped, quoted, and JSON-escaped. Base64-encoded values are not caught.
- **Redirects are off by default.** See [Redirects](#redirects).

## Redirects

By default the connector follows no redirects, not even to the same host. A 3xx fails as a non-retryable `ClientError` whose details hold the response, including its `Location`; the caller can request that URL itself if it should be followed.

Set `HTTP_CONNECTOR_ALLOW_REDIRECTS=true` on the connector worker to follow them, up to 10. Any value other than `true` or `false` (or another value Go's `strconv.ParseBool` accepts) makes the worker exit at startup. With redirects on:

- **Never to plaintext.** A redirect from `https` to a non-`https` URL is not followed; it is returned as a `ClientError`.
- **Only to allowed domains.** With `HTTP_CONNECTOR_ALLOWED_DOMAINS` set, every redirect is checked against it (see [Allowed domains](#allowed-domains)). Unset, a redirect can go to any host.
- **Secrets stay with the original site.** Once a redirect goes to a host that is neither the original host nor a subdomain of it, Go drops the `Authorization` and `Cookie` headers, and the connector also drops every header filled from an `HTTP_CONNECTOR_SECRET_*` variable. They stay dropped if a later redirect comes back. `Referer`, which would carry a query-parameter API key, is never sent on a redirect.
- **Literal headers follow.** A header with a literal value, such as `"X-Api-Key": "abc"`, is sent to every host the redirect reaches, as Go, curl and Python `requests` do. Use a secret variable for any credential.

## Allowed domains

Set `HTTP_CONNECTOR_ALLOWED_DOMAINS` on the connector worker to a comma-separated list of domains, such as `temporal.io, google.com, stripe.com`. Spaces are ignored; case does not matter. A domain allows itself and all its subdomains: `temporal.io` allows `docs.temporal.io` but not `eviltemporal.io`. An IPv4 address such as `10.0.0.5` allows only that exact address. Each entry must be a bare domain with no scheme, port, path, or wildcard. An invalid or empty entry makes the worker exit at startup.

Unset or empty, the connector calls any `http` or `https` URL, including addresses on the connector's own network.

A request to a disallowed host fails with `InvalidRequest`:

```
domain "example.com" is not on the connector's list of allowed domains
```

Followed redirects are checked too. A redirect to a host not on the list is not followed and fails as a `ClientError`:

```
https://docs.example.com/start redirected to "evil.example.net", which is not on the connector's
list of allowed domains (HTTP_CONNECTOR_ALLOWED_DOMAINS), so the redirect wasn't followed
```

The error details hold the redirect's status and headers, including the `Location` it pointed to. The list checks DNS names, not resolved addresses: an allowed name that resolves to an internal address is still called.

## APIs that report errors in a 200

The connector decides what to do from the HTTP status code alone. For an API that answers
`200 OK` with an error or a rate limit in the body, see
[How it works: APIs that don't follow the standard](how-it-works.md#apis-that-dont-follow-the-standard).
