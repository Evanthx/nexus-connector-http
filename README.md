![Nexus Connectors](docs/images/NexusConnectors.webp)


# Outbound HTTP Connector

## What are Nexus Connectors?

A Nexus connector lets a Temporal workflow work with a system outside Temporal, such as a message
broker, a payment provider, a chat platform, or an HTTP API, without writing that integration
itself. Each connector runs as a Temporal worker and offers its operations through a
[Nexus](https://docs.temporal.io/nexus) endpoint. Some operations map directly to one call on the
outside system; others wrap several calls into one durable step, such as creating, finalizing, and
sending an invoice. Because callers reach the connector through Nexus, it is written once and
called from any language Temporal supports, with [typed clients](docs/clients.md) generated from
the connector's contract.

Connectors lower the cost of adopting Temporal. Teams arriving from platforms that ship
hundreds of prebuilt connectors can reach the systems they already depend on without building
each integration by hand, and a service moving into Temporal can keep talking to the services
around it over the Kafka topics or HTTP calls they already use. Running a connector behind a Nexus
endpoint also keeps credentials in one place: the connector holds the keys, and you decide which
namespaces may call which endpoint, such as a read-only endpoint for one team and a read-write one
for another, following the principle of least privilege.

## What is the Outbound HTTP Connector?

**Durable HTTP calls, ready to use.** The HTTP connector makes HTTP requests for your workflows.
It runs as one program, the Outbound Connector, which runs as a Nexus operation and makes HTTP
requests: it sends requests **out** of Temporal to the APIs you call. A workflow describes a request (URL, method, headers, body, and which API key to use)
and calls the Outbound Connector's one operation, `HTTPService.request`. The Outbound Connector
sends the request, retries it when retrying can help, and returns the response or the final error.
By default each call runs as a standalone activity with the Outbound Connector's own retries; for
fast calls, `HTTP_CONNECTOR_MODE=sync` runs it within the Nexus request instead, with no activity,
which costs less (see [Activity or sync mode](docs/errors.md#activity-or-sync-mode)).
It works on its own, but it is [best used as a building block](#best-used-as-a-building-block) for
API-specific connectors.

```
 your namespace                         namespace: connectors
┌──────────────────────┐    Nexus     ┌──────────────────────────────────────┐
│ your workflow        │────────────▶ │ HTTPService.request                  │
│  request(req)        │              │   retries, rate-limit backoff,       │
│  → status, headers,  │ ◀─────────── │   timeouts, secrets ──▶ HTTP request │
│    body              │              └──────────────────────────────────────┘
└──────────────────────┘
```

## Best used as a building block

**You can call the Outbound Connector directly as a generic HTTP connector, but it is far better
used as the building block for an API-specific connector.**

Because it is generic, the Outbound Connector can't know which URLs you will call or how each API
authenticates. Every caller has to spell out the URL, method, headers, and which secret goes where,
so every caller needs to know the API's HTTP details. That works, but it isn't a clean interface.

So the Outbound Connector is also a Go library (`httpconnector`) that you can wrap. A thin
connector for one API keeps all of the Outbound Connector's handling (retries, rate limiting,
timeouts, secrets and redaction, the domain allowlist and redirect checks) and gives callers a
typed interface for that API instead:

- **One Nexus operation per thing callers need**, with API-specific inputs and outputs, such as
  `getWeatherByZip(zip) → weather`. Callers never see a URL or a credential.
- **Each operation maps its input onto an HTTP request** and names the secret to use and where
  the API expects it. It never reads the secret itself. It sends the request with
  `httpconnector`'s `Send` (one attempt), called as a function inside its own activity, or inside
  a synchronous Nexus operation with `RequestSync`.
- **A standalone activity for one call, a workflow for several.** An operation that makes one
  call is backed by a standalone activity. One that needs several calls can be backed by a
  workflow that runs each call as its own activity, so Temporal orchestrates them and doesn't
  repeat calls that already succeeded.
- **Only those operations are exposed.** The generic `HTTPService.request` operation isn't
  registered, so callers can make only the calls you chose, to the hosts you configured.

```
 your namespace                         namespace: connectors
┌──────────────────────┐    Nexus     ┌──────────────────────────────────────┐
│ your workflow        │────────────▶ │ WeatherService.getWeatherByZip       │
│  getWeatherByZip(zip)│              │   activity: builds the request,      │
│  → weather           │ ◀─────────── │   httpconnector.Request ──▶ HTTP API │
└──────────────────────┘              └──────────────────────────────────────┘
```

**Sample code:** [nexus-connector-openapi](https://github.com/Evanthx/nexus-connector-openapi)
shows how to wrap the Outbound Connector, and generates the wrapper from an API's OpenAPI spec. Its
[examples](https://github.com/Evanthx/nexus-connector-openapi#the-examples) cover one call per
operation, several calls in one activity, and several calls in a workflow, starting with a
[weather API connector](https://github.com/Evanthx/nexus-connector-openapi/tree/main/apis/weather).
[Wrapping the Outbound Connector](docs/how-it-works.md#apis-that-dont-follow-the-standard) lists
the exported functions a wrapper uses, and how to handle APIs that report errors in a `200`
response.

## The problem it solves

Every workflow that calls an HTTP API needs the same handful of things done right, and each team
tends to write them again:

- **Deciding what to retry.** A 503 or a network error is worth retrying; a 404 is not. The
  Outbound Connector sorts every status into a [typed error](docs/errors.md) and retries only what can
  succeed.
- **Backing off from rate limits.** After a 429, the Outbound Connector holds back every request to that
  endpoint and key for a window, instead of each caller hammering it.
- **Timeouts.** Each attempt has a timeout, and in the default activity mode retries stop just
  before the caller's own deadline, so the caller gets the real last error rather than a bare
  timeout.
- **Keeping API keys out of workflow history.** Requests name an environment variable on the
  Outbound Connector; the key is read at send time and redacted from responses and error messages.
- **Limiting where requests can go.** An optional allowlist restricts the hosts the Outbound Connector will
  call, redirects included.

## When to use it

Use it when:

- Workflows call third-party or internal HTTP APIs, and you want those calls retried and
  rate-limited correctly without writing that logic for each integration.
- Workflows in several namespaces or languages call the same APIs, and you want one place that
  holds the keys and enforces the rules.
- You want to control who can call what by granting access to a Nexus endpoint rather than handing
  out API keys.

It is not a fit when a response is larger than 2 MiB, or when you need streaming responses. An API
that does not follow the expected standards (for example, one that returns a 200 success with a body that reports an error) needs a small wrapper; see
[APIs that don't follow the standard](docs/how-it-works.md#apis-that-dont-follow-the-standard).

## FAQ

**Why do I need this? I can write an Activity that makes HTTP calls myself.**
You can, and for a single call that is often enough. The Outbound Connector is that activity, written
once with the details that are easy to get wrong: which statuses to retry, honoring 429s across
every caller of the Outbound Connector, stopping retries before the caller's deadline, and keeping keys out of
history and logs. Running it behind a Nexus endpoint adds what an activity in your own worker
can't: other teams, namespaces, and languages call the same Outbound Connector, the API keys live only on
the Outbound Connector, and access is granted per endpoint. The trade-off is a Nexus call in front of each
request.

**How do I set the `Authorization` header on outgoing requests?**
Export the key on the Outbound Connector in a variable named `HTTP_CONNECTOR_SECRET_<NAME>`, with
`HTTP_CONNECTOR_SECRET_<NAME>_DOMAINS` listing the hosts it may be sent to, and name that variable
in the request's `apiKeyEnvVar`. The Outbound Connector sends it as a `Bearer` token, or as
the basic auth password when the request has a `username`. For any other scheme, put the whole
header value (such as `Basic <base64>`) in the variable and set the request's `Authorization`
header to the variable's name. The key never enters the request or workflow history. See
[API keys and secrets](docs/usage.md#api-keys-and-secrets).

**Can I generate strongly typed clients for this Outbound Connector?**
Yes. The Outbound Connector's operations are defined in a contract file, `http.nexusrpc.yaml`, and a tool
called nexgen turns it into typed request and response classes in Go, Python, TypeScript, and
Java. See [Typed clients](docs/clients.md).

**What happens when the API is down or rate limiting me?**
5xx responses and network errors are retried with backoff; 429s wait out a rate-limit window; both
stop just before your operation's timeout. In sync mode your Temporal server retries them with its
own backoff instead, and if they never succeed you get a timeout. See
[Errors, timeouts, and rate limiting](docs/errors.md).

**Can I stop the Outbound Connector from calling arbitrary hosts?**
Yes, and by default it must: `HTTP_CONNECTOR_ALLOWED_DOMAINS` is required and lists the domains it
may call. Set it to `DISABLE ALLOWLIST` to allow any host. See [Allowed domains](docs/usage.md#allowed-domains).

**Can my data reach Temporal only encrypted?**
Yes. Set `TEMPORAL_CODEC_ENDPOINT` to your payload codec server on the Outbound Connector, or
build it with your codec, and each request, response and error is encrypted before it leaves the
process. Your own workers use the same codec. The HTTP API gets the request decrypted. See
[Encryption](docs/encryption.md).

## Documentation

- [Setup](docs/setup.md): prerequisites, starting Temporal, creating the Nexus endpoint, running the
  Outbound Connector, environment variables, and connecting to Temporal Cloud. To work out and test the
  environment variables in a form, run `make configui` and open <http://localhost:8090>: its
  **Test** checks every variable as the Outbound Connector does at startup, the connection to Temporal and the
  Nexus endpoint, and can send a test request (see [Setup page](docs/setup.md#setup-page)).
- [Usage](docs/usage.md): calling the Outbound Connector from a workflow, every request option, API keys and
  secrets, and allowed domains.
- [Typed clients](docs/clients.md): the contract, and generating clients for Go, Python, TypeScript,
  and Java.
- [Errors, timeouts, and rate limiting](docs/errors.md): error types, reading error details,
  timeouts, rate limiting, activity or sync mode, and known limitations.
- [Testing](docs/testing.md): running the test workflow and the test suites.
- [Encryption](docs/encryption.md): encrypting every payload so data reaches Temporal only
  encrypted, with your codec server or a codec compiled in, and trying it with the test workflow.
- [How it works](docs/how-it-works.md): architecture, the path of a request through the code,
  changing the contract, and APIs that don't follow the standard.
- [Troubleshooting](docs/troubleshooting.md): common problems, their causes, and fixes.

AI agents: see [AGENTS.md](AGENTS.md) for how to set up, run, call, and change this Outbound Connector.

## License

MIT License, please see [LICENSE](LICENSE) for details.
