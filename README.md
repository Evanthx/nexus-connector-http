![Nexus Connectors](docs/images/NexusConnectors.webp)


# Outbound HTTP Connector

## What are Nexus Connectors?

A Nexus Connector lets a Temporal workflow work with a system outside Temporal, such as a message
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

**Durable HTTP calls, ready to use.** The HTTP connector is a Temporal worker that makes HTTP
requests for your workflows. A workflow describes a request (URL, method, headers, body, and which
API key to use) and calls the connector's one operation, `HTTPService.sendRequest`. The connector
sends the request, retries it when retrying can help, and returns the response or the final error.

```
 your namespace                         connector namespace
┌──────────────────────┐    Nexus     ┌──────────────────────────────────────┐
│ your workflow        │────────────▶ │ HTTPService.sendRequest              │
│  sendRequest(req)    │              │   retries, rate-limit backoff,       │
│  → status, headers,  │ ◀─────────── │   timeouts, secrets ──▶ HTTP request │
│    body              │              └──────────────────────────────────────┘
└──────────────────────┘
```

## The problem it solves

Every workflow that calls an HTTP API needs the same handful of things done right, and each team
tends to write them again:

- **Deciding what to retry.** A 503 or a network error is worth retrying; a 404 is not. The
  connector sorts every status into a [typed error](docs/errors.md) and retries only what can
  succeed.
- **Backing off from rate limits.** After a 429, the connector holds back every request to that
  endpoint and key for a window, instead of each caller hammering it.
- **Timeouts.** Each attempt has a timeout, and retries stop just before the caller's own deadline,
  so the caller gets the real last error rather than a bare timeout.
- **Keeping API keys out of workflow history.** Requests name an environment variable on the
  connector; the key is read at send time and redacted from responses and error messages.
- **Limiting where requests can go.** An optional allowlist restricts the hosts the connector will
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
You can, and for a single call that is often enough. The connector is that activity, written
once with the details that are easy to get wrong: which statuses to retry, honoring 429s across
every caller on the worker, stopping retries before the caller's deadline, and keeping keys out of
history and logs. Running it behind a Nexus endpoint adds what an activity in your own worker
can't: other teams, namespaces, and languages call the same connector, the API keys live only on
the connector, and access is granted per endpoint. The trade-off is a Nexus call in front of each
request.

**How do I set the `Authorization` header on outgoing requests?**
Export the key on the connector worker in a variable named `HTTP_CONNECTOR_SECRET_<NAME>`, with
`HTTP_CONNECTOR_SECRET_<NAME>_DOMAINS` listing the hosts it may be sent to, and name that variable
in the request's `apiKeyEnvVar`. The connector sends it as a `Bearer` token, or as
the basic auth password when the request has a `username`. For any other scheme, put the whole
header value (such as `Basic <base64>`) in the variable and set the request's `Authorization`
header to the variable's name. The key never enters the request or workflow history. See
[API keys and secrets](docs/usage.md#api-keys-and-secrets).

**Can I generate strongly typed clients for this connector?**
Yes. The connector's operations are defined in a contract file, `http.nexusrpc.yaml`, and a tool
called nexgen turns it into typed request and response classes in Go, Python, TypeScript, and
Java. See [Typed clients](docs/clients.md).

**What happens when the API is down or rate limiting me?**
5xx responses and network errors are retried with backoff; 429s wait out a rate-limit window; both
stop just before your operation's timeout. See [Errors, timeouts, and rate limiting](docs/errors.md).

**Can I stop the connector from calling arbitrary hosts?**
Yes, and by default it must: `HTTP_CONNECTOR_ALLOWED_DOMAINS` is required and lists the domains it
may call. Set it to `DISABLE ALLOWLIST` to allow any host. See [Allowed domains](docs/usage.md#allowed-domains).

## Documentation

- [Setup](docs/setup.md): prerequisites, starting Temporal, creating the Nexus endpoint, running the
  worker, environment variables, and connecting to Temporal Cloud.
- [Usage](docs/usage.md): calling the connector from a workflow, every request option, API keys and
  secrets, and allowed domains.
- [Typed clients](docs/clients.md): the contract, and generating clients for Go, Python, TypeScript,
  and Java.
- [Errors, timeouts, and rate limiting](docs/errors.md): error types, reading error details,
  timeouts, rate limiting, and known limitations.
- [Testing](docs/testing.md): running the test workflow and the test suites.
- [How it works](docs/how-it-works.md): architecture, the path of a request through the code,
  changing the contract, and APIs that don't follow the standard.
- [Troubleshooting](docs/troubleshooting.md): common problems, their causes, and fixes.

AI agents: see [AGENTS.md](AGENTS.md) for how to set up, run, call, and change this connector.

## License

MIT License, please see [LICENSE](LICENSE) for details.
