# Encryption

The Outbound Connector can encrypt every payload it sends to Temporal and decrypt every payload it
reads, so your requests and responses reach Temporal, and Temporal Cloud, only encrypted. It uses a
**payload codec**, the standard Temporal mechanism: the same codec your own workers use, and the
same codec server the Temporal UI uses to show payloads decrypted.

With no codec set, payloads go to Temporal as they are. The `payload codec` line in the startup
report's Temporal section says which.

## How it fits together

```
 your workflow ──encrypt──▶ Temporal ──▶ Outbound Connector ──decrypt──▶ HTTP API
 (codec)                    (ciphertext)  (same codec)
```

**Everything that reads or writes these payloads must use the same codec and keys**: your callers'
workers and the Outbound Connector, and the codec server behind the Temporal UI, if you want to read
payloads there. A worker without the codec fails to decode what the others encrypted.

The Outbound Connector has to see plaintext to do its job: it sends the request your workflow passed
to the HTTP API, and returns the response. Run it where you would run any code that handles the
data.

## Turn it on

### With a codec server (no code)

Point the Outbound Connector at your codec server, the HTTP service that encodes and decodes
payloads for the Temporal UI and CLI:

```bash
export TEMPORAL_CODEC_ENDPOINT=https://codec.example.com
export TEMPORAL_CODEC_AUTH="Bearer <token>"     # if the server wants an Authorization header
```

Or set them in the temporal CLI's profile file, which the Outbound Connector reads like the CLI
does:

```toml
[profile.default.codec]
endpoint = "https://codec.example.com"
auth = "Bearer <token>"
```

The Outbound Connector sends each payload to `<endpoint>/encode` and `<endpoint>/decode`, with the
`X-Namespace` header set to its namespace, as the UI and CLI do, so a codec server that picks keys
by namespace works unchanged. The Outbound Connector holds no keys.

At startup, after logging its startup report, the Outbound Connector encrypts a test payload and decrypts it
again:

```
INFO Payload codec works: payloads are encoded with the remote codec at https://codec.example.com (TEMPORAL_CODEC_AUTH set)
```

Until that works it waits, logging why every 5 seconds:

```
WARN Can't use the payload codec yet; retrying in 5s. Check the codec server is running and TEMPORAL_CODEC_ENDPOINT and TEMPORAL_CODEC_AUTH are right. error="encode a test payload: Unauthorized: ..."
```

An endpoint that isn't an `http` or `https` URL stops the Outbound Connector at once. A codec
server that returns the test payload unchanged is a warning: payloads would reach Temporal
unencrypted.

Each call to the codec server is an HTTP round trip, a few per `request`. Run the codec server
close to the Outbound Connector.

### With a codec compiled in

If you would rather not run a codec server, build the Outbound Connector with your codec. Add a
file to the `codec` package, such as `nexus-connector-shared/codec/mycodec.go`:

```go
package codec

import "example.com/yourcompany/temporalcodec"

func init() { PayloadCodecs = append(PayloadCodecs, temporalcodec.New()) }
```

then build as usual (`go build -o bin/ ./cmd/outbound`). The startup log says
`encoded with 1 codec(s) compiled in`, and the same startup check runs. You can combine both: the
codec server's encoding runs first, then the compiled-in codecs.

## What is encrypted

| | Encrypted | Not encrypted |
|---|---|---|
| **Outbound Connector** | Each `request`'s input (the URL, method, headers, query parameters, body and username) and its result (the response's status, headers and body), and, in activity mode, the same as the `Request` activity's input and result. Errors it returns: their details (on a non-2xx response, the response's status, headers and body), messages (which name the URL) and stack traces. | Each error's type. In activity mode, the activity ID (`http-<Nexus request ID>`: no request content). The endpoint, service and operation names. |
| **The HTTP API** | | Requests and responses: the Outbound Connector sends what the caller passed, decrypted, and reads the response as the API sends it. Use `https` URLs to keep them encrypted on the network. |

**Errors are encrypted too.** With a codec set, the Outbound Connector uses a failure converter that
encrypts each error's details, message and stack trace (`EncodeCommonAttributes`). Temporal then
stores the message as `Encoded failure`; the error's type (such as `InvalidRequest`) and whether it
is retryable stay readable, so callers can still branch on them. Your workers need the same failure
converter to read the messages: `temporal.NewDefaultFailureConverter` with your data converter and
`EncodeCommonAttributes: true`; the demo's ENCRYPTION block sets it. Without it, a worker sees `Encoded failure` and no error is
raised. The Temporal UI shows the messages through the codec server; without it, `Encoded failure`.
A worker that encrypts errors reads unencrypted ones as they are, so the two can be mixed.

Secrets are never in payloads: a request names a `HTTP_CONNECTOR_SECRET_*` variable, and the value
stays in the Outbound Connector's environment (see
[API keys and secrets](usage.md#api-keys-and-secrets)).

## Try it in the demo

The test workflow ([Testing](testing.md#test-workflow)) can run encrypted end to end. It uses a demo
codec, AES-256-GCM with a key derived from a passphrase (`nexus-connector-shared/test/encryption`),
and a demo codec server that serves it (`nexus-connector-shared/test/encryption/codecserver`). They
are for the demo only: a real codec gets its keys from a key management service.

1. Pick a passphrase and use it in every terminal below:

   ```bash
   export DEMO_ENCRYPTION_KEY="correct horse battery staple"
   ```

2. Start the demo codec server (its own terminal):

   ```bash
   go run ./nexus-connector-shared/test/encryption/codecserver     # serves http://localhost:8089
   ```

3. Start Temporal with the UI using it ([Setup](setup.md#1-start-temporal) step 1, with one more
   flag):

   ```bash
   temporal server start-dev \
     --dynamic-config-value activity.enableCallbacks=true \
     --namespace connectors \
     --namespace http-testcaller \
     --ui-codec-endpoint http://localhost:8089
   ```

4. Create the endpoint and start the Outbound Connector as in [Setup](setup.md), with this in the
   Outbound Connector's terminal:

   ```bash
   export TEMPORAL_CODEC_ENDPOINT=http://localhost:8089
   ```

   Then run the test workflow as in [Testing](testing.md#test-workflow), with `DEMO_ENCRYPTION_KEY`
   set in the test caller's worker's and starter's terminals. They encrypt with the codec directly,
   in a block of `testcaller/client.go` marked `BEGIN ENCRYPTION (optional)`; the Outbound Connector
   uses the codec server. With the same passphrase everywhere, everything works as before.

5. See that Temporal holds only ciphertext. The UI shows payloads decrypted, through the codec
   server; the CLI, without it, shows what Temporal stores:

   ```bash
   temporal workflow show --namespace http-testcaller --workflow-id <the ID the starter printed> --output json | grep -m3 encoding
   ```

   Each payload's `encoding` is `binary/encrypted` (base64 in the output).

### Leaving encryption out of your own code

The demo's encryption is self-contained so you can copy the test caller without it. In
`testcaller/client.go`, delete the block between `BEGIN ENCRYPTION (optional)` and
`END ENCRYPTION (optional)`, and the import marked `ENCRYPTION (optional)`. Nothing else uses them.
To keep encryption, keep both and replace the demo codec with yours.

## Troubleshooting

- **`Can't use the payload codec yet`** at startup: the codec server isn't reachable, or refused the
  request (`Unauthorized`: check `TEMPORAL_CODEC_AUTH`). The Outbound Connector waits; fix it and it
  carries on.
- **A worker fails to decode payloads with encoding `binary/encrypted`**: that worker has no codec,
  or a different one. Give every worker the same codec and keys.
- **`payload encrypted with key "...", but this codec has key "..."`** (demo codec): the programs
  have different `DEMO_ENCRYPTION_KEY` values.
- **The UI shows `binary/encrypted` payloads**: start it with the codec server
  (`--ui-codec-endpoint` for the dev server; on Temporal Cloud, set the codec server in the UI's
  settings).
