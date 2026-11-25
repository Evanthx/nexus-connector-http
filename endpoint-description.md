# Outbound HTTP Connector

Makes an outbound HTTP request on behalf of a caller in another namespace. The `HTTPService`
service has one operation, `request`. It takes a description of the request (URL, method,
headers, query parameters, auth, body) and returns the response (status, headers, body).

Failures are `ApplicationError`s whose type is `InvalidRequest`, `ClientError`, `Transient` or
`RateLimited`. By default (activity mode), `Transient` and `RateLimited` failures are retried until
10 seconds before your Nexus operation times out, then the operation fails with the last error.
With no timeout, or a longer one, retries stop after the Outbound Connector's maximum (25 hours by
default). A timeout too short for one attempt plus those 10 seconds is rejected with
`InvalidRequest`.

If the Outbound Connector runs in sync mode (`HTTP_CONNECTOR_MODE=sync`), each call must finish
within one Nexus request (about 10 seconds). Your Temporal server retries `Transient` and
`RateLimited` failures until your operation times out, and then the operation fails with a timeout,
not the last error. Always give the operation a timeout.
