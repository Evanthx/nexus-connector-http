# Outbound HTTP Connector

Makes an outbound HTTP request on behalf of a caller in another namespace. The `HTTPService`
service has one operation, `sendRequest`. It takes a description of the request (URL, method,
headers, query parameters, auth, body) and returns the response (status, headers, body).

Failures are `ApplicationError`s whose type is `InvalidRequest`, `ClientError`, `Transient` or
`RateLimited`. `Transient` and `RateLimited` failures are retried until 10 seconds before your
Nexus operation times out, then the operation fails with the last error. With no timeout, or a
longer one, retries stop after the connector's maximum (25 hours by default). A timeout too short
for one attempt plus those 10 seconds is rejected with `InvalidRequest`.
