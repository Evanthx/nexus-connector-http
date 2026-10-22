# nexus-connector-http

Makes an outbound HTTP request on behalf of a caller in another namespace. The `HTTPService`
service has one operation, `sendRequest`. It takes a description of the request (URL, method,
headers, query parameters, auth, body) and returns the response (status, headers, body).

Failures are `ApplicationError`s whose type is `InvalidRequest`, `ClientError`, `Transient` or
`RateLimited`. `Transient` and `RateLimited` failures are retried for the request's
`scheduleToCloseSeconds` (25 hours by default, and at most) before the operation completes. Your
Nexus operation's timeout must be longer than `scheduleToCloseSeconds`, or the request is rejected
with `InvalidRequest`.
