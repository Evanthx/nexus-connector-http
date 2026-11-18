package httpconnector

import (
	"time"

	"go.temporal.io/sdk/temporal"
)

// Error types. Every failure the Connector returns is an ApplicationError with one of these as
// its Type, so callers can branch on it.
const (
	// ErrorTypeRateLimited: the endpoint returned 429, or another call for the same rate-limit key
	// did within the rate-limit window (RateLimitMinutesEnvVar). Retried once the window is up.
	ErrorTypeRateLimited = "RateLimited"
	// ErrorTypeInvalidRequest: the request can't be made as given, so it was never sent. Not
	// retried.
	ErrorTypeInvalidRequest = "InvalidRequest"
	// ErrorTypeClientError: the endpoint answered with a status that sending the same request
	// again won't change: most 4xx, 501, 505, 511, and a 3xx that wasn't followed. Also a response
	// body over maxResponseBytes, whatever the status, and a TLS certificate that doesn't verify.
	// Not retried.
	ErrorTypeClientError = "ClientError"
	// ErrorTypeTransient: 408, 409, 420, 421, 423, 424, 425, any other 5xx, or a network failure
	// or timeout. Retried with backoff.
	ErrorTypeTransient = "Transient"
)

// InvalidRequestError is the non-retryable error for a request that can't be made.
func InvalidRequestError(err error) error {
	return temporal.NewApplicationErrorWithOptions(err.Error(), ErrorTypeInvalidRequest,
		temporal.ApplicationErrorOptions{NonRetryable: true})
}

// Retry delays. Transient errors leave the delay to the activity's retry policy (see
// ActivityOptions): 5s, then three times longer each attempt, at most 5m apart. RateLimited errors
// set their own with NextRetryDelay: the rest of the rate-limit window, or a 429's shorter
// Retry-After (see retryAfter). The policy counts every attempt, rate-limited ones too, so a
// Transient error after a few rate-limited attempts waits the full 5m: after a 429, that is
// welcome.

// rateLimitedError is a retryable RateLimited error whose next attempt waits for wait instead of
// the retry policy's backoff. details, if non-nil, is the response that triggered it.
func rateLimitedError(msg string, wait time.Duration, details any) error {
	opts := temporal.ApplicationErrorOptions{NextRetryDelay: wait}
	if details != nil {
		opts.Details = []any{details}
	}
	return temporal.NewApplicationErrorWithOptions(msg, ErrorTypeRateLimited, opts)
}

// transientError is a retryable Transient error, retried with the retry policy's backoff.
// details, if non-nil, is the response that triggered it.
func transientError(msg string, details any) error {
	var opts temporal.ApplicationErrorOptions
	if details != nil {
		opts.Details = []any{details}
	}
	return temporal.NewApplicationErrorWithOptions(msg, ErrorTypeTransient, opts)
}
