package httpconnector

import (
	"context"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
)

// Error types. Every failure the connector returns is an ApplicationError with one of these as
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
	// body over maxResponseBytes, whatever the status. Not retried.
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

// Retry delays. Every retryable error sets its next attempt's delay itself, with NextRetryDelay:
//
//   - RateLimited waits the rate-limit window: all of it after a 429, or what is left of it when
//     an earlier 429 for the same key is still cached. Earlier Transient errors don't change this.
//   - Transient waits backoffInitial, doubling for each Transient error in a row before it, up to
//     backoffMax.
//
// Two counts are involved, and only one of them resets:
//
//   - The server's attempt number goes up by one every attempt, RateLimited or not, and never
//     resets. The retry policy's own backoff is computed from it, so after rate-limited attempts it
//     would make a Transient error wait as if they had been Transient too. That backoff now applies
//     only when an attempt hits its StartToCloseTimeout, since the activity can't set a delay then.
//     The policy has no MaximumAttempts, so the attempt number never ends the retries; only the
//     ScheduleToCloseTimeout (scheduleToCloseSeconds) does.
//   - The Transient count is how many Transient errors in a row there have been. Each attempt
//     records it as heartbeat details, which the server hands to the next attempt. A RateLimited
//     error resets it to 0, so the first Transient error after a rate limit waits backoffInitial
//     again. The first heartbeat of an attempt is sent at once, not batched, so it reaches the
//     server before the attempt fails. If that call fails, the next attempt sees an earlier
//     attempt's count, or none, and backs off from that; the retries still stop at the
//     ScheduleToCloseTimeout.
//
// For example, 503, 503, 429, 503 waits 1s, 2s, the window, then 1s.

// rateLimitedError is a retryable RateLimited error whose next attempt waits for wait instead of
// the retry policy's backoff. details, if non-nil, is the response that triggered it.
func rateLimitedError(ctx context.Context, msg string, wait time.Duration, details any) error {
	activity.RecordHeartbeat(ctx, 0)
	opts := temporal.ApplicationErrorOptions{NextRetryDelay: wait}
	if details != nil {
		opts.Details = []any{details}
	}
	return temporal.NewApplicationErrorWithOptions(msg, ErrorTypeRateLimited, opts)
}

// transientError is a retryable Transient error. Its next attempt waits backoffInitial, doubling
// for each Transient failure in a row before it, up to backoffMax. details, if non-nil, is the
// response that triggered it.
func transientError(ctx context.Context, msg string, details any) error {
	var inARow int
	if activity.HasHeartbeatDetails(ctx) {
		_ = activity.GetHeartbeatDetails(ctx, &inARow)
	}
	activity.RecordHeartbeat(ctx, inARow+1)
	opts := temporal.ApplicationErrorOptions{NextRetryDelay: transientDelay(inARow)}
	if details != nil {
		opts.Details = []any{details}
	}
	return temporal.NewApplicationErrorWithOptions(msg, ErrorTypeTransient, opts)
}

// transientDelay is the wait after a Transient failure that follows inARow others.
func transientDelay(inARow int) time.Duration {
	delay := backoffInitial
	for range inARow {
		delay *= 2
		if delay >= backoffMax {
			return backoffMax
		}
	}
	return delay
}
