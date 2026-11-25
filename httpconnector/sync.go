package httpconnector

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/temporalio/nexus-connector-http/generated/httpapi"
	"go.temporal.io/sdk/temporal"
)

// ModeEnvVar names the environment variable on the Outbound Connector that chooses how each call
// runs: ModeActivity or ModeSync.
const ModeEnvVar = "HTTP_CONNECTOR_MODE"

// Mode is how the request operation runs each call.
type Mode string

const (
	// ModeActivity backs each call with a standalone activity: the connector's own retry policy,
	// rate-limit waits (NextRetryDelay), attempts of up to AttemptTimeoutEnvVar, and the caller
	// gets the last real error before its timeout. It costs the activity's actions on top of the
	// Nexus operation's.
	ModeActivity Mode = "activity"
	// ModeSync runs each call within the Nexus request, with no activity: the cheapest, but each
	// attempt must finish before the request's deadline (10 seconds by default on the server), the
	// caller's server retries a failure with its own backoff, and a caller whose retries never
	// succeed gets a timeout, with the last error only on the pending operation.
	ModeSync Mode = "sync"
)

// ParseMode reads ModeEnvVar's value, v, in any case; unset or empty is ModeActivity.
func ParseMode(v string) (Mode, error) {
	switch m := Mode(strings.ToLower(strings.TrimSpace(v))); m {
	case "":
		return ModeActivity, nil
	case ModeActivity, ModeSync:
		return m, nil
	}
	return "", fmt.Errorf("%s must be %q or %q, not %q", ModeEnvVar, ModeActivity, ModeSync, v)
}

// replyMargin is how long before the Nexus request's deadline a synchronous call stops waiting
// for the endpoint, so the handler can still reply with why it failed rather than let the request
// time out.
const replyMargin = time.Second

// RequestSync makes one attempt of req within a synchronous Nexus operation's handler, whose
// context is ctx: Send, stopped replyMargin before the request's deadline, with its error turned
// into a Nexus handler error by HandlerError. The caller's server retries the retryable ones.
func (a *HTTPActivities) RequestSync(ctx context.Context, req httpapi.RequestInput) (httpapi.RequestOutput, error) {
	sendCtx := ctx
	if d, ok := ctx.Deadline(); ok {
		var cancel context.CancelFunc
		sendCtx, cancel = context.WithDeadline(ctx, d.Add(-replyMargin))
		defer cancel()
	}
	out, err := a.Send(sendCtx, req)
	if err == nil {
		return out, nil
	}
	if sendCtx.Err() != nil && ctx.Err() == nil {
		err = transientError(fmt.Sprintf("calling %s: no response before the Nexus request's deadline. With %s=%s, a call "+
			"must finish within one Nexus request (about 10 seconds); set %s=%s for slower endpoints",
			req.URL, ModeEnvVar, ModeSync, ModeEnvVar, ModeActivity), nil)
	}
	return httpapi.RequestOutput{}, HandlerError(err)
}

// HandlerError turns an error from Send into what a synchronous Nexus operation returns, keeping
// the ApplicationError, with its type and details, as the cause, so callers find it with
// errors.As as they would in activity mode:
//
//   - InvalidRequest: a BAD_REQUEST handler error, not retried.
//   - Any other non-retryable error (ClientError): a non-retryable INTERNAL handler error.
//   - RateLimited: a retryable RESOURCE_EXHAUSTED handler error. The caller's server retries it
//     with its own backoff, not after the rate-limit window, so the cache refuses the retries
//     without sending them until the window ends.
//   - Anything else (Transient): a retryable UNAVAILABLE handler error.
func HandlerError(err error) error {
	var appErr *temporal.ApplicationError
	if !errors.As(err, &appErr) {
		return &nexus.HandlerError{Type: nexus.HandlerErrorTypeInternal, Message: err.Error(), Cause: err}
	}
	he := &nexus.HandlerError{Message: appErr.Message(), Cause: appErr}
	switch {
	case appErr.Type() == ErrorTypeInvalidRequest:
		he.Type = nexus.HandlerErrorTypeBadRequest
	case appErr.NonRetryable():
		he.Type = nexus.HandlerErrorTypeInternal
		he.RetryBehavior = nexus.HandlerErrorRetryBehaviorNonRetryable
	case appErr.Type() == ErrorTypeRateLimited:
		he.Type = nexus.HandlerErrorTypeResourceExhausted
	default:
		he.Type = nexus.HandlerErrorTypeUnavailable
	}
	return he
}
