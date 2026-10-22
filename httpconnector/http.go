// Package httpconnector is the HTTP connector: a Nexus service whose one operation, sendRequest,
// is backed by a standalone activity, SendRequest, which makes an HTTP request. There is no handler
// workflow: the operation starts SendRequest and completes when the activity does.
package httpconnector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/temporalio/nexus-connector-http/generated/httpapi"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/temporalnexus"
	"go.temporal.io/sdk/worker"
)

// TaskQueue is the connector worker's task queue. The Nexus endpoint targets it.
const TaskQueue = "nexus-connector-http"

// HTTPActivities is the SendRequest activity plus the rate-limit cache it shares across calls.
// Register one instance per worker, so every call on that worker shares the cache.
type HTTPActivities struct {
	lim *limiter
}

// New returns HTTPActivities with an empty rate-limit cache, whose window it reads from
// RateLimitMinutesEnvVar. An invalid value is an error.
func New() (*HTTPActivities, error) {
	window, err := rateLimitWindow()
	if err != nil {
		return nil, err
	}
	return newWithWindow(window), nil
}

func newWithWindow(window time.Duration) *HTTPActivities {
	return &HTTPActivities{lim: newLimiter(window)}
}

// Register registers the HTTPService Nexus service and the SendRequest activity its operation
// starts on w. Both use one HTTPActivities, so every call on the worker shares one rate-limit
// cache. It fails if the rate-limit window's environment variable is invalid.
//
// The sendRequest operation rejects a request that can't be made, or whose scheduleToCloseSeconds
// outlasts the caller's operation timeout, with a BadRequest handler error, without starting an
// activity; otherwise it starts SendRequest as a standalone activity. The activity ID comes from
// the Nexus request ID, so a retried start of the same operation reaches the same activity.
func Register(w worker.Registry) error {
	a, err := New()
	if err != nil {
		return err
	}
	op := temporalnexus.MustNewTemporalOperation(temporalnexus.TemporalOperationOptions[httpapi.SendRequestInput, httpapi.SendRequestOutput]{
		Name: httpapi.HTTPService.SendRequest.Name(),
		Start: func(
			ctx context.Context,
			nc temporalnexus.NexusClient,
			req httpapi.SendRequestInput,
			opts temporalnexus.StartTemporalOperationOptions,
		) (temporalnexus.TemporalOperationResult[httpapi.SendRequestOutput], error) {
			err := Validate(req)
			if err == nil {
				err = CheckCallerTimeout(req, opts.Header.Get(nexus.HeaderOperationTimeout))
			}
			if err != nil {
				return temporalnexus.TemporalOperationResult[httpapi.SendRequestOutput]{}, &nexus.HandlerError{
					Type:  nexus.HandlerErrorTypeBadRequest,
					Cause: InvalidRequestError(err),
				}
			}
			return temporalnexus.StartActivity(ctx, nc, ActivityOptions("http-"+opts.RequestID, req), a.SendRequest, req)
		},
	})
	svc := nexus.NewService(httpapi.HTTPService.ServiceName)
	svc.MustRegister(op)
	w.RegisterNexusService(svc)
	w.RegisterActivity(a.SendRequest)
	return nil
}

// attemptTimeout bounds one activity attempt, and so one HTTP request: a request's timeoutSeconds
// can't be longer.
const attemptTimeout = 60 * time.Second

// Transient errors back off from backoffInitial, doubling up to backoffMax.
const (
	backoffInitial = time.Second
	backoffMax     = 30 * time.Second
)

// maxResponseBytes is the largest response body SendRequest reads. A larger one couldn't be
// returned within Temporal's payload size limit (2 MB by default) anyway.
const maxResponseBytes = 2 << 20

// maxRedirects is how many redirects a request follows, as http.Client does by default.
const maxRedirects = 10

// maxScheduleToClose is the longest scheduleToCloseSeconds a request can set.
const maxScheduleToClose = 25 * time.Hour

// ScheduleToClose is how long the call for req may take in all, including every retry and
// rate-limit wait: its scheduleToCloseSeconds, which build has checked. It must end before the
// caller's Nexus operation times out (CheckCallerTimeout).
func ScheduleToClose(req httpapi.SendRequestInput) time.Duration {
	return time.Duration(req.ScheduleToCloseSecondsOrDefault()) * time.Second
}

// ActivityOptions are the options for starting SendRequest for req as a standalone activity with
// the given ID. Transient errors retry with backoff (1s, doubling, at most 30s apart) and
// RateLimited errors after the rest of the rate-limit window. SendRequest sets each delay itself
// (see transientError); the policy's backoff applies only when an attempt times out.
// ScheduleToCloseTimeout bounds the whole call, including every retry and rate-limit wait.
func ActivityOptions(id string, req httpapi.SendRequestInput) client.StartActivityOptions {
	return client.StartActivityOptions{
		ID:                     id,
		StartToCloseTimeout:    attemptTimeout,
		ScheduleToCloseTimeout: ScheduleToClose(req),
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    backoffInitial,
			BackoffCoefficient: 2.0,
			MaximumInterval:    backoffMax,
		},
	}
}

// categorizeError maps a non-2xx status code to an error type: Transient if sending the same
// request again could get a different answer, ClientError if it can't.
func categorizeError(status int) string {
	switch status {
	case http.StatusTooManyRequests:
		return ErrorTypeRateLimited
	case http.StatusRequestTimeout, http.StatusConflict,
		420, // Enhance Your Calm: a rate limit some servers send instead of 429
		http.StatusMisdirectedRequest, http.StatusLocked, http.StatusFailedDependency, http.StatusTooEarly:
		return ErrorTypeTransient
	case http.StatusNotImplemented, http.StatusHTTPVersionNotSupported, http.StatusNetworkAuthenticationRequired:
		return ErrorTypeClientError
	}
	if status >= 500 && status < 600 {
		return ErrorTypeTransient
	}
	// Every other 4xx, a 3xx the client didn't follow, and any other status.
	return ErrorTypeClientError
}

// SendRequest makes one HTTP attempt per activity attempt:
//
//   - A request that can't be made fails with InvalidRequest, without being sent.
//   - If another call for the same rate-limit key got a 429 within the last window, it fails with
//     RateLimited without being sent, and the next attempt waits for the rest of the window.
//   - Otherwise it is sent. 2xx returns the response and clears the key. 429 marks the key and
//     fails with RateLimited. Other statuses fail with ClientError or Transient (see categorizeError),
//     with the response in the error's details.
func (a *HTTPActivities) SendRequest(ctx context.Context, req httpapi.SendRequestInput) (httpapi.SendRequestOutput, error) {
	logger := activity.GetLogger(ctx)

	httpReq, err := build(ctx, req)
	if err != nil {
		return httpapi.SendRequestOutput{}, InvalidRequestError(err)
	}

	key := RateLimitKey(req)
	if wait := a.lim.remaining(key, time.Now()); wait > 0 {
		logger.Warn("Rate limited by an earlier 429, not sending", "key", key, "retryIn", wait.String())
		return httpapi.SendRequestOutput{}, rateLimitedError(ctx, fmt.Sprintf("rate limited (cached) for %s", key), wait, nil)
	}

	secrets := secrets(req)
	start := time.Now()
	resp, err := (&http.Client{Timeout: timeout(req), CheckRedirect: checkRedirect}).Do(httpReq)
	if err != nil {
		// A *url.Error repeats the URL sent, which can have the API key in its query; req.URL can't.
		var urlErr *url.Error
		if errors.As(err, &urlErr) {
			err = urlErr.Err
		}
		// The rest can still hold a secret the server echoed, such as in a redirect's Location.
		msg := redact(fmt.Sprintf("calling %s: %v", req.URL, err), secrets)
		return httpapi.SendRequestOutput{}, transientError(ctx, msg, nil)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes+1))
	elapsed := time.Since(start).Milliseconds()
	if err != nil {
		msg := redact(fmt.Sprintf("reading the response from %s: %v", req.URL, err), secrets)
		return httpapi.SendRequestOutput{}, transientError(ctx, msg, nil)
	}
	if len(body) > maxResponseBytes {
		msg := fmt.Sprintf("%s returned status %d with a body over %d bytes", req.URL, resp.StatusCode, maxResponseBytes)
		return httpapi.SendRequestOutput{}, temporal.NewApplicationErrorWithOptions(msg, ErrorTypeClientError,
			temporal.ApplicationErrorOptions{NonRetryable: true})
	}

	out := httpapi.SendRequestOutput{
		StatusCode:     int64(resp.StatusCode),
		Status:         resp.Status,
		Headers:        httpapi.SendRequestOutputHeaders{AdditionalProperties: resp.Header},
		Body:           string(body),
		DurationMillis: elapsed,
	}
	redactResponse(&out, secrets)
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		a.lim.clear(key)
		return out, nil
	}

	msg := fmt.Sprintf("%s returned status %d", req.URL, resp.StatusCode)
	errType := categorizeError(resp.StatusCode)
	if errType == ErrorTypeRateLimited {
		// Every call on this worker with the same key now waits out the window.
		a.lim.mark(key, time.Now())
		logger.Warn("Rate limited: endpoint returned 429", "key", key, "window", a.lim.ttl.String())
		return httpapi.SendRequestOutput{}, rateLimitedError(ctx, msg, a.lim.ttl, out)
	}
	if errType == ErrorTypeTransient {
		return httpapi.SendRequestOutput{}, transientError(ctx, msg, out)
	}
	return httpapi.SendRequestOutput{}, temporal.NewApplicationErrorWithOptions(msg, errType, temporal.ApplicationErrorOptions{
		Details:      []any{out},
		NonRetryable: true,
	})
}

// checkRedirect is http.Client's default redirect policy, changed so that credentials go only
// where the caller sent them:
//
//   - A redirect from https to http isn't followed. SendRequest gets the redirect itself as the
//     response, which fails as a ClientError.
//   - The Authorization header goes only to the first URL's origin (scheme, host and port).
//     http.Client alone also sends it to another scheme, port or subdomain of the same host.
//   - No Referer is sent. It would carry the previous URL, and any API key in its query, onward.
func checkRedirect(req *http.Request, via []*http.Request) error {
	if len(via) >= maxRedirects {
		return fmt.Errorf("stopped after %d redirects", maxRedirects)
	}
	if via[len(via)-1].URL.Scheme == "https" && req.URL.Scheme != "https" {
		return http.ErrUseLastResponse
	}
	if first := via[0].URL; req.URL.Scheme != first.Scheme || !strings.EqualFold(req.URL.Host, first.Host) {
		req.Header.Del("Authorization")
	}
	req.Header.Del("Referer")
	return nil
}

// redact replaces every secret in s with [REDACTED], in each form it can take in a response or an
// error message: as is, escaped in a query or path, quoted by Go, and escaped in a JSON string.
func redact(s string, secrets []string) string {
	for _, secret := range secrets {
		quoted := strconv.Quote(secret)
		inJSON, _ := json.Marshal(secret) // a string always marshals
		for _, form := range []string{
			secret,
			url.QueryEscape(secret),
			url.PathEscape(secret),
			quoted[1 : len(quoted)-1],
			string(inJSON[1 : len(inJSON)-1]),
		} {
			s = strings.ReplaceAll(s, form, "[REDACTED]")
		}
	}
	return s
}

// redactResponse redacts secrets from everything in out that came from the endpoint, which can
// echo what it was sent.
func redactResponse(out *httpapi.SendRequestOutput, secrets []string) {
	if len(secrets) == 0 {
		return
	}
	out.Status = redact(out.Status, secrets)
	out.Body = redact(out.Body, secrets)
	for _, vs := range out.Headers.AdditionalProperties {
		for i, v := range vs {
			vs[i] = redact(v, secrets)
		}
	}
}
