// Package httpconnector is the HTTP connector: a Nexus service whose one operation, sendRequest,
// is backed by a standalone activity, SendRequest, which makes an HTTP request. There is no handler
// workflow: the operation starts SendRequest and completes when the activity does.
package httpconnector

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
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
	lim            *limiter
	domains        []string      // allowed domains; nil allows any host
	allowRedirects bool          // whether redirects are followed
	maxTotal       time.Duration // the longest a call may take in all
}

// New returns HTTPActivities with an empty rate-limit cache, whose window it reads from
// RateLimitMinutesEnvVar, the allowed domains from AllowedDomainsEnvVar, whether to follow
// redirects from AllowRedirectsEnvVar, and the longest a call may take from MaxDurationEnvVar. An
// invalid value of any of them is an error.
func New() (*HTTPActivities, error) {
	window, err := rateLimitWindow()
	if err != nil {
		return nil, err
	}
	domains, err := allowedDomains()
	if err != nil {
		return nil, err
	}
	redirects, err := allowRedirects()
	if err != nil {
		return nil, err
	}
	maxDuration, err := readMaxDuration()
	if err != nil {
		return nil, err
	}
	a := newWithWindow(window)
	a.domains = domains
	a.allowRedirects = redirects
	a.maxTotal = maxDuration
	return a, nil
}

func newWithWindow(window time.Duration) *HTTPActivities {
	return &HTTPActivities{lim: newLimiter(window), maxTotal: defaultMaxDuration}
}

// Register registers the HTTPService Nexus service and the SendRequest activity its operation
// starts on w. Both use one HTTPActivities, so every call on the worker shares one rate-limit
// cache. It fails if one of the environment variables New reads is invalid.
//
// The sendRequest operation rejects a request that can't be made, whose host isn't an allowed
// domain (see CheckDomain), or for which the caller's Nexus operation has too little time left
// (see ScheduleToClose), with a BadRequest handler error, without starting an activity; otherwise
// it starts SendRequest as a standalone activity that ends shortly before the operation times out.
// The activity ID comes from the Nexus request ID, so a retried start of the same operation
// reaches the same activity.
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
				err = a.CheckDomain(req.URL)
			}
			var total time.Duration
			if err == nil {
				total, err = a.ScheduleToClose(req, opts.Header.Get(nexus.HeaderOperationTimeout))
			}
			if err != nil {
				return temporalnexus.TemporalOperationResult[httpapi.SendRequestOutput]{}, &nexus.HandlerError{
					Type:  nexus.HandlerErrorTypeBadRequest,
					Cause: InvalidRequestError(err),
				}
			}
			return temporalnexus.StartActivity(ctx, nc, ActivityOptions("http-"+opts.RequestID, total), a.SendRequest, req)
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

// ActivityOptions are the options for starting SendRequest as a standalone activity with the
// given ID. Transient errors retry with backoff (1s, doubling, at most 30s apart) and
// RateLimited errors after the rest of the rate-limit window. SendRequest sets each delay itself
// (see transientError); the policy's backoff applies only when an attempt times out.
// scheduleToClose, from ScheduleToClose, bounds the whole call, including every retry and
// rate-limit wait.
func ActivityOptions(id string, scheduleToClose time.Duration) client.StartActivityOptions {
	return client.StartActivityOptions{
		ID:                     id,
		StartToCloseTimeout:    attemptTimeout,
		ScheduleToCloseTimeout: scheduleToClose,
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
//   - A request that can't be made, or whose host isn't an allowed domain, fails with
//     InvalidRequest, without being sent.
//   - If another call for the same rate-limit key got a 429 within the last window, it fails with
//     RateLimited without being sent, and the next attempt waits for the rest of the window.
//   - Otherwise it is sent, and the response's status decides the result (see ApplyStatus). 2xx
//     returns the response and clears the key. 429 marks the key and fails with RateLimited. Other
//     statuses fail with ClientError or Transient (see categorizeError), with the response in the
//     error's details.
func (a *HTTPActivities) SendRequest(ctx context.Context, req httpapi.SendRequestInput) (httpapi.SendRequestOutput, error) {
	logger := activity.GetLogger(ctx)

	httpReq, err := build(ctx, req)
	if err == nil {
		err = a.CheckDomain(req.URL)
	}
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
	resp, err := (&http.Client{Timeout: timeout(req), CheckRedirect: a.checkRedirect(secretHeaderVars(req))}).Do(httpReq)
	if err != nil {
		// A *url.Error repeats the URL sent, which can have the API key in its query; req.URL can't.
		var urlErr *url.Error
		if errors.As(err, &urlErr) {
			err = urlErr.Err
		}
		// A redirect away from the allowed domains won't be allowed on the next attempt either. The
		// client returns the redirect itself, with its body closed, and the caller gets its status
		// and headers, so they can see the Location it pointed to.
		var notAllowed *redirectNotAllowedError
		if errors.As(err, &notAllowed) {
			var details []any
			if resp != nil {
				out := httpapi.SendRequestOutput{
					StatusCode:     int64(resp.StatusCode),
					Status:         resp.Status,
					Headers:        httpapi.SendRequestOutputHeaders{AdditionalProperties: resp.Header},
					DurationMillis: time.Since(start).Milliseconds(),
				}
				redactResponse(&out, secrets)
				details = append(details, out)
			}
			return httpapi.SendRequestOutput{}, temporal.NewApplicationErrorWithOptions(
				redact(fmt.Sprintf("%s %v", req.URL, err), secrets), ErrorTypeClientError,
				temporal.ApplicationErrorOptions{NonRetryable: true, Details: details})
		}
		if attemptTimedOut(err, start, req) {
			msg := fmt.Sprintf("calling %s: no response within timeoutSeconds (%s)", req.URL, timeout(req))
			return httpapi.SendRequestOutput{}, transientError(ctx, msg, nil)
		}
		// The rest can still hold a secret the server echoed, such as in a redirect's Location.
		msg := redact(fmt.Sprintf("calling %s: %v", req.URL, err), secrets)
		// A certificate that doesn't verify won't on the next attempt either.
		var certErr *tls.CertificateVerificationError
		if errors.As(err, &certErr) {
			return httpapi.SendRequestOutput{}, temporal.NewApplicationErrorWithOptions(msg, ErrorTypeClientError,
				temporal.ApplicationErrorOptions{NonRetryable: true})
		}
		return httpapi.SendRequestOutput{}, transientError(ctx, msg, nil)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes+1))
	elapsed := time.Since(start).Milliseconds()
	if err != nil {
		if attemptTimedOut(err, start, req) {
			msg := fmt.Sprintf("reading the response from %s: not finished within timeoutSeconds (%s)", req.URL, timeout(req))
			return httpapi.SendRequestOutput{}, transientError(ctx, msg, nil)
		}
		msg := redact(fmt.Sprintf("reading the response from %s: %v", req.URL, err), secrets)
		return httpapi.SendRequestOutput{}, transientError(ctx, msg, nil)
	}
	if len(body) > maxResponseBytes {
		msg := fmt.Sprintf("%s returned status %d, but its body is over the connector's %d MiB limit, so the response isn't returned",
			req.URL, resp.StatusCode, maxResponseBytes>>20)
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
	if err := a.ApplyStatus(ctx, req, resp.StatusCode, out); err != nil {
		return httpapi.SendRequestOutput{}, err
	}
	return out, nil
}

// ApplyStatus makes out, a response SendRequest returned for req, count as if the endpoint had
// answered with status. It is what SendRequest itself does with every response, exported for an
// activity that calls SendRequest and finds the real answer in the body, such as a 200 whose body
// says the caller is rate limited:
//
//	resp, err := a.SendRequest(ctx, req)
//	if err == nil && bodySaysRateLimited(resp) {
//		return nil, a.ApplyStatus(ctx, req, http.StatusTooManyRequests, resp)
//	}
//
// For a 2xx it clears req's rate-limit key and returns nil. Otherwise it returns the error the
// activity must return, with out in its details, by the status's type (see categorizeError):
//
//   - RateLimited (429): marks req's rate-limit key, so every call on this worker with the same
//     key waits out the window without being sent, and the next attempt waits the window too.
//   - Transient: the next attempt backs off as for any Transient error (1s, doubling up to 30s),
//     counting Transient errors in a row through the activity's heartbeat details.
//   - ClientError: not retried.
//
// ctx must be the activity's context. out should be SendRequest's response, which is already
// redacted; ApplyStatus doesn't redact it again.
func (a *HTTPActivities) ApplyStatus(ctx context.Context, req httpapi.SendRequestInput, status int, out httpapi.SendRequestOutput) error {
	key := RateLimitKey(req)
	if status >= 200 && status < 300 {
		a.lim.clear(key)
		return nil
	}

	msg := fmt.Sprintf("%s returned status %d", req.URL, status)
	if int64(status) != out.StatusCode {
		msg = fmt.Sprintf("%s returned status %d, treated as %d", req.URL, out.StatusCode, status)
	}
	errType := categorizeError(status)
	if errType == ErrorTypeRateLimited {
		// Every call on this worker with the same key now waits out the window.
		a.lim.mark(key, time.Now())
		activity.GetLogger(ctx).Warn("Rate limited", "status", status, "key", key, "window", a.lim.ttl.String())
		return rateLimitedError(ctx, msg, a.lim.ttl, out)
	}
	if errType == ErrorTypeTransient {
		return transientError(ctx, msg, out)
	}
	return temporal.NewApplicationErrorWithOptions(msg, errType, temporal.ApplicationErrorOptions{
		Details:      []any{out},
		NonRetryable: true,
	})
}

// attemptTimedOut reports whether err is the attempt running out its timeoutSeconds. Other
// timeouts, such as the transport's 30-second dial timeout, keep Go's own message.
func attemptTimedOut(err error, start time.Time, req httpapi.SendRequestInput) bool {
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout() && time.Since(start) >= timeout(req)
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
