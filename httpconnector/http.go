// Package httpconnector is the HTTP connector: a Nexus service whose one operation, request,
// makes an HTTP request. Send is the request itself, with no Temporal context needed. The
// operation runs it one of two ways, by Config.Mode: backed by a standalone activity, Request,
// which the operation starts and completes with (ModeActivity, the default); or within the Nexus
// request, with no activity (ModeSync, RequestSync). There is no handler workflow either way.
package httpconnector

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/temporalio/nexus-connector-http/generated/httpapi"
	"github.com/temporalio/nexus-connector-http/nexus-connector-shared/settings"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/temporalnexus"
	"go.temporal.io/sdk/worker"
)

// TaskQueue is the Outbound Connector's task queue. The Nexus endpoint targets it.
const TaskQueue = "nexus-connector-http"

// HTTPActivities is the HTTP connector's calls, Send and its two ways to run, plus the rate-limit
// cache they share. Register one instance per worker, so every call on that worker shares the
// cache.
type HTTPActivities struct {
	mode           Mode // how the operation runs each call (ModeEnvVar)
	lim            *limiter
	domains        []string      // allowed domains; nil allows any host (DisableAllowlist)
	allowRedirects bool          // whether redirects are followed
	maxTotal       time.Duration // the longest a call may take in all
	attemptTimeout time.Duration // the longest one attempt may take; each request gets requestMargin less
	// secretDomains maps each secret variable to the domains it may be sent to (SecretDomainsSuffix);
	// a nil list allows any allowed host. A variable not in it isn't sent anywhere.
	secretDomains map[string][]string
	// env is where secrets are read from when a request names one.
	env settings.Env
}

// Config is the Outbound Connector's configuration. The setup package reads it from the
// environment variables named here, checking each.
type Config struct {
	// Mode is how the request operation runs each call (ModeEnvVar); "" is ModeActivity.
	Mode Mode
	// AllowedDomains are the hosts requests may go to, each with its subdomains
	// (AllowedDomainsEnvVar), lower case; nil allows any host (DisableAllowlist).
	AllowedDomains []string
	// AllowRedirects is whether redirects are followed (AllowRedirectsEnvVar; see checkRedirect).
	AllowRedirects bool
	// AttemptTimeout is the longest one attempt may take (AttemptTimeoutEnvVar), at least
	// MinAttemptTimeout.
	AttemptTimeout time.Duration
	// RateLimitWindow is how long a key stays rate-limited after a 429 (RateLimitMinutesEnvVar).
	RateLimitWindow time.Duration
	// MaxDuration is the longest a call may take in all (MaxDurationEnvVar).
	MaxDuration time.Duration
	// SecretDomains maps each secret variable to the domains it may be sent to, from its
	// SecretDomainsSuffix variable (see ReadSecretDomains); nil allows any allowed host.
	SecretDomains map[string][]string
}

// New returns HTTPActivities with cfg and an empty rate-limit cache. Requests' secrets are read
// from env when a request names one.
func New(cfg Config, env settings.Env) *HTTPActivities {
	mode := cfg.Mode
	if mode == "" {
		mode = ModeActivity
	}
	return &HTTPActivities{
		mode:           mode,
		lim:            newLimiter(cfg.RateLimitWindow),
		domains:        cfg.AllowedDomains,
		allowRedirects: cfg.AllowRedirects,
		maxTotal:       cfg.MaxDuration,
		attemptTimeout: cfg.AttemptTimeout,
		secretDomains:  cfg.SecretDomains,
		env:            env,
	}
}

// Register registers the HTTPService Nexus service on w, with a's Mode. Everything uses a, so
// every call on the worker shares one rate-limit cache.
//
// With ModeSync, the request operation is synchronous: RequestSync makes one attempt within the
// Nexus request, and the caller's server retries a retryable failure. Nothing else is registered.
//
// With ModeActivity, the request operation rejects a request that can't be made, whose host isn't an allowed
// domain (see CheckDomain), that names a secret its host may not be sent (see CheckSecretDomains),
// or for which the caller's Nexus operation has too little time left
// (see ScheduleToClose), with a BadRequest handler error, without starting an activity; otherwise
// it starts Request as a standalone activity that ends shortly before the operation times out.
// The activity ID comes from the Nexus request ID, so a retried start of the same operation
// reaches the same activity.
func Register(w worker.Registry, a *HTTPActivities) {
	svc := nexus.NewService(httpapi.HTTPService.ServiceName)
	if a.mode == ModeSync {
		svc.MustRegister(nexus.NewSyncOperation(httpapi.HTTPService.Request.Name(),
			func(ctx context.Context, req httpapi.RequestInput, _ nexus.StartOperationOptions) (httpapi.RequestOutput, error) {
				return a.RequestSync(ctx, req)
			}))
		w.RegisterNexusService(svc)
		return
	}
	op := temporalnexus.MustNewTemporalOperation(temporalnexus.TemporalOperationOptions[httpapi.RequestInput, httpapi.RequestOutput]{
		Name: httpapi.HTTPService.Request.Name(),
		Start: func(
			ctx context.Context,
			nc temporalnexus.NexusClient,
			req httpapi.RequestInput,
			opts temporalnexus.StartTemporalOperationOptions,
		) (temporalnexus.TemporalOperationResult[httpapi.RequestOutput], error) {
			_, err := build(ctx, a.env, req)
			if err == nil {
				err = a.CheckDomain(req.URL)
			}
			if err == nil {
				err = a.CheckSecretDomains(req)
			}
			var total time.Duration
			if err == nil {
				total, err = a.ScheduleToClose(opts.Header.Get(nexus.HeaderOperationTimeout))
			}
			if err != nil {
				return temporalnexus.TemporalOperationResult[httpapi.RequestOutput]{}, &nexus.HandlerError{
					Type:  nexus.HandlerErrorTypeBadRequest,
					Cause: InvalidRequestError(err),
				}
			}
			return temporalnexus.StartActivity(ctx, nc, a.ActivityOptions("http-"+opts.RequestID, total), a.Request, req)
		},
	})
	svc.MustRegister(op)
	w.RegisterNexusService(svc)
	w.RegisterActivity(a.Request)
}

// Mode is how the request operation runs each call (ModeEnvVar).
func (a *HTTPActivities) Mode() Mode {
	return a.mode
}

// The retry policy backs off from backoffInitial, three times longer each attempt, up to
// backoffMax: 5s, 15s, 45s, 2m15s, then every 5m. A failing endpoint is rarely helped by being
// called more often.
const (
	backoffInitial     = 5 * time.Second
	backoffCoefficient = 3.0
	backoffMax         = 5 * time.Minute
)

// maxResponseBytes is the largest response body Request reads. A larger one couldn't be
// returned within Temporal's payload size limit (2 MB by default) anyway.
const maxResponseBytes = 2 << 20

// maxRedirects is how many redirects a request follows, as http.Client does by default.
const maxRedirects = 10

// ActivityOptions are the options for starting Request as a standalone activity with the
// given ID. Transient errors and attempt timeouts retry with the policy's backoff; RateLimited
// errors after the rest of the rate-limit window, or a 429's shorter Retry-After, which they set
// themselves (see rateLimitedError).
// scheduleToClose, from ScheduleToClose, bounds the whole call, including every retry and
// rate-limit wait.
func (a *HTTPActivities) ActivityOptions(id string, scheduleToClose time.Duration) client.StartActivityOptions {
	return client.StartActivityOptions{
		ID:                     id,
		StartToCloseTimeout:    a.attemptTimeout,
		ScheduleToCloseTimeout: scheduleToClose,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    backoffInitial,
			BackoffCoefficient: backoffCoefficient,
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

// Request is the activity behind the request operation in ModeActivity: Send, once per activity
// attempt. The activity's retry policy and the errors' NextRetryDelay do the retrying.
func (a *HTTPActivities) Request(ctx context.Context, req httpapi.RequestInput) (httpapi.RequestOutput, error) {
	return a.Send(ctx, req)
}

// Send makes one HTTP attempt. It needs no Temporal context: an activity (Request), a synchronous
// Nexus handler (RequestSync), or a library caller can run it. Its errors are ApplicationErrors
// whose types and NextRetryDelay say how to retry; an activity returns them as they are, and
// HandlerError turns them into a Nexus handler's.
//
//   - A request that can't be made, whose host isn't an allowed domain, or that names a secret
//     its host may not be sent, fails with InvalidRequest, without being sent.
//   - If another call for the same rate-limit key got a 429 within the last window, it fails with
//     RateLimited without being sent, and the next attempt waits for the rest of the window.
//   - Otherwise it is sent, and the response's status decides the result (see ApplyStatus). 2xx
//     returns the response and clears the key. 429 marks the key and fails with RateLimited. Other
//     statuses fail with ClientError or Transient (see categorizeError), with the response in the
//     error's details.
func (a *HTTPActivities) Send(ctx context.Context, req httpapi.RequestInput) (httpapi.RequestOutput, error) {
	logger := loggerFor(ctx)

	httpReq, err := build(ctx, a.env, req)
	if err == nil {
		err = a.CheckDomain(req.URL)
	}
	if err == nil {
		err = a.CheckSecretDomains(req)
	}
	if err != nil {
		return httpapi.RequestOutput{}, InvalidRequestError(err)
	}

	key := RateLimitKey(req)
	if wait := a.lim.remaining(key, time.Now()); wait > 0 {
		logger.Warn("Rate limited by an earlier 429, not sending", "key", key, "retryIn", wait.String())
		return httpapi.RequestOutput{}, rateLimitedError(fmt.Sprintf("rate limited (cached) for %s", key), wait, nil)
	}

	secrets := secrets(a.env, req)
	start := time.Now()
	resp, err := (&http.Client{Timeout: a.requestTimeout(), CheckRedirect: a.checkRedirect(secretHeaderVars(req))}).Do(httpReq)
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
				out := httpapi.RequestOutput{
					StatusCode:     int64(resp.StatusCode),
					Status:         resp.Status,
					Headers:        httpapi.RequestOutputHeaders{AdditionalProperties: resp.Header},
					DurationMillis: time.Since(start).Milliseconds(),
				}
				redactResponse(&out, secrets)
				details = append(details, out)
			}
			return httpapi.RequestOutput{}, temporal.NewApplicationErrorWithOptions(
				redact(fmt.Sprintf("%s %v", req.URL, err), secrets), ErrorTypeClientError,
				temporal.ApplicationErrorOptions{NonRetryable: true, Details: details})
		}
		if timedOut(err, start, a.requestTimeout()) {
			msg := fmt.Sprintf("calling %s: no response within %s", req.URL, a.requestTimeout())
			return httpapi.RequestOutput{}, transientError(msg, nil)
		}
		// The rest can still hold a secret the server echoed, such as in a redirect's Location.
		msg := redact(fmt.Sprintf("calling %s: %v", req.URL, err), secrets)
		// A certificate that doesn't verify won't on the next attempt either.
		var certErr *tls.CertificateVerificationError
		if errors.As(err, &certErr) {
			return httpapi.RequestOutput{}, temporal.NewApplicationErrorWithOptions(msg, ErrorTypeClientError,
				temporal.ApplicationErrorOptions{NonRetryable: true})
		}
		return httpapi.RequestOutput{}, transientError(msg, nil)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes+1))
	elapsed := time.Since(start).Milliseconds()
	if err != nil {
		if timedOut(err, start, a.requestTimeout()) {
			msg := fmt.Sprintf("reading the response from %s: not finished within %s", req.URL, a.requestTimeout())
			return httpapi.RequestOutput{}, transientError(msg, nil)
		}
		msg := redact(fmt.Sprintf("reading the response from %s: %v", req.URL, err), secrets)
		return httpapi.RequestOutput{}, transientError(msg, nil)
	}
	if len(body) > maxResponseBytes {
		msg := fmt.Sprintf("%s returned status %d, but its body is over the Outbound Connector's %d MiB limit, so the response isn't returned",
			req.URL, resp.StatusCode, maxResponseBytes>>20)
		return httpapi.RequestOutput{}, temporal.NewApplicationErrorWithOptions(msg, ErrorTypeClientError,
			temporal.ApplicationErrorOptions{NonRetryable: true})
	}

	out := httpapi.RequestOutput{
		StatusCode:     int64(resp.StatusCode),
		Status:         resp.Status,
		Headers:        httpapi.RequestOutputHeaders{AdditionalProperties: resp.Header},
		Body:           string(body),
		DurationMillis: elapsed,
	}
	redactResponse(&out, secrets)
	if err := a.ApplyStatus(ctx, req, resp.StatusCode, out); err != nil {
		return httpapi.RequestOutput{}, err
	}
	return out, nil
}

// ApplyStatus makes out, a response Send returned for req, count as if the endpoint had
// answered with status. It is what Send itself does with every response, exported for code that
// calls Send (or Request) and finds the real answer in the body, such as a 200 whose body
// says the caller is rate limited:
//
//	resp, err := a.Send(ctx, req)
//	if err == nil && bodySaysRateLimited(resp) {
//		return nil, a.ApplyStatus(ctx, req, http.StatusTooManyRequests, resp)
//	}
//
// For a 2xx it clears req's rate-limit key and returns nil. Otherwise it returns the error the
// activity must return, with out in its details, by the status's type (see categorizeError):
//
//   - RateLimited (429): marks req's rate-limit key, so every call on this worker with the same
//     key waits out the window without being sent, and the next attempt waits the window too. A
//     Retry-After header in out shorter than the window is the wait instead (see retryAfter).
//   - Transient: the next attempt waits the retry policy's backoff.
//   - ClientError: not retried.
//
// ctx is the caller's context: an activity's, a Nexus handler's, or any other. out should be Send's
// response, which is already redacted; ApplyStatus doesn't redact it again.
func (a *HTTPActivities) ApplyStatus(ctx context.Context, req httpapi.RequestInput, status int, out httpapi.RequestOutput) error {
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
		// Every call on this worker with the same key now waits as long as this one.
		now := time.Now()
		wait := retryAfter(out, now, a.lim.ttl)
		a.lim.mark(key, now, wait)
		loggerFor(ctx).Warn("Rate limited", "status", status, "key", key, "retryIn", wait.String())
		return rateLimitedError(msg, wait, out)
	}
	if errType == ErrorTypeTransient {
		return transientError(msg, out)
	}
	return temporal.NewApplicationErrorWithOptions(msg, errType, temporal.ApplicationErrorOptions{
		Details:      []any{out},
		NonRetryable: true,
	})
}

// timedOut reports whether err is the request, started at start, running out its timeout. Other
// timeouts, such as the transport's 30-second dial timeout, keep Go's own message.
func timedOut(err error, start time.Time, timeout time.Duration) bool {
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout() && time.Since(start) >= timeout
}

// loggerFor is the logger for a call running with ctx: the activity's or the Nexus operation's,
// with their tags, or else slog's default.
func loggerFor(ctx context.Context) log.Logger {
	switch {
	case activity.IsActivity(ctx):
		return activity.GetLogger(ctx)
	case temporalnexus.IsNexusOperation(ctx):
		return temporalnexus.GetLogger(ctx)
	}
	return log.NewStructuredLogger(slog.Default())
}
