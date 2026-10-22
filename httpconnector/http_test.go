package httpconnector

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/temporalio/nexus-connector-http/generated/httpapi"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
)

const testSecretVar = SecretEnvPrefix + "TEST"

// testWindow is the rate-limit window the tests use, independent of the environment.
const testWindow = 2 * time.Minute

// runSendRequest runs SendRequest in the activity test environment, so activity.GetLogger works.
// It runs against a, so the rate-limit cache keeps its state between calls.
func runSendRequest(t *testing.T, a *HTTPActivities, req httpapi.SendRequestInput) (httpapi.SendRequestOutput, error) {
	t.Helper()
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()
	env.RegisterActivity(a.SendRequest)

	val, err := env.ExecuteActivity(a.SendRequest, req)
	if err != nil {
		return httpapi.SendRequestOutput{}, err
	}
	var resp httpapi.SendRequestOutput
	require.NoError(t, val.Get(&resp))
	return resp, nil
}

func appErr(t *testing.T, err error) *temporal.ApplicationError {
	t.Helper()
	var ae *temporal.ApplicationError
	require.ErrorAs(t, err, &ae)
	return ae
}

// countingServer answers every request with status and body, and counts the requests.
func countingServer(t *testing.T, status int, body string) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(status)
		_, _ = io.WriteString(w, body)
	}))
	t.Cleanup(srv.Close)
	return srv, &hits
}

func TestCategorizeError(t *testing.T) {
	for status, want := range map[int]string{
		429: ErrorTypeRateLimited,
		300: ErrorTypeClientError,
		304: ErrorTypeClientError,
		400: ErrorTypeClientError,
		401: ErrorTypeClientError,
		404: ErrorTypeClientError,
		418: ErrorTypeClientError,
		419: ErrorTypeClientError,
		422: ErrorTypeClientError,
		426: ErrorTypeClientError,
		427: ErrorTypeClientError,
		428: ErrorTypeClientError,
		451: ErrorTypeClientError,
		501: ErrorTypeClientError,
		505: ErrorTypeClientError,
		511: ErrorTypeClientError,
		408: ErrorTypeTransient,
		409: ErrorTypeTransient,
		420: ErrorTypeTransient,
		421: ErrorTypeTransient,
		423: ErrorTypeTransient,
		424: ErrorTypeTransient,
		425: ErrorTypeTransient,
		500: ErrorTypeTransient,
		502: ErrorTypeTransient,
		503: ErrorTypeTransient,
		504: ErrorTypeTransient,
	} {
		require.Equal(t, want, categorizeError(status), "status %d", status)
	}
}

func TestSendRequest_Success(t *testing.T) {
	type seen struct{ method, query, header, body string }
	got := make(chan seen, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		got <- seen{r.Method, r.URL.Query().Get("foo"), r.Header.Get("X-H"), string(b)}
		w.Header().Set("X-Resp", "ok")
		w.WriteHeader(201)
		_, _ = io.WriteString(w, "hello")
	}))
	defer srv.Close()

	resp, err := runSendRequest(t, newWithWindow(testWindow), httpapi.SendRequestInput{
		URL:         srv.URL,
		Method:      new(httpapi.SendRequestInputMethodPost),
		QueryParams: &httpapi.SendRequestInputQueryParams{AdditionalProperties: map[string]string{"foo": "bar"}},
		Headers:     &httpapi.SendRequestInputHeaders{AdditionalProperties: map[string]string{"X-H": "hv"}},
		Body:        new(`{"a":1}`),
	})
	require.NoError(t, err)
	require.Equal(t, seen{"POST", "bar", "hv", `{"a":1}`}, <-got)
	require.Equal(t, int64(201), resp.StatusCode)
	require.Equal(t, "201 Created", resp.Status)
	require.Equal(t, "hello", resp.Body)
	require.Equal(t, []string{"ok"}, resp.Headers.AdditionalProperties["X-Resp"])
}

func TestSendRequest_DefaultsToGet(t *testing.T) {
	got := make(chan string, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got <- r.Method
	}))
	defer srv.Close()

	_, err := runSendRequest(t, newWithWindow(testWindow), httpapi.SendRequestInput{URL: srv.URL})
	require.NoError(t, err)
	require.Equal(t, http.MethodGet, <-got)
}

// Each non-2xx status fails with its type and retryability, and the error carries the response.
// itemsServer is a small API: GET /greeting returns a short text body, POST /items creates an
// item from a JSON body and returns it with an ID, and anything else fails with a JSON error.
func itemsServer(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/greeting":
			_, _ = io.WriteString(w, "hi")
		case r.Method == http.MethodPost && r.URL.Path == "/items":
			var item struct{ Name string }
			if err := json.NewDecoder(r.Body).Decode(&item); err != nil || item.Name == "" {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusBadRequest)
				_, _ = io.WriteString(w, `{"error":"name is required"}`)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			_, _ = io.WriteString(w, `{"id":"42","name":"`+item.Name+`"}`)
		case r.URL.Path == "/broken":
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = io.WriteString(w, "try later")
		default:
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			_, _ = io.WriteString(w, `{"error":"not found"}`)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

// A GET and a POST each return the endpoint's status code and body: as the result on success,
// and in the error's details on failure.
func TestSendRequest_GETAndPOST(t *testing.T) {
	srv := itemsServer(t)
	get := func(path string) httpapi.SendRequestInput {
		return httpapi.SendRequestInput{URL: srv.URL + path}
	}
	post := func(path, body string) httpapi.SendRequestInput {
		return httpapi.SendRequestInput{
			URL:     srv.URL + path,
			Method:  new(httpapi.SendRequestInputMethodPost),
			Headers: &httpapi.SendRequestInputHeaders{AdditionalProperties: map[string]string{"Content-Type": "application/json"}},
			Body:    new(body),
		}
	}
	for _, tc := range []struct {
		name       string
		req        httpapi.SendRequestInput
		wantStatus int64
		wantBody   string
		wantErr    string // the error type; empty for success
	}{
		{"GET succeeds with a short body", get("/greeting"), 200, "hi", ""},
		{"POST succeeds and returns the created item", post("/items", `{"name":"widget"}`), 201, `{"id":"42","name":"widget"}`, ""},
		{"GET of a missing resource fails with its body", get("/nothing"), 404, `{"error":"not found"}`, ErrorTypeClientError},
		{"POST of an invalid item fails with its body", post("/items", `{}`), 400, `{"error":"name is required"}`, ErrorTypeClientError},
		{"GET of a failing endpoint fails with its body", get("/broken"), 503, "try later", ErrorTypeTransient},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := runSendRequest(t, newWithWindow(testWindow), tc.req)
			if tc.wantErr != "" {
				ae := appErr(t, err)
				require.Equal(t, tc.wantErr, ae.Type())
				require.NoError(t, ae.Details(&resp))
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.wantStatus, resp.StatusCode)
			require.Equal(t, tc.wantBody, resp.Body)
		})
	}
}

func TestSendRequest_Classifies(t *testing.T) {
	for _, tc := range []struct {
		status       int
		wantType     string
		wantNonRetry bool
	}{
		{429, ErrorTypeRateLimited, false}, // retried after the window via NextRetryDelay
		{401, ErrorTypeClientError, true},
		{404, ErrorTypeClientError, true},
		{408, ErrorTypeTransient, false},
		{500, ErrorTypeTransient, false},
	} {
		srv, _ := countingServer(t, tc.status, "why")
		_, err := runSendRequest(t, newWithWindow(testWindow), httpapi.SendRequestInput{URL: srv.URL})
		ae := appErr(t, err)
		require.Equal(t, tc.wantType, ae.Type(), "status %d", tc.status)
		require.Equal(t, tc.wantNonRetry, ae.NonRetryable(), "status %d", tc.status)

		var resp httpapi.SendRequestOutput
		require.NoError(t, ae.Details(&resp), "status %d", tc.status)
		require.Equal(t, int64(tc.status), resp.StatusCode)
		require.Equal(t, "why", resp.Body)
	}
}

// A server that can't be reached fails with a retryable Transient error.
func TestSendRequest_NetworkFailureIsTransient(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	url := srv.URL
	srv.Close()

	_, err := runSendRequest(t, newWithWindow(testWindow), httpapi.SendRequestInput{URL: url})
	ae := appErr(t, err)
	require.Equal(t, ErrorTypeTransient, ae.Type())
	require.False(t, ae.NonRetryable())
}

// A server slower than timeoutSeconds fails with a retryable Transient error.
func TestSendRequest_TimeoutIsTransient(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	defer srv.Close()
	defer close(release)

	start := time.Now()
	_, err := runSendRequest(t, newWithWindow(testWindow), httpapi.SendRequestInput{URL: srv.URL, TimeoutSeconds: new(int64(1))})
	ae := appErr(t, err)
	require.Equal(t, ErrorTypeTransient, ae.Type())
	require.False(t, ae.NonRetryable())
	require.Less(t, time.Since(start), 10*time.Second)
}

// A request that can't be made fails with a non-retryable InvalidRequest, without being sent.
func TestSendRequest_InvalidRequestIsNotSent(t *testing.T) {
	srv, hits := countingServer(t, 200, "")
	_, err := runSendRequest(t, newWithWindow(testWindow), httpapi.SendRequestInput{URL: srv.URL, APIKeyParam: new("key")})
	ae := appErr(t, err)
	require.Equal(t, ErrorTypeInvalidRequest, ae.Type())
	require.True(t, ae.NonRetryable())
	require.Zero(t, hits.Load())
}

// An invalid request is reported as invalid even while its rate-limit key is cached: retrying it
// after the window would fail anyway.
func TestSendRequest_InvalidBeatsRateLimit(t *testing.T) {
	a := newWithWindow(testWindow)
	req := httpapi.SendRequestInput{URL: "ftp://x.test"}
	a.lim.mark(RateLimitKey(req), time.Now())

	_, err := runSendRequest(t, a, req)
	require.Equal(t, ErrorTypeInvalidRequest, appErr(t, err).Type())
}

// A live cache entry fails the call with RateLimited without sending it, and the next attempt
// waits until the entry expires.
func TestSendRequest_GateBlocksWhenCached(t *testing.T) {
	t.Setenv(testSecretVar, "s3cr3t")
	srv, hits := countingServer(t, 200, "")
	a := newWithWindow(testWindow)
	req := httpapi.SendRequestInput{URL: srv.URL, Username: new("bob"), APIKeyEnvVar: new(testSecretVar)}
	a.lim.mark(RateLimitKey(req), time.Now().Add(-20*time.Second)) // 20s of the window gone

	_, err := runSendRequest(t, a, req)
	ae := appErr(t, err)
	require.Equal(t, ErrorTypeRateLimited, ae.Type())
	require.False(t, ae.NonRetryable())
	require.Zero(t, hits.Load())
	require.InDelta(t, testWindow-20*time.Second, ae.NextRetryDelay(), float64(2*time.Second))
}

// A 429 caches the key and delays the next attempt by the whole window.
func TestSendRequest_429CachesKey(t *testing.T) {
	srv, _ := countingServer(t, 429, "")
	a := newWithWindow(testWindow)
	req := httpapi.SendRequestInput{URL: srv.URL}
	key := RateLimitKey(req)
	require.Zero(t, a.lim.remaining(key, time.Now()))

	_, err := runSendRequest(t, a, req)
	ae := appErr(t, err)
	require.Equal(t, ErrorTypeRateLimited, ae.Type())
	require.Equal(t, testWindow, ae.NextRetryDelay())
	require.Positive(t, a.lim.remaining(key, time.Now()))
}

// The rate-limit window starts when the 429 arrives, not when the request was sent, so a slow 429
// doesn't shorten it.
func TestSendRequest_WindowStartsAtThe429(t *testing.T) {
	const slow = time.Second
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(slow) //nolint:forbidigo // a server that answers slowly is what this test is about
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()
	a := newWithWindow(testWindow)
	req := httpapi.SendRequestInput{URL: srv.URL}

	_, err := runSendRequest(t, a, req)
	require.Equal(t, ErrorTypeRateLimited, appErr(t, err).Type())
	require.Greater(t, a.lim.remaining(RateLimitKey(req), time.Now()), testWindow-slow/2)
}

// A success clears its key, even one another call marked while this request was in flight, and
// leaves other keys alone.
func TestSendRequest_SuccessClearsKey(t *testing.T) {
	a := newWithWindow(testWindow)
	var key string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		a.lim.mark(key, time.Now()) // a concurrent call got a 429 for the same key
	}))
	defer srv.Close()
	req := httpapi.SendRequestInput{URL: srv.URL}
	key = RateLimitKey(req)
	a.lim.mark("other", time.Now())

	_, err := runSendRequest(t, a, req)
	require.NoError(t, err)
	require.Zero(t, a.lim.remaining(key, time.Now()))
	require.Positive(t, a.lim.remaining("other", time.Now()))
}

// A 429 limits only its own URL and caller: the same URL with another username or an API key, and
// the same username on another URL, still go out, and their successes don't reopen the limited key.
func TestSendRequest_429LimitsOnlyItsOwnKey(t *testing.T) {
	t.Setenv(testSecretVar, "s3cr3t")
	var mu sync.Mutex
	hits := map[string]int{} // "user path" -> requests received
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, _, _ := r.BasicAuth()
		mu.Lock()
		hits[user+" "+r.URL.Path]++
		mu.Unlock()
		if user == "bob" && r.URL.Path == "/a" {
			w.WriteHeader(http.StatusTooManyRequests)
		}
	}))
	defer srv.Close()
	a := newWithWindow(testWindow)
	bobOnA := httpapi.SendRequestInput{URL: srv.URL + "/a", Username: new("bob"), APIKeyEnvVar: new(testSecretVar)}

	_, err := runSendRequest(t, a, bobOnA)
	require.Equal(t, ErrorTypeRateLimited, appErr(t, err).Type())

	for name, req := range map[string]httpapi.SendRequestInput{
		"another user, same URL":   {URL: srv.URL + "/a", Username: new("alice"), APIKeyEnvVar: new(testSecretVar)},
		"an API key, same URL":     {URL: srv.URL + "/a", APIKeyEnvVar: new(testSecretVar)},
		"same user, another URL":   {URL: srv.URL + "/b", Username: new("bob"), APIKeyEnvVar: new(testSecretVar)},
		"no credentials, same URL": {URL: srv.URL + "/a"},
	} {
		_, err := runSendRequest(t, a, req)
		require.NoError(t, err, name)
	}

	_, err = runSendRequest(t, a, bobOnA)
	require.Equal(t, ErrorTypeRateLimited, appErr(t, err).Type())
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, 1, hits["bob /a"], "the limited key must not be sent again")
	require.Equal(t, map[string]int{"bob /a": 1, "alice /a": 1, " /a": 2, "bob /b": 1}, hits)
}

// The activity's ScheduleToCloseTimeout is the request's scheduleToCloseSeconds, 25 hours if unset.
func TestActivityOptions_ScheduleToClose(t *testing.T) {
	require.Equal(t, 25*time.Hour, ActivityOptions("id", httpapi.SendRequestInput{URL: "https://x.test"}).ScheduleToCloseTimeout)
	require.Equal(t, 10*time.Minute, ActivityOptions("id", httpapi.SendRequestInput{URL: "https://x.test", ScheduleToCloseSeconds: new(int64(600))}).ScheduleToCloseTimeout)
}

// A request whose scheduleToCloseSeconds doesn't end before the caller's Nexus operation times out
// is rejected, with a message that says what to change.
func TestCheckCallerTimeout(t *testing.T) {
	tenMinutes := httpapi.SendRequestInput{URL: "https://x.test", ScheduleToCloseSeconds: new(int64(600))}
	unset := httpapi.SendRequestInput{URL: "https://x.test"}
	for _, tc := range []struct {
		name    string
		req     httpapi.SendRequestInput
		header  string
		wantErr string // empty: accepted
	}{
		{"no operation timeout", unset, "", ""},
		{"operation outlasts the request", tenMinutes, "900000ms", ""},
		{"operation outlasts the default", unset, "90060000ms", ""},
		{"operation ends first", tenMinutes, "300000ms", "scheduleToCloseSeconds is 600 (10m0s), but the caller's Nexus operation times out in 5m0s"},
		{"operation ends at the same time", tenMinutes, "600000ms", "times out in 10m0s"},
		{"default outlasts the operation", unset, "3600000ms", "scheduleToCloseSeconds is 90000 (25h0m0s), but the caller's Nexus operation times out in 1h0m0s"},
		{"unreadable header", tenMinutes, "soon", `can't read the operation-timeout header "soon"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := CheckCallerTimeout(tc.req, tc.header)
			if tc.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tc.wantErr)
			require.NotContains(t, err.Error(), "s3cr3t")
		})
	}
}

// retryDelays runs one attempt per status against a server that answers with those statuses in
// turn, the way the server retries the activity: each attempt gets the heartbeat details the one
// before it recorded, and the rate-limit window has passed by the time it runs. It returns the
// NextRetryDelay each attempt asked for.
func retryDelays(t *testing.T, statuses ...int) []time.Duration {
	t.Helper()
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(statuses[hits.Add(1)-1])
	}))
	defer srv.Close()
	a := newWithWindow(testWindow)
	req := httpapi.SendRequestInput{URL: srv.URL}

	var delays []time.Duration
	var details *int
	for range statuses {
		a.lim.clear(RateLimitKey(req)) // the window has passed
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestActivityEnvironment()
		env.RegisterActivity(a.SendRequest)
		if details != nil {
			env.SetHeartbeatDetails(*details)
		}
		env.SetOnActivityHeartbeatListener(func(_ *activity.Info, d converter.EncodedValues) {
			var n int
			require.NoError(t, d.Get(&n))
			details = &n
		})
		_, err := env.ExecuteActivity(a.SendRequest, req)
		delays = append(delays, appErr(t, err).NextRetryDelay())
	}
	return delays
}

// Transient errors back off from 1s, doubling up to 30s. After a rate limit, the window is the
// wait, and the next Transient error starts again from 1s rather than continuing from the attempt
// count.
func TestSendRequest_RetryDelays(t *testing.T) {
	const s = time.Second
	for _, tc := range []struct {
		name     string
		statuses []int
		want     []time.Duration
	}{
		{"transient only", []int{503, 503, 503}, []time.Duration{1 * s, 2 * s, 4 * s}},
		{"rate limited, then transient", []int{429, 503, 503, 503}, []time.Duration{testWindow, 1 * s, 2 * s, 4 * s}},
		{"rate limited twice, then transient", []int{429, 429, 503}, []time.Duration{testWindow, testWindow, 1 * s}},
		{"transient, rate limited, transient", []int{503, 503, 429, 503}, []time.Duration{1 * s, 2 * s, testWindow, 1 * s}},
		{"backoff is capped at 30s", []int{503, 503, 503, 503, 503, 503, 503}, []time.Duration{1 * s, 2 * s, 4 * s, 8 * s, 16 * s, 30 * s, 30 * s}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, retryDelays(t, tc.statuses...))
		})
	}
}

func TestRateLimitKey(t *testing.T) {
	require.Equal(t, "https://x.test|bob", RateLimitKey(httpapi.SendRequestInput{URL: "https://x.test", Username: new("bob"), APIKeyEnvVar: new(testSecretVar)}))
	require.Equal(t, "https://x.test|"+testSecretVar, RateLimitKey(httpapi.SendRequestInput{URL: "https://x.test", APIKeyEnvVar: new(testSecretVar)}))
	require.Equal(t, "https://x.test|anon", RateLimitKey(httpapi.SendRequestInput{URL: "https://x.test"}))
	authVar := httpapi.SendRequestInput{URL: "https://x.test", Headers: &httpapi.SendRequestInputHeaders{
		AdditionalProperties: map[string]string{"authorization": testSecretVar},
	}}
	require.Equal(t, "https://x.test|"+testSecretVar, RateLimitKey(authVar))
	authVar.APIKeyEnvVar = new(SecretEnvPrefix + "OTHER")
	require.Equal(t, "https://x.test|"+SecretEnvPrefix+"OTHER", RateLimitKey(authVar))
}

// An Authorization header whose value is a secret variable's name is sent as that variable's
// value, whatever the header key's case. Any other Authorization value is sent as it is. The
// value never appears in the error message.
func TestSendRequest_AuthorizationFromEnvVar(t *testing.T) {
	t.Setenv(testSecretVar, "Bearer s3cr3t")
	got := make(chan string, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got <- r.Header.Get("Authorization")
		w.WriteHeader(403)
	}))
	defer srv.Close()

	for header, want := range map[string]string{
		testSecretVar:    "Bearer s3cr3t",
		"Bearer literal": "Bearer literal",
	} {
		_, err := runSendRequest(t, newWithWindow(testWindow), httpapi.SendRequestInput{
			URL:     srv.URL,
			Headers: &httpapi.SendRequestInputHeaders{AdditionalProperties: map[string]string{"authorization": header}},
		})
		require.Equal(t, want, <-got)
		ae := appErr(t, err)
		require.Equal(t, ErrorTypeClientError, ae.Type())
		require.NotContains(t, ae.Error(), "s3cr3t")
	}
}

// The API key goes in the named query parameter. It never appears in the error message.
func TestSendRequest_APIKeyAsQueryParam(t *testing.T) {
	t.Setenv(testSecretVar, "s3cr3t")
	got := make(chan string, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got <- r.URL.Query().Get("key")
		w.WriteHeader(403)
	}))
	defer srv.Close()

	_, err := runSendRequest(t, newWithWindow(testWindow), httpapi.SendRequestInput{
		URL:          srv.URL,
		APIKeyEnvVar: new(testSecretVar),
		APIKeyParam:  new("key"),
	})
	require.Equal(t, "s3cr3t", <-got)
	ae := appErr(t, err)
	require.Equal(t, ErrorTypeClientError, ae.Type())
	require.NotContains(t, ae.Error(), "s3cr3t")
}

// A request that fails on the network doesn't put the URL it sent, which has the API key in its
// query, in the error message.
func TestSendRequest_NetworkErrorHidesAPIKey(t *testing.T) {
	t.Setenv(testSecretVar, "s3cr3t")
	srv := httptest.NewServer(nil)
	srv.Close()

	_, err := runSendRequest(t, newWithWindow(testWindow), httpapi.SendRequestInput{
		URL:          srv.URL,
		APIKeyEnvVar: new(testSecretVar),
		APIKeyParam:  new("key"),
	})
	ae := appErr(t, err)
	require.Equal(t, ErrorTypeTransient, ae.Type())
	require.Contains(t, ae.Error(), "connection refused")
	require.NotContains(t, ae.Error(), "s3cr3t")
}

// A redirect the client can't follow doesn't put the API key in the error, even when the server
// echoes it, escaped, in the Location header.
func TestSendRequest_RedirectErrorHidesAPIKey(t *testing.T) {
	t.Setenv(testSecretVar, "s3cr3t/x")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", "/%zz?"+r.URL.RawQuery) // unparseable
		w.WriteHeader(http.StatusFound)
	}))
	defer srv.Close()

	_, err := runSendRequest(t, newWithWindow(testWindow), httpapi.SendRequestInput{
		URL:          srv.URL,
		APIKeyEnvVar: new(testSecretVar),
		APIKeyParam:  new("key"),
	})
	ae := appErr(t, err)
	require.Equal(t, ErrorTypeTransient, ae.Type())
	require.Contains(t, ae.Error(), "failed to parse Location header")
	require.Contains(t, ae.Error(), "[REDACTED]")
	require.NotContains(t, ae.Error(), "s3cr3t")
}

// A redirect sends no Referer, which would carry the API key in the first URL's query to the next
// server.
func TestSendRequest_RedirectSendsNoReferer(t *testing.T) {
	t.Setenv(testSecretVar, "s3cr3t")
	referer := make(chan string, 1)
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		referer <- r.Header.Get("Referer")
	}))
	defer other.Close()
	srv := httptest.NewServer(http.RedirectHandler(other.URL, http.StatusFound))
	defer srv.Close()

	_, err := runSendRequest(t, newWithWindow(testWindow), httpapi.SendRequestInput{
		URL:          srv.URL,
		APIKeyEnvVar: new(testSecretVar),
		APIKeyParam:  new("key"),
	})
	require.NoError(t, err)
	require.Empty(t, <-referer)
}

// trustTLS makes SendRequest, which uses http.DefaultTransport, trust srv's test certificate for
// the rest of the test. It changes a process-wide value, so a test that uses it can't run in
// parallel.
func trustTLS(t *testing.T, srv *httptest.Server) {
	t.Helper()
	transport, ok := http.DefaultTransport.(*http.Transport)
	require.True(t, ok)
	transport.TLSClientConfig = srv.Client().Transport.(*http.Transport).TLSClientConfig
	t.Cleanup(func() { transport.TLSClientConfig = nil })
}

// A redirect from https to http isn't followed, so nothing goes to the http server, credentials
// included. The redirect itself fails as a ClientError with its response.
func TestSendRequest_NoRedirectFromHTTPSToHTTP(t *testing.T) {
	t.Setenv(testSecretVar, "Bearer s3cr3t")
	plain, hits := countingServer(t, 200, "")
	srv := httptest.NewTLSServer(http.RedirectHandler(plain.URL, http.StatusFound))
	defer srv.Close()
	trustTLS(t, srv)

	_, err := runSendRequest(t, newWithWindow(testWindow), httpapi.SendRequestInput{
		URL:     srv.URL,
		Headers: &httpapi.SendRequestInputHeaders{AdditionalProperties: map[string]string{"Authorization": testSecretVar}},
	})
	ae := appErr(t, err)
	require.Equal(t, ErrorTypeClientError, ae.Type())
	var out httpapi.SendRequestOutput
	require.NoError(t, ae.Details(&out))
	require.Equal(t, int64(http.StatusFound), out.StatusCode)
	require.Equal(t, []string{plain.URL}, out.Headers.AdditionalProperties["Location"])
	require.Zero(t, hits.Load())
}

// A redirect keeps the Authorization header only within the first URL's origin. http.Client alone
// would also send it to another port, scheme or subdomain of the same host.
func TestSendRequest_RedirectDropsAuthorizationForAnotherOrigin(t *testing.T) {
	t.Setenv(testSecretVar, "Bearer s3cr3t")
	got := make(chan string, 1)
	final := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/start" {
			http.Redirect(w, r, "/", http.StatusFound)
			return
		}
		got <- r.Header.Get("Authorization")
	}))
	defer final.Close()
	auth := &httpapi.SendRequestInputHeaders{AdditionalProperties: map[string]string{"Authorization": testSecretVar}}

	// A redirect within the same server: the same origin.
	_, err := runSendRequest(t, newWithWindow(testWindow), httpapi.SendRequestInput{
		URL: final.URL + "/start", Headers: auth,
	})
	require.NoError(t, err)
	require.Equal(t, "Bearer s3cr3t", <-got)

	// Another port on the same host: another origin.
	other := httptest.NewServer(http.RedirectHandler(final.URL, http.StatusFound))
	defer other.Close()
	_, err = runSendRequest(t, newWithWindow(testWindow), httpapi.SendRequestInput{URL: other.URL, Headers: auth})
	require.NoError(t, err)
	require.Empty(t, <-got)
}

// A secret the endpoint echoes back is redacted from the response's headers and body, as is and
// as escaped in JSON.
func TestSendRequest_RedactsEchoedSecrets(t *testing.T) {
	t.Setenv(testSecretVar, `s3cr\3t`)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")
		echo, err := json.Marshal(map[string]string{"key": key})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("X-Echo", key)
		w.WriteHeader(http.StatusBadRequest)
		_, _ = io.WriteString(w, key+" "+string(echo))
	}))
	defer srv.Close()

	_, err := runSendRequest(t, newWithWindow(testWindow), httpapi.SendRequestInput{
		URL:          srv.URL,
		APIKeyEnvVar: new(testSecretVar),
		APIKeyParam:  new("key"),
	})
	ae := appErr(t, err)
	var out httpapi.SendRequestOutput
	require.NoError(t, ae.Details(&out))
	require.Equal(t, `[REDACTED] {"key":"[REDACTED]"}`, out.Body)
	require.Equal(t, []string{"[REDACTED]"}, out.Headers.AdditionalProperties["X-Echo"])
}

// The redacted value of an Authorization variable includes its credentials without the scheme,
// which an endpoint can echo alone.
func TestSendRequest_RedactsAuthorizationCredentials(t *testing.T) {
	t.Setenv(testSecretVar, "Bearer s3cr3t")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "token "+strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "))
	}))
	defer srv.Close()

	out, err := runSendRequest(t, newWithWindow(testWindow), httpapi.SendRequestInput{
		URL:     srv.URL,
		Headers: &httpapi.SendRequestInputHeaders{AdditionalProperties: map[string]string{"Authorization": testSecretVar}},
	})
	require.NoError(t, err)
	require.Equal(t, "token [REDACTED]", out.Body)
}

// A secret Go quotes in an error message, escaping its backslash, is redacted too.
func TestSendRequest_RedirectErrorHidesQuotedAPIKey(t *testing.T) {
	t.Setenv(testSecretVar, `s3cr\3t`)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", "/%zz?"+r.URL.Query().Get("key")) // unparseable
		w.WriteHeader(http.StatusFound)
	}))
	defer srv.Close()

	_, err := runSendRequest(t, newWithWindow(testWindow), httpapi.SendRequestInput{
		URL:          srv.URL,
		APIKeyEnvVar: new(testSecretVar),
		APIKeyParam:  new("key"),
	})
	msg := appErr(t, err).Error()
	require.Contains(t, msg, "failed to parse Location header")
	require.NotContains(t, msg, `s3cr\\3t`)
	require.NotContains(t, msg, `s3cr\3t`)
}

// Redirects stop after maxRedirects, as with http.Client's default policy.
func TestSendRequest_RedirectLimit(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		http.Redirect(w, r, "/again", http.StatusFound)
	}))
	defer srv.Close()

	_, err := runSendRequest(t, newWithWindow(testWindow), httpapi.SendRequestInput{URL: srv.URL})
	require.ErrorContains(t, err, "stopped after 10 redirects")
	require.Equal(t, int32(maxRedirects), hits.Load())
}

// A body of up to maxResponseBytes is returned; a longer one fails with a non-retryable
// ClientError instead of being read into memory.
func TestSendRequest_ResponseSizeLimit(t *testing.T) {
	for _, tc := range []struct {
		size    int
		wantErr bool
	}{{maxResponseBytes, false}, {maxResponseBytes + 1, true}, {10 * maxResponseBytes, true}} {
		srv, _ := countingServer(t, 200, strings.Repeat("x", tc.size))
		out, err := runSendRequest(t, newWithWindow(testWindow), httpapi.SendRequestInput{URL: srv.URL})
		if !tc.wantErr {
			require.NoError(t, err)
			require.Len(t, out.Body, tc.size)
			continue
		}
		ae := appErr(t, err)
		require.Equal(t, ErrorTypeClientError, ae.Type(), "size %d", tc.size)
		require.True(t, ae.NonRetryable())
		require.Contains(t, ae.Error(), "body over")
	}
}

// With no apiKeyParam, the API key goes in an Authorization Bearer header.
func TestSendRequest_APIKeyAsBearerHeader(t *testing.T) {
	t.Setenv(testSecretVar, "s3cr3t")
	got := make(chan string, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got <- r.Header.Get("Authorization")
	}))
	defer srv.Close()

	_, err := runSendRequest(t, newWithWindow(testWindow), httpapi.SendRequestInput{URL: srv.URL, APIKeyEnvVar: new(testSecretVar)})
	require.NoError(t, err)
	require.Equal(t, "Bearer s3cr3t", <-got)
}

// With a username, the API key is the basic auth password instead of a Bearer token. The key
// never appears in the error message, and an echo of the encoded header is redacted.
func TestSendRequest_BasicAuth(t *testing.T) {
	t.Setenv(testSecretVar, "s3cr3t")
	type creds struct {
		user, pass string
		ok         bool
	}
	got := make(chan creds, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u, p, ok := r.BasicAuth()
		got <- creds{u, p, ok}
		w.WriteHeader(http.StatusForbidden)
		_, _ = io.WriteString(w, r.Header.Get("Authorization"))
	}))
	defer srv.Close()

	_, err := runSendRequest(t, newWithWindow(testWindow), httpapi.SendRequestInput{
		URL:          srv.URL,
		Username:     new("bob"),
		APIKeyEnvVar: new(testSecretVar),
	})
	require.Equal(t, creds{"bob", "s3cr3t", true}, <-got)
	ae := appErr(t, err)
	require.NotContains(t, ae.Error(), "s3cr3t")
	var out httpapi.SendRequestOutput
	require.NoError(t, ae.Details(&out))
	require.Equal(t, "Basic [REDACTED]", out.Body)
}

// A variable without the prefix is refused, even when it is set, and the request isn't sent: a
// caller must not be able to send any of the worker's environment to a URL it chooses.
func TestSendRequest_APIKeyEnvVarNeedsPrefix(t *testing.T) {
	t.Setenv("TEMPORAL_API_KEY_FOR_TEST", "s3cr3t")
	srv, hits := countingServer(t, 200, "")

	_, err := runSendRequest(t, newWithWindow(testWindow), httpapi.SendRequestInput{URL: srv.URL, APIKeyEnvVar: new("TEMPORAL_API_KEY_FOR_TEST")})
	ae := appErr(t, err)
	require.Equal(t, ErrorTypeInvalidRequest, ae.Type())
	require.True(t, ae.NonRetryable())
	require.Contains(t, ae.Error(), SecretEnvPrefix)
	require.NotContains(t, ae.Error(), "s3cr3t")
	require.Zero(t, hits.Load())
}

// The contract limits method to a fixed set of upper-case names, so a request with any other
// method is rejected when it is decoded, before the operation or the activity sees it.
func TestContract_RejectsUnlistedMethod(t *testing.T) {
	for _, method := range []string{"FROB", "get"} {
		var req httpapi.SendRequestInput
		err := json.Unmarshal([]byte(`{"url":"https://x.test","method":"`+method+`"}`), &req)
		ae := appErr(t, err)
		require.Equal(t, "PayloadValidationError", ae.Type())
		var violations []httpapi.Violation
		require.NoError(t, ae.Details(&violations))
		require.Len(t, violations, 1)
		require.Equal(t, "method", violations[0].Path)
	}
}

func TestValidate(t *testing.T) {
	t.Setenv(testSecretVar, "s3cr3t")
	t.Setenv(SecretEnvPrefix+"EMPTY", "")
	t.Setenv(SecretEnvPrefix+"NEWLINE", "s3cr3t\n")
	for _, tc := range []struct {
		name    string
		req     httpapi.SendRequestInput
		wantErr string // empty: valid
	}{
		{"https", httpapi.SendRequestInput{URL: "https://x.test/a?b=c"}, ""},
		{"http with method", httpapi.SendRequestInput{URL: "http://x.test", Method: new(httpapi.SendRequestInputMethodDelete)}, ""},
		{"api key", httpapi.SendRequestInput{URL: "https://x.test", APIKeyEnvVar: new(testSecretVar), APIKeyParam: new("key")}, ""},
		{"empty URL", httpapi.SendRequestInput{}, "scheme must be http or https"},
		{"relative URL", httpapi.SendRequestInput{URL: "/path"}, "scheme must be http or https"},
		{"other scheme", httpapi.SendRequestInput{URL: "ftp://x.test"}, "scheme must be http or https"},
		{"no host", httpapi.SendRequestInput{URL: "https:///path"}, "no host"},
		{"unparseable URL", httpapi.SendRequestInput{URL: "https://x.test/%zz"}, "invalid URL"},
		{"bad method", httpapi.SendRequestInput{URL: "https://x.test", Method: new(httpapi.SendRequestInputMethod("BAD METHOD"))}, "can't build the request"},
		{"key without prefix", httpapi.SendRequestInput{URL: "https://x.test", APIKeyEnvVar: new("HOME")}, "must start with " + SecretEnvPrefix},
		{"key unset", httpapi.SendRequestInput{URL: "https://x.test", APIKeyEnvVar: new(SecretEnvPrefix + "UNSET")}, "unset or empty"},
		{"key empty", httpapi.SendRequestInput{URL: "https://x.test", APIKeyEnvVar: new(SecretEnvPrefix + "EMPTY")}, "unset or empty"},
		{"authorization from a variable", httpapi.SendRequestInput{URL: "https://x.test", Headers: &httpapi.SendRequestInputHeaders{
			AdditionalProperties: map[string]string{"Authorization": testSecretVar},
		}}, ""},
		{"authorization from an unset variable", httpapi.SendRequestInput{URL: "https://x.test", Headers: &httpapi.SendRequestInputHeaders{
			AdditionalProperties: map[string]string{"Authorization": SecretEnvPrefix + "UNSET"},
		}}, `"` + SecretEnvPrefix + `UNSET" is unset or empty`},
		{"header value with a newline", httpapi.SendRequestInput{URL: "https://x.test", Headers: &httpapi.SendRequestInputHeaders{
			AdditionalProperties: map[string]string{"X-A": "a\r\nX-B: b"},
		}}, `header "X-A" has an invalid value`},
		{"header name with a space", httpapi.SendRequestInput{URL: "https://x.test", Headers: &httpapi.SendRequestInputHeaders{
			AdditionalProperties: map[string]string{"X A": "a"},
		}}, `invalid header name "X A"`},
		{"authorization variable with a newline", httpapi.SendRequestInput{URL: "https://x.test", Headers: &httpapi.SendRequestInputHeaders{
			AdditionalProperties: map[string]string{"Authorization": SecretEnvPrefix + "NEWLINE"},
		}}, `header "Authorization" has an invalid value`},
		{"username with a key", httpapi.SendRequestInput{URL: "https://x.test", Username: new("bob"), APIKeyEnvVar: new(testSecretVar)}, ""},
		{"username without a key", httpapi.SendRequestInput{URL: "https://x.test", Username: new("bob")}, "username needs apiKeyEnvVar"},
		{"username with a query key", httpapi.SendRequestInput{
			URL: "https://x.test", Username: new("bob"), APIKeyEnvVar: new(testSecretVar), APIKeyParam: new("key"),
		}, "username can't be used with apiKeyParam"},
		{"param without key", httpapi.SendRequestInput{URL: "https://x.test", APIKeyParam: new("key")}, "apiKeyEnvVar is not"},
		{"shortest timeout", httpapi.SendRequestInput{URL: "https://x.test", TimeoutSeconds: new(int64(1))}, ""},
		{"longest timeout", httpapi.SendRequestInput{URL: "https://x.test", TimeoutSeconds: new(int64(60))}, ""},
		{"zero timeout", httpapi.SendRequestInput{URL: "https://x.test", TimeoutSeconds: new(int64(0))}, "between 1 and 60"},
		{"negative timeout", httpapi.SendRequestInput{URL: "https://x.test", TimeoutSeconds: new(int64(-5))}, "between 1 and 60"},
		{"timeout over the attempt limit", httpapi.SendRequestInput{URL: "https://x.test", TimeoutSeconds: new(int64(61))}, "between 1 and 60"},
		{"total of 25 hours", httpapi.SendRequestInput{URL: "https://x.test", ScheduleToCloseSeconds: new(int64(90000))}, ""},
		{"total equal to the attempt timeout", httpapi.SendRequestInput{URL: "https://x.test", TimeoutSeconds: new(int64(20)), ScheduleToCloseSeconds: new(int64(20))}, ""},
		{"total over 25 hours", httpapi.SendRequestInput{URL: "https://x.test", ScheduleToCloseSeconds: new(int64(90001))}, "between timeoutSeconds (30) and 90000"},
		{"total shorter than the default attempt timeout", httpapi.SendRequestInput{URL: "https://x.test", ScheduleToCloseSeconds: new(int64(29))}, "between timeoutSeconds (30) and 90000"},
		{"total shorter than a set attempt timeout", httpapi.SendRequestInput{URL: "https://x.test", TimeoutSeconds: new(int64(45)), ScheduleToCloseSeconds: new(int64(40))}, "between timeoutSeconds (45) and 90000"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := Validate(tc.req)
			if tc.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tc.wantErr)
		})
	}
}
