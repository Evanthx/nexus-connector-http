package testcaller_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	"github.com/temporalio/nexus-connector-http/generated/httpapi"
	"github.com/temporalio/nexus-connector-http/httpconnector"
	"github.com/temporalio/nexus-connector-http/testcaller"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
)

const connectorNamespace = "connectors"

// TestEndToEnd runs the Connector and the test caller in separate namespaces on a dev server, as
// the README describes, and makes requests through the Nexus endpoint. It needs the temporal CLI
// on the PATH.
func TestEndToEnd(t *testing.T) {
	if testing.Short() {
		t.Skip("starts a dev server")
	}
	cli, err := exec.LookPath("temporal")
	if err != nil {
		t.Skip("the temporal CLI is not on the PATH")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	server, err := testsuite.StartDevServer(ctx, testsuite.DevServerOptions{
		ExistingPath:  cli,
		ClientOptions: &client.Options{Namespace: connectorNamespace},
		ExtraArgs: []string{
			"--namespace", testcaller.Namespace,
			"--dynamic-config-value", "activity.enableCallbacks=true",
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = server.Stop() })

	connectorClient := server.Client()
	_, err = connectorClient.OperatorService().CreateNexusEndpoint(ctx, &operatorservice.CreateNexusEndpointRequest{
		Spec: &nexuspb.EndpointSpec{
			Name: testcaller.EndpointName,
			Target: &nexuspb.EndpointTarget{Variant: &nexuspb.EndpointTarget_Worker_{Worker: &nexuspb.EndpointTarget_Worker{
				Namespace: connectorNamespace,
				TaskQueue: httpconnector.TaskQueue,
			}}},
		},
	})
	require.NoError(t, err)

	// The httptest servers listen on 127.0.0.1, so only they are allowed.
	t.Setenv(httpconnector.AllowedDomainsEnvVar, "127.0.0.1")
	// One secret may go to the httptest servers, the other only to a host they aren't.
	t.Setenv(httpconnector.SecretEnvPrefix+"LOCAL", "l0cal")
	t.Setenv(httpconnector.SecretEnvPrefix+"LOCAL"+httpconnector.SecretDomainsSuffix, "127.0.0.1")
	t.Setenv(httpconnector.SecretEnvPrefix+"ELSEWHERE", "elsewh3re")
	t.Setenv(httpconnector.SecretEnvPrefix+"ELSEWHERE"+httpconnector.SecretDomainsSuffix, "api.example.com")
	connectorWorker := worker.New(connectorClient, httpconnector.TaskQueue, worker.Options{})
	require.NoError(t, httpconnector.Register(connectorWorker))
	require.NoError(t, connectorWorker.Start())
	t.Cleanup(connectorWorker.Stop)

	callerClient, err := client.Dial(client.Options{HostPort: server.FrontendHostPort(), Namespace: testcaller.Namespace})
	require.NoError(t, err)
	t.Cleanup(callerClient.Close)
	callerWorker := worker.New(callerClient, testcaller.TaskQueue, worker.Options{})
	callerWorker.RegisterWorkflow(testcaller.CallerWorkflow)
	require.NoError(t, callerWorker.Start())
	t.Cleanup(callerWorker.Stop)

	// callWithTimeout runs CallerWorkflow with a Nexus operation timeout; call, with none.
	callWithTimeout := func(t *testing.T, req httpapi.RequestInput, timeout time.Duration) (httpapi.RequestOutput, error) {
		t.Helper()
		run, err := callerClient.ExecuteWorkflow(ctx, client.StartWorkflowOptions{TaskQueue: testcaller.TaskQueue}, testcaller.CallerWorkflow, req, timeout)
		require.NoError(t, err)
		var resp httpapi.RequestOutput
		err = run.Get(ctx, &resp)
		return resp, err
	}
	call := func(t *testing.T, req httpapi.RequestInput) (httpapi.RequestOutput, error) {
		t.Helper()
		return callWithTimeout(t, req, 0)
	}

	t.Run("success", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.WriteString(w, "echo "+r.URL.Query().Get("q")+" "+r.Header.Get("X-H"))
		}))
		defer srv.Close()

		resp, err := call(t, httpapi.RequestInput{
			URL:         srv.URL,
			QueryParams: &httpapi.RequestInputQueryParams{AdditionalProperties: map[string]string{"q": "one"}},
			Headers:     &httpapi.RequestInputHeaders{AdditionalProperties: map[string]string{"X-H": "two"}},
		})
		require.NoError(t, err)
		require.Equal(t, int64(200), resp.StatusCode)
		require.Equal(t, "echo one two", resp.Body)
	})

	// The server waits the retry policy's 5s before the second attempt.
	t.Run("transient failure is retried after 5s", func(t *testing.T) {
		var mu sync.Mutex
		var times []time.Time
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			times = append(times, time.Now())
			n := len(times)
			mu.Unlock()
			if n == 1 {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			_, _ = io.WriteString(w, "second time")
		}))
		defer srv.Close()

		resp, err := call(t, httpapi.RequestInput{URL: srv.URL})
		require.NoError(t, err)
		require.Equal(t, "second time", resp.Body)
		mu.Lock()
		defer mu.Unlock()
		require.Len(t, times, 2)
		require.InDelta(t, 5*time.Second, times[1].Sub(times[0]), float64(time.Second))
	})

	t.Run("retries stop before the caller's timeout, with the Connector's error", func(t *testing.T) {
		var hits atomic.Int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			hits.Add(1)
			w.WriteHeader(http.StatusServiceUnavailable)
		}))
		defer srv.Close()

		// The Connector retries for 25s - 10s margin = 15s: attempts at 0s and 5s, then the next
		// 15s wait doesn't fit, so it fails with the last 503. Had it kept retrying, the caller
		// would time out at 25s with no ApplicationError.
		const timeout = 25 * time.Second
		start := time.Now()
		_, err := callWithTimeout(t, httpapi.RequestInput{URL: srv.URL, TimeoutSeconds: new(int64(1))}, timeout)
		var appErr *temporal.ApplicationError
		require.ErrorAs(t, err, &appErr)
		require.Equal(t, httpconnector.ErrorTypeTransient, appErr.Type())
		require.Contains(t, appErr.Message(), "returned status 503")
		require.Less(t, time.Since(start), timeout)
		require.Equal(t, int32(2), hits.Load())
	})

	t.Run("client error reaches the caller with the response", func(t *testing.T) {
		var hits atomic.Int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			hits.Add(1)
			w.WriteHeader(http.StatusNotFound)
			_, _ = io.WriteString(w, "no such thing")
		}))
		defer srv.Close()

		_, err := call(t, httpapi.RequestInput{URL: srv.URL})
		var opErr *temporal.NexusOperationError
		require.ErrorAs(t, err, &opErr)
		var appErr *temporal.ApplicationError
		require.ErrorAs(t, err, &appErr)
		require.Equal(t, httpconnector.ErrorTypeClientError, appErr.Type())
		var resp httpapi.RequestOutput
		require.NoError(t, appErr.Details(&resp))
		require.Equal(t, int64(404), resp.StatusCode)
		require.Equal(t, "no such thing", resp.Body)
		require.Equal(t, int32(1), hits.Load(), "a client error must not be retried")
	})

	t.Run("caller timeout too short for one attempt is rejected", func(t *testing.T) {
		// timeoutSeconds defaults to 30, plus the 10s margin, is more than 20s.
		_, err := callWithTimeout(t, httpapi.RequestInput{URL: "http://127.0.0.1:1/never"}, 20*time.Second)
		var handlerErr *nexus.HandlerError
		require.ErrorAs(t, err, &handlerErr)
		require.Equal(t, nexus.HandlerErrorTypeBadRequest, handlerErr.Type)
		var appErr *temporal.ApplicationError
		require.ErrorAs(t, err, &appErr)
		require.Equal(t, httpconnector.ErrorTypeInvalidRequest, appErr.Type())
		require.Contains(t, appErr.Message(), "which leaves no time for an attempt: it needs at least timeoutSeconds (30s) plus 10s")
	})

	t.Run("invalid request fails when the operation starts", func(t *testing.T) {
		_, err := call(t, httpapi.RequestInput{URL: "ftp://example.com/file"})
		var handlerErr *nexus.HandlerError
		require.ErrorAs(t, err, &handlerErr)
		require.Equal(t, nexus.HandlerErrorTypeBadRequest, handlerErr.Type)
		var appErr *temporal.ApplicationError
		require.ErrorAs(t, err, &appErr)
		require.Equal(t, httpconnector.ErrorTypeInvalidRequest, appErr.Type())
		require.Contains(t, appErr.Message(), "scheme must be http or https")
	})

	t.Run("secret is sent to a host in its list", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.WriteString(w, r.Header.Get("Authorization"))
		}))
		defer srv.Close()
		resp, err := call(t, httpapi.RequestInput{URL: srv.URL, APIKeyEnvVar: new(httpconnector.SecretEnvPrefix + "LOCAL")})
		require.NoError(t, err)
		// The server got the key; the Connector redacts it where the server echoes it back.
		require.Equal(t, "Bearer [REDACTED]", resp.Body)
	})

	t.Run("secret not sent to a host outside its list, rejected when the operation starts", func(t *testing.T) {
		var hits atomic.Int32
		srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { hits.Add(1) }))
		defer srv.Close()
		_, err := call(t, httpapi.RequestInput{URL: srv.URL, Headers: &httpapi.RequestInputHeaders{
			AdditionalProperties: map[string]string{"X-Api-Key": httpconnector.SecretEnvPrefix + "ELSEWHERE"},
		}})
		var handlerErr *nexus.HandlerError
		require.ErrorAs(t, err, &handlerErr)
		require.Equal(t, nexus.HandlerErrorTypeBadRequest, handlerErr.Type)
		var appErr *temporal.ApplicationError
		require.ErrorAs(t, err, &appErr)
		require.Equal(t, httpconnector.ErrorTypeInvalidRequest, appErr.Type())
		require.Contains(t, appErr.Message(), httpconnector.SecretEnvPrefix+`ELSEWHERE may not be sent to "127.0.0.1"`)
		require.NotContains(t, appErr.Message(), "elsewh3re")
		require.Zero(t, hits.Load(), "nothing sent")
	})

	t.Run("domain not allowed fails when the operation starts", func(t *testing.T) {
		_, err := call(t, httpapi.RequestInput{URL: "https://example.com/file"})
		var handlerErr *nexus.HandlerError
		require.ErrorAs(t, err, &handlerErr)
		require.Equal(t, nexus.HandlerErrorTypeBadRequest, handlerErr.Type)
		var appErr *temporal.ApplicationError
		require.ErrorAs(t, err, &appErr)
		require.Equal(t, httpconnector.ErrorTypeInvalidRequest, appErr.Type())
		require.Contains(t, appErr.Message(), `domain "example.com" is not on the Connector's list of allowed domains`)
	})
}
