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
	"go.temporal.io/sdk/workflow"
)

const connectorNamespace = "http-connector"

// oneHourCaller calls the operation as CallerWorkflow does, but with a fixed one-hour Nexus
// operation timeout.
func oneHourCaller(ctx workflow.Context, req httpapi.SendRequestInput) error {
	c := workflow.NewNexusClient(testcaller.EndpointName, httpapi.HTTPService.ServiceName)
	return c.ExecuteOperation(ctx, httpapi.HTTPService.SendRequest, req, workflow.NexusOperationOptions{
		ScheduleToCloseTimeout: time.Hour,
	}).Get(ctx, nil)
}

// TestEndToEnd runs the connector and the test caller in separate namespaces on a dev server, as
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

	connectorWorker := worker.New(connectorClient, httpconnector.TaskQueue, worker.Options{})
	require.NoError(t, httpconnector.Register(connectorWorker))
	require.NoError(t, connectorWorker.Start())
	t.Cleanup(connectorWorker.Stop)

	callerClient, err := client.Dial(client.Options{HostPort: server.FrontendHostPort(), Namespace: testcaller.Namespace})
	require.NoError(t, err)
	t.Cleanup(callerClient.Close)
	callerWorker := worker.New(callerClient, testcaller.TaskQueue, worker.Options{})
	callerWorker.RegisterWorkflow(testcaller.CallerWorkflow)
	callerWorker.RegisterWorkflow(oneHourCaller)
	require.NoError(t, callerWorker.Start())
	t.Cleanup(callerWorker.Stop)

	call := func(t *testing.T, req httpapi.SendRequestInput) (httpapi.SendRequestOutput, error) {
		t.Helper()
		run, err := callerClient.ExecuteWorkflow(ctx, client.StartWorkflowOptions{TaskQueue: testcaller.TaskQueue}, testcaller.CallerWorkflow, req)
		require.NoError(t, err)
		var resp httpapi.SendRequestOutput
		err = run.Get(ctx, &resp)
		return resp, err
	}

	t.Run("success", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.WriteString(w, "echo "+r.URL.Query().Get("q")+" "+r.Header.Get("X-H"))
		}))
		defer srv.Close()

		resp, err := call(t, httpapi.SendRequestInput{
			URL:         srv.URL,
			QueryParams: &httpapi.SendRequestInputQueryParams{AdditionalProperties: map[string]string{"q": "one"}},
			Headers:     &httpapi.SendRequestInputHeaders{AdditionalProperties: map[string]string{"X-H": "two"}},
		})
		require.NoError(t, err)
		require.Equal(t, int64(200), resp.StatusCode)
		require.Equal(t, "echo one two", resp.Body)
	})

	t.Run("transient failure is retried", func(t *testing.T) {
		var hits atomic.Int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if hits.Add(1) == 1 {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			_, _ = io.WriteString(w, "second time")
		}))
		defer srv.Close()

		resp, err := call(t, httpapi.SendRequestInput{URL: srv.URL})
		require.NoError(t, err)
		require.Equal(t, "second time", resp.Body)
		require.Equal(t, int32(2), hits.Load())
	})

	// Each attempt passes its count of Transient failures in a row to the next as heartbeat
	// details, which only the server carries between attempts.
	t.Run("transient failures in a row back off 1s, 2s, 4s", func(t *testing.T) {
		var mu sync.Mutex
		var times []time.Time
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			times = append(times, time.Now())
			n := len(times)
			mu.Unlock()
			if n <= 3 {
				w.WriteHeader(http.StatusServiceUnavailable)
			}
		}))
		defer srv.Close()

		_, err := call(t, httpapi.SendRequestInput{URL: srv.URL})
		require.NoError(t, err)
		mu.Lock()
		defer mu.Unlock()
		require.Len(t, times, 4)
		for i, want := range []time.Duration{time.Second, 2 * time.Second, 4 * time.Second} {
			require.InDelta(t, want, times[i+1].Sub(times[i]), float64(500*time.Millisecond), "wait before attempt %d", i+2)
		}
	})

	t.Run("retries stop at the request's scheduleToCloseSeconds", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusServiceUnavailable)
		}))
		defer srv.Close()

		start := time.Now()
		_, err := call(t, httpapi.SendRequestInput{
			URL:                    srv.URL,
			TimeoutSeconds:         new(int64(1)),
			ScheduleToCloseSeconds: new(int64(3)),
		})
		var appErr *temporal.ApplicationError
		require.ErrorAs(t, err, &appErr)
		require.Equal(t, httpconnector.ErrorTypeTransient, appErr.Type())
		require.Less(t, time.Since(start), 30*time.Second, "the default 10 minutes would still be retrying")
	})

	t.Run("client error reaches the caller with the response", func(t *testing.T) {
		var hits atomic.Int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			hits.Add(1)
			w.WriteHeader(http.StatusNotFound)
			_, _ = io.WriteString(w, "no such thing")
		}))
		defer srv.Close()

		_, err := call(t, httpapi.SendRequestInput{URL: srv.URL})
		var opErr *temporal.NexusOperationError
		require.ErrorAs(t, err, &opErr)
		var appErr *temporal.ApplicationError
		require.ErrorAs(t, err, &appErr)
		require.Equal(t, httpconnector.ErrorTypeClientError, appErr.Type())
		var resp httpapi.SendRequestOutput
		require.NoError(t, appErr.Details(&resp))
		require.Equal(t, int64(404), resp.StatusCode)
		require.Equal(t, "no such thing", resp.Body)
		require.Equal(t, int32(1), hits.Load(), "a client error must not be retried")
	})

	t.Run("request that outlasts the caller's timeout is rejected", func(t *testing.T) {
		run, err := callerClient.ExecuteWorkflow(ctx, client.StartWorkflowOptions{TaskQueue: testcaller.TaskQueue}, oneHourCaller,
			httpapi.SendRequestInput{URL: "https://x.test"}) // scheduleToCloseSeconds defaults to 25 hours
		require.NoError(t, err)
		err = run.Get(ctx, nil)
		var handlerErr *nexus.HandlerError
		require.ErrorAs(t, err, &handlerErr)
		require.Equal(t, nexus.HandlerErrorTypeBadRequest, handlerErr.Type)
		var appErr *temporal.ApplicationError
		require.ErrorAs(t, err, &appErr)
		require.Equal(t, httpconnector.ErrorTypeInvalidRequest, appErr.Type())
		require.Contains(t, appErr.Message(), "scheduleToCloseSeconds is 90000 (25h0m0s), but the caller's Nexus operation times out in")
	})

	t.Run("invalid request fails when the operation starts", func(t *testing.T) {
		_, err := call(t, httpapi.SendRequestInput{URL: "ftp://example.com/file"})
		var handlerErr *nexus.HandlerError
		require.ErrorAs(t, err, &handlerErr)
		require.Equal(t, nexus.HandlerErrorTypeBadRequest, handlerErr.Type)
		var appErr *temporal.ApplicationError
		require.ErrorAs(t, err, &appErr)
		require.Equal(t, httpconnector.ErrorTypeInvalidRequest, appErr.Type())
		require.Contains(t, appErr.Message(), "scheme must be http or https")
	})
}
