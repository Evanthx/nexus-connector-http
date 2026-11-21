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
	"github.com/temporalio/nexus-connector-http/nexus-connector-shared/temporalclient"
	"github.com/temporalio/nexus-connector-http/nexus-connector-shared/test/codectest"
	"github.com/temporalio/nexus-connector-http/testcaller"
	commonpb "go.temporal.io/api/common/v1"
	historypb "go.temporal.io/api/history/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
)

const connectorNamespace = "connectors"

// TestEndToEnd runs the Outbound Connector and the test caller in separate namespaces on a dev server, as
// the README describes, and makes requests through the Nexus endpoint. It needs the temporal CLI
// on the PATH.
func TestEndToEnd(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	server := startDevServer(ctx, t)

	connectorClient := server.Client()

	// The httptest servers listen on 127.0.0.1, so only they are allowed.
	t.Setenv(httpconnector.AllowedDomainsEnvVar, "127.0.0.1")
	// Attempts of 10s, so requests of 5s, and callers need at least 10s + 10s margin.
	t.Setenv(httpconnector.AttemptTimeoutEnvVar, "10")
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

	t.Run("retries stop before the caller's timeout, with the Outbound Connector's error", func(t *testing.T) {
		var hits atomic.Int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			hits.Add(1)
			w.WriteHeader(http.StatusServiceUnavailable)
		}))
		defer srv.Close()

		// The Outbound Connector retries for 25s - 10s margin = 15s: attempts at 0s and 5s, then the next
		// 15s wait doesn't fit, so it fails with the last 503. Had it kept retrying, the caller
		// would time out at 25s with no ApplicationError.
		const timeout = 25 * time.Second
		start := time.Now()
		_, err := callWithTimeout(t, httpapi.RequestInput{URL: srv.URL}, timeout)
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
		// One 10s attempt plus the 10s margin is more than 15s.
		_, err := callWithTimeout(t, httpapi.RequestInput{URL: "http://127.0.0.1:1/never"}, 15*time.Second)
		var handlerErr *nexus.HandlerError
		require.ErrorAs(t, err, &handlerErr)
		require.Equal(t, nexus.HandlerErrorTypeBadRequest, handlerErr.Type)
		var appErr *temporal.ApplicationError
		require.ErrorAs(t, err, &appErr)
		require.Equal(t, httpconnector.ErrorTypeInvalidRequest, appErr.Type())
		require.Contains(t, appErr.Message(), "which leaves no time for an attempt: it needs at least one attempt (10s) plus 10s")
		require.Contains(t, appErr.Message(), "(one attempt's time is "+httpconnector.AttemptTimeoutEnvVar+")")
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
		// The server got the key; the Outbound Connector redacts it where the server echoes it back.
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
		require.Contains(t, appErr.Message(), `domain "example.com" is not on the Outbound Connector's list of allowed domains`)
	})
}

// startDevServer starts a dev server with the temporal CLI, with the Outbound Connector's namespace
// and the test caller's, and creates the endpoint to the Outbound Connector's task queue. It skips
// the test with -short or without the CLI on the PATH.
func startDevServer(ctx context.Context, t *testing.T) *testsuite.DevServer {
	t.Helper()
	if testing.Short() {
		t.Skip("starts a dev server")
	}
	cli, err := exec.LookPath("temporal")
	if err != nil {
		t.Skip("the temporal CLI is not on the PATH")
	}
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

	_, err = server.Client().OperatorService().CreateNexusEndpoint(ctx, &operatorservice.CreateNexusEndpointRequest{
		Spec: &nexuspb.EndpointSpec{
			Name: testcaller.EndpointName,
			Target: &nexuspb.EndpointTarget{Variant: &nexuspb.EndpointTarget_Worker_{Worker: &nexuspb.EndpointTarget_Worker{
				Namespace: connectorNamespace,
				TaskQueue: httpconnector.TaskQueue,
			}}},
		},
	})
	require.NoError(t, err)
	return server
}

// TestEndToEnd_EncryptedRequest runs the Outbound Connector with a payload codec server set, as
// temporalclient.Options applies TEMPORAL_CODEC_ENDPOINT, and the test caller encrypting with the same key,
// as the demo does with DEMO_ENCRYPTION_KEY. The request works, and Temporal holds only ciphertext.
func TestEndToEnd_EncryptedRequest(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	server := startDevServer(ctx, t)
	t.Setenv(httpconnector.AllowedDomainsEnvVar, "127.0.0.1")

	// The Outbound Connector's client options, loaded as it loads them, with the codec server set
	// as it would be in its environment.
	opts, err := temporalclient.Options(codectest.Server(t))
	require.NoError(t, err)
	require.Equal(t, connectorNamespace, opts.Namespace)
	connectorClient, err := client.NewClientFromExisting(server.Client(), opts)
	require.NoError(t, err)
	t.Cleanup(connectorClient.Close)
	connectorWorker := worker.New(connectorClient, httpconnector.TaskQueue, worker.Options{})
	require.NoError(t, httpconnector.Register(connectorWorker))
	require.NoError(t, connectorWorker.Start())
	t.Cleanup(connectorWorker.Stop)

	// The caller, encrypting with the same key.
	callerClient := codectest.WithDemoCodec(t, server.Client(), testcaller.Namespace)
	callerWorker := worker.New(callerClient, testcaller.TaskQueue, worker.Options{})
	callerWorker.RegisterWorkflow(testcaller.CallerWorkflow)
	require.NoError(t, callerWorker.Start())
	t.Cleanup(callerWorker.Stop)

	const body, reply, errorBody = "plaintext request body", "plaintext response body", "plaintext error body"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/missing" {
			http.Error(w, errorBody, http.StatusNotFound)
			return
		}
		got, _ := io.ReadAll(r.Body)
		_, _ = io.WriteString(w, reply+" to "+string(got))
	}))
	defer srv.Close()
	run, err := callerClient.ExecuteWorkflow(ctx, client.StartWorkflowOptions{TaskQueue: testcaller.TaskQueue},
		testcaller.CallerWorkflow, httpapi.RequestInput{URL: srv.URL, Method: new(httpapi.RequestInputMethodPost), Body: new(body)}, time.Duration(0))
	require.NoError(t, err)
	var resp httpapi.RequestOutput
	require.NoError(t, run.Get(ctx, &resp))
	require.Equal(t, reply+" to "+body, resp.Body)

	// In Temporal, the Nexus operation's input and result are ciphertext.
	history := callerClient.GetWorkflowHistory(ctx, run.GetID(), run.GetRunID(), false, 0)
	var scheduled *historypb.NexusOperationScheduledEventAttributes
	var completed *historypb.NexusOperationCompletedEventAttributes
	for history.HasNext() {
		event, err := history.Next()
		require.NoError(t, err)
		if a := event.GetNexusOperationScheduledEventAttributes(); a != nil {
			scheduled = a
		}
		if a := event.GetNexusOperationCompletedEventAttributes(); a != nil {
			completed = a
		}
	}
	require.NotNil(t, scheduled, "no Nexus operation in the caller's history")
	codectest.RequireEncrypted(t, scheduled.GetInput(), body)
	require.NotNil(t, completed, "the Nexus operation didn't complete")
	codectest.RequireEncrypted(t, completed.GetResult(), reply)

	// So are the standalone activity's, whose ID comes from the Nexus request ID.
	activity, err := server.Client().WorkflowService().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
		Namespace: connectorNamespace, ActivityId: "http-" + scheduled.GetRequestId(), IncludeInput: true, IncludeOutcome: true,
	})
	require.NoError(t, err)
	require.Len(t, activity.GetInput().GetPayloads(), 1)
	codectest.RequireEncrypted(t, activity.GetInput().GetPayloads()[0], body)
	require.Len(t, activity.GetOutcome().GetResult().GetPayloads(), 1)
	codectest.RequireEncrypted(t, activity.GetOutcome().GetResult().GetPayloads()[0], reply)

	// A failed request's error is ciphertext too: its details (status, headers, body) and its
	// message, which names the URL. The caller, with the same codec, reads the message.
	run, err = callerClient.ExecuteWorkflow(ctx, client.StartWorkflowOptions{TaskQueue: testcaller.TaskQueue},
		testcaller.CallerWorkflow, httpapi.RequestInput{URL: srv.URL + "/missing"}, time.Duration(0))
	require.NoError(t, err)
	runErr := run.Get(ctx, nil)
	require.ErrorContains(t, runErr, "/missing", "the caller reads the decrypted message")
	history = callerClient.GetWorkflowHistory(ctx, run.GetID(), run.GetRunID(), false, 0)
	var details []*commonpb.Payload
	appFailures := 0
	for history.HasNext() {
		event, err := history.Next()
		require.NoError(t, err)
		for f := event.GetNexusOperationFailedEventAttributes().GetFailure(); f != nil; f = f.GetCause() {
			if f.GetApplicationFailureInfo() == nil {
				continue
			}
			appFailures++
			require.Equal(t, "Encoded failure", f.GetMessage(), "the error message is stored in plaintext")
			codectest.RequireEncrypted(t, f.GetEncodedAttributes(), "/missing")
			details = append(details, f.GetApplicationFailureInfo().GetDetails().GetPayloads()...)
		}
	}
	require.Positive(t, appFailures, "the failed operation has no application error")
	require.NotEmpty(t, details, "the failed operation's error has no details")
	for _, d := range details {
		codectest.RequireEncrypted(t, d, errorBody)
	}
}
