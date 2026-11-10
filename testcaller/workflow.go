// Package testcaller is for testing only. It holds a workflow that runs in its own namespace and
// makes an HTTP request through the connector's Nexus endpoint, the way a real caller would.
package testcaller

import (
	"time"

	"github.com/temporalio/nexus-connector-http/generated/httpapi"
	"go.temporal.io/sdk/workflow"
)

const (
	// TaskQueue is the test caller worker's task queue.
	TaskQueue = "http-testcaller"
	// Namespace is the test caller's default namespace.
	Namespace = "http-testcaller"
	// EndpointName is the Nexus endpoint that routes to the connector.
	EndpointName = "http-endpoint"
)

// CallerWorkflow sends req through the connector and returns the response. timeout is the Nexus
// operation's timeout: the connector keeps retrying until shortly before it, then fails with its
// last error. Zero means no timeout, so the connector's maximum applies (25 hours by default). A
// failed call is a NexusOperationError; errors.As finds the ApplicationError inside it, whose Type
// is one of the httpconnector.ErrorType* values.
func CallerWorkflow(ctx workflow.Context, req httpapi.SendRequestInput, timeout time.Duration) (httpapi.SendRequestOutput, error) {
	logger := workflow.GetLogger(ctx)
	c := workflow.NewNexusClient(EndpointName, httpapi.HTTPService.ServiceName)

	var resp httpapi.SendRequestOutput
	err := c.ExecuteOperation(ctx, httpapi.HTTPService.SendRequest, req, workflow.NexusOperationOptions{
		ScheduleToCloseTimeout: timeout,
	}).Get(ctx, &resp)
	if err != nil {
		logger.Error("HTTP request failed", "url", req.URL, "error", err)
		return httpapi.SendRequestOutput{}, err
	}
	logger.Info("HTTP response", "url", req.URL, "status", resp.StatusCode, "body", resp.Body)
	return resp, nil
}
