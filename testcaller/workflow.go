// Package testcaller is for testing only. It holds a workflow that runs in its own namespace and
// makes an HTTP request through the connector's Nexus endpoint, the way a real caller would.
package testcaller

import (
	"time"

	"github.com/temporalio/nexus-connector-http/generated/httpapi"
	"github.com/temporalio/nexus-connector-http/httpconnector"
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

// CallerWorkflow sends req through the connector and returns the response. A failed call is a
// NexusOperationError; errors.As finds the ApplicationError inside it, whose Type is one of the
// httpconnector.ErrorType* values.
func CallerWorkflow(ctx workflow.Context, req httpapi.SendRequestInput) (httpapi.SendRequestOutput, error) {
	logger := workflow.GetLogger(ctx)
	c := workflow.NewNexusClient(EndpointName, httpapi.HTTPService.ServiceName)

	var resp httpapi.SendRequestOutput
	err := c.ExecuteOperation(ctx, httpapi.HTTPService.SendRequest, req, workflow.NexusOperationOptions{
		// How long this workflow waits for the whole operation. It must be longer than the time
		// the connector keeps retrying (the request's scheduleToCloseSeconds, which the connector
		// uses as its activity's ScheduleToCloseTimeout, 25 hours if the request doesn't set it);
		// otherwise the connector rejects the request. Five minutes more leaves room for the
		// connector's last error to arrive.
		ScheduleToCloseTimeout: httpconnector.ScheduleToClose(req) + 5*time.Minute,
	}).Get(ctx, &resp)
	if err != nil {
		logger.Error("HTTP request failed", "url", req.URL, "error", err)
		return httpapi.SendRequestOutput{}, err
	}
	logger.Info("HTTP response", "url", req.URL, "status", resp.StatusCode, "body", resp.Body)
	return resp, nil
}
