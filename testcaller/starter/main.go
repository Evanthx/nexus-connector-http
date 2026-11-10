// Command starter runs CallerWorkflow once and prints the response. For testing only.
//
// With no flags it GETs https://httpbin.org/get, which echoes the request back. Flags choose a
// different request:
//
//	go run ./testcaller/starter -url https://httpbin.org/post -method POST -body '{"hello":"world"}'
package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"time"

	"github.com/temporalio/nexus-connector-http/generated/httpapi"
	"github.com/temporalio/nexus-connector-http/testcaller"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
)

func main() {
	url := flag.String("url", "https://httpbin.org/get", "URL to request")
	method := flag.String("method", "GET", "HTTP method")
	body := flag.String("body", "", "Request body")
	timeout := flag.Duration("timeout", 0, "The Nexus operation's timeout, such as 10m; the connector retries until shortly before it. 0 means none, so the connector's maximum applies (25h by default)")
	flag.Parse()

	m := httpapi.SendRequestInputMethod(*method)
	headers := map[string]string{"X-Demo-Header": "nexus-connector-http"}
	req := httpapi.SendRequestInput{
		URL:         *url,
		Method:      &m,
		QueryParams: &httpapi.SendRequestInputQueryParams{AdditionalProperties: map[string]string{"source": "temporal-nexus"}},
		Headers:     &httpapi.SendRequestInputHeaders{AdditionalProperties: headers},
	}
	if *body != "" {
		req.Body = body
		headers["Content-Type"] = "application/json"
	}

	opts, err := testcaller.ClientOptions()
	if err != nil {
		log.Fatalf("Failed to load Temporal client configuration: %v", err)
	}
	c, err := client.Dial(opts)
	if err != nil {
		log.Fatalf("Failed to create Temporal client: %v", err)
	}
	defer c.Close()

	run, err := c.ExecuteWorkflow(context.Background(), client.StartWorkflowOptions{
		ID:        "http-testcaller-" + time.Now().Format("20060102150405"),
		TaskQueue: testcaller.TaskQueue,
	}, testcaller.CallerWorkflow, req, *timeout)
	if err != nil {
		log.Fatalf("Failed to start the workflow: %v", err)
	}
	log.Printf("Started workflow %s", run.GetID())

	var resp httpapi.SendRequestOutput
	if err := run.Get(context.Background(), &resp); err != nil {
		var appErr *temporal.ApplicationError
		if errors.As(err, &appErr) {
			log.Fatalf("Request failed with %s: %s", appErr.Type(), appErr.Message())
		}
		log.Fatalf("Workflow failed: %v", err)
	}
	log.Printf("Response: %s (%d ms)\n%s", resp.Status, resp.DurationMillis, resp.Body)
}
