// Command worker runs the test caller worker, which hosts CallerWorkflow. For testing only.
//
// It connects with the standard Temporal client configuration (the TEMPORAL_* environment
// variables and the temporal CLI's profile file). The namespace defaults to http-testcaller.
package main

import (
	"log"

	"github.com/temporalio/nexus-connector-http/testcaller"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

func main() {
	opts, err := testcaller.ClientOptions()
	if err != nil {
		log.Fatalf("Failed to load Temporal client configuration: %v", err)
	}
	c, err := client.Dial(opts)
	if err != nil {
		log.Fatalf("Failed to create Temporal client: %v", err)
	}
	defer c.Close()

	w := worker.New(c, testcaller.TaskQueue, worker.Options{})
	w.RegisterWorkflow(testcaller.CallerWorkflow)

	if err := w.Run(worker.InterruptCh()); err != nil {
		log.Fatalf("Worker exited with error: %v", err)
	}
}
