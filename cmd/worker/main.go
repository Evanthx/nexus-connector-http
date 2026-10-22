// Command worker runs the HTTP connector: the HTTPService Nexus service and the SendRequest
// activity its operation starts.
//
// It connects with the standard Temporal client configuration: the TEMPORAL_* environment
// variables and the profile file the temporal CLI uses. The namespace defaults to
// http-connector.
package main

import (
	"log"

	"github.com/temporalio/nexus-connector-http/httpconnector"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/contrib/envconfig"
	"go.temporal.io/sdk/worker"
)

// defaultNamespace is used when neither TEMPORAL_NAMESPACE nor the profile sets one.
const defaultNamespace = "http-connector"

func main() {
	opts, err := envconfig.LoadDefaultClientOptions()
	if err != nil {
		log.Fatalf("Failed to load Temporal client configuration: %v", err)
	}
	if opts.Namespace == "" {
		opts.Namespace = defaultNamespace
	}
	c, err := client.Dial(opts)
	if err != nil {
		log.Fatalf("Failed to create Temporal client: %v", err)
	}
	defer c.Close()

	w := worker.New(c, httpconnector.TaskQueue, worker.Options{})
	if err := httpconnector.Register(w); err != nil {
		log.Fatalf("Failed to configure the connector: %v", err)
	}

	log.Printf("Starting worker on namespace %q, task queue %q", opts.Namespace, httpconnector.TaskQueue)
	if err := w.Run(worker.InterruptCh()); err != nil {
		log.Fatalf("Worker exited with error: %v", err)
	}
}
