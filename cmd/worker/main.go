// Command worker runs the HTTP connector: the HTTPService Nexus service and the SendRequest
// activity its operation starts.
//
// It connects with the standard Temporal client configuration: the TEMPORAL_* environment
// variables and the profile file the temporal CLI uses. The namespace defaults to
// connectors.
package main

import (
	"log"
	"strings"

	"github.com/temporalio/nexus-connector-http/httpconnector"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/contrib/envconfig"
	"go.temporal.io/sdk/worker"
)

// defaultNamespace is used when neither TEMPORAL_NAMESPACE nor the profile sets one.
const defaultNamespace = "connectors"

func main() {
	// Check the connector's configuration and the connection to Temporal before stopping on
	// either, so one run reports every problem rather than only the first.
	var problems []string
	if _, err := httpconnector.New(); err != nil {
		problems = append(problems, "Invalid connector configuration:\n"+indent(err.Error()))
	}
	opts, err := envconfig.LoadDefaultClientOptions()
	if err != nil {
		problems = append(problems, "Failed to load Temporal client configuration:\n"+indent(err.Error()))
	}
	if opts.Namespace == "" {
		opts.Namespace = defaultNamespace
	}
	var c client.Client
	if err == nil {
		c, err = client.Dial(opts)
		if err != nil {
			problems = append(problems, "Failed to connect to Temporal:\n"+indent(err.Error()))
		}
	}
	if len(problems) > 0 {
		if c != nil {
			c.Close()
		}
		log.Fatalf("The connector can't start:\n%s", strings.Join(problems, "\n"))
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

// indent prefixes each line of s with "  - ", one line per problem errors.Join reported.
func indent(s string) string {
	return "  - " + strings.ReplaceAll(s, "\n", "\n  - ")
}
