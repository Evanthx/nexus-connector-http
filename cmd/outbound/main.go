// Command outbound runs the Outbound Connector, the HTTP connector's one program: the HTTPService Nexus
// service and the Request activity its operation starts.
//
// It connects with the standard Temporal client configuration: the TEMPORAL_* environment
// variables and the profile file the temporal CLI uses. The namespace defaults to
// connectors. Every variable it reads is listed in httpconnector.Settings. With a payload codec
// set (TEMPORAL_CODEC_ENDPOINT), it checks the codec works before serving: see docs/encryption.md.
package main

import (
	"context"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/temporalio/nexus-connector-http/codec"
	"github.com/temporalio/nexus-connector-http/httpconnector"
	"github.com/temporalio/nexus-connector-http/settings"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

func main() {
	// Check the Outbound Connector's configuration and the connection to Temporal before stopping on
	// either, so one run reports every problem rather than only the first.
	var problems []string
	env := settings.OS()
	connector, err := httpconnector.New()
	if err != nil {
		problems = append(problems, "Invalid Outbound Connector configuration:\n"+indent(err.Error()))
	}
	opts, err := httpconnector.ClientOptions(env)
	if err != nil {
		problems = append(problems, "Failed to load Temporal client configuration:\n"+indent(err.Error()))
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
		log.Fatalf("The Outbound Connector can't start:\n%s", strings.Join(problems, "\n"))
	}
	defer c.Close()

	w := worker.New(c, httpconnector.TaskQueue, worker.Options{})
	if err := httpconnector.Register(w); err != nil {
		log.Fatalf("Failed to configure the Outbound Connector: %v", err)
	}

	for _, line := range connector.Describe() {
		log.Print(line)
	}
	log.Printf("Payload codec: %s", codec.Describe(env))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := codec.WaitAtStartup(ctx, env, opts.DataConverter, slog.Default()); err != nil {
		if ctx.Err() != nil {
			log.Print("Stopped while waiting for the payload codec")
			return
		}
		log.Fatal(err)
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
