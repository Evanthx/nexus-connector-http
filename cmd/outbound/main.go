// Command outbound runs the Outbound Connector, the HTTP connector's one program: the HTTPService Nexus
// service and the Request activity its operation starts.
//
// It connects with the standard Temporal client configuration: the TEMPORAL_* environment
// variables and the profile file the temporal CLI uses. The namespace defaults to
// connectors. Every variable it reads is listed in httpconnector.Settings.
package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/temporalio/nexus-connector-http/httpconnector"
	"github.com/temporalio/nexus-connector-http/nexus-connector-shared/settings"
	"github.com/temporalio/nexus-connector-http/nexus-connector-shared/startup"
	"go.temporal.io/sdk/worker"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	env := settings.OS()
	s := startup.New("Outbound Connector", env)
	connector, err := httpconnector.NewWithEnv(env)
	s.Config(err)
	opts, c := s.Temporal()
	if err := s.Problems(); err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	w := worker.New(c, httpconnector.TaskQueue, worker.Options{})
	if err := httpconnector.Register(w); err != nil {
		log.Fatalf("Failed to configure the Outbound Connector: %v", err)
	}

	log.Print(connector.Report(opts))
	if err := s.Ready(ctx); err != nil {
		if errors.Is(err, startup.ErrStopped) {
			log.Print("Stopped while starting")
			return
		}
		log.Fatal(err)
	}
	log.Printf("Starting worker on namespace %q, task queue %q", opts.Namespace, httpconnector.TaskQueue)
	if err := w.Run(worker.InterruptCh()); err != nil {
		log.Fatalf("Worker exited with error: %v", err)
	}
}
