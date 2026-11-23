package main

import (
	"github.com/temporalio/nexus-connector-http/httpconnector"
	"github.com/temporalio/nexus-connector-http/nexus-connector-shared/settings"
	"github.com/temporalio/nexus-connector-http/setup"
)

// configFrom checks every variable the Outbound Connector reads (setup.Outbound) in env, reporting
// every problem at once, and returns its configuration.
func configFrom(env settings.Env) (httpconnector.Config, error) {
	vals, err := setup.Outbound.Load(env)
	if err != nil {
		return httpconnector.Config{}, err
	}
	return setup.Config(vals), nil
}
