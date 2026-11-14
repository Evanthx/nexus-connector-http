package main

// This file is what the setup page knows about this repo's connector: its programs and the checks
// particular to it. The variables themselves come from httpconnector.Settings. The rest of
// configui is the same in every Go connector repo.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"

	"github.com/temporalio/nexus-connector-http/generated/httpapi"
	"github.com/temporalio/nexus-connector-http/httpconnector"
	"github.com/temporalio/nexus-connector-http/settings"
	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
)

const pageTitle = "HTTP connector setup"

var programs = []program{{
	Settings:                httpconnector.Settings,
	ClientOptions:           httpconnector.ClientOptions,
	TaskQueue:               func(settings.Env) string { return httpconnector.TaskQueue },
	EndpointName:            "http-endpoint",
	EndpointDescriptionFile: "./endpoint-description.md",
	TestFields: []testField{
		{
			ID: "url", Kind: "text", Label: "Test request URL", Placeholder: "https://httpbin.org/get",
			Help: "Optional. The Test sends one GET to this URL through the connector's own request code, with the " +
				"settings above, and shows the status or the error. The host must be allowed.",
		},
		{
			ID: "secret", Kind: "instance", Family: httpconnector.SecretEnvPrefix, Label: "Send a secret",
			Help: "Optional. Send this secret with the test request as a Bearer token, as a request naming it in " +
				"`apiKeyEnvVar` would. The URL's host must be in its allowed domains.",
		},
	},
	Check: checkConnector,
}}

// checkConnector builds the connector from env as the worker does, then reports what it allows,
// any secret domain the allowlist doesn't allow, and, if fields has a url, the result of one GET
// sent through the connector's SendRequest activity.
func checkConnector(_ context.Context, env settings.Env, fields map[string]string) check {
	c := check{Title: "Connector"}
	a, err := httpconnector.NewWithEnv(env)
	if err != nil {
		c.Skipped = "Fix the settings first."
		return c
	}
	c.Passed = a.Describe()
	if err := a.CheckSecretsAllowed(); err != nil {
		c.Problems = strings.Split(err.Error(), "\n")
	}
	url := strings.TrimSpace(fields["url"])
	if url == "" {
		return c
	}
	req := httpapi.SendRequestInput{URL: url}
	label := "GET " + url
	if name := fields["secret"]; name != "" {
		envVar := httpconnector.SecretEnvPrefix + name
		req.APIKeyEnvVar = &envVar
		label += " with " + envVar
	}
	out, err := sendTest(a, req)
	if err != nil {
		c.Problems = append(c.Problems, label+": "+describeError(err))
		return c
	}
	c.Passed = append(c.Passed, fmt.Sprintf("%s: %s in %d ms; %s", label, out.Status, out.DurationMillis, summary(out)))
	return c
}

// sendTest runs a's SendRequest activity once for req, outside any worker, in the SDK's test
// activity environment: the activity needs an activity context, and only a worker or the test
// environment makes one. The request goes out for real.
func sendTest(a *httpconnector.HTTPActivities, req httpapi.SendRequestInput) (httpapi.SendRequestOutput, error) {
	var suite testsuite.WorkflowTestSuite
	suite.SetLogger(log.NewStructuredLogger(slog.New(slog.DiscardHandler)))
	env := suite.NewTestActivityEnvironment()
	env.RegisterActivity(a.SendRequest)
	val, err := env.ExecuteActivity(a.SendRequest, req)
	if err != nil {
		return httpapi.SendRequestOutput{}, err
	}
	var out httpapi.SendRequestOutput
	err = val.Get(&out)
	return out, err
}

// describeError is the connector's error type and message, with the response's status if it has
// one.
func describeError(err error) string {
	var ae *temporal.ApplicationError
	if !errors.As(err, &ae) {
		return err.Error()
	}
	msg := ae.Type() + ": " + ae.Message()
	var out httpapi.SendRequestOutput
	if ae.HasDetails() && ae.Details(&out) == nil && out.Status != "" {
		msg += " (" + out.Status + "; " + summary(out) + ")"
	}
	return msg
}

// summary is a response's headers in brief: its Content-Type, how many headers, and the body's
// size. The body and other header values aren't shown.
func summary(out httpapi.SendRequestOutput) string {
	h := http.Header(out.Headers.AdditionalProperties)
	parts := []string{fmt.Sprintf("%d headers", len(h))}
	if ct := h.Get("Content-Type"); ct != "" {
		parts = append(parts, "Content-Type "+ct)
	}
	return strings.Join(append(parts, fmt.Sprintf("%d-byte body", len(out.Body))), ", ")
}
