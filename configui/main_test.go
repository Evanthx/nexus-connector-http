package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/temporalio/nexus-connector-http/httpconnector"
	"github.com/temporalio/nexus-connector-http/nexus-connector-shared/configui"
	"github.com/temporalio/nexus-connector-http/nexus-connector-shared/settings"
	ct "github.com/temporalio/nexus-connector-http/nexus-connector-shared/test/configuitest"
	"github.com/temporalio/nexus-connector-http/setup"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"
)

const (
	stripe        = httpconnector.SecretEnvPrefix + "STRIPE"
	stripeDomains = stripe + httpconnector.SecretDomainsSuffix
)

// connectorClientMakers are the client constructors of the library the Outbound Connector calls
// its service with, as "import path.Name", which configui must not call itself (see
// TestNoOwnClient). None here: the Outbound Connector uses net/http, which clientMakers covers. Other
// repos list theirs, such as "github.com/stripe/stripe-go/v82.NewClient",
// "github.com/slack-go/slack.New" or "github.com/twmb/franz-go/pkg/kgo.NewClient".
var connectorClientMakers = []string{}

// The page gets every variable in the settings list, in order, with what index.html reads, and
// the values set now, but never a secret's value.
func TestSettings_JSON(t *testing.T) {
	h := configui.NewServer(pageTitle, ct.BaseEnv(t, settings.Map{
		httpconnector.AllowedDomainsEnvVar: "api.stripe.com",
		stripe:                             "sk_live_never_sent",
		stripeDomains:                      "api.stripe.com",
		httpconnector.SecretEnvPrefix + "ORPHAN_DOMAINS": "example.com", // a list alone is an instance too
		"TEMPORAL_API_KEY": "temporal_key_never_sent",
		"HTTPS_PROXY":      "http://user:proxy_pw_never_sent@proxy.example:8080",
		"http_proxy":       "plain:alias%2Fpw_never_sent@proxy2.example:3128", // HTTP_PROXY unset: Go reads this
	}), programs)
	rec := ct.Get(t, h, "/api/settings")
	require.Equal(t, http.StatusOK, rec.Code)
	require.NotContains(t, rec.Body.String(), "sk_live_never_sent")
	require.NotContains(t, rec.Body.String(), "temporal_key_never_sent")
	require.NotContains(t, rec.Body.String(), "pw_never_sent")

	var page configui.Page
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &page))
	require.Equal(t, pageTitle, page.Title)
	require.Len(t, page.Programs, 1)
	p := page.Programs[0]
	require.Equal(t, "outbound", p.ID)
	require.Equal(t, "go run ./cmd/outbound", p.Command)

	var names []string
	for _, s := range p.Sections {
		for _, v := range s.Variables {
			names = append(names, v.Name)
			require.NotEmpty(t, v.Help, v.Name)
		}
	}
	var want []string
	for _, v := range setup.Outbound.Variables() {
		want = append(want, v.Name)
	}
	require.Equal(t, want, names, "every variable in the settings list, in its order")

	require.Equal(t, "Secrets", p.Sections[1].Title)
	f := p.Sections[1].Families[0]
	require.Equal(t, httpconnector.SecretEnvPrefix, f.Prefix)
	require.Equal(t, []string{"ORPHAN", "STRIPE"}, f.Instances)
	require.Equal(t, settings.InstanceNamePattern, f.NamePattern)
	require.NotEmpty(t, f.AddButton)
	require.NotEmpty(t, f.AddHelp)
	require.Len(t, f.Members, 2)
	require.True(t, f.Members[0].Secret)
	require.Equal(t, httpconnector.SecretDomainsSuffix, f.Members[1].Suffix)
	require.Equal(t, []string{"url", "secret"}, []string{p.TestFields[0].ID, p.TestFields[1].ID})

	require.Equal(t, map[string]string{
		httpconnector.AllowedDomainsEnvVar:               "api.stripe.com",
		stripeDomains:                                    "api.stripe.com",
		httpconnector.SecretEnvPrefix + "ORPHAN_DOMAINS": "example.com",
		"TEMPORAL_CONFIG_FILE":                           filepath.Join(filepath.Dir(page.TemporalProfile.File), "none.toml"),
	}, page.Values)
	require.Equal(t, map[string]bool{
		stripe: true, httpconnector.SecretEnvPrefix + "ORPHAN": false, "TEMPORAL_API_KEY": true,
		"TEMPORAL_CODEC_AUTH": false, "TEMPORAL_TLS_CLIENT_KEY_DATA": false,
	}, page.SecretsSet)

	// External variables: read-only, as Go reads them, masked, and never among the values the page
	// sends.
	require.Equal(t, map[string]configui.PageExternal{
		"HTTPS_PROXY": {Set: true, From: "HTTPS_PROXY", Value: "http://user:****@proxy.example:8080"},
		"HTTP_PROXY":  {Set: true, From: "http_proxy", Value: "plain:****@proxy2.example:3128"},
		"NO_PROXY":    {},
	}, page.External)
	network := p.Sections[len(p.Sections)-1]
	require.Equal(t, "Network", network.Title)
	require.Equal(t, "Go's HTTP client and gRPC", network.Variables[0].External)
	require.Equal(t, []string{"https_proxy"}, network.Variables[0].Aliases)
	require.Equal(t, []string{}, p.Sections[0].Variables[0].Aliases, "[], not null")

	require.Equal(t, &configui.PageProfile{
		File: page.TemporalProfile.File, Profile: "default", Variable: "TEMPORAL_CONFIG_FILE",
		Values: map[string]string{}, SecretsSet: map[string]bool{},
	}, page.TemporalProfile, "no file")

	// The JSON's field names are what index.html reads.
	var raw map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &raw))
	for _, key := range []string{"title", "programs", "values", "secretsSet", "external", "temporalProfile"} {
		require.Contains(t, raw, key)
	}
	variable := raw["programs"].([]any)[0].(map[string]any)["sections"].([]any)[0].(map[string]any)["variables"].([]any)[0].(map[string]any)
	for _, key := range []string{"name", "help", "default", "required", "secret", "external", "aliases"} {
		require.Contains(t, variable, key)
	}
	for _, key := range []string{"file", "profile", "variable", "exists", "found", "error", "values", "secretsSet"} {
		require.Contains(t, raw["temporalProfile"], key)
	}
	for _, key := range []string{"set", "from", "value"} {
		require.Contains(t, raw["external"].(map[string]any)["HTTPS_PROXY"], key)
	}
}

// The Settings check reports exactly what the Outbound Connector would at startup, every problem at once.
func TestTest_ReportsEverySettingsProblemAsStartupDoes(t *testing.T) {
	programs := ct.WithTemporalTimeout(programs, time.Second)
	values := map[string]string{
		httpconnector.AllowedDomainsEnvVar:   "https://example.com",
		httpconnector.RateLimitMinutesEnvVar: "0",
		httpconnector.AllowRedirectsEnvVar:   "yes",
		stripe:                               "sk_typed",
		"TEMPORAL_ADDRESS":                   "127.0.0.1:1",
	}
	_, resp := ct.PostTest(t, configui.NewServer(pageTitle, ct.BaseEnv(t, nil), programs), configui.TestRequest{Program: "outbound", Values: values})
	require.False(t, resp.OK)

	_, startupErr := setup.Outbound.Load(settings.Map(values))
	require.Error(t, startupErr)
	require.Equal(t, strings.Split(startupErr.Error(), "\n"), ct.CheckTitled(t, resp, "Settings").Problems)
	require.Len(t, ct.CheckTitled(t, resp, "Settings").Problems, 4)
	require.Contains(t, ct.CheckTitled(t, resp, "Temporal").Problems[0], "Failed to connect to Temporal at 127.0.0.1:1")
	require.Equal(t, "Fix the settings first.", ct.CheckTitled(t, resp, "Outbound Connector").Skipped)
}

// A secret typed on the page is used for the test request and never comes back; a kept one is
// read where configui runs and never comes back either.
func TestTest_SendsARequestWithPageValuesAndNeverReturnsSecrets(t *testing.T) {
	programs := ct.WithTemporalTimeout(programs, time.Second)
	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.Header().Set("X-Echo", gotAuth)
		w.Header().Set("Content-Type", "text/plain")
		_, _ = io.WriteString(w, "you sent "+gotAuth)
	}))
	defer srv.Close()

	base := ct.BaseEnv(t, settings.Map{
		httpconnector.SecretEnvPrefix + "KEPT":         "kept_s3cret",
		httpconnector.SecretEnvPrefix + "KEPT_DOMAINS": "127.0.0.1",
		httpconnector.SecretEnvPrefix + "GONE":         "gone_s3cret", // not on the page: unset for the test
		httpconnector.AllowedDomainsEnvVar:             "example.com", // replaced by the page's value
	})
	h := configui.NewServer(pageTitle, base, programs)
	req := configui.TestRequest{
		Program: "outbound",
		Values: map[string]string{
			httpconnector.AllowedDomainsEnvVar:             "127.0.0.1",
			httpconnector.SecretEnvPrefix + "KEPT_DOMAINS": "127.0.0.1",
			stripe:             "typed_s3cret",
			stripeDomains:      "127.0.0.1",
			"TEMPORAL_ADDRESS": "127.0.0.1:1",
		},
		KeepSecrets: map[string]bool{httpconnector.SecretEnvPrefix + "KEPT": true},
		Fields:      map[string]string{"url": srv.URL + "/check", "secret": "STRIPE"},
	}
	raw, resp := ct.PostTest(t, h, req)
	require.Equal(t, "Bearer typed_s3cret", gotAuth, "the request used the page's secret")
	require.NotContains(t, raw, "s3cret")

	connector := ct.CheckTitled(t, resp, "Outbound Connector")
	require.Empty(t, connector.Problems)
	require.Contains(t, connector.Passed, "Secret "+httpconnector.SecretEnvPrefix+"KEPT may be sent to: 127.0.0.1")
	require.Contains(t, connector.Passed, "Secret "+stripe+" may be sent to: 127.0.0.1")
	require.NotContains(t, strings.Join(connector.Passed, "\n"), "GONE")
	last := connector.Passed[len(connector.Passed)-1]
	require.Contains(t, last, "GET "+srv.URL+"/check with "+stripe+": 200 OK in ")
	require.Contains(t, last, "Content-Type text/plain")

	// The kept secret, sent the same way.
	req.Fields["secret"] = "KEPT"
	raw, _ = ct.PostTest(t, h, req)
	require.Equal(t, "Bearer kept_s3cret", gotAuth)
	require.NotContains(t, raw, "s3cret")
}

// A request the Outbound Connector refuses shows its error, and a secret's domain the allowlist doesn't
// allow is a problem.
func TestTest_ConnectorProblems(t *testing.T) {
	programs := ct.WithTemporalTimeout(programs, time.Second)
	_, resp := ct.PostTest(t, configui.NewServer(pageTitle, ct.BaseEnv(t, nil), programs), configui.TestRequest{
		Program: "outbound",
		Values: map[string]string{
			httpconnector.AllowedDomainsEnvVar: "example.com",
			stripe:                             "sk",
			stripeDomains:                      "api.stripe.com, api.example.com",
			"TEMPORAL_ADDRESS":                 "127.0.0.1:1",
		},
		Fields: map[string]string{"url": "https://not-allowed.test/"},
	})
	require.False(t, resp.OK)
	require.Equal(t, []string{
		stripeDomains + ` lists "api.stripe.com", which isn't within ` + httpconnector.AllowedDomainsEnvVar + ", so the Outbound Connector won't call it",
		`GET https://not-allowed.test/: InvalidRequest: domain "not-allowed.test" is not on the Outbound Connector's list of allowed domains (` +
			httpconnector.AllowedDomainsEnvVar + ")",
	}, ct.CheckTitled(t, resp, "Outbound Connector").Problems)
}

// Against a dev server: the namespace exists, and the Test reports a missing endpoint with the
// command to create it, then finds the endpoint once it exists.
func TestTest_Temporal(t *testing.T) {
	if testing.Short() {
		t.Skip("starts a dev server")
	}
	cli, err := exec.LookPath("temporal")
	if err != nil {
		t.Skip("the temporal CLI is not on the PATH")
	}
	programs := ct.WithTemporalTimeout(programs, 10*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	server, err := testsuite.StartDevServer(ctx, testsuite.DevServerOptions{
		ExistingPath: cli, ClientOptions: &client.Options{Namespace: "connectors"},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = server.Stop() })

	base := ct.BaseEnv(t, nil)
	h := configui.NewServer(pageTitle, base, programs)
	req := configui.TestRequest{Program: "outbound", Values: map[string]string{
		httpconnector.AllowedDomainsEnvVar: "example.com",
		"TEMPORAL_ADDRESS":                 server.FrontendHostPort(),
		"TEMPORAL_CONFIG_FILE":             base["TEMPORAL_CONFIG_FILE"], // as the page sends every variable it shows
	}}
	_, resp := ct.PostTest(t, h, req)
	temporal := ct.CheckTitled(t, resp, "Temporal")
	require.Equal(t, []string{
		"No Temporal profile file at " + base["TEMPORAL_CONFIG_FILE"] + ": only the TEMPORAL_* variables apply.",
		"Connected to Temporal at " + server.FrontendHostPort() + ".", `Namespace "connectors" exists.`,
	}, temporal.Passed)
	require.Len(t, temporal.Problems, 1)
	require.Contains(t, temporal.Problems[0], `No Nexus endpoint targets namespace "connectors" and task queue "nexus-connector-http"`)
	require.Contains(t, temporal.Problems[0], "temporal operator nexus endpoint create --name http-endpoint --target-namespace connectors "+
		"--target-task-queue nexus-connector-http --description-file ./endpoint-description.md")

	_, err = server.Client().OperatorService().CreateNexusEndpoint(ctx, &operatorservice.CreateNexusEndpointRequest{
		Spec: &nexuspb.EndpointSpec{
			Name: "http-endpoint",
			Target: &nexuspb.EndpointTarget{Variant: &nexuspb.EndpointTarget_Worker_{Worker: &nexuspb.EndpointTarget_Worker{
				Namespace: "connectors", TaskQueue: httpconnector.TaskQueue,
			}}},
		},
	})
	require.NoError(t, err)
	_, resp = ct.PostTest(t, h, req)
	require.True(t, resp.OK, "%+v", resp.Checks)
	require.Contains(t, ct.CheckTitled(t, resp, "Temporal").Passed,
		`Nexus endpoint "http-endpoint" targets namespace "connectors" and task queue "nexus-connector-http".`)

	req.Values["TEMPORAL_NAMESPACE"] = "missing"
	_, resp = ct.PostTest(t, h, req)
	require.Contains(t, ct.CheckTitled(t, resp, "Temporal").Problems[0], `Namespace "missing" doesn't exist.`)
}

// The proxy variables come from where configui runs, as Go's HTTP client reads them: the page
// can't send them, HTTP_PROXY and its lower-case alias included.
func TestTest_ExternalVariables(t *testing.T) {
	h := configui.NewServer(pageTitle, ct.BaseEnv(t, nil), programs)
	for _, name := range []string{"HTTPS_PROXY", "https_proxy", "HTTP_PROXY", "http_proxy", "NO_PROXY", "no_proxy"} {
		rec := ct.Post(t, h, `{"program":"outbound","values":{"`+name+`":"http://x"}}`)
		require.Equal(t, http.StatusBadRequest, rec.Code, name)
		require.Contains(t, rec.Body.String(), name+" is read by Go's HTTP client", name)
	}
}

// The page's checks go through setup and httpconnector (Outbound.Load, New, Request), not a client of the page's own.
func TestNoOwnClient(t *testing.T) {
	ct.RequireNoOwnClient(t, ".", connectorClientMakers...)
}
