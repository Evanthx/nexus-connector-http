package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/temporalio/nexus-connector-http/httpconnector"
	"github.com/temporalio/nexus-connector-http/settings"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/contrib/envconfig"
	"go.temporal.io/sdk/testsuite"
)

const (
	stripe        = httpconnector.SecretEnvPrefix + "STRIPE"
	stripeDomains = stripe + httpconnector.SecretDomainsSuffix
)

// connectorClientMakers are the client constructors of the library this repo's connector calls
// its service with, as "import path.Name", which configui must not call itself (see
// TestNoOwnClient). None here: the connector uses net/http, which clientMakers covers. Other
// repos list theirs, such as "github.com/stripe/stripe-go/v82.NewClient",
// "github.com/slack-go/slack.New" or "github.com/twmb/franz-go/pkg/kgo.NewClient".
var connectorClientMakers = []string{}

// baseEnv is the environment configui was started in, for a test: nothing of the developer's own,
// and no Temporal profile file.
func baseEnv(t *testing.T, vars settings.Map) settings.Map {
	env := settings.Map{"TEMPORAL_CONFIG_FILE": filepath.Join(t.TempDir(), "none.toml")}
	for k, v := range vars {
		env[k] = v
	}
	return env
}

// setTemporalTimeout sets temporalTimeout for the test.
func setTemporalTimeout(t *testing.T, d time.Duration) {
	old := temporalTimeout
	temporalTimeout = d
	t.Cleanup(func() { temporalTimeout = old })
}

func get(t *testing.T, h http.Handler, path string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "http://localhost:8090"+path, nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func post(t *testing.T, h http.Handler, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "http://localhost:8090/api/test", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// postTest posts req and returns the response, raw and decoded.
func postTest(t *testing.T, h http.Handler, req testRequest) (string, testResponse) {
	t.Helper()
	b, err := json.Marshal(req)
	require.NoError(t, err)
	rec := post(t, h, string(b))
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	var resp testResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	return rec.Body.String(), resp
}

func checkTitled(t *testing.T, resp testResponse, title string) check {
	t.Helper()
	for _, c := range resp.Checks {
		if c.Title == title {
			return c
		}
	}
	require.Failf(t, "no check", "%q in %+v", title, resp.Checks)
	return check{}
}

// The page gets every variable in the settings list, in order, with what index.html reads, and
// the values set now, but never a secret's value.
func TestSettings_JSON(t *testing.T) {
	h := newServer(baseEnv(t, settings.Map{
		httpconnector.AllowedDomainsEnvVar: "api.stripe.com",
		stripe:                             "sk_live_never_sent",
		stripeDomains:                      "api.stripe.com",
		httpconnector.SecretEnvPrefix + "ORPHAN_DOMAINS": "example.com", // a list alone is an instance too
		"TEMPORAL_API_KEY": "temporal_key_never_sent",
		"HTTPS_PROXY":      "http://user:proxy_pw_never_sent@proxy.example:8080",
		"http_proxy":       "plain:alias%2Fpw_never_sent@proxy2.example:3128", // HTTP_PROXY unset: Go reads this
	}), programs)
	rec := get(t, h, "/api/settings")
	require.Equal(t, http.StatusOK, rec.Code)
	require.NotContains(t, rec.Body.String(), "sk_live_never_sent")
	require.NotContains(t, rec.Body.String(), "temporal_key_never_sent")
	require.NotContains(t, rec.Body.String(), "pw_never_sent")

	var page pageJSON
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &page))
	require.Equal(t, pageTitle, page.Title)
	require.Len(t, page.Programs, 1)
	p := page.Programs[0]
	require.Equal(t, "connector", p.ID)
	require.Equal(t, "go run ./cmd/worker", p.Command)

	var names []string
	for _, s := range p.Sections {
		for _, v := range s.Variables {
			names = append(names, v.Name)
			require.NotEmpty(t, v.Help, v.Name)
		}
	}
	var want []string
	for _, v := range httpconnector.Settings.Variables() {
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
		"TEMPORAL_TLS_CLIENT_KEY_DATA": false,
	}, page.SecretsSet)

	// External variables: read-only, as Go reads them, masked, and never among the values the page
	// sends.
	require.Equal(t, map[string]externalJSON{
		"HTTPS_PROXY": {Set: true, From: "HTTPS_PROXY", Value: "http://user:****@proxy.example:8080"},
		"HTTP_PROXY":  {Set: true, From: "http_proxy", Value: "plain:****@proxy2.example:3128"},
		"NO_PROXY":    {},
	}, page.External)
	network := p.Sections[len(p.Sections)-1]
	require.Equal(t, "Network", network.Title)
	require.Equal(t, "Go's HTTP client and gRPC", network.Variables[0].External)
	require.Equal(t, []string{"https_proxy"}, network.Variables[0].Aliases)
	require.Equal(t, []string{}, p.Sections[0].Variables[0].Aliases, "[], not null")

	require.Equal(t, &profileJSON{
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

// Every variable envconfig reads and the connector uses is in the Temporal section, and every
// setting of a profile maps to one of them.
func TestTemporalSection_ListsWhatEnvconfigReads(t *testing.T) {
	names := map[string]bool{}
	for _, v := range httpconnector.Settings.Variables() {
		names[v.Name] = true
	}
	// Every setting a profile can have, as profileVariables maps it.
	vars := profileVariables(&envconfig.ClientConfigProfile{
		Address: "a", Namespace: "n", APIKey: "k", Authority: "au", GRPCMeta: map[string]string{"x-tenant": "t"},
		TLS: &envconfig.ClientConfigTLS{
			ClientCertPath: "c", ClientCertData: []byte("c"), ClientKeyPath: "k", ClientKeyData: []byte("k"),
			ServerCACertPath: "ca", ServerCACertData: []byte("ca"), ServerName: "s", DisableHostVerification: true,
		},
	})
	for name, value := range vars {
		require.NotEmpty(t, value, name)
		require.True(t, httpconnector.Settings.Owns(name), "%s isn't in the Temporal section", name)
	}
	// Every variable envconfig's ApplyEnvVars reads, but TEMPORAL_CODEC_*, which do nothing without a
	// remote codec, and TEMPORAL_PROFILE and TEMPORAL_CONFIG_FILE, which choose the profile.
	for _, name := range []string{
		"TEMPORAL_ADDRESS", "TEMPORAL_NAMESPACE", "TEMPORAL_API_KEY", "TEMPORAL_CLIENT_AUTHORITY", "TEMPORAL_TLS",
		"TEMPORAL_TLS_CLIENT_CERT_PATH", "TEMPORAL_TLS_CLIENT_CERT_DATA", "TEMPORAL_TLS_CLIENT_KEY_PATH",
		"TEMPORAL_TLS_CLIENT_KEY_DATA", "TEMPORAL_TLS_SERVER_CA_CERT_PATH", "TEMPORAL_TLS_SERVER_CA_CERT_DATA",
		"TEMPORAL_TLS_SERVER_NAME", "TEMPORAL_TLS_DISABLE_HOST_VERIFICATION", "TEMPORAL_GRPC_META_X_TENANT",
	} {
		require.Contains(t, vars, name)
		require.True(t, httpconnector.Settings.Owns(name), name)
	}
	require.True(t, names["TEMPORAL_PROFILE"])
	require.True(t, names["TEMPORAL_CONFIG_FILE"])
	for _, secret := range []string{"TEMPORAL_API_KEY", "TEMPORAL_TLS_CLIENT_KEY_DATA", "TEMPORAL_GRPC_META_X_TENANT"} {
		require.True(t, httpconnector.Settings.IsSecret(secret), secret)
	}
}

// A value from the Temporal profile file is shown with the file and profile, a secret only as set;
// a gRPC header it sets is an instance; and the Test says which profile it read.
func TestSettings_TemporalProfile(t *testing.T) {
	setTemporalTimeout(t, time.Second)
	file := filepath.Join(t.TempDir(), "temporal.toml")
	require.NoError(t, os.WriteFile(file, []byte(`
[profile.default]
address = "wrong:7233"

[profile.staging]
address = "staging.example:7233"
namespace = "from-file"
api_key = "file_key_never_sent"
grpc_meta = { x-tenant = "acme" }

[profile.staging.tls]
server_name = "staging.example"
`), 0o600))
	base := settings.Map{"TEMPORAL_CONFIG_FILE": file, "TEMPORAL_PROFILE": "staging", "TEMPORAL_NAMESPACE": "from-env"}
	h := newServer(base, programs)
	rec := get(t, h, "/api/settings")
	require.NotContains(t, rec.Body.String(), "file_key_never_sent")
	require.NotContains(t, rec.Body.String(), "acme")
	var page pageJSON
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &page))
	require.Equal(t, &profileJSON{
		File: file, Profile: "staging", Variable: "TEMPORAL_CONFIG_FILE", Exists: true, Found: true,
		Values: map[string]string{
			"TEMPORAL_ADDRESS": "staging.example:7233", "TEMPORAL_NAMESPACE": "from-file",
			"TEMPORAL_TLS": "true", "TEMPORAL_TLS_SERVER_NAME": "staging.example",
		},
		SecretsSet: map[string]bool{"TEMPORAL_API_KEY": true, "TEMPORAL_GRPC_META_X_TENANT": true},
	}, page.TemporalProfile)
	require.Equal(t, "from-env", page.Values["TEMPORAL_NAMESPACE"], "the environment's value is shown as the value; the file's as a note")
	var meta familyJSON
	for _, s := range page.Programs[0].Sections {
		for _, f := range s.Families {
			if f.Prefix == "TEMPORAL_GRPC_META_" {
				meta = f
			}
		}
	}
	require.Equal(t, []string{"X_TENANT"}, meta.Instances, "a header the file sets is shown, so a value can override it")

	// The Test reads the profile the page's values choose, the file's values under the page's.
	raw, resp := postTest(t, h, testRequest{Program: "connector", Values: map[string]string{
		"TEMPORAL_CONFIG_FILE": file, "TEMPORAL_PROFILE": "staging", "TEMPORAL_ADDRESS": "127.0.0.1:1",
		httpconnector.AllowedDomainsEnvVar: "example.com",
	}})
	require.NotContains(t, raw, "file_key_never_sent")
	temporal := checkTitled(t, resp, "Temporal")
	require.Equal(t, []string{`Read profile "staging" from ` + file + `; it sets TEMPORAL_ADDRESS, TEMPORAL_API_KEY, ` +
		`TEMPORAL_GRPC_META_X_TENANT, TEMPORAL_NAMESPACE, TEMPORAL_TLS, TEMPORAL_TLS_SERVER_NAME, each overridden by its variable if set.`},
		temporal.Passed)
	require.Contains(t, temporal.Problems[0], "Failed to connect to Temporal at 127.0.0.1:1", "the page's address, not the file's")

	_, resp = postTest(t, h, testRequest{Program: "connector", Values: map[string]string{
		"TEMPORAL_CONFIG_FILE": file, "TEMPORAL_PROFILE": "missing", httpconnector.AllowedDomainsEnvVar: "example.com",
	}})
	require.Contains(t, checkTitled(t, resp, "Temporal").Problems[0], "unable to find profile missing")
}

// External variables come from where configui runs: the page can't send them, and a proxy's
// password never appears in the Test's output, though a library's error quotes the URL.
func TestTest_ExternalVariables(t *testing.T) {
	setTemporalTimeout(t, time.Second)
	h := newServer(baseEnv(t, nil), programs)
	for _, name := range []string{"HTTPS_PROXY", "https_proxy", "NO_PROXY"} {
		rec := post(t, h, `{"program":"connector","values":{"`+name+`":"http://x"}}`)
		require.Equal(t, http.StatusBadRequest, rec.Code, name)
		require.Contains(t, rec.Body.String(), name+" is read by Go's HTTP client", name)
	}

	resp := testResponse{Checks: []check{{
		Title:    "Connector",
		Problems: []string{`proxyconnect tcp: invalid proxy address "http://u:pw%2Fsecret@proxy:bad": parse error`, "pw/secret alone"},
	}}}
	env := settings.Map{"HTTPS_PROXY": "http://u:pw%2Fsecret@proxy:bad"}
	redactAll(&resp, redactions(httpconnector.Settings, env))
	require.Equal(t, []string{`proxyconnect tcp: invalid proxy address "http://u:****@proxy:bad": parse error`, "[REDACTED] alone"},
		resp.Checks[0].Problems)
}

func TestLocalOnly(t *testing.T) {
	h := newServer(baseEnv(t, nil), programs)

	req := httptest.NewRequest(http.MethodGet, "http://evil.example:8090/api/settings", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusForbidden, rec.Code, "a Host other than localhost is refused")

	req = httptest.NewRequest(http.MethodPost, "http://127.0.0.1:8090/api/test", strings.NewReader(`{}`))
	req.Header.Set("Content-Type", "text/plain")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusUnsupportedMediaType, rec.Code, "a POST that isn't JSON is refused")

	require.Equal(t, http.StatusOK, get(t, h, "/").Code)
	require.Equal(t, http.StatusOK, get(t, h, "/api/settings").Code)
}

func TestTest_RefusesABadRequest(t *testing.T) {
	h := newServer(baseEnv(t, nil), programs)
	for body, want := range map[string]string{
		`{"program":`:         "bad request",
		`{"program":"relay"}`: `unknown program "relay"`,
		`{"program":"connector","values":{"A=B":"x"}}`:     `"A=B" isn't a variable name`,
		`{"program":"connector","values":{"1ABC":"x"}}`:    `"1ABC" isn't a variable name`,
		`{"program":"connector","values":{"":"x"}}`:        `"" isn't a variable name`,
		`{"program":"connector","values":{"A B":"x"}}`:     `"A B" isn't a variable name`,
		`{"program":"connector","values":{"A\u0000":"x"}}`: `isn't a variable name`,
	} {
		rec := post(t, h, body)
		require.Equal(t, http.StatusBadRequest, rec.Code, body)
		require.Contains(t, rec.Body.String(), want, body)
	}
}

// The Settings check reports exactly what the worker would at startup, every problem at once.
func TestTest_ReportsEverySettingsProblemAsStartupDoes(t *testing.T) {
	setTemporalTimeout(t, time.Second)
	values := map[string]string{
		httpconnector.AllowedDomainsEnvVar:   "https://example.com",
		httpconnector.RateLimitMinutesEnvVar: "0",
		httpconnector.AllowRedirectsEnvVar:   "yes",
		stripe:                               "sk_typed",
		"TEMPORAL_ADDRESS":                   "127.0.0.1:1",
	}
	_, resp := postTest(t, newServer(baseEnv(t, nil), programs), testRequest{Program: "connector", Values: values})
	require.False(t, resp.OK)

	_, startupErr := httpconnector.NewWithEnv(settings.Map(values))
	require.Error(t, startupErr)
	require.Equal(t, strings.Split(startupErr.Error(), "\n"), checkTitled(t, resp, "Settings").Problems)
	require.Len(t, checkTitled(t, resp, "Settings").Problems, 4)
	require.Contains(t, checkTitled(t, resp, "Temporal").Problems[0], "Failed to connect to Temporal at 127.0.0.1:1")
	require.Equal(t, "Fix the settings first.", checkTitled(t, resp, "Connector").Skipped)
}

// A secret typed on the page is used for the test request and never comes back; a kept one is
// read where configui runs and never comes back either.
func TestTest_SendsARequestWithPageValuesAndNeverReturnsSecrets(t *testing.T) {
	setTemporalTimeout(t, time.Second)
	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.Header().Set("X-Echo", gotAuth)
		w.Header().Set("Content-Type", "text/plain")
		_, _ = io.WriteString(w, "you sent "+gotAuth)
	}))
	defer srv.Close()

	base := baseEnv(t, settings.Map{
		httpconnector.SecretEnvPrefix + "KEPT":         "kept_s3cret",
		httpconnector.SecretEnvPrefix + "KEPT_DOMAINS": "127.0.0.1",
		httpconnector.SecretEnvPrefix + "GONE":         "gone_s3cret", // not on the page: unset for the test
		httpconnector.AllowedDomainsEnvVar:             "example.com", // replaced by the page's value
	})
	h := newServer(base, programs)
	req := testRequest{
		Program: "connector",
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
	raw, resp := postTest(t, h, req)
	require.Equal(t, "Bearer typed_s3cret", gotAuth, "the request used the page's secret")
	require.NotContains(t, raw, "s3cret")

	connector := checkTitled(t, resp, "Connector")
	require.Empty(t, connector.Problems)
	require.Contains(t, connector.Passed, "Secret "+httpconnector.SecretEnvPrefix+"KEPT may be sent to: 127.0.0.1")
	require.Contains(t, connector.Passed, "Secret "+stripe+" may be sent to: 127.0.0.1")
	require.NotContains(t, strings.Join(connector.Passed, "\n"), "GONE")
	last := connector.Passed[len(connector.Passed)-1]
	require.Contains(t, last, "GET "+srv.URL+"/check with "+stripe+": 200 OK in ")
	require.Contains(t, last, "Content-Type text/plain")

	// The kept secret, sent the same way.
	req.Fields["secret"] = "KEPT"
	raw, _ = postTest(t, h, req)
	require.Equal(t, "Bearer kept_s3cret", gotAuth)
	require.NotContains(t, raw, "s3cret")
}

// A request the connector refuses shows its error, and a secret's domain the allowlist doesn't
// allow is a problem.
func TestTest_ConnectorProblems(t *testing.T) {
	setTemporalTimeout(t, time.Second)
	_, resp := postTest(t, newServer(baseEnv(t, nil), programs), testRequest{
		Program: "connector",
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
		stripeDomains + ` lists "api.stripe.com", which isn't within ` + httpconnector.AllowedDomainsEnvVar + ", so the connector won't call it",
		`GET https://not-allowed.test/: InvalidRequest: domain "not-allowed.test" is not on the connector's list of allowed domains (` +
			httpconnector.AllowedDomainsEnvVar + ")",
	}, checkTitled(t, resp, "Connector").Problems)
}

// The page's values replace every variable the program owns; others pass through.
func TestPageEnv(t *testing.T) {
	base := settings.Map{
		"HOME":                               "/home/x",
		"HTTPS_PROXY":                        "http://proxy.example:8080", // external: the page can't change it
		"TEMPORAL_TLS_CLIENT_CERT_PATH":      "/cert",                     // owned, not sent: unset
		httpconnector.AllowedDomainsEnvVar:   "old.example.com",
		httpconnector.RateLimitMinutesEnvVar: "9",
		stripe:                               "kept",
		stripeDomains:                        "api.stripe.com",
		httpconnector.SecretEnvPrefix + "REMOVED": "x",
	}
	env := pageEnv(base, httpconnector.Settings, testRequest{
		Values: map[string]string{
			httpconnector.AllowedDomainsEnvVar:   "new.example.com",
			httpconnector.RateLimitMinutesEnvVar: "  ",
			stripeDomains:                        "api.stripe.com",
			stripe:                               "ignored: kept",
			"EXTRA":                              "added",
		},
		KeepSecrets: map[string]bool{stripe: true, "HOME": true},
	})
	require.Equal(t, settings.Map{
		"HOME":                             "/home/x",
		"HTTPS_PROXY":                      "http://proxy.example:8080",
		httpconnector.AllowedDomainsEnvVar: "new.example.com",
		stripe:                             "kept",
		stripeDomains:                      "api.stripe.com",
		"EXTRA":                            "added",
	}, env)
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
	setTemporalTimeout(t, 10*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	server, err := testsuite.StartDevServer(ctx, testsuite.DevServerOptions{
		ExistingPath: cli, ClientOptions: &client.Options{Namespace: "connectors"},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = server.Stop() })

	base := baseEnv(t, nil)
	h := newServer(base, programs)
	req := testRequest{Program: "connector", Values: map[string]string{
		httpconnector.AllowedDomainsEnvVar: "example.com",
		"TEMPORAL_ADDRESS":                 server.FrontendHostPort(),
		"TEMPORAL_CONFIG_FILE":             base["TEMPORAL_CONFIG_FILE"], // as the page sends every variable it shows
	}}
	_, resp := postTest(t, h, req)
	temporal := checkTitled(t, resp, "Temporal")
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
	_, resp = postTest(t, h, req)
	require.True(t, resp.OK, "%+v", resp.Checks)
	require.Contains(t, checkTitled(t, resp, "Temporal").Passed,
		`Nexus endpoint "http-endpoint" targets namespace "connectors" and task queue "nexus-connector-http".`)

	req.Values["TEMPORAL_NAMESPACE"] = "missing"
	_, resp = postTest(t, h, req)
	require.Contains(t, checkTitled(t, resp, "Temporal").Problems[0], `Namespace "missing" doesn't exist.`)
}

func TestValidName(t *testing.T) {
	for _, ok := range []string{"A", "_A", "a1_B"} {
		require.True(t, validName(ok), ok)
	}
	for _, bad := range []string{"", "1A", "A-B", "A=B", "A B", "Ä"} {
		require.False(t, validName(bad), bad)
	}
}
