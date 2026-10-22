package httpconnector

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/temporalio/nexus-connector-http/generated/httpapi"
	"golang.org/x/net/http/httpguts"
)

// SecretEnvPrefix is the prefix every apiKeyEnvVar must have. Callers choose the URL and the
// variable, so without it any caller could send any of the worker's environment variables, such
// as its Temporal API key, to a server of its choosing.
const SecretEnvPrefix = "HTTP_CONNECTOR_SECRET_"

// Validate reports why req can't be made, or nil if it can. The operation calls it before starting
// the activity, so a bad request fails at once without an activity.
func Validate(req httpapi.SendRequestInput) error {
	_, err := build(context.Background(), req)
	return err
}

// CheckCallerTimeout reports an error if req's scheduleToCloseSeconds wouldn't end before the
// caller's Nexus operation times out. Otherwise the caller would time out while the connector kept
// trying, and get a timeout instead of the connector's error. operationTimeout is the
// operation-timeout header, which the caller's server sets to the time the operation has left;
// empty means the operation has no timeout.
func CheckCallerTimeout(req httpapi.SendRequestInput, operationTimeout string) error {
	if operationTimeout == "" {
		return nil
	}
	left, err := time.ParseDuration(operationTimeout)
	if err != nil {
		return fmt.Errorf("can't read the operation-timeout header %q: %w", operationTimeout, err)
	}
	if total := ScheduleToClose(req); total >= left {
		return fmt.Errorf("scheduleToCloseSeconds is %d (%s), but the caller's Nexus operation times out in %s: "+
			"set scheduleToCloseSeconds lower than that, or give the operation a longer timeout",
			req.ScheduleToCloseSecondsOrDefault(), total, left.Round(time.Second))
	}
	return nil
}

// build turns req into an HTTP request, resolving any API key from the worker's environment. An
// error means the request can't be made as given.
func build(ctx context.Context, req httpapi.SendRequestInput) (*http.Request, error) {
	u, err := parseURL(req.URL)
	if err != nil {
		return nil, err
	}
	if err := checkTimeouts(req); err != nil {
		return nil, err
	}
	headerKey, err := setQuery(u, req)
	if err != nil {
		return nil, err
	}
	var body io.Reader // stays a nil interface with no body, which NewRequestWithContext needs
	if b := value(req.Body); b != "" {
		body = strings.NewReader(b)
	}
	httpReq, err := http.NewRequestWithContext(ctx, string(req.MethodOrDefault()), u.String(), body)
	if err != nil {
		return nil, fmt.Errorf("can't build the request: %w", err)
	}
	if err := setHeaders(httpReq, req, headerKey); err != nil {
		return nil, err
	}
	return httpReq, nil
}

// parseURL parses rawURL, which must be an absolute http or https URL.
func parseURL(rawURL string) (*url.URL, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("invalid URL %q: %w", rawURL, err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("invalid URL %q: the scheme must be http or https", rawURL)
	}
	if u.Host == "" {
		return nil, fmt.Errorf("invalid URL %q: it has no host", rawURL)
	}
	return u, nil
}

// checkTimeouts checks that req's timeoutSeconds fits in one attempt, and its
// scheduleToCloseSeconds is from timeoutSeconds to maxScheduleToClose.
func checkTimeouts(req httpapi.SendRequestInput) error {
	maxSecs := int64(attemptTimeout / time.Second)
	secs := req.TimeoutSecondsOrDefault()
	if secs < 1 || secs > maxSecs {
		return fmt.Errorf("timeoutSeconds %d must be between 1 and %d", secs, maxSecs)
	}
	maxTotal := int64(maxScheduleToClose / time.Second)
	if total := req.ScheduleToCloseSecondsOrDefault(); total < secs || total > maxTotal {
		return fmt.Errorf("scheduleToCloseSeconds %d must be between timeoutSeconds (%d) and %d", total, secs, maxTotal)
	}
	return nil
}

// setQuery adds req's query parameters to u's, and its API key if it goes in the query. It returns
// the API key if it goes in the Authorization header instead.
func setQuery(u *url.URL, req httpapi.SendRequestInput) (headerKey string, err error) {
	q := u.Query()
	if req.QueryParams != nil {
		for k, v := range req.QueryParams.AdditionalProperties {
			q.Set(k, v)
		}
	}
	envVar, param, user := value(req.APIKeyEnvVar), value(req.APIKeyParam), value(req.Username)
	switch {
	case user != "" && envVar == "":
		return "", errors.New("username needs apiKeyEnvVar, whose value is the basic auth password")
	case user != "" && param != "":
		return "", errors.New("username can't be used with apiKeyParam: the API key is the basic auth password")
	case param != "" && envVar == "":
		return "", errors.New("apiKeyParam is set but apiKeyEnvVar is not")
	case envVar == "":
		u.RawQuery = q.Encode()
		return "", nil
	default:
	}
	secret, err := apiKey(envVar)
	if err != nil {
		return "", err
	}
	if param != "" {
		q.Set(param, secret)
	} else {
		headerKey = secret
	}
	u.RawQuery = q.Encode()
	return headerKey, nil
}

// setHeaders sets req's headers on httpReq. Later sources of the Authorization header replace
// earlier ones: req's headers, then the variable an Authorization header names, then headerKey
// (the API key, if it doesn't go in the query): the basic auth password with a username, else a
// Bearer token.
func setHeaders(httpReq *http.Request, req httpapi.SendRequestInput, headerKey string) error {
	if req.Headers != nil {
		for k, v := range req.Headers.AdditionalProperties {
			httpReq.Header.Set(k, v)
		}
	}
	if envVar := authorizationEnvVar(req); envVar != "" {
		secret, err := secretEnv(envVar)
		if err != nil {
			return err
		}
		httpReq.Header.Set("Authorization", secret)
	}
	if headerKey != "" {
		if user := value(req.Username); user != "" {
			httpReq.SetBasicAuth(user, headerKey)
		} else {
			httpReq.Header.Set("Authorization", "Bearer "+headerKey)
		}
	}
	// The transport would reject these too, but only when sending, as a retryable error. The
	// error names the header, never its value, which can be a secret.
	for k, vs := range httpReq.Header {
		if !httpguts.ValidHeaderFieldName(k) {
			return fmt.Errorf("invalid header name %q", k)
		}
		for _, v := range vs {
			if !httpguts.ValidHeaderFieldValue(v) {
				return fmt.Errorf("header %q has an invalid value", k)
			}
		}
	}
	return nil
}

// apiKey reads the API key named by envVar. The error names the variable, never its value.
func apiKey(envVar string) (string, error) {
	if !strings.HasPrefix(envVar, SecretEnvPrefix) {
		return "", fmt.Errorf("apiKeyEnvVar %q must start with %s", envVar, SecretEnvPrefix)
	}
	return secretEnv(envVar)
}

// secretEnv reads the secret environment variable envVar. The error names the variable, never its
// value.
func secretEnv(envVar string) (string, error) {
	secret := os.Getenv(envVar)
	if secret == "" {
		return "", fmt.Errorf("environment variable %q is unset or empty on the connector worker", envVar)
	}
	return secret, nil
}

// authorizationEnvVar is the variable named by req's Authorization header, or "" if the header
// isn't a variable name. A header whose whole value starts with SecretEnvPrefix names a variable
// on the connector worker, and the connector sends that variable's value as the header instead,
// so the secret stays out of the request and its history.
func authorizationEnvVar(req httpapi.SendRequestInput) string {
	if req.Headers == nil {
		return ""
	}
	for k, v := range req.Headers.AdditionalProperties {
		if http.CanonicalHeaderKey(k) == "Authorization" && strings.HasPrefix(v, SecretEnvPrefix) {
			return v
		}
	}
	return ""
}

// secrets are the values of req's secret variables that are set: its API key (and, with a
// username, the encoded basic auth credentials it is part of), and its Authorization header's
// variable along with that value's credentials without the scheme (the "abc" of "Bearer abc"),
// which an endpoint can echo alone.
func secrets(req httpapi.SendRequestInput) []string {
	var values []string
	if envVar := value(req.APIKeyEnvVar); strings.HasPrefix(envVar, SecretEnvPrefix) {
		if v := os.Getenv(envVar); v != "" {
			values = append(values, v)
			if user := value(req.Username); user != "" {
				values = append(values, base64.StdEncoding.EncodeToString([]byte(user+":"+v)))
			}
		}
	}
	if envVar := authorizationEnvVar(req); envVar != "" {
		if v := os.Getenv(envVar); v != "" {
			values = append(values, v)
			if _, credentials, ok := strings.Cut(v, " "); ok && credentials != "" {
				values = append(values, credentials)
			}
		}
	}
	return values
}

// timeout is the per-attempt timeout for req, which build has checked.
func timeout(req httpapi.SendRequestInput) time.Duration {
	return time.Duration(req.TimeoutSecondsOrDefault()) * time.Second
}

// value is *p, or "" if p is nil.
func value(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

// RateLimitKey identifies a rate-limit bucket: the URL plus who is calling. The caller is the
// basic auth username, else the API key's environment variable name, else the Authorization
// header's environment variable name (never either value), else "anon".
func RateLimitKey(req httpapi.SendRequestInput) string {
	identity := value(req.Username)
	if identity == "" {
		identity = value(req.APIKeyEnvVar)
	}
	if identity == "" {
		identity = authorizationEnvVar(req)
	}
	if identity == "" {
		identity = "anon"
	}
	return req.URL + "|" + identity
}
