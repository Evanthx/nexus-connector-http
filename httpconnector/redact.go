package httpconnector

import (
	"encoding/json"
	"net/url"
	"strconv"
	"strings"

	"github.com/temporalio/nexus-connector-http/generated/httpapi"
)

// redact replaces every secret in s with [REDACTED], in each form it can take in a response or an
// error message: as is, escaped in a query or path, quoted by Go, and escaped in a JSON string.
func redact(s string, secrets []string) string {
	for _, secret := range secrets {
		quoted := strconv.Quote(secret)
		inJSON, _ := json.Marshal(secret) // a string always marshals
		for _, form := range []string{
			secret,
			url.QueryEscape(secret),
			url.PathEscape(secret),
			quoted[1 : len(quoted)-1],
			string(inJSON[1 : len(inJSON)-1]),
		} {
			s = strings.ReplaceAll(s, form, "[REDACTED]")
		}
	}
	return s
}

// redactResponse redacts secrets from everything in out that came from the endpoint, which can
// echo what it was sent.
func redactResponse(out *httpapi.RequestOutput, secrets []string) {
	if len(secrets) == 0 {
		return
	}
	out.Status = redact(out.Status, secrets)
	out.Body = redact(out.Body, secrets)
	for _, vs := range out.Headers.AdditionalProperties {
		for i, v := range vs {
			vs[i] = redact(v, secrets)
		}
	}
}
