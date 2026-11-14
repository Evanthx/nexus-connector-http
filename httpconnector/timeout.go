package httpconnector

import (
	"fmt"
	"time"

	"github.com/temporalio/nexus-connector-http/generated/httpapi"
)

// MaxDurationEnvVar names the environment variable on the connector worker that sets the longest
// a call may take in all, including every retry and rate-limit wait, in whole minutes, at least
// 1. It applies when the caller's Nexus operation has longer left, or no timeout.
const MaxDurationEnvVar = "HTTP_CONNECTOR_MAX_DURATION_MINUTES"

// defaultMaxDuration is the maximum when MaxDurationEnvVar is unset or empty.
const defaultMaxDuration = 25 * time.Hour

// callbackMargin is how long before the caller's Nexus operation times out the connector stops
// trying, so its last error reaches the caller before the caller gives up: time to start the
// activity and to deliver the completion callback.
const callbackMargin = 10 * time.Second

// ScheduleToClose is how long the call for req may take in all, including every retry and
// rate-limit wait. operationTimeout is the operation-timeout header, the time the caller's Nexus
// operation has left, which its server sets when the operation has a timeout; empty means it has
// none. The call ends callbackMargin before the operation times out, so the caller gets the
// connector's last error rather than a timeout, and never takes longer than the connector's
// maximum (MaxDurationEnvVar).
//
// It reports an error if the operation has too little time left for one attempt
// (timeoutSeconds) plus callbackMargin, or if the header can't be read.
func (a *HTTPActivities) ScheduleToClose(req httpapi.SendRequestInput, operationTimeout string) (time.Duration, error) {
	if operationTimeout == "" {
		return a.MaxDuration(), nil
	}
	left, err := time.ParseDuration(operationTimeout)
	if err != nil {
		return 0, fmt.Errorf("can't read the operation-timeout header %q: %w", operationTimeout, err)
	}
	total := left - callbackMargin
	if total < timeout(req) {
		return 0, fmt.Errorf("the caller's Nexus operation times out in %s, which leaves no time for an attempt: "+
			"it needs at least timeoutSeconds (%s) plus %s for the result to reach the caller; "+
			"give the operation a longer timeout or lower timeoutSeconds",
			left.Round(time.Millisecond), timeout(req), callbackMargin)
	}
	return min(total, a.MaxDuration()), nil
}

// MaxDuration is the longest a call may take in all, from MaxDurationEnvVar: what ScheduleToClose
// gives a caller with no timeout, or a longer one.
func (a *HTTPActivities) MaxDuration() time.Duration {
	return a.maxTotal
}
