package httpconnector

import (
	"fmt"
	"time"
)

// MaxDurationEnvVar names the environment variable on the Outbound Connector that sets the longest
// a call may take in all, including every retry and rate-limit wait, in whole minutes, at least
// 1. It applies when the caller's Nexus operation has longer left, or no timeout.
const MaxDurationEnvVar = "HTTP_CONNECTOR_MAX_DURATION_MINUTES"

// defaultMaxDuration is the maximum when MaxDurationEnvVar is unset or empty.
const defaultMaxDuration = 25 * time.Hour

// AttemptTimeoutEnvVar names the environment variable on the Outbound Connector that sets how long one
// attempt may take, in whole seconds, at least minAttemptTimeout. Each HTTP request gets
// requestMargin less, so it times out, with the Outbound Connector's own error, before the attempt does.
const AttemptTimeoutEnvVar = "HTTP_CONNECTOR_ATTEMPT_TIMEOUT_SECONDS"

const (
	// defaultAttemptTimeout is the attempt timeout when AttemptTimeoutEnvVar is unset or empty.
	defaultAttemptTimeout = 60 * time.Second
	// minAttemptTimeout leaves each request at least 5s.
	minAttemptTimeout = 10 * time.Second
	// requestMargin is how much shorter than the attempt each HTTP request's timeout is: time to
	// redact the result and report it to the server once the request has timed out.
	requestMargin = 5 * time.Second
)

// requestTimeout is how long one HTTP request may take: requestMargin less than an attempt.
func (a *HTTPActivities) requestTimeout() time.Duration {
	return a.attemptTimeout - requestMargin
}

// callbackMargin is how long before the caller's Nexus operation times out the Outbound Connector stops
// trying, so its last error reaches the caller before the caller gives up: time to start the
// activity and to deliver the completion callback.
const callbackMargin = 10 * time.Second

// ScheduleToClose is how long the call for req may take in all, including every retry and
// rate-limit wait. operationTimeout is the operation-timeout header, the time the caller's Nexus
// operation has left, which its server sets when the operation has a timeout; empty means it has
// none. The call ends callbackMargin before the operation times out, so the caller gets the
// Outbound Connector's last error rather than a timeout, and never takes longer than the Outbound Connector's
// maximum (MaxDurationEnvVar).
//
// It reports an error if the operation has too little time left for one attempt
// (AttemptTimeoutEnvVar) plus callbackMargin, or if the header can't be read.
func (a *HTTPActivities) ScheduleToClose(operationTimeout string) (time.Duration, error) {
	if operationTimeout == "" {
		return a.MaxDuration(), nil
	}
	left, err := time.ParseDuration(operationTimeout)
	if err != nil {
		return 0, fmt.Errorf("can't read the operation-timeout header %q: %w", operationTimeout, err)
	}
	total := left - callbackMargin
	if total < a.attemptTimeout {
		return 0, fmt.Errorf("the caller's Nexus operation times out in %s, which leaves no time for an attempt: "+
			"it needs at least one attempt (%s, %s) plus %s for the result to reach the caller; "+
			"give the operation a longer timeout",
			left.Round(time.Millisecond), a.attemptTimeout, AttemptTimeoutEnvVar, callbackMargin)
	}
	return min(total, a.MaxDuration()), nil
}

// MaxDuration is the longest a call may take in all, from MaxDurationEnvVar: what ScheduleToClose
// gives a caller with no timeout, or a longer one.
func (a *HTTPActivities) MaxDuration() time.Duration {
	return a.maxTotal
}
