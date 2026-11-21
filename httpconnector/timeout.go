package httpconnector

import (
	"fmt"
	"time"

	"github.com/temporalio/nexus-connector-http/nexus-connector-shared/deadline"
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

// ScheduleToClose is how long the call may take in all, including every retry and rate-limit
// wait: the Outbound Connector's maximum (MaxDurationEnvVar), or less if the caller's Nexus
// operation times out first (deadline.ScheduleToClose). operationTimeout is the operation-timeout
// header.
//
// The error wraps deadline.ErrTooShort if the operation has too little time left for one attempt
// (AttemptTimeoutEnvVar) plus deadline.CallbackMargin, or if the header can't be read.
func (a *HTTPActivities) ScheduleToClose(operationTimeout string) (time.Duration, error) {
	total, err := deadline.ScheduleToClose(operationTimeout, a.MaxDuration(), a.attemptTimeout)
	if err != nil {
		if _, parseErr := time.ParseDuration(operationTimeout); parseErr == nil {
			// Too short for an attempt: say which variable sets the attempt's time.
			return 0, fmt.Errorf("%w (one attempt's time is %s)", err, AttemptTimeoutEnvVar)
		}
		return 0, err
	}
	return total, nil
}

// MaxDuration is the longest a call may take in all, from MaxDurationEnvVar: what ScheduleToClose
// gives a caller with no timeout, or a longer one.
func (a *HTTPActivities) MaxDuration() time.Duration {
	return a.maxTotal
}
