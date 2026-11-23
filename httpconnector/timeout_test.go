package httpconnector

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/temporalio/nexus-connector-http/nexus-connector-shared/deadline"
)

// A caller's operation too short for one attempt is rejected naming the variable that sets the
// attempt's time; an unreadable header isn't. The limit's arithmetic is deadline.ScheduleToClose's.
func TestScheduleToClose_NamesAttemptTimeout(t *testing.T) {
	a := newWithWindow(testWindow)
	a.attemptTimeout = 30 * time.Second
	_, err := a.ScheduleToClose("39999ms")
	require.ErrorIs(t, err, deadline.ErrTooShort)
	require.ErrorContains(t, err, "(one attempt's time is "+AttemptTimeoutEnvVar+")")

	_, err = a.ScheduleToClose("soon")
	require.ErrorIs(t, err, deadline.ErrTooShort)
	require.NotContains(t, err.Error(), AttemptTimeoutEnvVar)
}

// Each attempt may take the attempt timeout; the whole call, scheduleToClose.
func TestActivityOptions_Timeouts(t *testing.T) {
	a := newWithWindow(testWindow)
	a.attemptTimeout = 45 * time.Second
	opts := a.ActivityOptions("id", 10*time.Minute)
	require.Equal(t, 45*time.Second, opts.StartToCloseTimeout)
	require.Equal(t, 10*time.Minute, opts.ScheduleToCloseTimeout)
}

// Each request gets requestMargin less than the attempt.
func TestRequestTimeout(t *testing.T) {
	a := newWithWindow(testWindow)
	require.Equal(t, 55*time.Second, a.requestTimeout())
	a.attemptTimeout = MinAttemptTimeout
	require.Equal(t, 5*time.Second, a.requestTimeout())
}
