package httpconnector

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The call ends callbackMargin before the caller's operation times out, never takes longer than
// the maximum, and is rejected when the operation hasn't time for one attempt plus the margin.
func TestScheduleToClose(t *testing.T) {
	a := newWithWindow(testWindow)
	a.maxTotal = 2 * time.Hour
	a.attemptTimeout = 30 * time.Second
	for _, tc := range []struct {
		name    string
		header  string
		want    time.Duration
		wantErr string // empty: accepted
	}{
		{"no operation timeout", "", 2 * time.Hour, ""},
		{"operation shorter than the maximum", "3600000ms", time.Hour - callbackMargin, ""},
		{"operation longer than the maximum", "90000000ms", 2 * time.Hour, ""},
		{"exactly one attempt plus the margin", "40s", 30 * time.Second, ""},
		{"under one attempt plus the margin", "39999ms",
			0, "the caller's Nexus operation times out in 39.999s, which leaves no time for an attempt: it needs at least one attempt (30s, " + AttemptTimeoutEnvVar + ") plus 10s"},
		{"already timed out", "-1s", 0, "times out in -1s"},
		{"unreadable header", "soon", 0, `can't read the operation-timeout header "soon"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := a.ScheduleToClose(tc.header)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

// Each attempt may take the attempt timeout; the whole call, scheduleToClose.
func TestActivityOptions_Timeouts(t *testing.T) {
	a := newWithWindow(testWindow)
	a.attemptTimeout = 45 * time.Second
	opts := a.ActivityOptions("id", 10*time.Minute)
	require.Equal(t, 45*time.Second, opts.StartToCloseTimeout)
	require.Equal(t, 10*time.Minute, opts.ScheduleToCloseTimeout)
}

// New reads the attempt timeout from the environment, 60 seconds if unset, and fails on an
// invalid one. Each request gets 5 seconds less.
func TestNew_AttemptTimeout(t *testing.T) {
	t.Setenv(AllowedDomainsEnvVar, DisableAllowlist)
	t.Setenv(AttemptTimeoutEnvVar, "")
	a, err := New()
	require.NoError(t, err)
	require.Equal(t, 60*time.Second, a.attemptTimeout)
	require.Equal(t, 55*time.Second, a.requestTimeout())

	t.Setenv(AttemptTimeoutEnvVar, "10")
	a, err = New()
	require.NoError(t, err)
	require.Equal(t, 10*time.Second, a.attemptTimeout)
	require.Equal(t, 5*time.Second, a.requestTimeout())

	for _, bad := range []string{"9", "0", "-1", "1.5", "30s", " 30"} {
		t.Setenv(AttemptTimeoutEnvVar, bad)
		_, err := New()
		require.ErrorContains(t, err, AttemptTimeoutEnvVar, "value %q", bad)
	}
}

// New reads the maximum from the environment, 25 hours if unset, and fails on an invalid one.
func TestNew_MaxDuration(t *testing.T) {
	t.Setenv(AllowedDomainsEnvVar, DisableAllowlist)
	t.Setenv(MaxDurationEnvVar, "")
	a, err := New()
	require.NoError(t, err)
	require.Equal(t, 25*time.Hour, a.maxTotal)

	t.Setenv(MaxDurationEnvVar, "90")
	a, err = New()
	require.NoError(t, err)
	require.Equal(t, 90*time.Minute, a.maxTotal)

	for _, bad := range []string{"0", "-1", "1.5", "1h", " 2"} {
		t.Setenv(MaxDurationEnvVar, bad)
		_, err := New()
		require.ErrorContains(t, err, MaxDurationEnvVar, "value %q", bad)
	}
}
