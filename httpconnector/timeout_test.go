package httpconnector

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/temporalio/nexus-connector-http/generated/httpapi"
)

// The call ends callbackMargin before the caller's operation times out, never takes longer than
// the maximum, and is rejected when the operation hasn't time for one attempt plus the margin.
func TestScheduleToClose(t *testing.T) {
	a := newWithWindow(testWindow)
	a.maxTotal = 2 * time.Hour
	defaultAttempt := httpapi.SendRequestInput{URL: "https://x.test"} // timeoutSeconds 30
	oneSecond := httpapi.SendRequestInput{URL: "https://x.test", TimeoutSeconds: new(int64(1))}
	for _, tc := range []struct {
		name    string
		req     httpapi.SendRequestInput
		header  string
		want    time.Duration
		wantErr string // empty: accepted
	}{
		{"no operation timeout", defaultAttempt, "", 2 * time.Hour, ""},
		{"operation shorter than the maximum", defaultAttempt, "3600000ms", time.Hour - callbackMargin, ""},
		{"operation longer than the maximum", defaultAttempt, "90000000ms", 2 * time.Hour, ""},
		{"exactly one attempt plus the margin", defaultAttempt, "40s", 30 * time.Second, ""},
		{"short attempt", oneSecond, "11s", time.Second, ""},
		{"under one attempt plus the margin", defaultAttempt, "39999ms",
			0, "the caller's Nexus operation times out in 39.999s, which leaves no time for an attempt: it needs at least timeoutSeconds (30s) plus 10s"},
		{"under the margin", oneSecond, "5s", 0, "times out in 5s, which leaves no time for an attempt"},
		{"already timed out", oneSecond, "-1s", 0, "times out in -1s"},
		{"unreadable header", defaultAttempt, "soon", 0, `can't read the operation-timeout header "soon"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := a.ScheduleToClose(tc.req, tc.header)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestActivityOptions_ScheduleToClose(t *testing.T) {
	require.Equal(t, 10*time.Minute, ActivityOptions("id", 10*time.Minute).ScheduleToCloseTimeout)
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
