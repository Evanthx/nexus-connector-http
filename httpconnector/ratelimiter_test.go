package httpconnector

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLimiterTTL(t *testing.T) {
	l := newLimiter(time.Minute)
	base := time.Unix(1000, 0)

	require.Zero(t, l.remaining("k", base)) // nothing cached yet

	// mark starts a TTL window; remaining counts down within it.
	l.mark("k", base)
	require.Equal(t, 30*time.Second, l.remaining("k", base.Add(30*time.Second)))
	require.Zero(t, l.remaining("k", base.Add(time.Minute))) // at expiry -> expired + evicted
	require.Zero(t, l.remaining("k", base))                  // eviction is permanent

	// clear ends a live window early.
	l.mark("k", base)
	l.clear("k")
	require.Zero(t, l.remaining("k", base.Add(time.Second)))
}

// Each key has its own window: one expiring or being cleared leaves the others limited, and an
// unmarked key is never limited.
func TestLimiterKeysAreIndependent(t *testing.T) {
	l := newLimiter(time.Minute)
	base := time.Unix(1000, 0)

	l.mark("a", base)
	l.mark("b", base.Add(30*time.Second))
	require.Zero(t, l.remaining("c", base))

	// a expires; b keeps the rest of its own window.
	require.Zero(t, l.remaining("a", base.Add(time.Minute)))
	require.Equal(t, 30*time.Second, l.remaining("b", base.Add(time.Minute)))

	// Clearing b leaves a's new window alone.
	l.mark("a", base.Add(time.Minute))
	l.clear("b")
	require.Zero(t, l.remaining("b", base.Add(time.Minute)))
	require.Equal(t, time.Minute, l.remaining("a", base.Add(time.Minute)))
}

// Marking a key evicts every expired entry, so keys that are never looked up again don't stay.
func TestLimiterMarkEvictsExpired(t *testing.T) {
	l := newLimiter(time.Minute)
	base := time.Unix(1000, 0)

	l.mark("a", base)
	l.mark("b", base.Add(30*time.Second))
	l.mark("c", base.Add(time.Minute)) // a has expired; b hasn't
	require.Len(t, l.until, 2)
	require.Equal(t, 30*time.Second, l.remaining("b", base.Add(time.Minute)))
}

func TestRateLimitWindow(t *testing.T) {
	t.Setenv(RateLimitMinutesEnvVar, "")
	window, err := rateLimitWindow()
	require.NoError(t, err)
	require.Equal(t, 5*time.Minute, window, "unset defaults to 5 minutes")

	t.Setenv(RateLimitMinutesEnvVar, "2")
	window, err = rateLimitWindow()
	require.NoError(t, err)
	require.Equal(t, 2*time.Minute, window)

	for _, bad := range []string{"0", "-1", "1.5", "five", " 2"} {
		t.Setenv(RateLimitMinutesEnvVar, bad)
		_, err := rateLimitWindow()
		require.ErrorContains(t, err, RateLimitMinutesEnvVar, "value %q", bad)
	}
}

// New reads the window from the environment, and fails on an invalid one.
func TestNew_RateLimitWindow(t *testing.T) {
	t.Setenv(RateLimitMinutesEnvVar, "3")
	a, err := New()
	require.NoError(t, err)
	require.Equal(t, 3*time.Minute, a.lim.ttl)

	t.Setenv(RateLimitMinutesEnvVar, "0")
	_, err = New()
	require.Error(t, err)
}
