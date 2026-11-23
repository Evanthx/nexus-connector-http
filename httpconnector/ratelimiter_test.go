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
	l.mark("k", base, l.ttl)
	require.Equal(t, 30*time.Second, l.remaining("k", base.Add(30*time.Second)))
	require.Zero(t, l.remaining("k", base.Add(time.Minute))) // at expiry -> expired + evicted
	require.Zero(t, l.remaining("k", base))                  // eviction is permanent

	// clear ends a live window early.
	l.mark("k", base, l.ttl)
	l.clear("k")
	require.Zero(t, l.remaining("k", base.Add(time.Second)))
}

// Each key has its own window: one expiring or being cleared leaves the others limited, and an
// unmarked key is never limited.
func TestLimiterKeysAreIndependent(t *testing.T) {
	l := newLimiter(time.Minute)
	base := time.Unix(1000, 0)

	l.mark("a", base, l.ttl)
	l.mark("b", base.Add(30*time.Second), l.ttl)
	require.Zero(t, l.remaining("c", base))

	// a expires; b keeps the rest of its own window.
	require.Zero(t, l.remaining("a", base.Add(time.Minute)))
	require.Equal(t, 30*time.Second, l.remaining("b", base.Add(time.Minute)))

	// Clearing b leaves a's new window alone.
	l.mark("a", base.Add(time.Minute), l.ttl)
	l.clear("b")
	require.Zero(t, l.remaining("b", base.Add(time.Minute)))
	require.Equal(t, time.Minute, l.remaining("a", base.Add(time.Minute)))
}

// Marking a key evicts every expired entry, so keys that are never looked up again don't stay.
func TestLimiterMarkEvictsExpired(t *testing.T) {
	l := newLimiter(time.Minute)
	base := time.Unix(1000, 0)

	l.mark("a", base, l.ttl)
	l.mark("b", base.Add(30*time.Second), l.ttl)
	l.mark("c", base.Add(time.Minute), l.ttl) // a has expired; b hasn't
	require.Len(t, l.until, 2)
	require.Equal(t, 30*time.Second, l.remaining("b", base.Add(time.Minute)))
}
