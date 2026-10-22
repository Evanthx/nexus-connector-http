package httpconnector

import (
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"
)

// RateLimitMinutesEnvVar names the environment variable on the connector worker that sets how
// many minutes a key stays rate-limited after a 429. It must be a whole number, at least 1.
const RateLimitMinutesEnvVar = "HTTP_CONNECTOR_RATE_LIMIT_MINUTES"

// defaultRateLimitWindow is the window when RateLimitMinutesEnvVar is unset or empty.
const defaultRateLimitWindow = 5 * time.Minute

// rateLimitWindow reads the window from RateLimitMinutesEnvVar. A live entry blocks every
// caller with that key for the window, so the endpoint is probed about once per window: the
// waiting requests all retry when it ends, and if the first one's 429 comes back before the
// others check, they wait another window. Requests that check at the same moment all go out.
func rateLimitWindow() (time.Duration, error) {
	v := os.Getenv(RateLimitMinutesEnvVar)
	if v == "" {
		return defaultRateLimitWindow, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 {
		return 0, fmt.Errorf("%s must be a whole number of minutes, at least 1; got %q", RateLimitMinutesEnvVar, v)
	}
	return time.Duration(n) * time.Minute, nil
}

// limiter is an in-process, TTL-based rate-limit cache shared by every SendRequest
// attempt on this worker: a key is "limited" until its entry expires. It is
// intentionally NOT durable and NOT shared across worker processes — see the
// README's known limitations.
//
// Ideally this would be stored in a durable, shared cache. A local cache still
// captures most of the value (hammering a rate-limited endpoint is dramatically reduced)
// and is simple; it can move to a central store (e.g. Redis with a TTL) later.
type limiter struct {
	mu    sync.Mutex
	ttl   time.Duration
	until map[string]time.Time // key -> time the entry expires
}

func newLimiter(ttl time.Duration) *limiter {
	return &limiter{ttl: ttl, until: make(map[string]time.Time)}
}

// remaining reports how long key stays limited as of now; 0 means not limited.
// Expired entries are evicted lazily.
func (l *limiter) remaining(key string, now time.Time) time.Duration {
	l.mu.Lock()
	defer l.mu.Unlock()
	exp, ok := l.until[key]
	if !ok {
		return 0
	}
	if !now.Before(exp) { // now >= exp: expired
		delete(l.until, key)
		return 0
	}
	return exp.Sub(now)
}

// mark starts a fresh TTL window for key (used on a 429). It also evicts every expired entry, so
// the cache holds only keys marked within the last window, not every key ever marked.
func (l *limiter) mark(key string, now time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for k, exp := range l.until {
		if !now.Before(exp) {
			delete(l.until, k)
		}
	}
	l.until[key] = now.Add(l.ttl)
}

// clear removes any entry for key (used on a success).
func (l *limiter) clear(key string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.until, key)
}
