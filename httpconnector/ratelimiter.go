package httpconnector

import (
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/temporalio/nexus-connector-http/generated/httpapi"
)

// RateLimitMinutesEnvVar names the environment variable on the Connector that sets how
// many minutes a key stays rate-limited after a 429. It must be a whole number, at least 1.
const RateLimitMinutesEnvVar = "HTTP_CONNECTOR_RATE_LIMIT_MINUTES"

// defaultRateLimitWindow is the window when RateLimitMinutesEnvVar is unset or empty. A live entry
// blocks every caller with that key for the window, so the endpoint is probed about once per
// window: the waiting requests all retry when it ends, and if the first one's 429 comes back before
// the others check, they wait another window. Requests that check at the same moment all go out.
const defaultRateLimitWindow = 5 * time.Minute

// limiter is an in-process, TTL-based rate-limit cache shared by every Request
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

// mark limits key for wait from now (used on a 429; wait is at most the TTL). It also evicts every
// expired entry, so the cache holds only keys marked within the last window, not every key ever
// marked.
func (l *limiter) mark(key string, now time.Time, wait time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for k, exp := range l.until {
		if !now.Before(exp) {
			delete(l.until, k)
		}
	}
	l.until[key] = now.Add(wait)
}

// clear removes any entry for key (used on a success).
func (l *limiter) clear(key string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.until, key)
}

// retryAfter is how long to wait after the 429 in out, as of now: its Retry-After header, in
// seconds or as an HTTP date, if that is shorter than window, else window. A header that is
// missing, unparseable, zero or in the past also means window.
func retryAfter(out httpapi.RequestOutput, now time.Time, window time.Duration) time.Duration {
	v := http.Header(out.Headers.AdditionalProperties).Get("Retry-After")
	var wait time.Duration
	if secs, err := strconv.ParseInt(v, 10, 64); err == nil {
		if secs > int64(window/time.Second) {
			return window // also keeps a huge value from overflowing
		}
		wait = time.Duration(secs) * time.Second
	} else if t, err := http.ParseTime(v); err == nil {
		wait = t.Sub(now)
	}
	if wait <= 0 || wait > window {
		return window
	}
	return wait
}
