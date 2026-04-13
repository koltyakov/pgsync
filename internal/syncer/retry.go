package syncer

import (
	"context"
	"math"
	"math/rand/v2"
	"time"
)

const (
	maxRetries     = 3
	baseDelay      = 500 * time.Millisecond
	maxDelay       = 10 * time.Second
	jitterFraction = 0.1
)

func isRetryableDBError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	retryablePatterns := []string{
		"connection reset by peer",
		"broken pipe",
		"deadlock detected",
		"could not serialize access",
		"connection refused",
		"i/o timeout",
		"temporary",
		"could not connect to server",
		"the database system is starting up",
		"server closed the connection",
		"SSL connection has been closed",
	}
	for _, pattern := range retryablePatterns {
		if containsInsensitive(msg, pattern) {
			return true
		}
	}
	return false
}

func containsInsensitive(s, substr string) bool {
	ls := len(s)
	lsub := len(substr)
	if lsub > ls {
		return false
	}
	for i := 0; i <= ls-lsub; i++ {
		match := true
		for j := 0; j < lsub; j++ {
			sc := s[i+j]
			tc := substr[j]
			if sc >= 'A' && sc <= 'Z' {
				sc += 32
			}
			if tc >= 'A' && tc <= 'Z' {
				tc += 32
			}
			if sc != tc {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	return false
}

func retry(ctx context.Context, fn func() error) error {
	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		err := fn()
		if err == nil {
			return nil
		}

		if !isRetryableDBError(err) {
			return err
		}

		lastErr = err

		if attempt < maxRetries-1 {
			delay := calculateBackoff(attempt)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
			}
		}
	}
	return lastErr
}

func calculateBackoff(attempt int) time.Duration {
	delay := float64(baseDelay) * math.Pow(2, float64(attempt))
	if delay > float64(maxDelay) {
		delay = float64(maxDelay)
	}
	jitter := delay * jitterFraction
	delay += (rand.Float64()*2 - 1) * jitter //nolint:gosec // G404 - jitter doesn't need cryptographic randomness
	if delay < 0 {
		delay = float64(baseDelay)
	}
	return time.Duration(delay)
}
