package internals

import (
	"context"
	"math"
	"time"
)

func retryWithBackoff(
	ctx context.Context,
	maxRetries int,
	baseDelay time.Duration,
	handler func() error,
) error {
	var err error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		err = handler()
		if err == nil {
			return nil
		}

		delay := time.Duration(math.Pow(2, float64(attempt))) * baseDelay
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}
	return err
}
