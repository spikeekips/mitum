package util

import (
	"context"
	"time"

	"github.com/pkg/errors"
)

func Retry(ctx context.Context, f func() (bool, error), limit int, interval time.Duration) error {
	var i int

	var lerr error

	for {
		if i == limit {
			if lerr != nil {
				return lerr
			}

			return errors.Errorf("stop retrying; over limit")
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			keep, err := f()
			if !keep {
				return err
			}

			if err != nil {
				lerr = err
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(interval):
				i++
			}
		}
	}
}
