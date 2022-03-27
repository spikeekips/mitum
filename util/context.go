package util

import "context"

func AwareContext(
	ctx context.Context,
	f func() error,
) error {
	errch := make(chan error, 1)
	go func() {
		errch <- f()
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errch:
		return err
	}
}
