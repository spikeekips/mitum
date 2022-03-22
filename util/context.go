package util

import "context"

func AwareContext(
	ctx context.Context,
	f func() error,
) error {
	errch := make(chan error)
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
