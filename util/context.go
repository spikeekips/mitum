package util

import (
	"context"
	"io"

	"github.com/pkg/errors"
)

var EmptyCancelFunc = func() error { return nil }

type ContextKey string

func AwareContext(ctx context.Context, f func(context.Context) error) error {
	_, err := AwareContextValue[any](ctx, func(ctx context.Context) (any, error) {
		return nil, f(ctx)
	})

	return err
}

func AwareContextValue[T any](ctx context.Context, f func(context.Context) (T, error)) (T, error) {
	donech := make(chan [2]any, 1)

	go func() {
		i, err := f(ctx)
		donech <- [2]any{i, err}
	}()

	var t T

	select {
	case <-ctx.Done():
		return t, errors.WithStack(ctx.Err())
	case i := <-donech:
		if i[0] != nil {
			t = i[0].(T) //nolint:forcetypeassert //...
		}

		var err error
		if i[1] != nil {
			err = i[1].(error) //nolint:forcetypeassert //...
		}

		return t, err
	}
}

func LoadFromContextOK(ctx context.Context, a ...interface{}) error {
	if err := checkLoadFromContextOK(ctx, a...); err != nil {
		return err
	}

	return loadFromContext(ctx, load, a...)
}

func LoadFromContext(ctx context.Context, a ...interface{}) error {
	if err := checkLoadArgs(a); err != nil {
		return err
	}

	return loadFromContext(ctx, load, a...)
}

func checkLoadFromContextOK(ctx context.Context, a ...interface{}) error {
	switch {
	case len(a) < 1:
		return errors.Errorf("empty [key value] pairs")
	case len(a)%2 != 0:
		return errors.Errorf("should be, [key value] pairs")
	}

	for i := 0; i < len(a)/2; i++ {
		b := a[i*2]

		k, ok := b.(ContextKey)
		if !ok {
			return errors.Errorf("expected ContextKey, not %T", b)
		}

		if ctx.Value(k) == nil {
			return ErrNotFound.Errorf("key not found, %q", k)
		}
	}

	return nil
}

func checkLoadArgs(a []interface{}) error {
	switch {
	case len(a) < 1:
		return errors.Errorf("empty [key value] pairs")
	case len(a)%2 != 0:
		return errors.Errorf("should be, [key value] pairs")
	}

	for i := 0; i < len(a)/2; i++ {
		b := a[i*2]

		if _, ok := b.(ContextKey); !ok {
			return errors.Errorf("expected ContextKey, not %T", b)
		}
	}

	return nil
}

func loadFromContext(
	ctx context.Context,
	load func(context.Context, ContextKey, interface{}) error,
	a ...interface{},
) error {
	for i := 0; i < len(a)/2; i++ {
		b := a[i*2]

		v := a[i*2+1]

		if err := load(ctx, b.(ContextKey), v); err != nil { //nolint:forcetypeassert //...
			return err
		}
	}

	return nil
}

func load(ctx context.Context, key ContextKey, v interface{}) error {
	i := ctx.Value(key)
	if i == nil {
		return nil
	}

	if err := InterfaceSetValue(i, v); err != nil {
		return errors.WithMessagef(err, "load value from context, %q", key)
	}

	return nil
}

func PipeReadWrite(
	ctx context.Context,
	read func(context.Context, io.Reader) error,
	write func(context.Context, io.Writer) error,
) error {
	pr, pw := io.Pipe()

	defer func() {
		_ = pw.Close()
		_ = pr.Close()
	}()

	errch := make(chan error, 1)

	go func() {
		err := read(ctx, pr)
		if err != nil {
			_ = pw.Close()
			_ = pr.Close()
		}

		errch <- err
	}()

	if err := write(ctx, pw); err != nil {
		return err
	}

	_ = pw.Close()

	select {
	case <-ctx.Done():
		return errors.WithStack(ctx.Err())
	case err := <-errch:
		return err
	}
}
