package util

import (
	"context"
	"iter"
	"sync"
)

var ErrContextUnlimitedChanInsertDone = NewSError("insert done")

func NewUnlimitedChan[T any](ctx context.Context, bufSize, outBufSize uint64) (
	InErrorFunc[T],
	func(),
	iter.Seq[T],
) {
	in := make(chan T)
	out := make(chan T, outBufSize)

	donech := make(chan struct{})

	done := sync.OnceFunc(func() {
		close(donech)
	})

	go func() {
		buf := make([]T, 0, bufSize)
		defer ClearSlice(&buf)

		for {
			for len(buf) > 0 {
				select {
				case <-ctx.Done():
					close(out)

					return
				case t := <-in:
					buf = append(buf, t)
				case out <- buf[0]:
					buf = buf[1:]
				}
			}

			select {
			case <-ctx.Done():
				close(out)

				return
			case <-donech:
				close(out)

				return
			case t := <-in:
				buf = append(buf, t)
			}
		}
	}()

	return func(t T) error {
			select {
			case <-ctx.Done():
				done()

				return ctx.Err()
			case <-donech:
				return ErrContextUnlimitedChanInsertDone
			case in <- t:
				return nil
			}
		},
		done,
		func(yield func(T) bool) {
			defer done()

			if yield == nil {
				return
			}

			for t := range out {
				if !yield(t) {
					return
				}
			}
		}
}
