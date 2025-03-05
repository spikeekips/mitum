package util

import (
	"context"
	"iter"
	"sync"

	"github.com/spikeekips/mitum/util"
)

var ErrContextUnlimitedChansInsertDone = util.NewIDError("insert done")

func NewContextUnlimitedChans[T any](ctx context.Context, bufSize, outBufSize uint64) (
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

	end:
		for {
			for len(buf) > 0 {
				select {
				case t := <-in:
					buf = append(buf, t)
				case out <- buf[0]:
					buf = buf[1:]
				}
			}

			select {
			case <-ctx.Done():
				close(out)

				break end
			case <-donech:
				close(out)

				break end
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
				return ErrContextUnlimitedChansInsertDone
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

func NewUnlimitedChans[T any](bufSize, outBufSize uint64) (
	InErrorFunc[T],
	func(),
	iter.Seq[T],
) {
	return NewContextUnlimitedChans[T](context.Background(), bufSize, outBufSize)
}
