package util

import (
	"iter"
)

func FilterSlice[T any](s iter.Seq[T], f func(T) bool) iter.Seq[T] {
	return func(yield func(T) bool) {
		if yield == nil {
			return
		}

	end:
		for i := range s {
			switch {
			case !f(i):
				continue end
			case !yield(i):
				break end
			}
		}
	}
}

func ClearSlice[T any](s *[]T) {
	if s == nil {
		return
	}

	i := *s
	clear(i)
	i = i[:0]
	*s = i
}

func SeqAll[T any](s iter.Seq[T]) iter.Seq2[int, T] {
	return func(yield func(int, T) bool) {
		if yield == nil {
			return
		}

		var i int

		for t := range s {
			if !yield(i, t) {
				return
			}

			i++
		}
	}
}
