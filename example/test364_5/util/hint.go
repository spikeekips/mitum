package util

import (
	"context"
	"strconv"

	"github.com/spikeekips/mitum/util/hint"
)

func NewHintset[T any]() (
	func(hint.Type, uint64, T) bool,
	func(hint.Type, uint64) (T, bool /* found */),
	func(), /* close */
) {
	pool := map[string]T{}

	return func(ht hint.Type, v uint64, t T) bool {
			key := hintSetKey(ht, v)

			if _, found := pool[key]; found {
				return false
			}

			pool[key] = t

			return true
		},
		func(ht hint.Type, v uint64) (T, bool) {
			t, found := pool[hintSetKey(ht, v)]

			return t, found
		},
		func() { clear(pool) }
}

func hintSetKey(ht hint.Type, v uint64) string {
	return ht.String() + "v" + strconv.FormatUint(v, 10)
}

func NewHintCache(ctx context.Context, cacheSize int, bufSize uint64) (
	func(string) (hint.Hint, error),
	func(),
) {
	get, _, lazySet, closef := NewLastFirstLazyCache[string, [2]any /* {*hint.Hint, error} */](
		ctx, cacheSize, bufSize, bufSize)

	return func(s string) (hint.Hint, error) {
			if i, found := get(s); found {
				if i[1] != nil {
					return hint.Hint{}, i[1].(error) //nolint:forcetypeassert
				}

				return *(i[0].(*hint.Hint)), nil //nolint:forcetypeassert
			}

			var ht hint.Hint
			var htp *hint.Hint

			h, err := hint.ParseHint(s)
			if err == nil {
				ht = h
				htp = &h
			}

			lazySet(s, [2]any{htp, err}, 0)

			return ht, err
		},
		closef
}
