package m2

import (
	"maps"
	"slices"

	dutil "github.com/spikeekips/mitum/example/test364_5/util"
)

type (
	extractSMVKeysFunc     = func(opindex uint64, _ SMV, _ []SMV) ([]string, error)
	getStateOpsFunc        = func(string) []uint64
	collectOpsTraverseFunc = func(int, getStateOpsFunc) error
)

func newFuncSMVPool(size uint64) (
	func(uint64, SMV, []SMV) bool,
	func(uint64) (SMV, []SMV, bool),
	func(), /* close */
	error,
) {
	var pool dutil.LockedMap[uint64, [2][]SMV]

	switch p, err := dutil.NewLockedMap[uint64, [2][]SMV](nil, size); {
	case err != nil:
		return nil, nil, nil, err
	default:
		pool = p
	}

	return func(opindex uint64, sub SMV, adds []SMV) bool {
			created, _ := pool.Set(opindex, func(v [2][]SMV, found bool) ([2][]SMV, error) {
				if found {
					return v, dutil.ErrLockedSetIgnore
				}

				return [2][]SMV{{sub}, adds}, nil
			})

			return created
		},
		func(opindex uint64) (SMV, []SMV, bool) {
			vs, found := pool.Value(opindex)
			if !found {
				return nil, nil, false
			}

			var sub SMV
			if len(vs[0]) > 0 {
				sub = vs[0][0]
			}

			return sub, vs[1], true
		},
		pool.Close,
		nil
}

func newFuncCollectStateOps(
	mapSize uint64,
) (
	func(uint64, string, string) bool,
	func(uint64, string, string) bool,
	dutil.InErrorFunc[collectOpsTraverseFunc],
	error,
) {
	var sts dutil.LockedMap[string, map[uint64]struct{}]
	switch m, err := dutil.NewLockedMap[string, map[uint64]struct{}](nil, mapSize); {
	case err != nil:
		return nil, nil, nil, err
	default:
		sts = m
	}

	var unmerged dutil.LockedMap[uint64, struct{}]
	switch m, err := dutil.NewLockedMap[uint64, struct{}](nil, mapSize); {
	case err != nil:
		return nil, nil, nil, err
	default:
		unmerged = m
	}

	return func(opindex uint64, key string, t string) bool {
			if unmerged.Exists(opindex) {
				return false
			}

			created, _ := sts.Set(key, func(r map[uint64]struct{}, _ bool) (map[uint64]struct{}, error) {
				if _, f := r[opindex]; f {
					return nil, dutil.ErrLockedSetIgnore
				}

				if r == nil {
					r = map[uint64]struct{}{}
				}

				r[opindex] = struct{}{}

				return r, nil
			})

			return created
		},
		func(opindex uint64, key string, t string) bool {
			_, _ = unmerged.Set(opindex, func(_ struct{}, found bool) (struct{}, error) {
				if found {
					return struct{}{}, dutil.ErrLockedSetIgnore
				}

				return struct{}{}, nil
			})

			created, _ := sts.Set(key, func(r map[uint64]struct{}, _ bool) (map[uint64]struct{}, error) {
				if r == nil {
					return nil, dutil.ErrLockedSetIgnore
				}

				if _, f := r[opindex]; !f {
					return nil, dutil.ErrLockedSetIgnore
				}

				delete(r, opindex)

				return r, nil
			})

			return created
		},
		func(f collectOpsTraverseFunc) error {
			defer func() {
				sts.Close()
				unmerged.Close()
			}()

			return f(
				sts.Len(),
				func(stateKey string) []uint64 {
					var v []uint64

					_ = sts.Get(stateKey, func(m map[uint64]struct{}, found bool) error {
						if found && len(m) > 0 {
							v = slices.Collect(maps.Keys(m))
							slices.Sort(v)
						}

						return nil
					})

					return v
				},
			)
		},
		nil
}
