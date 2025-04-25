package m2

import (
	"context"
	"maps"
	"slices"

	"github.com/pkg/errors"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
)

type (
	extractSMVKeysFunc     = func(opindex uint64, _ SMV, _ []SMV) ([]string, error)
	getStateOpsFunc        = func(string) []uint64
	collectOpsTraverseFunc = func(int, getStateOpsFunc) error
)

func newFuncCollectStateOps(
	mapSize uint64,
	extractSMVKeys extractSMVKeysFunc,
) (
	func(uint64, SMV, []SMV) bool,
	func(uint64) bool,
	dutil.InErrorFunc[collectOpsTraverseFunc],
	error,
) {
	var unmerged dutil.LockedMap[uint64, struct{}]
	switch m, err := dutil.NewLockedMap[uint64, struct{}](nil, mapSize); {
	case err != nil:
		return nil, nil, nil, err
	default:
		unmerged = m
	}

	var ops dutil.LockedMap[uint64, []string]
	switch m, err := dutil.NewLockedMap[uint64, []string](nil, mapSize); {
	case err != nil:
		return nil, nil, nil, err
	default:
		ops = m
	}

	var sts dutil.LockedMap[string, map[uint64]struct{}]
	switch m, err := dutil.NewLockedMap[string, map[uint64]struct{}](nil, mapSize); {
	case err != nil:
		return nil, nil, nil, err
	default:
		sts = m
	}

	clearStates := func(opindex uint64, keys []string) {
		for key := range slices.Values(keys) {
			_, _ = sts.Set(key, func(v map[uint64]struct{}, found bool) (map[uint64]struct{}, error) {
				if !found {
					return nil, dutil.ErrLockedSetIgnore
				}

				delete(v, opindex)

				return v, nil
			})
		}
	}

	return func(opindex uint64, subv SMV, addvs []SMV) bool {
			created, _ := ops.Set(
				opindex,
				func(keys []string, found bool) ([]string, error) {
					if _, f := unmerged.Value(opindex); f {
						if found {
							clearStates(opindex, keys)
						}

						return nil, nil
					}

					if found {
						return nil, dutil.ErrLockedSetIgnore
					}

					switch i, err := extractSMVKeys(opindex, subv, addvs); {
					case err != nil:
						return nil, err
					default:
						keys = i
					}

					for key := range slices.Values(keys) {
						_, _ = sts.Set(key, func(v map[uint64]struct{}, _ bool) (map[uint64]struct{}, error) {
							if v == nil {
								v = map[uint64]struct{}{}
							}

							v[opindex] = struct{}{}

							return v, nil
						})
					}

					return keys, nil
				},
			)

			return created
		},
		func(opindex uint64) bool {
			removed, _ := ops.Remove(opindex, func(keys []string, found bool) error {
				_ = unmerged.SetValue(opindex, struct{}{})

				if !found {
					return dutil.ErrLockedSetIgnore
				}

				clearStates(opindex, keys)

				return nil
			})

			return removed
		},
		func(f collectOpsTraverseFunc) error {
			defer func() {
				unmerged.Close()
				ops.Close()
				sts.Close()
			}()

			return f(
				sts.Len(),
				func(stateKey string) []uint64 {
					var v []uint64

					_ = sts.Get(stateKey, func(m map[uint64]struct{}, found bool) error {
						if found {
							v = slices.Collect(maps.Keys(m))
						}

						return nil
					})

					return v
				},
			)
		},
		nil
}

func extractSMVKeysFromValues(values ...SMV) []string {
	switch i := len(values); {
	case i < 1:
		return nil
	case i < 2: //nolint:mnd
		return []string{values[0].Key()}
	}

	stateKeys := make([]string, len(values))

	for i, value := range slices.All(values) {
		stateKeys[i] = value.Key()
	}

	return stateKeys
}

func newFuncExtractSMVKeys(ctx context.Context, cacheSize int) (
	extractSMVKeysFunc,
	func(),
) {
	get, _, lazySet, closef := dutil.NewLastFirstLazyCache[uint64, []string](
		ctx,
		cacheSize,
		1<<9, 1<<9,
	)

	return func(opindex uint64, subv SMV, addvs []SMV) ([]string, error) {
			if subv == nil && len(addvs) < 1 {
				return nil, errors.New("empty state values")
			}

			var stateKeys []string

			switch i, found := get(opindex); {
			case found:
				stateKeys = i
			default:
				stateKeys = extractSMVKeysFromValues(addvs...)
				if subv != nil {
					stateKeys = append(stateKeys, extractSMVKeysFromValues(subv)...)
				}

				lazySet(opindex, stateKeys, 0)
			}

			if len(stateKeys) < 1 {
				return nil, errors.New("empty extracted state keys")
			}

			return stateKeys, nil
		},
		closef
}
