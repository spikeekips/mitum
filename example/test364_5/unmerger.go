package main

import (
	"context"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
)

type (
	unmergedFunc      func(unmerged []uint64) error
	opindexFilterFunc func(opindex uint64) (bool, error)
)

func newFuncUnmerger(
	ctx context.Context,
	newJobWorker newJobWorkerFunc,
	maxOpsSize uint64,
	getStateMergeValues opindexGetStateMergeValuesFunc,
	getMergers mergerGetStateMergersFunc,
) (
	unmerge inErrorFunc[uint64],
	done func(),
	wait inErrorFunc[unmergedFunc],
	_ error,
) {
	log := newModuleLoggerFromContext(ctx, "unmerger", nil)

	var newJob jobWorkerNewJobFunc
	var jobDone func()
	var jobWait errorFunc

	switch n, d, w, err := newJobWorker(ctx); {
	case err != nil:
		return nil, nil, nil, err
	default:
		newJob = n
		jobDone = d
		jobWait = w
	}

	var addOps compactAppendSliceAddFunc[uint64]
	var doneOps compactAppendSliceDoneFunc[uint64]

	if err := rCompactAppendSlice[uint64](maxOpsSize, 1<<5, &addOps, &doneOps); err != nil {
		return nil, nil, nil, err
	}

	unmergef := func(opindex uint64) (committed bool, _ error) {
		switch c, nextMergeOps, err := unmergeValues(getStateMergeValues, getMergers, opindex); {
		case err != nil:
			return false, err
		case len(nextMergeOps) > 0:
			return false, errors.Errorf("unexpected next merge ops, %v", nextMergeOps)
		default:
			return c, nil
		}
	}

	return func(opindex uint64) error {
			l := log.With().Uint64("opindex", opindex).Logger()

			return newJob(func(context.Context) error {
				switch committed, err := unmergef(opindex); {
				case err != nil:
					l.Error().Err(err).Msg("failed to unmerge")

					return err
				case !committed:
					l.Debug().Msg("not unmerged")
				default:
					_, _ = addOps(opindex)

					l.Debug().Msg("unmerged")
				}

				return nil
			})
		},
		jobDone,
		func(f unmergedFunc) error {
			defer func() {
				_ = doneOps(nil)
			}()

			if err := jobWait(); err != nil {
				return err
			}

			if f == nil {
				return nil
			}

			return doneOps(func(unmerged []uint64) error {
				// NOTE local unmerged not included
				return f(unmerged)
			})
		},
		nil
}

func newFuncOpindexFilter(
	mapSize uint64,
	getStateMergeValues opindexGetStateMergeValuesFunc,
	localUnmerged []uint64,
) (
	_ opindexFilterFunc,
	closef func(),
	_ error,
) {
	var m util.LockedMap[uint64, struct{}]

	switch i, err := util.NewShardedMap[uint64, struct{}](mapSize, nil); {
	case err != nil:
		return nil, nil, err
	default:
		m = i

		_ = traverseSlice(localUnmerged, func(_ int, opindex uint64) (bool, error) {
			_ = m.SetValue(opindex, struct{}{})

			return true, nil
		})
	}

	return func(opindex uint64) (bool, error) {
			_, created, _ := m.Set(opindex, func(_ struct{}, found bool) (v struct{}, _ error) {
				if found {
					return v, util.ErrLockedSetIgnore
				}

				return v, nil
			})

			if created {
				if i, err := getStateMergeValues(opindex); err != nil || len(i) < 1 {
					return false, err
				}
			}

			return created, nil
		},
		m.Close,
		nil
}
