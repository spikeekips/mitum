package main

import (
	"context"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
)

type (
	unmergerWaitCallbackFunc func(unmergedOps []uint64) error
	unmergerWaitFunc         func(unmergerWaitCallbackFunc) error
)

func newFuncUnmerger(
	ctx context.Context,
	newJobWorker newJobWorkerFunc,
	xMergedOpsMapSize uint64,
	getStateMergeValues mergerGetStateMergeValuesFunc,
	getMergers mergerGetStateMergersFunc,
) (
	unmerge phaseMergerXMergeFunc,
	done func(),
	wait unmergerWaitFunc,
	_ error,
) {
	log := newModuleLoggerFromContext(ctx, "unmerger", nil)

	var newJob jobWorkerNewJobFunc
	var jobDone jobWorkerJobDoneFunc
	var jobWait jobWorkerWaitFunc

	switch n, d, w, err := newJobWorker(ctx); {
	case err != nil:
		return nil, nil, nil, err
	default:
		newJob = n
		jobDone = d
		jobWait = w
	}

	var opindexFilter func(opindex uint64) bool
	var opindexFilterClose func()

	switch f, c, err := newFuncOpindexFilter(xMergedOpsMapSize); {
	case err != nil:
		return nil, nil, nil, err
	default:
		opindexFilter = f
		opindexFilterClose = c
	}

	var addOps compactAppendSliceAddFunc[uint64]
	var doneOps compactAppendSliceDoneFunc[uint64]

	if err := rCompactAppendSlice[uint64](xMergedOpsMapSize, xMergedOpsMapSize, &addOps, &doneOps); err != nil {
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
			if !opindexFilter(opindex) {
				return nil
			}

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
		func(f unmergerWaitCallbackFunc) error {
			defer func() {
				_ = doneOps(nil)
				opindexFilterClose()
			}()

			if err := jobWait(); err != nil {
				return err
			}

			if f == nil {
				return nil
			}

			return doneOps(func(unmergedOps []uint64) error {
				return f(unmergedOps)
			})
		},
		nil
}

func newFuncOpindexFilter(mapSize uint64) (
	filter func(opindex uint64) bool,
	closef func(),
	_ error,
) {
	switch m, err := util.NewShardedMap[uint64, struct{}](mapSize, nil); {
	case err != nil:
		return nil, nil, err
	default:
		return func(opindex uint64) bool {
				_, created, _ := m.Set(opindex, func(_ struct{}, found bool) (v struct{}, _ error) {
					if found {
						return v, util.ErrLockedSetIgnore
					}

					return v, nil
				})

				return created
			},
			m.Close,
			nil
	}
}
