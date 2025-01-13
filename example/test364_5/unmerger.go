package main

import (
	"context"
	"slices"

	"github.com/pkg/errors"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
)

type (
	opindexFilterFunc = func(opindex uint64) (bool, error)
	unmergedErrorFunc = func() ([]uint64, error)
)

func newFuncUnmerger(
	ctx context.Context,
	newJobWorker dutil.NewJobWorkerFunc,
	maxOpsSize uint64,
	getStateMergeValues opindexGetStateMergeValuesFunc,
	getMergers mergerGetStateMergersFunc,
) (
	unmerge dutil.InErrorFunc[uint64],
	done func(),
	wait unmergedErrorFunc,
	_ error,
) {
	log := dutil.NewModuleLoggerFromContext(ctx, "unmerger", nil)

	var newJob dutil.JobWorkerNewJobFunc
	var jobDone func()
	var jobWait dutil.InErrorFunc[error]

	switch n, d, w, err := newJobWorker(ctx, maxOpsSize); {
	case err != nil:
		return nil, nil, nil, err
	default:
		newJob, jobDone, jobWait = n, d, w
	}

	var addOps dutil.AppendSliceAddFunc[uint64]
	var doneOps dutil.OutFunc[[]uint64]

	switch a, d, err := dutil.NewLockAppendSlice[uint64](maxOpsSize, maxOpsSize); {
	case err != nil:
		return nil, nil, nil, err
	default:
		addOps, doneOps = a, d
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
		func() ([]uint64, error) {
			if err := jobWait(nil); err != nil {
				return nil, err
			}

			// NOTE local unmerged not included
			return doneOps(), nil
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

	switch i, err := util.NewLockedMap[uint64, struct{}](mapSize, nil); {
	case err != nil:
		return nil, nil, err
	default:
		m = i

		for opindex := range slices.Values(localUnmerged) {
			_ = m.SetValue(opindex, struct{}{})
		}
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
