package main

import (
	"context"
	"slices"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"golang.org/x/sync/singleflight"
)

type (
	phaseMergerMergeFunc   func(opindex uint64, values []StateMergeValue) error
	phaseMergerUnmergeFunc func(opindex uint64) error
	phaseMergerGetMergers  func(
		[]string,
		func(func(StateMergeValue) (StateMerger, error)) error,
	) error
)

type PhaseMergerArgs struct {
	NewPhaseJobWorkerFunc          NewPhaseJobWorkerFunc
	GetMergersFunc                 phaseMergerGetMergers
	GetValuesFunc                  func(opindex uint64) ([]StateMergeValue, error)
	UpdateReasonFunc               func(opindex uint64, reason string) error
	ExtractStateMergeValueKeysFunc func(opindex uint64, values []StateMergeValue) []string
}

func NewPhaseMergerArgs() *PhaseMergerArgs {
	return &PhaseMergerArgs{
		NewPhaseJobWorkerFunc: func(ctx context.Context) (
			PhaseJobWorkerNewJobFunc,
			func(),
			func() error,
			error,
		) {
			return NewPhaseJobWorker(
				ctx,
				func(ctx context.Context) (JobWorker, error) {
					return NewBaseJobWorker(ctx, 1<<9)
				},
				1<<9,
			)
		},
		GetMergersFunc: func(
			[]string,
			func(func(StateMergeValue) (StateMerger, error)) error,
		) error {
			return util.ErrNotImplemented.Errorf("GetMergersFunc")
		},
		GetValuesFunc: func(uint64) ([]StateMergeValue, error) {
			return nil, util.ErrNotImplemented.Errorf("GetValuesFunc")
		},
		UpdateReasonFunc: func(uint64, string) error {
			return util.ErrNotImplemented.Errorf("UpdateReasonFunc")
		},
		ExtractStateMergeValueKeysFunc: func(opindex uint64, values []StateMergeValue) []string {
			panic("ExtractStateMergeValueKeys")

			return nil
		},
	}
}

func NewPhaseMerger(
	ctx context.Context,
	args *PhaseMergerArgs,
) (
	merge phaseMergerMergeFunc,
	unmerge phaseMergerUnmergeFunc,
	done func(),
	wait func() error,
	_ error,
) {
	var newJob PhaseJobWorkerNewJobFunc
	var jobDone func()
	var jobWait func() error

	switch n, d, w, err := args.NewPhaseJobWorkerFunc(ctx); {
	case err != nil:
		return nil, nil, nil, nil, err
	default:
		newJob = n
		jobDone = d
		jobWait = w
	}

	var mergeCallback func(opindex uint64, values []StateMergeValue) PhaseJobWorkerCallback
	var unmergeCallback func(opindex uint64) PhaseJobWorkerCallback

	mergeCallback = func(opindex uint64, values []StateMergeValue) PhaseJobWorkerCallback {
		stateKeys := args.ExtractStateMergeValueKeysFunc(opindex, values)

		return func(ctx context.Context, phase int, j PhaseJobWorkerNewJobFunc) error {
			return args.GetMergersFunc(
				stateKeys,
				func(getMerger func(StateMergeValue) (StateMerger, error)) error {
					switch nexts, reason, err := mergeOp(
						ctx,
						opindex,
						values,
						getMerger,
					); {
					case err != nil:
						return err
					default:
						if len(reason) > 0 {
							if err := args.UpdateReasonFunc(opindex, reason); err != nil {
								return err
							}
						}

						return util.TraverseSliceV1(nexts, func(_ int, next uint64) (bool, error) {
							return true, j(next, unmergeCallback(next))
						})
					}
				},
			)
		}
	}

	unmergeCallback = func(opindex uint64) PhaseJobWorkerCallback {
		var values []StateMergeValue

		switch i, err := args.GetValuesFunc(opindex); {
		case err != nil:
			return func(context.Context, int, PhaseJobWorkerNewJobFunc) error {
				return err
			}
		default:
			values = i
		}

		stateKeys := args.ExtractStateMergeValueKeysFunc(opindex, values)

		return func(ctx context.Context, phase int, j PhaseJobWorkerNewJobFunc) error {
			return args.GetMergersFunc(
				stateKeys,
				func(getMerger func(StateMergeValue) (StateMerger, error)) error {
					switch nexts, err := unmergeOp(
						ctx,
						opindex,
						values,
						getMerger,
					); {
					case err != nil:
						return err
					default:
						return util.TraverseSliceV1(nexts, func(_ int, next uint64) (bool, error) {
							switch values, err := args.GetValuesFunc(next); {
							case err != nil:
								return false, err
							default:
								return true, j(next, mergeCallback(next, values))
							}
						})
					}
				},
			)
		}
	}

	return func(opindex uint64, values []StateMergeValue) error {
			return newJob(opindex, mergeCallback(opindex, values))
		},
		func(opindex uint64) error {
			return newJob(opindex, unmergeCallback(opindex))
		},
		jobDone,
		jobWait,
		nil
}

func extractStateMergeValueKeys(values []StateMergeValue) (stateKeys []string) {
	if len(values) < 2 {
		return []string{values[0].Key()}
	}

	add, done, _ := UniqueCompactAppendSliceV1[string](uint64(len(values)))

	_ = util.TraverseSliceV1(values, func(_ int, value StateMergeValue) (bool, error) {
		add(value.Key())

		return true, nil
	})

	_ = done(func(s []string) error {
		stateKeys = slices.Clone(s)

		return nil
	})

	return stateKeys
}

func newExtractStateMergeValueKeysFunc(cacheSize int) (
	func(uint64, []StateMergeValue) []string,
	func(),
) {
	cache := util.NewLFUGCache[uint64, []string](cacheSize)

	var sg singleflight.Group

	return func(opindex uint64, values []StateMergeValue) (stateKeys []string) {
			sg.Do(
				string(util.Uint64ToBytes(opindex)),
				func() (interface{}, error) {
					switch i, found := cache.Get(opindex); {
					case found:
						stateKeys = i
					default:
						stateKeys = extractStateMergeValueKeys(values)

						cache.Set(opindex, stateKeys, 0)
					}

					return nil, nil
				},
			)

			return stateKeys
		},
		func() {
			cache.Close()
		}
}

func newGetMergersFunc(
	m util.LockedMap[string, StateMerger],
	getState func(stateKey string) (base.State, bool, error),
) (
	phaseMergerGetMergers,
	func(),
) {
	mmergers := util.NewMultiKeysLockedMap[string, StateMerger](m)

	return func(
			stateKeys []string,
			f func(func(StateMergeValue) (StateMerger, error)) error,
		) error {
			return mmergers.LockFunc(
				stateKeys,
				func(m util.LockedMap[string, StateMerger]) error {
					return f(getOrCreateMergerFunc(m, getState))
				},
			)
		},
		mmergers.Close
}
