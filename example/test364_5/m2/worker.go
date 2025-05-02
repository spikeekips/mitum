package m2

import (
	"context"
	"iter"

	"github.com/spikeekips/mitum/base"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
)

type (
	reportStateOwnersFunc       = func(_ context.Context, subs, affected []string) error
	reportStatesFunc            = func(context.Context) (dutil.InErrorFunc[base.State], dutil.InErrorFunc[error])
	makeStateFunc               = func(stateKey string, _ base.StateValue, ops []util.Hash) (base.State, error)
	workerWorkerMergeOpFunc     = func(opindex uint64, facth util.Hash, subv SMV, addvs []SMV) error
	workerWorkerStateOwnersFunc = func(subs, affected []string) error
	workerWorkerUnmergeOpFunc   = func(opindex uint64) error
	workerWaitFunc              = dutil.InErrorFunc[makeStateFunc]
)

func NewWorker(
	ctx context.Context,
	local string,
	workers, opsWorkers []string,
	opsShare, statesShare,
	maxOpsSize uint64,
	newGetSVM NewSVMPoolFunc,
	reportUnmergedToManager, reportUnmergedToWorker mergerReportUnmergedFunc,
	reportStateOwnersToManager reportStateOwnersFunc,
	reportStatesToManager reportStatesFunc,
) error {
	wctx, wctxCancel, _, _ := dutil.NewContextWait(ctx)

	var makeSVM MergeSVMFunc
	var getSVM GetSVMFunc
	var closeSVMPool func()

	switch g, s, c, err := newGetSVM(min(maxOpsSize, 1<<9)); { //nolint:mnd
	case err != nil:
		return err
	default:
		makeSVM, getSVM, closeSVMPool = g, s, c
	}

	var delayedNewJob dutil.JobWorkerNewJobFunc
	var delayedJobDone func()
	var delayedJobWait dutil.InErrorFunc[error]

	switch n, j, w, err := dutil.NewFuncJobWorker(wctx, min(dutil.SafeUint64ToInt64(maxOpsSize), 1<<9)); { //nolint:mnd
	case err != nil:
		return err
	default:
		delayedNewJob, delayedJobDone, delayedJobWait = n, j, w
		_ = delayedNewJob
	}

	var mergerMerge mergerMergeSMVsFunc
	var mergerMergeReady func()
	var mergerUnmerge dutil.InErrorFunc[uint64]
	var mergerUnmergeReady func()
	var mergerWait mergerWaitFunc

	switch m, md, u, ud, w, err := newMerger(
		wctx,
		maxOpsSize,
		makeSVM,
		func(context.Context) (mergerUnmergedFunc, func()) {
			managerReportUnmerged, managerReportUnmergedDone := reportUnmergedToManager(wctx)
			workerReportUnmerged, workerReportUnmergedDone := reportUnmergedToWorker(wctx)

			return func(opindex uint64, reason string) error {
					if err := managerReportUnmerged(opindex, reason); err != nil {
						return err
					}

					return workerReportUnmerged(opindex, reason)
				},
				func() {
					managerReportUnmergedDone()
					workerReportUnmergedDone()
				}
		},
	); {
	case err != nil:
		return err
	default:
		mergerMerge, mergerMergeReady, mergerUnmerge, mergerUnmergeReady, mergerWait = m, md, u, ud, w
		_, _, _, _, _ = mergerMerge, mergerMergeReady, mergerUnmerge, mergerUnmergeReady, mergerWait
	}

	var workerMergerMerge workerMergerWorkerMergeSMVsFunc
	var workerMergerUnmerge workerMergerUnmergedFunc
	var workerMergerWait dutil.ErrorFunc

	var setFact dutil.In2Func[uint64, util.Hash, bool]
	var getFacts func([]uint64) ([]util.Hash, error)
	var closeFactPool func()

	switch s, g, c, err := newFactPool(maxOpsSize); {
	case err != nil:
		return err
	default:
		setFact, getFacts, closeFactPool = s, g, c
	}

	switch m, u, w, err := newFuncWorkerMerger(
		wctx,
		local,
		workers, opsWorkers,
		opsShare, statesShare,
		mergerMergeReady,
		mergerUnmergeReady,
		func(subs, affected []string) error {
			return nil
		},
	); {
	case err != nil:
		return err
	default:
		workerMergerMerge, workerMergerUnmerge, workerMergerWait = m, u, w
		_, _, _ = workerMergerMerge, workerMergerUnmerge, workerMergerWait
	}

	var wait workerWaitFunc

	switch w, err := newFuncWorkerWait(
		wctx,
		maxOpsSize,
		getSVM,
		getFacts,
		reportStatesToManager,
		workerMergerWait,
		mergerWait,
	); {
	case err != nil:
		return err
	default:
		wait = w
	}

	_, _, _, _, _ = 0,
		func(
			seq iter.Seq2[uint64, [2]util.Hash],
		) error {
			var gerr error
			var subs, affected []string

			for opindex, hs := range seq {
				_ = setFact(opindex, hs[1])

				var subv SMV
				var addvs []SMV

				// FIXME extract SMVs
				// FIXME distribute ops, fact hashes and state owners

				if err := mergerMerge(opindex, subv, addvs); err != nil {
					gerr = err

					break
				}
			}

			if err := reportStateOwnersToManager(ctx, subs, affected); err != nil {
				return err
			}

			if err := delayedNewJob(func(context.Context) error {
				// FIXME distribute not subs and not affected workers

				return nil
			}); err != nil {
				return err
			}

			return workerMergerMerge(local, subs, affected, gerr)
		},
		func(
			worker string,
			ops dutil.InErrorFunc[workerWorkerMergeOpFunc],
			owners dutil.InErrorFunc[workerWorkerStateOwnersFunc],
		) error {
			gerr := ops(func(opindex uint64, facth util.Hash, subv SMV, addvs []SMV) error {
				return mergerMerge(opindex, subv, addvs)
			})

			var subs, affected []string

			if gerr == nil {
				gerr = owners(func(s, a []string) error {
					subs, affected = s, a

					return nil
				})
			}

			return workerMergerMerge(worker, subs, affected, gerr)
		},
		func(worker string, f dutil.InErrorFunc[workerWorkerUnmergeOpFunc]) error {
			return workerMergerUnmerge(worker, f(func(opindex uint64) error {
				return mergerUnmerge(opindex)
			}))
		},
		func(makeState makeStateFunc) error {
			defer func() {
				_ = wctxCancel(nil)

				closeSVMPool()
				closeFactPool()
			}()

			if err := wait(makeState); err != nil {
				return wctxCancel(err)
			}

			delayedJobDone()

			// FIXME save states
			// FIXME save operations

			return delayedJobWait(nil)
		}

	return nil
}

func newFuncWorkerWait(
	ctx context.Context,
	maxOpsSize uint64,
	getSVM GetSVMFunc,
	getFacts func([]uint64) ([]util.Hash, error),
	reportStatesToManager reportStatesFunc,
	workerMergerWait dutil.ErrorFunc,
	mergerWait mergerWaitFunc,
) (workerWaitFunc, error) {
	var statePool dutil.LockedMap[string, base.State]

	switch m, err := dutil.NewLockedMap[string, base.State](nil, maxOpsSize*2); { //nolint:mnd
	case err != nil:
		return nil, err
	default:
		statePool = m
	}

	reportStateToManager, closeReportStateToManager := reportStatesToManager(ctx)

	return func(makeState makeStateFunc) error {
			defer func() {
				statePool.Close()
			}()

			if err := workerMergerWait(); err != nil {
				return err
			}

			return mergerWait(func(collect collectOpsSeqFunc) error {
				return closeReportStateToManager(closeSVMs(ctx, getSVM, collect,
					func(_ context.Context, stateKey string, v base.StateValue, ops []uint64) error {
						var st base.State

						switch facts, err := getFacts(ops); {
						case err != nil:
							return err
						default:
							s, err := makeState(stateKey, v, facts)
							if err != nil {
								return err
							}

							st = s
						}

						_ = statePool.SetValue(stateKey, st)

						return reportStateToManager(st)
					},
				))
			})
		},
		nil
}
