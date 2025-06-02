package dop6

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
	workerWaitFunc              = dutil.InErrorFunc[makeStateFunc]
	workerInsertOpsFunc         = func(
		GetStateOwnersFunc,
		NewWorkerDistributeSMVsFunc,
		iter.Seq2[uint64, [2]util.Hash],
	) error
	workerWorkerInsertOpsFunc = func(
		worker string,
		_ dutil.InErrorFunc[workerWorkerMergeOpFunc],
		_ dutil.InErrorFunc[workerWorkerStateOwnersFunc],
	) error
	workerWorkerUnmergeOpsFunc = func(worker string, f dutil.InErrorFunc[dutil.InErrorFunc[uint64]]) error
)

// FIXME report worker canceled to manager

func NewWorker(
	ctx context.Context,
	local string,
	workers, opsWorkers []string,
	opsShare, statesShare,
	maxOpsSize uint64,
	getOperationFact getOperationFactFunc,
	newGetSVM NewSVMPoolFunc,
	reportUnmergedToManager, reportUnmergedToWorker mergerReportUnmergedFunc,
	reportStateOwnersToManager reportStateOwnersFunc,
	reportStatesToManager reportStatesFunc,
) (
	workerInsertOpsFunc,
	workerWorkerInsertOpsFunc,
	workerWorkerUnmergeOpsFunc,
	dutil.InErrorFunc[makeStateFunc],
	error,
) {
	wctx, wctxCancel, _, _ := dutil.NewContextWait(ctx)

	var makeSVM MergeSVMFunc
	var getSVM GetSVMFunc
	var closeSVMPool func()

	switch g, s, c, err := newGetSVM(min(maxOpsSize, 1<<9)); { //nolint:mnd
	case err != nil:
		return nil, nil, nil, nil, err
	default:
		makeSVM, getSVM, closeSVMPool = g, s, c
	}

	var delayedNewJob dutil.JobWorkerNewJobFunc
	var delayedJobDone func()
	var delayedJobWait dutil.InErrorFunc[error]

	switch n, j, w, err := dutil.NewFuncJobWorker(wctx, min(dutil.SafeUint64ToInt64(maxOpsSize), 1<<9)); { //nolint:mnd
	case err != nil:
		return nil, nil, nil, nil, err
	default:
		delayedNewJob, delayedJobDone, delayedJobWait = n, j, w
	}

	managerReportUnmerged, managerReportUnmergedDone := reportUnmergedToManager(wctx)

	var smvExtractorInsert smvExtractorInsertFunc
	var smvExtractorWait dutil.ErrorFunc

	switch i, w, err := newSMVExtractor(wctx, maxOpsSize, getOperationFact, managerReportUnmerged); {
	case err != nil:
		return nil, nil, nil, nil, err
	default:
		smvExtractorInsert, smvExtractorWait = i, w
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
		return nil, nil, nil, nil, err
	default:
		mergerMerge, mergerMergeReady, mergerUnmerge, mergerUnmergeReady, mergerWait = m, md, u, ud, w
	}

	var setFact dutil.In2Func[uint64, util.Hash, bool]
	var getFacts func([]uint64) ([]util.Hash, error)
	var closeFactPool func()

	switch s, g, c, err := newFactPool(maxOpsSize); {
	case err != nil:
		return nil, nil, nil, nil, err
	default:
		setFact, getFacts, closeFactPool = s, g, c
	}

	var workerMergerMerge workerMergerWorkerMergeSMVsFunc
	var workerMergerUnmerge workerMergerUnmergedFunc
	var workerMergerWait dutil.ErrorFunc

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
		return nil, nil, nil, nil, err
	default:
		workerMergerMerge, workerMergerUnmerge, workerMergerWait = m, u, w
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
		return nil, nil, nil, nil, err
	default:
		wait = w
	}

	return func(
			getStateOwners GetStateOwnersFunc,
			newWorkerDistribute NewWorkerDistributeSMVsFunc,
			seq iter.Seq2[uint64, [2]util.Hash],
		) error {
			var distribute mergerMergeSMVsFunc
			var distributeDone func()
			var distributeWait func(error) (func() ([]string, []string), dutil.ErrorFunc, error)

			switch d, do, w, err := newWorkerDistributeSMVs(
				wctx,
				workers,
				getStateOwners,
				newWorkerDistribute,
			); {
			case err != nil:
				return err
			default:
				distribute, distributeDone, distributeWait = d, do, w
			}

			var gerr error
			if err := smvExtractorInsert(
				func(insert opindexOpHashesFunc) error {
					for opindex, hs := range seq {
						_ = setFact(opindex, hs[1])

						if err := insert(opindex, hs[0], hs[1]); err != nil {
							return err
						}
					}

					return nil
				},
				func(opindex uint64, subv SMV, addvs []SMV) error {
					if err := distribute(opindex, subv, addvs); err != nil {
						return err
					}

					return mergerMerge(opindex, subv, addvs)
				},
			); err != nil {
				gerr = err
			}

			distributeDone()

			defer func() {
				_ = wctxCancel(gerr)
			}()

			switch allOwners, distributeDelayedWait, err := distributeWait(gerr); {
			case err != nil:
				return err
			default:
				subs, affected := allOwners()

				gerr = delayedNewJob(func(ctx context.Context) error {
					if err := reportStateOwnersToManager(ctx, subs, affected); err != nil {
						return err
					}

					return distributeDelayedWait()
				})

				return workerMergerMerge(local, subs, affected, gerr)
			}
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
		func(worker string, f dutil.InErrorFunc[dutil.InErrorFunc[uint64]]) error {
			return workerMergerUnmerge(worker, f(mergerUnmerge))
		},
		func(makeState makeStateFunc) error {
			defer func() {
				_ = wctxCancel(nil)

				closeSVMPool()
				closeFactPool()
			}()

			if err := smvExtractorWait(); err != nil {
				return wctxCancel(err)
			}

			if err := wait(makeState); err != nil {
				return wctxCancel(err)
			}

			delayedJobDone()

			// FIXME save states
			// FIXME save operations

			return delayedJobWait(nil)
		},
		nil
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
