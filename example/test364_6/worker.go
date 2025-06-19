package dop6

import (
	"context"
	"iter"
	"slices"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
)

type (
	newWorkerReportUnmergedToWorkersFunc = func(_ context.Context, worker string) (
		mergerUnmergedFunc,
		dutil.InErrorFunc[error],
		error,
	)
	reportStateOwnersFunc       = func(_ context.Context, subs, affected []string) error
	reportStatesFunc            = func(context.Context) (dutil.InErrorFunc[base.State], dutil.InErrorFunc[error], error)
	makeStateFunc               = func(stateKey string, _ base.StateValue, ops []util.Hash) (base.State, error)
	workerWorkerMergeOpFunc     = func(opindex uint64, facth util.Hash, subv SMV, addvs []SMV, affected []string) error
	workerWorkerStateOwnersFunc = func(subs, affected []string) error
	workerWaitFunc              = dutil.InErrorFunc[makeStateFunc]
	workerInsertOpsFunc         = func(
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
	reportUnmergedToManager mergerReportUnmergedFunc,
	reportUnmergedToWorker newWorkerReportUnmergedToWorkersFunc,
	reportStates reportStatesFunc,
	getFactStateOwners getFactStateOwnersFunc,
	newWorkerDistribute NewWorkerDistributeSMVsFunc,
	reportStateOwners reportStateOwnersFunc,
) (
	workerInsertOpsFunc,
	workerWorkerInsertOpsFunc,
	workerWorkerUnmergeOpsFunc,
	dutil.InErrorFunc[makeStateFunc],
	error,
) {
	wctx, wctxCancel := dutil.ContextWithCancelCause(ctx)

	isOpsWorker := slices.Index(opsWorkers, local) >= 0

	var makeSVM MergeSVMFunc
	var getSVM GetSVMFunc
	var closeSVMPool func()

	opsMapSize := min(maxOpsSize, 1<<9) //nolint:mnd

	switch g, s, c, err := newGetSVM(opsMapSize); {
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
	smvExtractorWait := func() error { return nil }

	if isOpsWorker {
		switch i, w, err := newSMVExtractor(wctx, maxOpsSize, getOperationFact, managerReportUnmerged); {
		case err != nil:
			return nil, nil, nil, nil, err
		default:
			smvExtractorInsert, smvExtractorWait = i, w
		}
	}

	var addLocalAffected func(uint64, []string, []string)
	var getLocalFactAffected func(opindex uint64) ([]string, bool)
	var closeLocalAffected func()

	switch a, g, c, err := newWorkerUnmergedAffectedOwners(local, opsMapSize); {
	case err != nil:
		return nil, nil, nil, nil, err
	default:
		addLocalAffected, getLocalFactAffected, closeLocalAffected = a, g, c
	}

	var workerReportUnmerged func(uint64, string, []string) error
	var workerReportUnmergedDone dutil.InErrorFunc[error]

	switch r, d, err := newWorkerReportUnmergedToWorkers(wctx, uint64(len(workers)), reportUnmergedToWorker); {
	case err != nil:
		return nil, nil, nil, nil, err
	default:
		workerReportUnmerged, workerReportUnmergedDone = r, d
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
			return func(opindex uint64, reason string) error {
					if err := managerReportUnmerged(opindex, reason); err != nil {
						return err
					}

					switch affected, found := getLocalFactAffected(opindex); {
					case !found:
						return errors.Errorf("affected not found, %d", opindex)
					default:
						return workerReportUnmerged(opindex, reason, affected)
					}
				},
				func() {
					workerReportUnmergedDone(nil)
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
			return delayedNewJob(func(ctx context.Context) error {
				return reportStateOwners(ctx, subs, affected)
			})
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
		reportStates,
		workerMergerWait,
		mergerWait,
	); {
	case err != nil:
		return nil, nil, nil, nil, err
	default:
		wait = w
	}

	var distribute workerDistributeSMVsFunc
	var distributeDone func()
	var distributeWait func(error) (func() ([]string, []string), dutil.ErrorFunc, error)

	switch d, do, w, err := newWorkerDistributeSMVs(
		wctx,
		workers,
		getFactStateOwners,
		newWorkerDistribute,
	); {
	case err != nil:
		return nil, nil, nil, nil, err
	default:
		distribute, distributeDone, distributeWait = d, do, w
	}

	return func(
			seq iter.Seq2[uint64, [2]util.Hash],
		) error {
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
					switch subs, affected, err := distribute(opindex, subv, addvs); {
					case err != nil:
						return err
					default:
						addLocalAffected(opindex, subs, affected)
					}

					return mergerMerge(opindex, subv, addvs)
				},
			); err != nil {
				return wctxCancel(err)
			}

			return nil
		},
		func(
			worker string,
			ops dutil.InErrorFunc[workerWorkerMergeOpFunc],
			owners dutil.InErrorFunc[workerWorkerStateOwnersFunc],
		) error {
			gerr := ops(func(opindex uint64, facth util.Hash, subv SMV, addvs []SMV, affected []string) error {
				_ = setFact(opindex, facth)
				addLocalAffected(opindex, []string{local}, affected)

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
				closeLocalAffected()
			}()

			if err := smvExtractorWait(); err != nil {
				return wctxCancel(err)
			}

			distributeDone()

			switch allOwners, distributeDelayedWait, err := distributeWait(nil); {
			case err != nil:
				return err
			default:
				subs, affected := allOwners()

				if slices.Index(subs, local) >= 0 {
					managerReportUnmergedDone()
				}

				if err := delayedNewJob(func(context.Context) error {
					return distributeDelayedWait()
				}); err != nil {
					return err
				}

				if isOpsWorker {
					if err := workerMergerMerge(local, subs, affected, nil); err != nil {
						return err
					}
				}
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

	return func(makeState makeStateFunc) error {
			defer func() {
				statePool.Close()
			}()

			if err := workerMergerWait(); err != nil {
				return err
			}

			return mergerWait(func(inserted uint64, collect collectOpsSeqFunc) error {
				report := func(base.State) error { return nil }
				closeReport := func(err error) error { return err }

				if inserted > 0 {
					switch r, c, err := newWorkerWaitReportStatesToManager(
						ctx, dutil.SafeUint64ToInt64(maxOpsSize), reportStatesToManager); {
					case err != nil:
						return err
					default:
						report, closeReport = r, c
					}
				}

				return closeReport(closeSVMs(ctx, getSVM, collect,
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

						return report(st)
					},
				))
			})
		},
		nil
}

func newWorkerWaitReportStatesToManager(
	ctx context.Context,
	semSize int64,
	f reportStatesFunc,
) (dutil.InErrorFunc[base.State], dutil.InErrorFunc[error], error) {
	var newJob dutil.JobWorkerNewJobFunc
	var jobDone func()
	var jobWait dutil.InErrorFunc[error]

	switch n, j, w, err := dutil.NewFuncJobWorker(ctx, semSize); {
	case err != nil:
		return nil, nil, err
	default:
		newJob, jobDone, jobWait = n, j, w
	}

	var report dutil.InErrorFunc[base.State]
	var closeReport dutil.InErrorFunc[error]

	switch r, c, err := f(ctx); {
	case err != nil:
		return nil, nil, err
	default:
		report, closeReport = r, c
	}

	return func(st base.State) error {
			return newJob(func(context.Context) error {
				return report(st)
			})
		},
		func(err error) error {
			jobDone()

			return closeReport(jobWait(err))
		},
		nil
}

func newWorkerUnmergedAffectedOwners(
	local string,
	mapSize uint64,
) (
	func(opindex uint64, subs, affected []string),
	func(opindex uint64) ([]string, bool),
	func(),
	error,
) {
	var m dutil.LockedMap[uint64 /* opindex */, []string /* affected */]

	switch i, err := dutil.NewLockedMap[uint64, []string](nil, mapSize); {
	case err != nil:
		return nil, nil, nil, err
	default:
		m = i
	}

	return func(opindex uint64, subs, affected []string) {
			switch {
			case slices.Index(subs, local) < 0:
			default:
				m.SetValue(opindex, affected)
			}
		},
		m.Value,
		m.Close,
		nil
}

func newWorkerReportUnmergedToWorkers(
	ctx context.Context,
	workersSize uint64,
	f newWorkerReportUnmergedToWorkersFunc,
) (
	func(uint64, string, []string) error,
	dutil.InErrorFunc[error],
	error,
) {
	var addWorker dutil.AppendSliceAddFunc[string]
	var doneWorker dutil.OutFunc[[]string]

	switch a, d, err := dutil.NewComparableUniqueAppendSlice[string](workersSize, 0); {
	case err != nil:
		return nil, nil, err
	default:
		addWorker, doneWorker = a, d
	}

	var sendNewJob dutil.JobWorkerNewJobFunc
	var sendJobDone func()
	var sendJobWait dutil.InErrorFunc[error]

	switch n, j, w, err := dutil.NewFuncJobWorker(ctx, dutil.SafeUint64ToInt64(workersSize)); {
	case err != nil:
		return nil, nil, err
	default:
		sendNewJob, sendJobDone, sendJobWait = n, j, w
	}

	m := dutil.NewSingleLockedMap[string, [3]any]()

	newf := func(worker string) (
		mergerUnmergedFunc,
		dutil.InErrorFunc[error],
		error,
	) {
		var send mergerUnmergedFunc
		var done dutil.InErrorFunc[error]

		_, err := m.Set(worker, func(p [3]any, found bool) ([3]any, error) {
			if found {
				if p[2] != nil {
					return [3]any{}, p[2].(error)
				}

				send, done = p[0].(mergerUnmergedFunc), p[1].(dutil.InErrorFunc[error])

				return [3]any{}, dutil.ErrLockedSetIgnore
			}

			var err error

			send, done, err = f(ctx, worker)

			return [3]any{send, done, err}, err
		})

		return send, done, err
	}

	return func(opindex uint64, reason string, affected []string) error {
			_, _ = addWorker(affected...)

			for worker := range slices.Values(affected) {
				if err := sendNewJob(func(context.Context) error {
					switch send, _, err := newf(worker); {
					case err != nil: // ignore error
					default:
						if err = send(opindex, reason); err != nil {
							_ = m.SetValue(worker, [3]any{nil, nil, err})
						}
					}

					return nil
				}); err != nil {
					return err
				}
			}

			return nil
		},
		func(err error) error {
			defer func() {
				m.Close()
			}()

			sendJobDone()

			if err := sendJobWait(nil); err != nil {
				return err
			}

			workers := doneWorker()
			if len(workers) < 1 {
				return err
			}

			var newJob dutil.JobWorkerNewJobFunc
			var jobDone func()
			var jobWait dutil.InErrorFunc[error]

			switch n, j, w, err := dutil.NewFuncJobWorker(ctx, int64(len(workers))); {
			case err != nil:
				return err
			default:
				newJob, jobDone, jobWait = n, j, w
			}

			for worker := range slices.Values(workers) {
				if err := newJob(func(context.Context) error {
					switch _, done, err := newf(worker); {
					case err != nil: // ignore error
					default:
						_ = done(err)
					}

					return nil
				}); err != nil {
					return err
				}
			}

			jobDone()

			return jobWait(err)
		},
		nil
}
