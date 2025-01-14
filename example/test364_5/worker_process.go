package main

import (
	"context"
	"maps"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
)

type (
	newStateFunc = func(string /* state key */, base.StateValue, []util.Hash /* op fact hashes */) (
		base.State,
		error,
	)
	workerDistributeWorkerStateMergeValuesFunc     = func(worker string, opindex uint64, fact util.Hash, _ []StateMergeValue) error
	newWorkerProcessDistributeStateMergeValuesFunc func(_ context.Context, worker string) (
		workerProcessInsertStateMergeValuesFunc,
		dutil.In2ErrorFunc[[]string /* subs */, []string /* affected */],
		error,
	)
	workerProcessInitialInsertFunc = func(
		getStateOwnersFunc,
		newWorkerProcessDistributeStateMergeValuesFunc,
		getOperationFunc,
		dutil.InErrorFunc[opindexOpHashesFunc],
		dutil.In2ErrorFunc[[]string /* owners */, map[uint64]string /* reasons */],
	) error
	workerProcessInitialInsertFunc2 = func(
		getStateOwnersFunc,
		newWorkerProcessDistributeStateMergeValuesFunc,
		getOperationFunc,
		dutil.InErrorFunc[opindexOpHashesFunc],
		dutil.In2ErrorFunc[uint64, Operation],
		dutil.In2ErrorFunc[[]string /* owners */, map[uint64]string /* reasons */],
	) error
	workerProcessInsertStateMergeValuesFunc     = func(opindex uint64, fact util.Hash, _ []StateMergeValue) error
	workerProcessInsertStateMergeValuesCallback = func(
		workerProcessInsertStateMergeValuesFunc,
		dutil.In2ErrorFunc[[]string /* sub */, []string /* affected */],
	) error
	workerProcessWorkerInsertStateMergeValuesFunc = func(worker string) (
		workerProcessInsertStateMergeValuesFunc,
		dutil.In2ErrorFunc[[]string /* sub */, []string /* affected */],
		error,
	)
	workerProcessReportResult func(
		_ context.Context,
		reasons map[uint64]string,
		statesSize uint64,
		_ dutil.InErrorFunc[dutil.InErrorFunc[base.State]],
	) error
	workerProcessTraverseOperationsFunc dutil.InErrorFunc[func(uint64, Operation) (bool, error)]
	workerProcessWaitFunc               = func(
		dutil.In3ErrorFunc[context.Context, string /* worker */, []uint64 /* unmerged */],
		workerProcessReportResult,
	) (
		workerProcessTraverseOperationsFunc,
		error,
	)
)

func newFuncWorkerProcess(
	ctx context.Context,
	local string,
	newJobWorker dutil.NewJobWorkerFunc,
	getState getStateFunc,
	newState newStateFunc,
	maxExtractStateKeyCacheSize int,
	workers, opsWorkers []string,
	opsShare, statesShare uint64,
	maxOpsSize uint64,
) (
	workerProcessInitialInsertFunc,
	workerProcessWorkerInsertStateMergeValuesFunc,
	workerUnmergedFunc,
	workerProcessWaitFunc,
	error,
) {
	log := dutil.NewModuleLoggerFromContext(ctx, "worker-process", nil)

	log.Debug().
		Uint64("ops_size", maxOpsSize).
		Dict("workers", zerolog.Dict().
			Strs("all", workers).
			Strs("ops", opsWorkers),
		).
		Dict("share", zerolog.Dict().
			Uint64("ops", opsShare).
			Uint64("states", statesShare),
		).
		Msg("new")

	extractStateMergeValueKeys, extractStateKeyClose := newFuncExtractStateMergeValueKeys(
		maxExtractStateKeyCacheSize, int(maxOpsSize*2))

	var getStateValueMergers mergerGetStateValueMergersFunc
	var traverseStateValueMergersf func() (uint64, traverseStateValueMergersFunc)

	switch g, t, err := newFuncStateValueMergers(
		maxOpsSize*2, // NOTE approximate states size
		getState,
		extractStateMergeValueKeys,
	); {
	case err != nil:
		return nil, nil, nil, nil, err
	default:
		getStateValueMergers, traverseStateValueMergersf = g, t
	}

	var initialInsert workerProcessInitialInsertFunc2
	var workerInsertStateValues workerProcessWorkerInsertStateMergeValuesFunc
	var unmerge workerUnmergedFunc
	var wait dutil.In2ErrorFunc[
		dutil.In3ErrorFunc[[]uint64, []string, []string],
		dutil.In2ErrorFunc[map[uint64]string, traverseStateOpsFunc],
	]

	switch i, s, u, w, err := newFuncWorkerProcessStateValueMerger(
		ctx,
		local,
		newJobWorker,
		getState,
		newState,
		workers,
		opsWorkers,
		opsShare, statesShare, maxOpsSize,
		getStateValueMergers,
	); {
	case err != nil:
		return nil, nil, nil, nil, err
	default:
		initialInsert, workerInsertStateValues, unmerge, wait = i, s, u, w
	}

	var savedOps util.LockedMap[uint64, Operation]

	switch i, err := util.NewLockedMap[uint64, Operation](maxOpsSize, nil); {
	case err != nil:
		return nil, nil, nil, nil, err
	default:
		savedOps = i
	}

	unusedOps := new(atomic.Value)

	return func(
			getStateOwners getStateOwnersFunc,
			distribute newWorkerProcessDistributeStateMergeValuesFunc,
			getOperation getOperationFunc,
			insertOp dutil.InErrorFunc[opindexOpHashesFunc],
			reportDone dutil.In2ErrorFunc[[]string, map[uint64]string],
		) error {
			return initialInsert(
				getStateOwners,
				distribute,
				getOperation,
				insertOp,
				func(opindex uint64, op Operation) error {
					_ = savedOps.SetValue(opindex, op)

					return nil
				},
				func(owners []string, reasons map[uint64]string) error {
					unusedOps.Store(reasons)

					if reportDone != nil {
						return reportDone(owners, reasons)
					}

					return nil
				},
			)
		},
		func(worker string) (
			workerProcessInsertStateMergeValuesFunc,
			dutil.In2ErrorFunc[[]string, []string],
			error,
		) {
			switch {
			case worker == managerName:
			case slices.Index(opsWorkers, worker) >= 0:
			default:
				return nil, nil, util.ErrInvalid.Errorf("unknown ops worker, %q", worker)
			}

			return workerInsertStateValues(worker)
		},
		unmerge,
		func(
			broacastUnmerged dutil.In3ErrorFunc[context.Context, string, []uint64],
			reportResult workerProcessReportResult,
		) (
			workerProcessTraverseOperationsFunc,
			error,
		) {
			defer extractStateKeyClose()

			if err := wait(
				func(unmerged []uint64, subws, affected []string) error {
					log.Debug().
						Dict("owners", zerolog.Dict().
							Strs("sub", subws).
							Strs("affected", affected),
						).Msg("state owners")

					if slices.Index(opsWorkers, local) < 0 {
						return nil
					}

					return workersBroadcastUnmerged(ctx, local, affected, unmerged, broacastUnmerged)
				},
				func(reasons map[uint64]string, traverseStateOps traverseStateOpsFunc) error {
					reasons = mergeUnusedReasons(reasons, unusedOps)

					statesSize, traverseStateValueMergers := traverseStateValueMergersf()

					return reportResult(ctx, reasons, statesSize, func(f dutil.InErrorFunc[base.State]) error {
						return workerProcessCloseStateValueMergers(
							ctx,
							newState,
							traverseStateValueMergers,
							traverseStateOps,
							func(st base.State) error { return f(st) },
						)
					})
				},
			); err != nil {
				log.Error().Err(err).Msg("wait done")

				return nil, err
			}

			log.Debug().Msg("wait done")

			return func(f func(uint64, Operation) (bool, error)) (gerr error) {
					defer savedOps.Close()

					if f == nil {
						return nil
					}

					_ = savedOps.Traverse(func(opindex uint64, op Operation) bool {
						switch ok, err := f(opindex, op); {
						case !ok:
							return false
						case err != nil:
							gerr = err

							return false
						default:
							return true
						}
					})

					return gerr
				},
				nil
		},
		nil
}

func newFuncWorkerProcessStateValueMerger(
	ctx context.Context,
	local string,
	newJobWorker dutil.NewJobWorkerFunc,
	getState getStateFunc,
	newState newStateFunc,
	workers []string,
	opsWorkers []string,
	opsShare, statesShare, maxOpsSize uint64,
	getStateValueMergers mergerGetStateValueMergersFunc,
) (
	workerProcessInitialInsertFunc2,
	workerProcessWorkerInsertStateMergeValuesFunc,
	workerUnmergedFunc,
	dutil.In2ErrorFunc[
		dutil.In3ErrorFunc[[]uint64, []string, []string],
		dutil.In2ErrorFunc[map[uint64]string, traverseStateOpsFunc],
	],
	error,
) {
	var mergeStateOp mergeStateOpFunc
	var unmergeStateOp unmergeStateOpFunc
	var traverseStateOps traverseStateOpsFunc

	switch m, u, t, err := newFuncSetStateOps(maxOpsSize); {
	case err != nil:
		return nil, nil, nil, nil, err
	default:
		mergeStateOp, unmergeStateOp, traverseStateOps = m, u, t
	}

	var workerInsertStateValues workerMergerInsertStateMergeValuesFunc
	var workerUnmerged workerUnmergedFunc
	var workerMergerWait workerMergerWaitFunc

	switch i, u, w, err := newFuncWorkerMerger(
		ctx,
		local,
		newJobWorker,
		getState,
		getStateValueMergers,
		uint64(len(workers)), maxOpsSize, maxOpsSize*2, // NOTE approximate states size
		opsWorkers, opsShare, statesShare,
	); {
	case err != nil:
		return nil, nil, nil, nil, err
	default:
		workerInsertStateValues, workerUnmerged, workerMergerWait = i, u, w
	}

	var localExtractedStateOwners localExtractedStateOwnersFunc
	var workerStateOwnersSet workerStateOwnersSetFunc
	var doneStateOwnersSet workerStateOwnersDoneFunc

	switch l, w, t, err := newFuncWorkerStateOwners(ctx, local, maxOpsSize); {
	case err != nil:
		return nil, nil, nil, nil, err
	default:
		localExtractedStateOwners, workerStateOwnersSet, doneStateOwnersSet = l, w, t
	}

	var initialInsert workerProcessInitialInsertFunc2

	switch i, err := newFuncWorkerProcessInitialInsert(
		ctx,
		local,
		newJobWorker,
		workers,
		maxOpsSize,
		localExtractedStateOwners,
		workerInsertStateValues,
		mergeStateOp,
	); {
	case err != nil:
		return nil, nil, nil, nil, err
	default:
		initialInsert = i
	}

	return func(
			getStateOwners getStateOwnersFunc,
			distribute newWorkerProcessDistributeStateMergeValuesFunc,
			getOperation getOperationFunc,
			insertOp dutil.InErrorFunc[opindexOpHashesFunc],
			saveOperation dutil.In2ErrorFunc[uint64, Operation],
			reportDone dutil.In2ErrorFunc[[]string, map[uint64]string],
		) error {
			return initialInsert(
				getStateOwners,
				distribute,
				getOperation,
				insertOp,
				saveOperation,
				reportDone,
			)
		},
		func(worker string) (
			workerProcessInsertStateMergeValuesFunc,
			dutil.In2ErrorFunc[[]string, []string],
			error,
		) {
			var insert insertStateMergeValuesFunc
			var done dutil.InErrorFunc[error]

			switch i, d, err := workerInsertStateValues(worker); {
			case err != nil:
				return nil, nil, err
			case i == nil:
				return nil, nil, nil
			default:
				insert, done = i, d
			}

			return func(opindex uint64, fact util.Hash, values []StateMergeValue) error {
					if err := mergeStateOpFromValues(mergeStateOp, opindex, fact, values); err != nil {
						return err
					}

					return insert(opindex, values)
				},
				func(subws, affected []string) error {
					return done(workerStateOwnersSet(worker, subws, affected))
				},
				nil
		},
		workerUnmerged,
		func(
			localDone dutil.In3ErrorFunc[[]uint64, []string, []string],
			f dutil.In2ErrorFunc[map[uint64]string, traverseStateOpsFunc],
		) error {
			defer func() {
				_, _, _ = doneStateOwnersSet()
			}()

			switch reasons, err := workerMergerWait(
				func(unmerged []uint64) ([]string, error) {
					_ = unmergeStateOp(unmerged)

					switch subws, affected, err := doneStateOwnersSet(); {
					case err != nil:
						return nil, err
					default:
						return subws, localDone(unmerged, subws, affected)
					}
				},
			); {
			case err != nil:
				return err
			default:
				_ = unmergeStateOp(slices.Collect(maps.Keys(reasons)))

				return f(reasons, traverseStateOps)
			}
		},
		nil
}

func newFuncWorkerProcessInitialInsert(
	ctx context.Context,
	local string,
	newJobWorker dutil.NewJobWorkerFunc,
	workers []string,
	maxOpsSize uint64,
	localExtractedStateOwners localExtractedStateOwnersFunc,
	workerInsertStateValues workerMergerInsertStateMergeValuesFunc,
	mergeStateOp mergeStateOpFunc,
) (
	workerProcessInitialInsertFunc2,
	error,
) {
	var insertOps stateMergeValueExtractorInsertFunc
	var extractorWait dutil.InErrorFunc[dutil.InErrorFunc[map[uint64]string]]

	switch i, e, err := newFuncStateMergeValueExtractor(ctx, newJobWorker, maxOpsSize); {
	case err != nil:
		return nil, err
	default:
		insertOps, extractorWait = i, e
	}

	return func(
			getStateOwners getStateOwnersFunc,
			distribute newWorkerProcessDistributeStateMergeValuesFunc,
			getOperation getOperationFunc,
			insertOp dutil.InErrorFunc[opindexOpHashesFunc],
			saveOperation dutil.In2ErrorFunc[uint64, Operation],
			reportDone dutil.In2ErrorFunc[[]string, map[uint64]string],
		) error {
			var extractStateOwnersStateMergeValues extractStateOwnersStateMergeValuesFunc
			var doneExtractStateOwnersStateMergeValues dutil.InErrorFunc[workerStateOwnersSetFunc]
			var localOwners dutil.OutFunc[[]string]

			switch e, s, l, err := localExtractedStateOwners(getStateOwners); {
			case err != nil:
				return err
			default:
				defer func() {
					_ = s(nil)
				}()

				extractStateOwnersStateMergeValues, doneExtractStateOwnersStateMergeValues, localOwners = e, s, l
			}

			var distributeWorkerStateMergeValues workerDistributeWorkerStateMergeValuesFunc
			var distributeWorkerStateMergeValuesDone workerStateOwnersSetFunc

			switch i, o, c, err := workersDistributeStateMergeValues(ctx, newJobWorker, local, workers, distribute); {
			case err != nil:
				return err
			default:
				defer func() {
					_ = c()
				}()

				distributeWorkerStateMergeValues, distributeWorkerStateMergeValuesDone = i, o
			}

			var insert insertStateMergeValuesFunc
			var insertDone dutil.InErrorFunc[error]

			switch i, d, err := workerInsertStateValues(local); {
			case err != nil:
				return err
			default:
				defer d(nil)

				insert, insertDone = i, d
			}

			extracted := newFuncorkerProcessExtracted(
				ctx,
				local,
				saveOperation,
				extractStateOwnersStateMergeValues,
				distributeWorkerStateMergeValues,
				mergeStateOp,
				insert,
			)

			if err := insertOps(getOperation, insertOp, extracted); err != nil {
				return err
			}

			return extractorWait(func(reasons map[uint64]string) error {
				if err := insertDone(nil); err != nil {
					return err
				}

				if err := reportDone(localOwners(), reasons); err != nil {
					return err
				}

				return doneExtractStateOwnersStateMergeValues(distributeWorkerStateMergeValuesDone)
			})
		},
		nil
}

func newFuncorkerProcessExtracted(
	ctx context.Context,
	local string,
	saveOperation dutil.In2ErrorFunc[uint64, Operation],
	extractStateOwners extractStateOwnersStateMergeValuesFunc,
	distribute workerDistributeWorkerStateMergeValuesFunc,
	mergeStateOp mergeStateOpFunc,
	insertToMerger insertStateMergeValuesFunc,
) stateMergeValueExtractorExtractedFunc {
	log := dutil.LoadContextValueLogger(ctx)

	return func(opindex uint64, op Operation, subv StateMergeValue, addvs []StateMergeValue) error {
		log.Trace().
			Dict("operation", zerolog.Dict().
				Uint64("opindex", opindex).
				Stringer("hash", op.Hash()).
				Stringer("fact", op.Fact().Hash()),
			).
			Msg("insert operation")

		if err := saveOperation(opindex, op); err != nil {
			return err
		}

		size := len(addvs)
		if subv != nil {
			size = len(addvs) + 1
		}

		stateKeys := make([]string, size)
		stateMergeValues := make([]StateMergeValue, size)

		add := func(i int, value StateMergeValue) {
			stateKeys[i] = value.Key()
			stateMergeValues[i] = value
		}

		for i, value := range slices.All(addvs) {
			add(i, value)
		}

		if subv != nil {
			add(size-1, subv)
		}

		fact := op.Fact().Hash()

		if err := extractStateOwners(opindex, subv, addvs)(func(worker string, values []StateMergeValue) error {
			if worker == local {
				return insertToMerger(opindex, values)
			}

			return distribute(worker, opindex, fact, values)
		}); err != nil {
			return err
		}

		_ = mergeStateOp(opindex, fact, stateKeys)

		return nil
	}
}

func workersBroadcastUnmerged(
	ctx context.Context,
	local string,
	affected []string,
	unmerged []uint64,
	f dutil.In3ErrorFunc[context.Context, string, []uint64],
) error {
	if len(affected) < 1 {
		return nil
	}

	log := dutil.LoadContextValueLogger(ctx)

	var newJob dutil.JobWorkerNewJobFunc
	var jobDone func()
	var jobWait dutil.InErrorFunc[error]

	it := dutil.FilterSlice(slices.Values(affected), func(worker string) bool {
		return worker != local
	})

	var once sync.Once

	for worker := range it {
		var gerr error

		once.Do(func() {
			switch n, d, w, err := dutil.NewFuncJobWorker(ctx, uint64(len(affected))); {
			case err != nil:
				gerr = err
			default:
				newJob, jobDone, jobWait = n, d, w
			}
		})
		if gerr != nil {
			return gerr
		}

		if err := newJob(func(ctx context.Context) error {
			if err := f(ctx, worker, unmerged); err != nil {
				log.Debug().Err(err).Msg("failed to broadcast unmerged")
			}

			return nil
		}); err != nil {
			return jobWait(err)
		}
	}

	if jobDone == nil {
		return nil
	}

	jobDone()

	return jobWait(nil)
}

func mergeStateOpFromValues(
	mergeStateOp mergeStateOpFunc,
	opindex uint64,
	fact util.Hash,
	values []StateMergeValue,
) error {
	var stateKeys []string

	stateKeys = make([]string, len(values))

	for i, value := range slices.All(values) {
		stateKeys[i] = value.Key()
	}

	_ = mergeStateOp(opindex, fact, stateKeys)

	return nil
}

func workerProcessCloseStateValueMergers(
	ctx context.Context,
	newState newStateFunc,
	traverseStateValueMergers traverseStateValueMergersFunc,
	traverseStateOps traverseStateOpsFunc,
	h dutil.InErrorFunc[base.State],
) error {
	return traverseStateOps(func(size int, getStateOps getStateOpsFunc) error {
		if size < 1 {
			return nil
		}

		var newJob dutil.JobWorkerNewJobFunc
		var jobDone func()
		var jobWait dutil.InErrorFunc[error]

		switch n, d, w, err := dutil.NewFuncJobWorker(ctx, uint64(size)); {
		case err != nil:
			return err
		default:
			newJob, jobDone, jobWait = n, d, w
		}

		if err := traverseStateValueMergers(func(stateKey string, merger StateValueMerger) (bool, error) {
			return true, newJob(func(context.Context) error {
				var facts []util.Hash

				switch i, err := getStateOps(stateKey); {
				case err != nil:
					return err
				case len(i) < 1:
					return nil
				default:
					facts = i
				}

				var stv base.StateValue

				switch v, err := merger.Close(); {
				case err != nil:
					return err
				case v == nil:
					return nil
				default:
					stv = v
				}

				switch st, err := newState(stateKey, stv, facts); {
				case err != nil:
					return err
				case st == nil:
					return errors.Errorf("nil state")
				default:
					return h(st)
				}
			})
		}); err != nil {
			return jobWait(err)
		}

		jobDone()

		return jobWait(nil)
	})
}

func workersDistributeStateMergeValues(
	ctx context.Context,
	newJobWorker dutil.NewJobWorkerFunc,
	local string,
	workers []string,
	newf newWorkerProcessDistributeStateMergeValuesFunc,
) (
	workerDistributeWorkerStateMergeValuesFunc,
	workerStateOwnersSetFunc,
	dutil.ErrorFunc,
	error,
) {
	var pool util.LockedMap[string, [2]interface{}]

	switch i, err := util.NewLockedMap[string, [2]interface{}](uint64(len(workers)), nil); {
	case err != nil:
		return nil, nil, nil, err
	default:
		pool = i
	}

	return func(worker string, opindex uint64, fact util.Hash, values []StateMergeValue) error {
			var insert dutil.In3ErrorFunc[uint64, util.Hash, []StateMergeValue]

			if _, _, err := pool.Set(worker, func(i [2]interface{}, found bool) (v [2]interface{}, _ error) {
				if found {
					if i[0] != nil {
						insert = i[0].(workerProcessInsertStateMergeValuesFunc)
					}

					return v, util.ErrLockedSetIgnore
				}

				switch s, o, err := newf(ctx, worker); {
				case err != nil:
					return v, err
				default:
					insert = s

					return [2]interface{}{s, o}, nil
				}
			}); err != nil {
				return err
			}

			if insert != nil {
				if err := insert(opindex, fact, values); err != nil { // NOTE ignore error
					_ = pool.SetValue(worker, [2]interface{}{nil, nil})
				}
			}

			return nil
		},
		func(worker string, subws, affected []string) error {
			var sendOwners dutil.In2ErrorFunc[[]string, []string]

			if _, _, err := pool.Set(
				worker,
				func(i [2]interface{}, found bool) (v [2]interface{}, _ error) {
					if found {
						if i[1] != nil {
							sendOwners = i[1].(dutil.In2ErrorFunc[[]string, []string])
						}

						return v, nil
					}

					switch _, o, err := newf(ctx, worker); {
					case err != nil:
						return v, err
					default:
						sendOwners = o

						return v, nil
					}
				},
			); err != nil {
				return err
			}

			if sendOwners != nil {
				_ = sendOwners(subws, affected) // NOTE ignore error
			}

			return nil
		},
		func() error {
			defer pool.Close()

			var newJob dutil.JobWorkerNewJobFunc
			var jobDone func()
			var jobWait dutil.InErrorFunc[error]

			switch n, d, w, err := newJobWorker(ctx, uint64(len(workers))); {
			case err != nil:
				return err
			default:
				newJob, jobDone, jobWait = n, d, w
			}

		end:
			for worker := range slices.Values(workers) {
				if worker == local {
					continue end
				}

				var sendOwners dutil.In2ErrorFunc[[]string, []string]

				switch i, found := pool.Value(worker); {
				case !found:
					switch _, o, err := newf(ctx, worker); {
					case err != nil:
						continue end
					default:
						sendOwners = o
					}
				case i[1] == nil:
					continue end
				default:
					sendOwners = i[1].(dutil.In2ErrorFunc[[]string, []string])
				}

				if sendOwners == nil {
					continue end
				}

				if err := newJob(func(context.Context) error {
					_ = sendOwners(nil, nil) // NOTE ignore error

					return nil
				}); err != nil {
					return err
				}
			}

			jobDone()

			return jobWait(nil)
		},
		nil
}

func mergeUnusedReasons(reasons map[uint64]string, unusedOps *atomic.Value) map[uint64]string {
	var u map[uint64]string

	if i := unusedOps.Load(); i != nil {
		u = maps.Clone(i.(map[uint64]string))
	}

	switch {
	case len(u) < 1:
		return reasons
	case len(reasons) < 1:
		return u
	}

	for opindex, reason := range maps.All(u) {
		if _, found := reasons[opindex]; !found {
			reasons[opindex] = reason
		}
	}

	return reasons
}
