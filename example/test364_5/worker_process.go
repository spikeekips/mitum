package main

import (
	"context"
	"maps"
	"slices"
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
	workerDistributeWorkerStateMergeValuesFunc  = func(worker string, opindex uint64, fact util.Hash, _ []StateMergeValue) error
	workerProcessDistributeStateMergeValuesFunc = func(_ context.Context, local string, workers []string) (
		workerDistributeWorkerStateMergeValuesFunc,
		workerStateOwnersSetFunc,
		dutil.ErrorFunc,
		error,
	)
	workerProcessInitialInsertFunc = func(
		getStateOwnersFunc,
		workerProcessDistributeStateMergeValuesFunc,
		getOperationFunc,
		dutil.InErrorFunc[opindexOpHashesFunc],
	) error
	workerProcessInitialInsertFunc2 = func(
		getStateOwnersFunc,
		workerProcessDistributeStateMergeValuesFunc,
		getOperationFunc,
		dutil.InErrorFunc[opindexOpHashesFunc],
		dutil.In2ErrorFunc[uint64, Operation],
		dutil.InErrorFunc[map[uint64]string],
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
		dutil.In2ErrorFunc[context.Context, []string /* all state owners */],
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

	var getStateMergers mergerGetStateMergersFunc
	var traverseStateMergersf func() (uint64, traverseStateMergersFunc)

	switch g, t, err := newFuncStateMergers(
		maxOpsSize*2, // NOTE approximate states size
		getState,
		extractStateMergeValueKeys,
	); {
	case err != nil:
		return nil, nil, nil, nil, err
	default:
		getStateMergers, traverseStateMergersf = g, t
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
		getStateMergers,
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

	var unusedOps atomic.Value

	return func(
			getStateOwners getStateOwnersFunc,
			distribute workerProcessDistributeStateMergeValuesFunc,
			getOperation getOperationFunc,
			insertOp dutil.InErrorFunc[opindexOpHashesFunc],
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
				func(reasons map[uint64]string) error {
					unusedOps.Store(reasons)

					return nil
				},
			)
		},
		workerInsertStateValues,
		unmerge,
		func(
			broacastUnmerged dutil.In3ErrorFunc[context.Context, string, []uint64],
			reportStateOwners dutil.In2ErrorFunc[context.Context, []string],
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

					return dutil.FuncsJobWorker(
						ctx,
						func(ctx context.Context) error {
							s := slices.Concat(subws, affected)
							slices.Sort(s)
							s = slices.Compact(s)

							defer clear(s)

							return reportStateOwners(ctx, s)
						},
						func(ctx context.Context) error {
							return workersBroadcastUnmerged(ctx, local, affected, unmerged, broacastUnmerged)
						},
					)
				},
				func(reasons map[uint64]string, traverseStateOps traverseStateOpsFunc) error {
					reasons = mergeUnusedReasons(reasons, unusedOps)
					defer func() {
						dutil.ClearMap(reasons)
					}()

					statesSize, traverseStateMergers := traverseStateMergersf()

					return reportResult(ctx, reasons, statesSize, func(f dutil.InErrorFunc[base.State]) error {
						return workerProcessCloseStateMergers(
							ctx,
							newState,
							traverseStateMergers,
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
	getStateMergers mergerGetStateMergersFunc,
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
		getStateMergers,
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
	var doneStateOwnersSet dutil.InErrorFunc[insertStateOwnersSetFunc]

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
			distribute workerProcessDistributeStateMergeValuesFunc,
			getOperation getOperationFunc,
			insertOp dutil.InErrorFunc[opindexOpHashesFunc],
			saveOperation dutil.In2ErrorFunc[uint64, Operation],
			reportReasons dutil.InErrorFunc[map[uint64]string],
		) error {
			return initialInsert(
				getStateOwners,
				distribute,
				getOperation,
				insertOp,
				saveOperation,
				reportReasons,
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
			default:
				insert = i
				done = d
			}

			return func(opindex uint64, fact util.Hash, values []StateMergeValue) error {
					if err := mergeStateOpFromValues(mergeStateOp, opindex, fact, nil, values); err != nil {
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
				_ = doneStateOwnersSet(nil)
			}()

			return workerMergerWait(
				func(unmerged []uint64) ([]string, error) {
					_ = unmergeStateOp(unmerged)

					var subOwners []string

					return subOwners, doneStateOwnersSet(func(subws, affected []string) error {
						subOwners = slices.Clone(subws)

						return localDone(unmerged, subws, affected)
					})
				},
				func(reasons map[uint64]string) error {
					_ = unmergeStateOp(slices.Collect(maps.Keys(reasons)))

					return f(reasons, traverseStateOps)
				},
			)
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
			distribute workerProcessDistributeStateMergeValuesFunc,
			getOperation getOperationFunc,
			insertOp dutil.InErrorFunc[opindexOpHashesFunc],
			saveOperation dutil.In2ErrorFunc[uint64, Operation],
			reportReasons dutil.InErrorFunc[map[uint64]string],
		) error {
			var extractStateOwnersStateMergeValues extractStateOwnersStateMergeValuesFunc
			var doneExtractStateOwnersStateMergeValues dutil.InErrorFunc[workerStateOwnersSetFunc]

			switch e, s, err := localExtractedStateOwners(getStateOwners); {
			case err != nil:
				return err
			default:
				defer func() {
					_ = s(nil)
				}()

				extractStateOwnersStateMergeValues, doneExtractStateOwnersStateMergeValues = e, s
			}

			var distributeWorkerStateMergeValues workerDistributeWorkerStateMergeValuesFunc
			var doneDistributeWorkerStateMergevalues workerStateOwnersSetFunc

			switch i, o, c, err := distribute(ctx, local, workers); {
			case err != nil:
				return err
			default:
				defer func() {
					_ = c()
				}()

				distributeWorkerStateMergeValues, doneDistributeWorkerStateMergevalues = i, o
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

				if len(reasons) > 0 && reportReasons != nil {
					if err := reportReasons(reasons); err != nil {
						return err
					}
				}

				return doneExtractStateOwnersStateMergeValues(doneDistributeWorkerStateMergevalues)
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

		_ = dutil.TraverseSlice(addvs, func(i int, value StateMergeValue) (bool, error) {
			add(i, value)

			return true, nil
		})

		if subv != nil {
			add(len(addvs), subv)
		}

		if err := insertToMerger(opindex, stateMergeValues); err != nil {
			return err
		}

		fact := op.Fact().Hash()

		if err := extractStateOwners(opindex, subv, addvs)(func(worker string, values []StateMergeValue) error {
			if worker == local {
				return nil
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
	log := dutil.LoadContextValueLogger(ctx)

	filtered := util.FilterSlice(affected, func(a string) bool {
		return a == local
	})

	if len(filtered) < 1 {
		return nil
	}

	var newJob dutil.JobWorkerNewJobFunc
	var jobDone func()
	var jobWait dutil.InErrorFunc[error]

	switch n, d, w, err := dutil.NewFuncJobWorker(ctx, uint64(len(filtered))); {
	case err != nil:
		return err
	default:
		newJob, jobDone, jobWait = n, d, w
	}

	go func() {
		defer jobDone()

		_ = dutil.TraverseSlice(filtered, func(_ int, worker string) (bool, error) {
			return true, newJob(func(ctx context.Context) error {
				if err := f(ctx, worker, unmerged); err != nil {
					log.Debug().Err(err).Msg("failed to broadcast unmerged")
				}

				return nil
			})
		})
	}()

	return jobWait(nil)
}

func mergeStateOpFromValues(
	mergeStateOp mergeStateOpFunc,
	opindex uint64,
	fact util.Hash,
	value StateMergeValue,
	values []StateMergeValue,
) error {
	var (
		add  dutil.AppendSliceAddFunc[string]
		done dutil.AppendSliceDoneFunc[string]
	)

	if err := dutil.RAppendSlice[string](
		uint64(len(values)+1), 0, &add, &done); err != nil {
		return err
	}

	defer done(nil)

	if value != nil {
		_, _ = add(value.Key())
	}

	_ = dutil.TraverseSlice(values, func(_ int, value StateMergeValue) (bool, error) {
		add(value.Key())

		return true, nil
	})

	return done(func(s []string) error {
		_ = mergeStateOp(opindex, fact, s)

		return nil
	})
}

func workerProcessCloseStateMergers(
	ctx context.Context,
	newState newStateFunc,
	traverseStateMergers traverseStateMergersFunc,
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
			defer func() {
				_ = jobWait(context.Canceled)
			}()

			newJob, jobDone, jobWait = n, d, w
		}

		if err := traverseStateMergers(func(stateKey string, merger StateMerger) (bool, error) {
			err := newJob(func(context.Context) error {
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

			return err == nil, err
		}); err != nil {
			return err
		}

		jobDone()

		return jobWait(nil)
	})
}

func newFuncDistributeStateMergeValues(
	newFuncs func(_ context.Context, worker string) (
		workerProcessInsertStateMergeValuesFunc,
		dutil.In2ErrorFunc[[]string /* subs */, []string /* affected */],
		error,
	),
) workerProcessDistributeStateMergeValuesFunc {
	return func(ctx context.Context, local string, workers []string) (
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

					switch s, o, err := newFuncs(ctx, worker); {
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
					return insert(opindex, fact, values)
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

						switch _, o, err := newFuncs(ctx, worker); {
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
					return sendOwners(subws, affected)
				}

				return nil
			},
			func() error {
				defer pool.Close()

				var newJob dutil.JobWorkerNewJobFunc
				var jobDone func()
				var jobWait dutil.InErrorFunc[error]

				switch n, d, w, err := dutil.NewFuncJobWorker(ctx, uint64(len(workers))); {
				case err != nil:
					return err
				default:
					defer func() {
						_ = jobWait(context.Canceled)
					}()

					newJob, jobDone, jobWait = n, d, w
				}

				if err := dutil.TraverseSlice(workers, func(_ int, worker string) (bool, error) {
					if worker == local {
						return true, nil
					}

					var sendOwners dutil.In2ErrorFunc[[]string, []string]

					switch i, found := pool.Value(worker); {
					case !found:
						switch _, o, err := newFuncs(ctx, worker); {
						case err != nil:
							return true, nil
						default:
							sendOwners = o
						}
					case i[1] == nil:
						return true, nil
					default:
						sendOwners = i[1].(dutil.In2ErrorFunc[[]string, []string])
					}

					return true, newJob(func(context.Context) error {
						_ = sendOwners(nil, nil)

						return nil
					})
				}); err != nil {
					return err
				}

				jobDone()

				return jobWait(nil)
			},
			nil
	}
}

func mergeUnusedReasons(reasons map[uint64]string, unusedOps atomic.Value) map[uint64]string {
	var u map[uint64]string

	if i := unusedOps.Load(); i != nil {
		u = i.(map[uint64]string)
	}

	if len(u) < 1 {
		defer dutil.ClearMap(u)

		return reasons
	}

	switch {
	case len(reasons) < 1:
		reasons = u
	default:
		defer dutil.ClearMap(u)

		_ = dutil.TraverseMap(u, func(opindex uint64, reason string) (bool, error) {
			if _, found := reasons[opindex]; !found {
				reasons[opindex] = reason
			}

			return true, nil
		})
	}

	return reasons
}
