package main

import (
	"context"
	"slices"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
)

type (
	newStateFunc = func(string /* state key */, base.StateValue, []util.Hash /* op fact hashes */) (
		base.State,
		error,
	)
	workerDistributeStateMergeValuesFunc = func(workID string, workerSize uint64) (
		workerStateMergeValuesFunc,
		workerStateOwnersSetFunc,
		func(),
		error,
	)
	workerProcessInitialInsertFunc = func(
		getStateOwnersFunc,
		workerDistributeStateMergeValuesFunc,
		getOperationFunc,
		inErrorFunc[opindexHashFunc],
		in2ErrorFunc[uint64, Operation],
	) error
	workerProcessInsertStateMergeValuesFunc     = func(opindex uint64, fact util.Hash, _ []StateMergeValue) error
	workerProcessInsertStateMergeValuesCallback = func(
		workerProcessInsertStateMergeValuesFunc,
		in2ErrorFunc[[]string /* sub */, []string /* affected */],
	) error
	workerProcessWorkerInsertStateMergeValuesFunc = func(
		worker string,
		_ workerProcessInsertStateMergeValuesCallback,
	) error
	workerProcessReportStates func(
		context.Context,
		[]uint64, /* unmerged */
		map[uint64]string, /* reasons */
		inErrorFunc[inErrorFunc[base.State]],
	) error
	workerProcessWaitFunc = func(
		in2Func[uint64 /*opindex */, Operation, bool],
		in3ErrorFunc[context.Context, string /* worker */, []uint64 /* unmerged */],
		in2ErrorFunc[context.Context, []string /* all state owners */],
		workerProcessReportStates,
	) error
)

func newFuncWorkerProcess(
	ctx context.Context,
	local string,
	newJobWorker newJobWorkerFunc,
	getState getStateFunc,
	newState newStateFunc,
	maxExtractStateKeyCacheSize int,
	workID string,
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
	ctx, log := updateModuleLoggerFromContext(ctx, "worker-process", func(zctx zerolog.Context) zerolog.Context {
		return zctx.
			Str("work_id", workID).
			Str("local", local)
	})

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
		Msg("newFuncWorkerProcess")

	extractStateMergeValueKeys, extractStateKeyClose := newFuncExtractStateMergeValueKeys(
		maxExtractStateKeyCacheSize, int(maxOpsSize*2))

	var getStateMergers mergerGetStateMergersFunc
	var traverseStateMergers traverseStateMergersFunc
	var stateMergersClose func()

	switch g, t, c, err := newFuncStateMergers(
		maxOpsSize*2, // NOTE approximate states size
		getState,
		extractStateMergeValueKeys,
	); {
	case err != nil:
		return nil, nil, nil, nil, err
	default:
		getStateMergers, traverseStateMergers, stateMergersClose = g, t, c
		_, _, _ = getStateMergers, traverseStateMergers, stateMergersClose
	}

	workersSize := uint64(len(workers))

	var initialInsert workerProcessInitialInsertFunc
	var workerInsertStateValues workerProcessWorkerInsertStateMergeValuesFunc
	var unmerge workerUnmergedFunc
	var wait in2ErrorFunc[
		in3ErrorFunc[[]uint64, []string, []string],
		in3ErrorFunc[[]uint64, map[uint64]string, traverseStateOpsFunc],
	]

	switch i, s, u, w, err := newFuncWorkerProcessStateValueMerger(
		ctx,
		local,
		newJobWorker,
		getState,
		newState,
		workID,
		workersSize,
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

	switch i, err := util.NewShardedMap[uint64, Operation](maxOpsSize, nil); {
	case err != nil:
		return nil, nil, nil, nil, err
	default:
		savedOps = i
	}

	return func(
			getStateOwners getStateOwnersFunc,
			distribute workerDistributeStateMergeValuesFunc,
			getOperation getOperationFunc,
			insertOp inErrorFunc[opindexHashFunc],
			saveOperation in2ErrorFunc[uint64, Operation],
		) error {
			return initialInsert(
				getStateOwners,
				distribute,
				getOperation,
				insertOp,
				func(opindex uint64, op Operation) error {
					savedOps.SetValue(opindex, op)

					if saveOperation != nil {
						return saveOperation(opindex, op)
					}

					return nil
				},
			)
		},
		workerInsertStateValues,
		unmerge,
		func(
			traverseOperations in2Func[uint64 /*opindex */, Operation, bool],
			broacastUnmerged in3ErrorFunc[context.Context, string, []uint64],
			reportStateOwners in2ErrorFunc[context.Context, []string],
			reportStates workerProcessReportStates,
		) error {
			defer func() {
				stateMergersClose()
				extractStateKeyClose()
				savedOps.Close()
			}()

			return contextCauseError(ctx, dummyErrorInFunc, func(ctx context.Context) error {
				if err := wait(
					func(unmerged []uint64, subws, affected []string) error {
						log.Debug().
							Dict("owners", zerolog.Dict().
								Strs("sub", subws).
								Strs("affected", affected),
							).Msg("state owners")

						return funcsJobWorker(
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
					func(unmerged []uint64, reasons map[uint64]string, traverseStateOps traverseStateOpsFunc) error {
						return reportStates(ctx, unmerged, reasons, func(f inErrorFunc[base.State]) error {
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

					return err
				}

				if traverseOperations != nil {
					_ = savedOps.Traverse(traverseOperations)
				}

				log.Debug().Msg("wait done")

				return nil
			})
		},
		nil
}

func newFuncWorkerProcessStateValueMerger(
	ctx context.Context,
	local string,
	newJobWorker newJobWorkerFunc,
	getState getStateFunc,
	newState newStateFunc,
	workID string,
	workersSize uint64,
	opsWorkers []string,
	opsShare, statesShare, maxOpsSize uint64,
	getStateMergers mergerGetStateMergersFunc,
) (
	workerProcessInitialInsertFunc,
	workerProcessWorkerInsertStateMergeValuesFunc,
	workerUnmergedFunc,
	in2ErrorFunc[
		in3ErrorFunc[[]uint64, []string, []string],
		in3ErrorFunc[[]uint64, map[uint64]string, traverseStateOpsFunc],
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
		workersSize, maxOpsSize, maxOpsSize*2, // NOTE approximate states size
		opsWorkers, opsShare, statesShare,
	); {
	case err != nil:
		return nil, nil, nil, nil, err
	default:
		workerInsertStateValues, workerUnmerged, workerMergerWait = i, u, w
	}

	var localExtractedStateOwners localExtractedStateOwnersFunc
	var workerStateOwnersSet workerStateOwnersSetFunc
	var doneStateOwnersSet inErrorFunc[insertStateOwnersSetFunc]

	switch l, w, t, err := newFuncWorkerStateOwners(ctx, local, maxOpsSize); {
	case err != nil:
		return nil, nil, nil, nil, err
	default:
		localExtractedStateOwners, workerStateOwnersSet, doneStateOwnersSet = l, w, t
	}

	var initialInsert workerProcessInitialInsertFunc

	switch i, err := newFuncWorkerProcessInitialInsert(
		ctx,
		local,
		workID,
		newJobWorker,
		workersSize,
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
			distribute workerDistributeStateMergeValuesFunc,
			getOperation getOperationFunc,
			insertOp inErrorFunc[opindexHashFunc],
			saveOperation in2ErrorFunc[uint64, Operation],
		) error {
			return contextCauseError(ctx, dummyErrorInFunc, func(context.Context) error {
				return initialInsert(
					getStateOwners,
					distribute,
					getOperation,
					insertOp,
					saveOperation,
				)
			})
		},
		func(worker string, f workerProcessInsertStateMergeValuesCallback) error {
			return workerInsertStateValues(
				worker,
				func(isf insertStateMergeValuesFunc) error {
					return f(
						func(opindex uint64, fact util.Hash, values []StateMergeValue) error {
							if err := mergeStateOpFromValues(mergeStateOp, opindex, fact, nil, values); err != nil {
								return err
							}

							return isf(opindex, values)
						},
						func(subws, affected []string) error {
							return workerStateOwnersSet(worker, subws, affected)
						},
					)
				},
			)
		},
		workerUnmerged,
		func(
			localDone in3ErrorFunc[[]uint64, []string, []string],
			f in3ErrorFunc[[]uint64, map[uint64]string, traverseStateOpsFunc],
		) error {
			defer func() {
				doneStateOwnersSet(nil)
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
				func(unmerged []uint64, reasons map[uint64]string) error {
					_ = unmergeStateOp(unmerged)

					return f(unmerged, reasons, traverseStateOps)
				},
			)
		},
		nil
}

func newFuncWorkerProcessInitialInsert(
	ctx context.Context,
	local string,
	workID string,
	newJobWorker newJobWorkerFunc,
	workersSize, maxOpsSize uint64,
	localExtractedStateOwners localExtractedStateOwnersFunc,
	workerInsertStateValues workerMergerInsertStateMergeValuesFunc,
	mergeStateOp mergeStateOpFunc,
) (
	workerProcessInitialInsertFunc,
	error,
) {
	var insertOps stateMergeValueExtractorInsertFunc
	var extractorWait inErrorFunc[stateMergeValueExtractorWaitCallback]

	switch i, e, err := newFuncStateMergeValueExtractor(ctx, newJobWorker, maxOpsSize); {
	case err != nil:
		return nil, err
	default:
		insertOps, extractorWait = i, e
	}

	return func(
			getStateOwners getStateOwnersFunc,
			distribute workerDistributeStateMergeValuesFunc,
			getOperation getOperationFunc,
			insertOp inErrorFunc[opindexHashFunc],
			saveOperation in2ErrorFunc[uint64, Operation],
		) error {
			var extractStateOwnersStateMergeValues extractStateOwnersStateMergeValuesFunc
			var doneExtractStateOwnersStateMergeValues inErrorFunc[workerStateOwnersSetFunc]

			switch e, s, err := localExtractedStateOwners(getStateOwners); {
			case err != nil:
				return err
			default:
				extractStateOwnersStateMergeValues = e
				doneExtractStateOwnersStateMergeValues = s
			}

			var distributeWorkerStateMergeValues workerStateMergeValuesFunc
			var doneDistributeWorkerStateMergevalues workerStateOwnersSetFunc

			switch i, o, c, err := distribute(workID, workersSize); {
			case err != nil:
				return err
			default:
				defer c()

				distributeWorkerStateMergeValues, doneDistributeWorkerStateMergevalues = i, o
			}

			return workerInsertStateValues(
				local,
				func(insertToMerger insertStateMergeValuesFunc) error {
					extracted := newFuncorkerProcessExtracted(
						ctx,
						local,
						saveOperation,
						extractStateOwnersStateMergeValues,
						distributeWorkerStateMergeValues,
						mergeStateOp,
						insertToMerger,
					)

					if err := insertOps(getOperation, insertOp, extracted); err != nil {
						return err
					}

					return extractorWait(func(reasons map[uint64]string) error {
						// FIXME report to manager

						return doneExtractStateOwnersStateMergeValues(doneDistributeWorkerStateMergevalues)
					})
				},
			)
		},
		nil
}

func newFuncorkerProcessExtracted(
	ctx context.Context,
	local string,
	saveOperation in2ErrorFunc[uint64, Operation],
	extractStateOwners extractStateOwnersStateMergeValuesFunc,
	distribute workerStateMergeValuesFunc,
	mergeStateOp mergeStateOpFunc,
	insertToMerger insertStateMergeValuesFunc,
) stateMergeValueExtractorExtractedFunc {
	log := loadContextValueLogger(ctx)

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

		_ = traverseSlice(addvs, func(i int, value StateMergeValue) (bool, error) {
			add(i, value)

			return true, nil
		})

		if subv != nil {
			add(len(addvs), subv)
		}

		if err := insertToMerger(opindex, stateMergeValues); err != nil {
			return err
		}

		switch d, err := extractStateOwners(opindex, subv, addvs); {
		case err != nil:
			return err
		default:
			if err := d(func(worker string, values []StateMergeValue) error {
				if worker == local {
					return nil
				}

				return distribute(worker, values)
			}); err != nil {
				return err
			}
		}

		_ = mergeStateOp(opindex, op.Fact().Hash(), stateKeys)

		return nil
	}
}

func workersBroadcastUnmerged(
	ctx context.Context,
	local string,
	affected []string,
	unmerged []uint64,
	f in3ErrorFunc[context.Context, string, []uint64],
) error {
	log := loadContextValueLogger(ctx)

	filtered := util.FilterSlice(affected, func(a string) bool {
		return a == local
	})

	if len(filtered) < 1 {
		return nil
	}

	var newJob jobWorkerNewJobFunc
	var jobDone func()
	var jobWait errorFunc

	switch n, d, w, err := newFuncJobWorker(ctx, int64(len(filtered))); {
	case err != nil:
		return err
	default:
		newJob, jobDone, jobWait = n, d, w
	}

	go func() {
		defer jobDone()

		_ = traverseSlice(filtered, func(_ int, worker string) (bool, error) {
			return true, newJob(func(ctx context.Context) error {
				if err := f(ctx, worker, unmerged); err != nil {
					log.Debug().Err(err).Msg("failed to broadcast unmerged")
				}

				return nil
			})
		})
	}()

	return jobWait()
}

func mergeStateOpFromValues(
	mergeStateOp mergeStateOpFunc,
	opindex uint64,
	fact util.Hash,
	value StateMergeValue,
	values []StateMergeValue,
) error {
	var (
		add  compactAppendSliceAddFunc[string]
		done compactAppendSliceDoneFunc[string]
	)

	if err := rCompactAppendSlice[string](
		uint64(len(values)+1), 0, &add, &done); err != nil {
		return err
	}

	defer done(nil)

	if value != nil {
		_, _ = add(value.Key())
	}

	_ = traverseSlice(values, func(_ int, value StateMergeValue) (bool, error) {
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
	h inErrorFunc[base.State],
) error {
	return traverseStateOps(func(size int, getStateOps getStateOpsFunc) error {
		if size < 1 {
			return nil
		}

		var newJob jobWorkerNewJobFunc
		var jobDone func()
		var jobWait errorFunc

		switch n, d, w, err := newFuncJobWorker(ctx, int64(size)); {
		case err != nil:
			return err
		default:
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

		return jobWait()
	})
}

func newFuncDistributeStateMergeValues(
	newFuncs func(workID, worker string) (
		inErrorFunc[[]StateMergeValue],
		in2ErrorFunc[[]string /* subs */, []string /* affected */],
		error,
	),
) workerDistributeStateMergeValuesFunc {
	return func(workID string, workerSize uint64) (
		workerStateMergeValuesFunc,
		workerStateOwnersSetFunc,
		func(),
		error,
	) {
		var pool util.LockedMap[string, [2]interface{}]

		switch i, err := util.NewLockedMap[string, [2]interface{}](workerSize, nil); {
		case err != nil:
			return nil, nil, nil, err
		default:
			pool = i
		}

		return func(worker string, values []StateMergeValue) error {
				var insert inErrorFunc[[]StateMergeValue]

				if _, _, err := pool.Set(worker, func(i [2]interface{}, found bool) (v [2]interface{}, _ error) {
					if found {
						insert = i[0].(inErrorFunc[[]StateMergeValue])

						return v, util.ErrLockedSetIgnore
					}

					switch s, o, err := newFuncs(workID, worker); {
					case err != nil:
						return v, err
					default:
						insert = s

						return [2]interface{}{s, o}, nil
					}
				}); err != nil {
					return err
				}

				return insert(values)
			},
			func(worker string, subws, affected []string) error {
				var sendOwners in2ErrorFunc[[]string, []string]

				if _, _, err := pool.Set(worker, func(i [2]interface{}, found bool) (v [2]interface{}, _ error) {
					if found {
						sendOwners = i[1].(in2ErrorFunc[[]string, []string])

						return v, util.ErrLockedSetIgnore
					}

					switch s, o, err := newFuncs(workID, worker); {
					case err != nil:
						return v, err
					default:
						sendOwners = o

						return [2]interface{}{s, o}, nil
					}
				}); err != nil {
					return err
				}

				return sendOwners(subws, affected)
			},
			pool.Close,
			nil
	}
}
