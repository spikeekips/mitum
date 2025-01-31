package main

import (
	"context"
	"iter"
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
	createStateFunc = func(string /* state key */, base.StateValue, util.Hash /* previous hash */, []util.Hash /* op fact hashes */) (
		base.State,
		error,
	)
	createStateFunc2 = func(string /* state key */, base.StateValue, []util.Hash /* op fact hashes */) (
		base.State,
		error,
	)
	workerDistributeWorkerStateMergeValuesFunc     = func(worker string, opindex uint64, fact util.Hash, _ []stateMergeValue) error
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
		dutil.In2ErrorFunc[uint64, operation],
		dutil.In2ErrorFunc[[]string /* owners */, map[uint64]string /* reasons */],
	) error
	workerProcessInsertStateMergeValuesFunc     = func(opindex uint64, fact util.Hash, _ []stateMergeValue) error
	workerProcessInsertStateMergeValuesCallback = func(
		workerProcessInsertStateMergeValuesFunc,
		dutil.In2ErrorFunc[[]string /* sub */, []string /* affected */],
	) error
	workerProcessWorkerInsertStateMergeValuesFunc = func(worker string) (
		workerProcessInsertStateMergeValuesFunc,
		dutil.In2ErrorFunc[[]string /* sub */, []string /* affected */],
		error,
	)
	workerProcessBroadcastUnmerged = dutil.In3ErrorFunc[context.Context, string /* worker */, []uint64 /* unmerged */]
	workerProcessReportResult      func(
		_ context.Context,
		reasons map[uint64]string,
		statesSize uint64,
		_ dutil.InErrorFunc[dutil.InErrorFunc[base.State]],
	) error
	workerProcessWaitFunc = func(
		dutil.In3ErrorFunc[context.Context, string /* worker */, []uint64 /* unmerged */],
		workerProcessReportResult,
	) (iter.Seq2[uint64, operation], error)
)

func newFuncWorkerProcess(
	ctx context.Context,
	local string,
	newJobWorker dutil.NewJobWorkerFunc,
	getStatef getStateFunc,
	createStatef createStateFunc,
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

	var getState getStateFunc
	var createState createStateFunc2
	var closeState func()

	switch g, s, c, err := newGetCreateStateFunc(maxOpsSize, getStatef, createStatef); {
	case err != nil:
		return nil, nil, nil, nil, err
	default:
		getState, createState, closeState = g, s, c
	}

	var getStateValueMergers mergerGetStateValueMergersFunc
	var traverseStateValueMergersf func() (uint64, iter.Seq2[string, stateValueMerger])

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
		createState,
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

	var savedOps util.LockedMap[uint64, operation]

	switch i, err := util.NewLockedMap[uint64, operation](maxOpsSize, nil); {
	case err != nil:
		return nil, nil, nil, nil, err
	default:
		savedOps = i
	}

	var stateValueInserted int64

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
				func(opindex uint64, op operation) error {
					_ = atomic.CompareAndSwapInt64(&stateValueInserted, 0, 1)

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

			switch wi, wd, err := workerInsertStateValues(worker); {
			case err != nil:
				return nil, nil, err
			case wi == nil:
				return nil, nil, nil
			default:
				return func(opindex uint64, fact util.Hash, values []stateMergeValue) error {
						_ = atomic.CompareAndSwapInt64(&stateValueInserted, 0, 1)

						return wi(opindex, fact, values)
					},
					wd,
					nil
			}
		},
		unmerge,
		func(
			broadcastUnmerged workerProcessBroadcastUnmerged,
			reportResult workerProcessReportResult,
		) (iter.Seq2[uint64, operation], error) {
			defer func() {
				extractStateKeyClose()

				closeState()
			}()

			if err := wait(
				func(unmerged []uint64, subws, affected []string) error {
					log.Debug().
						Dict("owners", zerolog.Dict().
							Strs("sub", subws).
							Strs("affected", affected),
						).Msg("states owners")

					for worker := range dutil.FilterSlice(slices.Values(affected), func(worker string) bool {
						return worker != local
					}) {
						if err := broadcastUnmerged(ctx, worker, unmerged); err != nil {
							log.Debug().Err(err).Msg("failed to broadcast unmerged")
						}
					}

					return nil
				},
				func(reasons map[uint64]string, traverseStateOps traverseStateOpsFunc) error {
					reasons = mergeUnusedReasons(reasons, unusedOps)

					approxStatesSize, traverseStateValueMergers := traverseStateValueMergersf()
					defer func() {
						traverseStateValueMergers(nil)
					}()

					if atomic.LoadInt64(&stateValueInserted) < 1 {
						return nil
					}

					return reportResult(ctx, reasons, approxStatesSize, func(f dutil.InErrorFunc[base.State]) error {
						return workerProcessCloseStateValueMergers(
							ctx,
							createState,
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

			return func(yield func(uint64, operation) bool) {
					defer savedOps.Close()

					if yield == nil {
						return
					}

					_ = savedOps.Traverse(yield)
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
	createState createStateFunc2,
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

	var localExtractedStatesOwners localExtractedStatesOwnersFunc
	var workerStatesOwnersSet workerStatesOwnersSetFunc
	var doneStatesOwnersSet workerStatesOwnersDoneFunc

	switch l, w, t, err := newFuncWorkerStatesOwners(ctx, local, uint64(len(workers))); {
	case err != nil:
		return nil, nil, nil, nil, err
	default:
		localExtractedStatesOwners, workerStatesOwnersSet, doneStatesOwnersSet = l, w, t
	}

	var initialInsert workerProcessInitialInsertFunc2

	switch i, err := newFuncWorkerProcessInitialInsert(
		ctx,
		local,
		newJobWorker,
		workers,
		maxOpsSize,
		localExtractedStatesOwners,
		workerInsertStateValues,
		mergeStateOp,
	); {
	case err != nil:
		return nil, nil, nil, nil, err
	default:
		initialInsert = i
	}

	return initialInsert,
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

			return func(opindex uint64, fact util.Hash, values []stateMergeValue) error {
					if err := mergeStateOpFromValues(mergeStateOp, opindex, fact, values); err != nil {
						return err
					}

					return insert(opindex, values)
				},
				func(subws, affected []string) error {
					return done(workerStatesOwnersSet(worker, subws, affected))
				},
				nil
		},
		workerUnmerged,
		func(
			localDone dutil.In3ErrorFunc[[]uint64, []string, []string],
			f dutil.In2ErrorFunc[map[uint64]string, traverseStateOpsFunc],
		) error {
			defer func() {
				_, _, _ = doneStatesOwnersSet()
			}()

			switch reasons, err := workerMergerWait(
				func(unmerged []uint64) ([]string, error) {
					_ = unmergeStateOp(unmerged)

					switch subws, affected, err := doneStatesOwnersSet(); {
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
	localExtractedStatesOwners localExtractedStatesOwnersFunc,
	workerInsertStateValues workerMergerInsertStateMergeValuesFunc,
	mergeStateOp mergeStateOpFunc,
) (
	workerProcessInitialInsertFunc2,
	error,
) {
	var insertOps stateMergeValueExtractorInsertFunc
	var extractorWait dutil.InErrorFunc[dutil.InErrorFunc[map[uint64]string]]

	switch i, e, err := newFuncStateMergeValueExtractor(
		context.WithValue(ctx, "local", local),
		newJobWorker, maxOpsSize); {
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
			saveOperation dutil.In2ErrorFunc[uint64, operation],
			reportDone dutil.In2ErrorFunc[[]string, map[uint64]string],
		) error {
			var extractStateOwnersStateMergeValues extractStateOwnersStateMergeValuesFunc
			var doneExtractStatesOwnersStateMergeValues dutil.InErrorFunc[workerStatesOwnersSetFunc]
			var localOwners dutil.OutFunc[[]string]

			switch e, s, l, err := localExtractedStatesOwners(getStateOwners); {
			case err != nil:
				return err
			default:
				extractStateOwnersStateMergeValues, doneExtractStatesOwnersStateMergeValues, localOwners = e, s, l
			}

			var distributeWorkerStateMergeValues workerDistributeWorkerStateMergeValuesFunc
			var distributeWorkerStateMergeValuesDone workerStatesOwnersSetFunc

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

			switch i, d, err := workerInsertStateValues(local); {
			case err != nil:
				return err
			default:
				defer d(nil)

				insert = i
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
				if err := reportDone(localOwners(), reasons); err != nil {
					return err
				}

				return doneExtractStatesOwnersStateMergeValues(distributeWorkerStateMergeValuesDone)
			})
		},
		nil
}

func newFuncorkerProcessExtracted(
	ctx context.Context,
	local string,
	saveOperation dutil.In2ErrorFunc[uint64, operation],
	extractStateOwners extractStateOwnersStateMergeValuesFunc,
	distribute workerDistributeWorkerStateMergeValuesFunc,
	mergeStateOp mergeStateOpFunc,
	insertToMerger insertStateMergeValuesFunc,
) stateMergeValueExtractorExtractedFunc {
	log := dutil.LoadContextValueLogger(ctx)

	return func(opindex uint64, op operation, subv stateMergeValue, addvs []stateMergeValue) error {
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
			size++
		}

		stateKeys := make([]string, size)
		stateMergeValues := make([]stateMergeValue, size)

		add := func(i int, value stateMergeValue) {
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

		if err := extractStateOwners(opindex, subv, addvs)(func(worker string, values []stateMergeValue) error {
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

func mergeStateOpFromValues(
	mergeStateOp mergeStateOpFunc,
	opindex uint64,
	fact util.Hash,
	values []stateMergeValue,
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
	createState createStateFunc2,
	traverseStateValueMergers iter.Seq2[string, stateValueMerger],
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

		for stateKey, merger := range traverseStateValueMergers {
			if err := newJob(func(context.Context) error {
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

				switch st, err := createState(stateKey, stv, facts); {
				case err != nil:
					return err
				case st == nil:
					return errors.Errorf("nil state")
				default:
					return h(st)
				}
			}); err != nil {
				return jobWait(err)
			}
		}

		jobDone()

		return jobWait(nil)
	})
}

func workersDistributeStateMergeValues( // FIXME review job worker
	ctx context.Context,
	newJobWorker dutil.NewJobWorkerFunc,
	local string,
	workers []string,
	newf newWorkerProcessDistributeStateMergeValuesFunc,
) (
	workerDistributeWorkerStateMergeValuesFunc,
	workerStatesOwnersSetFunc,
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

	return func(worker string, opindex uint64, fact util.Hash, values []stateMergeValue) error {
			if worker == local {
				return nil
			}

			var insert dutil.In3ErrorFunc[uint64, util.Hash, []stateMergeValue]

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
			if worker == local {
				return nil
			}

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

			var cnewJob dutil.JobWorkerNewJobFunc // FIXME remove job worker
			var cjobDone func()
			var cjobWait dutil.InErrorFunc[error]

			switch n, d, w, err := newJobWorker(ctx, uint64(len(workers))); {
			case err != nil:
				return err
			default:
				cnewJob, cjobDone, cjobWait = n, d, w
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

				if err := cnewJob(func(context.Context) error {
					_ = sendOwners(nil, nil) // NOTE ignore error

					return nil
				}); err != nil {
					return cjobWait(err)
				}
			}

			cjobDone()

			return cjobWait(nil)
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

func newGetCreateStateFunc(size uint64, get getStateFunc, create createStateFunc) (getStateFunc, createStateFunc2, func(), error) {
	var pool util.LockedMap[string, base.State]

	switch i, err := util.NewLockedMap[string, base.State](size, nil); {
	case err != nil:
		return nil, nil, nil, err
	default:
		pool = i
	}

	return func(stateKey string) (base.State, bool, error) {
			var st base.State

			err := pool.GetOrCreate(
				stateKey,
				func(i base.State, _ bool) error {
					st = i

					return nil
				},
				func() (base.State, error) {
					st, _, err := get(stateKey)

					return st, err
				},
			)

			return st, st != nil, err
		},
		func(stateKey string, value base.StateValue, ops []util.Hash) (base.State, error) {
			var prev util.Hash

			switch i, found := pool.Value(stateKey); {
			case !found:
				return nil, util.ErrNotFound.Errorf("state, %q", stateKey)
			case i != nil:
				prev = i.Hash()
			}

			return create(stateKey, value, prev, ops)
		},
		pool.Close,
		nil
}
