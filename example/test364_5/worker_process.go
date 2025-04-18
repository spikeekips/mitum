package dop5

import (
	"context"
	"iter"
	"maps"
	"slices"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
)

type (
	CreateStateFunc = func(
		string, /* state key */
		base.StateValue,
		util.Hash, /* previous hash */
		[]util.Hash, /* op fact hashes */
	) (
		base.State,
		error,
	)
	createStateFunc2 = func(string /* state key */, base.StateValue, []util.Hash /* op fact hashes */) (
		base.State,
		error,
	)
	workerDistributeWorkerSMVsFunc     = func(worker string, opindex uint64, facth util.Hash, _ []SMV) error
	newWorkerProcessDistributeSMVsFunc func(_ context.Context, worker string) (
		WorkerProcessInsertSMVsFunc,
		dutil.In2ErrorFunc[[]string /* subs */, []string /* affected */],
		error,
	)
	workerProcessInitialInsertFunc = func(
		getStateOwnersFunc,
		newWorkerProcessDistributeSMVsFunc,
		GetOperationFactFunc,
		dutil.InErrorFunc[OpindexOpHashesFunc],
		dutil.In2ErrorFunc[[]string /* owners */, map[uint64]string /* reasons */],
	) error
	workerProcessInitialInsertFunc2 = func(
		getStateOwnersFunc,
		newWorkerProcessDistributeSMVsFunc,
		GetOperationFactFunc,
		dutil.InErrorFunc[OpindexOpHashesFunc],
		dutil.In2ErrorFunc[[]string /* owners */, map[uint64]string /* reasons */],
	) error
	WorkerProcessInsertSMVsFunc       = func(opindex uint64, facth util.Hash, _ []SMV) error
	WorkerProcessWorkerInsertSMVsFunc = func(worker string) (
		WorkerProcessInsertSMVsFunc,
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
	) error
)

func newFuncWorkerProcess(
	ctx context.Context,
	local string,
	newJobWorker dutil.NewJobWorkerFunc,
	getStatef GetStateFunc,
	createStatef CreateStateFunc,
	maxExtractStateKeyCacheSize uint64,
	workers, opsWorkers []string,
	opsShare, statesShare uint64,
	maxOpsSize uint64,
) (
	workerProcessInitialInsertFunc,
	WorkerProcessWorkerInsertSMVsFunc,
	WorkerUnmergedFunc,
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

	extractSMVKeys, extractStateKeyClose := newFuncExtractSMVKeys(
		ctx,
		maxExtractStateKeyCacheSize, maxOpsSize*2) //nolint:mnd

	var getState GetStateFunc
	var createState createStateFunc2
	var closeState func()

	switch g, s, c, err := newGetCreateStateFunc(maxOpsSize, getStatef, createStatef); {
	case err != nil:
		return nil, nil, nil, nil, err
	default:
		getState, createState, closeState = g, s, c
	}

	var getSVMs mergerGetSVMsFunc
	var traverseSVMsf func() (uint64, iter.Seq2[string, SVM])

	switch g, t, err := newFuncSVMs(
		maxOpsSize*2, //nolint:mnd // NOTE approximate states size
		getState,
		extractSMVKeys,
	); {
	case err != nil:
		return nil, nil, nil, nil, err
	default:
		getSVMs, traverseSVMsf = g, t
	}

	var initialInsert workerProcessInitialInsertFunc2
	var workerInsertSVs WorkerProcessWorkerInsertSMVsFunc
	var unmerge WorkerUnmergedFunc
	var wait dutil.In2ErrorFunc[
		dutil.In3ErrorFunc[[]uint64, []string, []string],
		dutil.In2ErrorFunc[map[uint64]string, traverseStateOpsFunc],
	]

	switch i, s, u, w, err := newFuncWorkerProcessSVM(
		ctx,
		local,
		newJobWorker,
		workers,
		opsWorkers,
		opsShare, statesShare, maxOpsSize,
		getSVMs,
	); {
	case err != nil:
		return nil, nil, nil, nil, err
	default:
		initialInsert, workerInsertSVs, unmerge, wait = i, s, u, w
	}

	unusedOps := new(atomic.Value)
	insertedAt := new(atomic.Value)

	return func(
			getStateOwners getStateOwnersFunc,
			distribute newWorkerProcessDistributeSMVsFunc,
			getOperationFact GetOperationFactFunc,
			insertOp dutil.InErrorFunc[OpindexOpHashesFunc],
			reportDone dutil.In2ErrorFunc[[]string, map[uint64]string],
		) error {
			_ = insertedAt.CompareAndSwap(nil, time.Now())

			return initialInsert(
				getStateOwners,
				distribute,
				getOperationFact,
				func(f OpindexOpHashesFunc) error {
					return insertOp(f)
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
			WorkerProcessInsertSMVsFunc,
			dutil.In2ErrorFunc[[]string, []string],
			error,
		) {
			switch {
			case worker == ManagerName:
			case slices.Index(opsWorkers, worker) >= 0:
			default:
				return nil, nil, util.ErrInvalid.Errorf("unknown ops worker, %q", worker)
			}

			return workerInsertSVs(worker)
		},
		unmerge,
		func(
			broadcastUnmerged workerProcessBroadcastUnmerged,
			reportResult workerProcessReportResult,
		) error {
			defer func() {
				extractStateKeyClose()

				closeState()
			}()

			err := wait(
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

					approxStatesSize, traverseSVMs := traverseSVMsf()
					defer func() {
						traverseSVMs(nil)
					}()

					return reportResult(ctx, reasons, approxStatesSize, func(f dutil.InErrorFunc[base.State]) error {
						return workerProcessCloseSVMs(
							ctx,
							createState,
							traverseSVMs,
							traverseStateOps,
							func(st base.State) error { return f(st) },
						)
					})
				},
			)

			dutil.AtomicElapsedLogEvent(log.Debug(), insertedAt).
				Err(err).
				Msg("process wait done")

			return err
		},
		nil
}

func newFuncWorkerProcessSVM(
	ctx context.Context,
	local string,
	newJobWorker dutil.NewJobWorkerFunc,
	workers []string,
	opsWorkers []string,
	opsShare, statesShare, maxOpsSize uint64,
	getSVMs mergerGetSVMsFunc,
) (
	workerProcessInitialInsertFunc2,
	WorkerProcessWorkerInsertSMVsFunc,
	WorkerUnmergedFunc,
	dutil.In2ErrorFunc[
		dutil.In3ErrorFunc[[]uint64, []string, []string],
		dutil.In2ErrorFunc[map[uint64]string, traverseStateOpsFunc],
	],
	error,
) {
	log := dutil.LoadContextValueLogger(ctx)

	var mergeStateOp mergeStateOpFunc
	var unmergeStateOp unmergeStateOpFunc
	var traverseStateOps traverseStateOpsFunc

	switch m, u, t, err := newFuncSetStateOps(maxOpsSize); {
	case err != nil:
		return nil, nil, nil, nil, err
	default:
		mergeStateOp, unmergeStateOp, traverseStateOps = m, u, t
	}

	var workerInsertSVs workerMergerInsertSMVsFunc
	var workerUnmerged WorkerUnmergedFunc
	var workerMergerWait workerMergerWaitFunc

	switch i, u, w, err := newFuncWorkerMerger(
		ctx,
		local,
		newJobWorker,
		getSVMs,
		maxOpsSize, maxOpsSize*2, //nolint:mnd // NOTE approximate states size
		opsWorkers, opsShare, statesShare,
	); {
	case err != nil:
		return nil, nil, nil, nil, err
	default:
		workerInsertSVs, workerUnmerged, workerMergerWait = i, u, w
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
		workerInsertSVs,
		mergeStateOp,
	); {
	case err != nil:
		return nil, nil, nil, nil, err
	default:
		initialInsert = i
	}

	return initialInsert,
		func(worker string) (
			WorkerProcessInsertSMVsFunc,
			dutil.In2ErrorFunc[[]string, []string],
			error,
		) {
			l := dutil.ElapsedLogEvent(log.Debug)

			var insert insertSMVsFunc
			var done dutil.InErrorFunc[error]

			switch i, d, err := workerInsertSVs(worker); {
			case err != nil:
				return nil, nil, err
			case i == nil:
				return nil, nil, nil
			default:
				insert, done = i, d
			}

			return func(opindex uint64, facth util.Hash, values []SMV) error {
					if err := mergeStateOpFromValues(mergeStateOp, opindex, facth, values); err != nil {
						return err
					}

					return insert(opindex, values)
				},
				func(subws, affected []string) error {
					defer l().Msg("worker insert svs")

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
	workerInsertSVs workerMergerInsertSMVsFunc,
	mergeStateOp mergeStateOpFunc,
) (
	workerProcessInitialInsertFunc2,
	error,
) {
	log := dutil.LoadContextValueLogger(ctx)

	var insertOps smvExtractorInsertFunc
	var extractorWait dutil.InErrorFunc[dutil.InErrorFunc[map[uint64]string]]

	switch i, e, err := newFuncSMVExtractor(
		context.WithValue(ctx, dutil.ContextKey("local"), local),
		newJobWorker, maxOpsSize); {
	case err != nil:
		return nil, err
	default:
		insertOps, extractorWait = i, e
	}

	return func(
			getStateOwners getStateOwnersFunc,
			distribute newWorkerProcessDistributeSMVsFunc,
			getOperationFact GetOperationFactFunc,
			insertOp dutil.InErrorFunc[OpindexOpHashesFunc],
			reportDone dutil.In2ErrorFunc[[]string, map[uint64]string],
		) error {
			l := dutil.ElapsedLogEvent(log.Debug)

			var extractStateOwnersSMVs extractStateOwnersSMVsFunc
			var doneExtractStatesOwnersSMVs dutil.InErrorFunc[workerStatesOwnersSetFunc]
			var localOwners dutil.OutFunc[[]string]

			switch e, s, l, err := localExtractedStatesOwners(getStateOwners); {
			case err != nil:
				return err
			default:
				extractStateOwnersSMVs, doneExtractStatesOwnersSMVs, localOwners = e, s, l
			}

			var distributeWorkerSMVs workerDistributeWorkerSMVsFunc
			var distributeWorkerSMVsDone workerStatesOwnersSetFunc

			switch i, o, c, err := newFuncWorkerProcessWorkersDistributeSMVs(ctx, local, workers, distribute,
				func(ctx context.Context) (
					dutil.InErrorFunc[interface{}],
					func(),
					iter.Seq[interface{}],
				) {
					return dutil.NewUnlimitedChan[interface{}](
						ctx,
						maxOpsSize*2, maxOpsSize*2, //nolint:mnd // NOTE approximate states size
					)
				}); {
			case err != nil:
				return err
			default:
				defer func() {
					_ = c()
				}()

				distributeWorkerSMVs, distributeWorkerSMVsDone = i, o
			}

			var insert insertSMVsFunc

			switch i, d, err := workerInsertSVs(local); {
			case err != nil:
				return err
			default:
				defer func() { _ = d(nil) }()

				insert = i
			}

			extracted := newFuncorkerProcessExtracted(
				ctx,
				local,
				extractStateOwnersSMVs,
				distributeWorkerSMVs,
				mergeStateOp,
				insert,
			)

			if err := insertOps(getOperationFact, insertOp, extracted); err != nil {
				return err
			}

			return extractorWait(func(reasons map[uint64]string) error {
				defer l().Msg("initial insert done")

				if err := reportDone(localOwners(), reasons); err != nil {
					return err
				}

				return doneExtractStatesOwnersSMVs(distributeWorkerSMVsDone)
			})
		},
		nil
}

func newFuncorkerProcessExtracted(
	ctx context.Context,
	local string,
	extractStateOwners extractStateOwnersSMVsFunc,
	distribute workerDistributeWorkerSMVsFunc,
	mergeStateOp mergeStateOpFunc,
	insertToMerger insertSMVsFunc,
) smvExtractorExtractedFunc {
	log := dutil.LoadContextValueLogger(ctx)

	return func(opindex uint64, facth util.Hash, subv SMV, addvs []SMV) error {
		log.Trace().
			Dict("operation", zerolog.Dict().
				Uint64("opindex", opindex).
				Stringer("fact", facth),
			).
			Msg("insert operation")

		size := len(addvs)
		if subv != nil {
			size++
		}

		stateKeys := make([]string, size)
		smvs := make([]SMV, size)

		add := func(i int, value SMV) {
			stateKeys[i] = value.Key()
			smvs[i] = value
		}

		for i, value := range slices.All(addvs) {
			add(i, value)
		}

		if subv != nil {
			add(size-1, subv)
		}

		if err := extractStateOwners(opindex, subv, addvs)(func(worker string, values []SMV) error {
			if worker == local {
				return insertToMerger(opindex, values)
			}

			return distribute(worker, opindex, facth, values)
		}); err != nil {
			return err
		}

		_ = mergeStateOp(opindex, facth, stateKeys)

		return nil
	}
}

func mergeStateOpFromValues(
	mergeStateOp mergeStateOpFunc,
	opindex uint64,
	facth util.Hash,
	values []SMV,
) error {
	stateKeys := make([]string, len(values))

	for i, value := range slices.All(values) {
		stateKeys[i] = value.Key()
	}

	_ = mergeStateOp(opindex, facth, stateKeys)

	return nil
}

func workerProcessCloseSVMs(
	ctx context.Context,
	createState createStateFunc2,
	traverseSVMs iter.Seq2[string, SVM],
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

		switch n, d, w, err := dutil.NewFuncJobWorker(ctx, int64(size)); {
		case err != nil:
			return err
		default:
			newJob, jobDone, jobWait = n, d, w
		}

		for stateKey, merger := range traverseSVMs {
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
					return errors.New("nil state")
				default:
					return h(st)
				}
			}); err != nil {
				return err
			}
		}

		jobDone()

		return jobWait(nil)
	})
}

func mergeUnusedReasons(reasons map[uint64]string, unusedOps *atomic.Value) map[uint64]string {
	var u map[uint64]string

	if i := unusedOps.Load(); i != nil {
		u = maps.Clone(i.(map[uint64]string)) //nolint:forcetypeassert
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

func newGetCreateStateFunc(size uint64, get GetStateFunc, create CreateStateFunc) (
	GetStateFunc, createStateFunc2, func(), error,
) {
	var pool dutil.LockedMap[string, base.State]

	switch i, err := dutil.NewLockedMap[string, base.State](nil, size); {
	case err != nil:
		return nil, nil, nil, err
	default:
		pool = i
	}

	return func(stateKey string) (base.State, bool, error) {
			var st base.State

			err := pool.GetOrCreate(
				stateKey,
				func() (base.State, error) {
					st, _, err := get(stateKey)

					return st, err
				},
				func(i base.State, _ bool) error {
					st = i

					return nil
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
