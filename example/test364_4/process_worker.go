package main

import (
	"context"
	"slices"
	"sync"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
	"golang.org/x/sync/singleflight"
)

type (
	newStateValuesProcessorFunc func(context.Context, *stateValuesProcessorArgs) (
		insert func(opindex uint64) error,
		insertDone func(),
		wait func(func(reasons map[uint64]string) error) error,
		_ error,
	)
	newStateValuesProcessorFuncs func() (
		*stateValuesProcessorArgs,
		newStateValuesProcessorFunc,
		error,
	)
	newWorkerMergerFunc func(context.Context, *workerMergerArgs) (
		insert workerMergerPhaseDoneFunc,
		_ workerMergerWaitFunc,
		_ error,
	)
	newWorkerMergerFuncs func() (
		*workerMergerArgs,
		newWorkerMergerFunc,
		error,
	)

	insertOpFunc       func(opindex uint64, _ []StateMergeValue) error
	insertWorkerOpFunc func(worker string) (
		insertOpFunc,
		func(),
	)
	saveOperationFunc        func(opindex uint64, _ Operation) error
	stateOwnersFunc          func(_ context.Context, stateKey string) (owners []string, _ error)
	getValuesFunc            func(opindex uint64) ([]StateMergeValue, error)
	isLocalOpFunc            func(opindex uint64) bool
	reportResultUnmergedFunc func(unmergedOps []uint64) error
	reportResultStatesFuncs  func() (
		func(stateKey string, _ base.StateValue) error,
		func(),
		error,
	)
)

type processWorkerArgs struct {
	*logging.Logging
	NewJobWorker                 func(context.Context) (JobWorker, error)
	NewPhaseMergerFuncs          newPhaseMergerFuncs
	GetStateFunc                 func(stateKey string) (base.State, bool, error)
	NewStateValuesProcessorFuncs newStateValuesProcessorFuncs
	NewWorkerMergerFuncs         newWorkerMergerFuncs
	StateOwners                  stateOwnersFunc // NOTE local is `localWorkerName`
	DistributeStateMergeValues   func(_ context.Context, worker string, opindex uint64, _ []StateMergeValue) error
	UpdateReasons                UpdateReasonsFunc // FIXME filter sent opindex and reason
	ReportResult                 func(context.Context) (
		reportResultUnmergedFunc,
		reportResultStatesFuncs,
		error,
	)
	SaveOperation                saveOperationFunc
	StateMergeValueKeysCacheSize int
	EnoughWorkers                uint64 // NOTE include local worker
	BroadcastPhaseDone           BroadcastPhaseDoneFunc
	StateOwnersCacheSize         int
	isLocalOp                    isLocalOpFunc
}

func newProcessWorkerArgs() *processWorkerArgs {
	return &processWorkerArgs{
		Logging: logging.NewLogging(func(zctx zerolog.Context) zerolog.Context {
			return zctx.Str("module", "process-worker")
		}),
		NewJobWorker: func(ctx context.Context) (JobWorker, error) {
			return NewBaseJobWorker(ctx, 1<<6)
		},
		NewPhaseMergerFuncs: func() (
			*PhaseMergerArgs,
			newPhaseMergerFunc,
			error,
		) {
			return nil, nil, util.ErrNotImplemented.Errorf("NewPhaseMergerFuncs")
		},
		GetStateFunc: func(string) (base.State, bool, error) {
			return nil, false, util.ErrNotImplemented.Errorf("GetStateFunc")
		},
		NewStateValuesProcessorFuncs: func() (
			*stateValuesProcessorArgs,
			newStateValuesProcessorFunc,
			error,
		) {
			return nil, nil, util.ErrNotImplemented.Errorf("NewStateValuesProcessorFuncs")
		},
		NewWorkerMergerFuncs: func() (
			*workerMergerArgs,
			newWorkerMergerFunc,
			error,
		) {
			return nil, nil, util.ErrNotImplemented.Errorf("NewWorkerMergerFuncs")
		},
		StateOwners: func(context.Context, string) ([]string, error) {
			return nil, util.ErrNotImplemented.Errorf("StateOwners")
		},
		DistributeStateMergeValues: func(context.Context, string, uint64, []StateMergeValue) error {
			return util.ErrNotImplemented.Errorf("BroadcastStateMergeValues")
		},
		UpdateReasons: func(context.Context, int, map[uint64]string) error {
			return util.ErrNotImplemented.Errorf("UpdateReasons")
		},
		ReportResult: func(context.Context) (
			reportResultUnmergedFunc,
			reportResultStatesFuncs,
			error,
		) {
			return nil, nil, util.ErrNotImplemented.Errorf("ReportResult")
		},
		SaveOperation: func(uint64, Operation) error {
			return util.ErrNotImplemented.Errorf("SaveOperation")
		},
		StateMergeValueKeysCacheSize: 1 << 9,
		BroadcastPhaseDone: func(context.Context, int, []uint64, []uint64) error {
			return util.ErrNotImplemented.Errorf("BroadcastPhaseDone")
		},
		StateOwnersCacheSize: 1 << 9,
		isLocalOp:            func(uint64) bool { return false },
	}
}

func newProcessWorker(ctx context.Context, args *processWorkerArgs, ops []uint64) (
	insertWorkerOp insertWorkerOpFunc,
	mergeInsert workerMergerPhaseDoneFunc,
	processWait workerMergerWaitFunc,
	_ error,
) {
	if args.EnoughWorkers < 1 {
		return nil, nil, nil, util.ErrInvalid.Errorf("EnoughWorkers should be at least 1")
	}

	stateOwners, closeStateOwners := newStateOwnersWithLocalOpOwners(args, args.StateOwners)
	args.StateOwners = stateOwners
	args.BroadcastPhaseDone = newBroadcastPhaseDoneFunc(args)

	var valuesPool util.LockedMap[uint64, []StateMergeValue]

	switch i, err := util.NewShardedMap[uint64, []StateMergeValue](1<<9, nil); {
	case err != nil:
		return nil, nil, nil, err
	default:
		valuesPool = i
	}

	args.isLocalOp = newIsLocalOpFunc(valuesPool)

	var mergersPool util.LockedMap[string, StateMerger]

	switch i, err := util.NewShardedMap[string, StateMerger](1<<9, nil); {
	case err != nil:
		return nil, nil, nil, err
	default:
		mergersPool = i
	}

	pctx, pctxCancel := context.WithCancelCause(ctx)

	getMergers, closeGetMergers := newGetMergersFunc(mergersPool, args.GetStateFunc)
	getValues := newGetValuesFunc(valuesPool)

	initialMergeWaitLog := FuncErrorLog(args.Log(), "initial merge")

	var initialMergeWait phaseMergerWaitFunc

	switch i, c, err := newInsertWorkerOpFuncs(pctx, args, valuesPool.SetValue, getMergers, getValues); {
	case err != nil:
		return nil, nil, nil, err
	default:
		insertWorkerOp = i
		initialMergeWait = c
	}

	processWaitLog := FuncErrorLog(args.Log(), "process")

	var processValuesWait func(func(reasons map[uint64]string) error) error

	switch i, d, w, err := newStateValuesProcessorForProcessWorker(pctx, args, insertWorkerOp); {
	case err != nil:
		return nil, nil, nil, err
	default:
		if err := insertInitialOps(ops, args, i, d); err != nil {
			pctxCancel(err)

			return nil, nil, nil, err
		}

		processValuesWait = w
	}

	mergeWaitLog := FuncErrorLog(args.Log(), "merge")

	var mergeReady func() error
	var mergeWait workerMergerWaitFunc

	switch r, i, w, err := newWorkerMergerFuncsForProcessWorker(pctx, args, getMergers, getValues); {
	case err != nil:
		return nil, nil, nil, err
	default:
		mergeReady = r
		mergeInsert = i
		mergeWait = w
	}

	collectOps, collectOpsDone := newCollectXMergedOps()

	var report func(unmergedOps []uint64) error

	switch r, err := newReportResult(pctx, args, mergersPool); {
	case err != nil:
		return nil, nil, nil, err
	default:
		report = func(unmergedOps []uint64) error {
			collectOps(nil, unmergedOps)

			return collectOpsDone(r)
		}
	}

	return insertWorkerOp,
		mergeInsert,
		func(f workerMergerUnmergedOpsFunc) error {
			defer func() {
				_ = mergeWait(nil)
			}()

			defer closeStateOwners()
			defer closeGetMergers()
			defer pctxCancel(nil)

			if err := processWaitLog(processValuesWait(func(reasons map[uint64]string) error {
				return args.UpdateReasons(pctx, -1, reasons)
			})); err != nil {
				return err
			}

			var delayed func() error

			if err := initialMergeWaitLog(initialMergeWait(func(newlyMergedOps, newlyUnmergedOps []uint64, reasons map[uint64]string) error {
				if err := mergeReady(); err != nil {
					return err
				}

				collectOps(newlyMergedOps, newlyUnmergedOps)

				switch i, err := initialPhaseDoneFunc(pctx, args, mergeInsert, newlyMergedOps, newlyUnmergedOps, reasons); {
				case err != nil:
					return err
				default:
					delayed = i

					return nil
				}
			})); err != nil {
				return err
			}

			if err := mergeWaitLog(mergeWait(report)); err != nil {
				return err
			}

			return delayed()
		},
		nil
}

func insertInitialOps(
	ops []uint64,
	args *processWorkerArgs,
	insert func(opindex uint64) error,
	done func(),
) error {
	args.Log().Debug().Func(func(e *zerolog.Event) {
		var es []uint64

		switch l := len(ops); {
		case l < 1:
		case l < 2:
			es = ops
		default:
			es = []uint64{ops[0], ops[len(ops)-1]}
		}

		e.Uints64("ops", es)
	}).Msg("insert ops")

	for i := range ops {
		if err := insert(ops[i]); err != nil {
			return err
		}
	}

	done()

	return nil
}

func newStateValuesProcessorForProcessWorker(
	ctx context.Context,
	args *processWorkerArgs,
	newLocalInsert insertWorkerOpFunc,
) (
	insert func(opindex uint64) error,
	insertDone func(),
	waitf func(func(reasons map[uint64]string) error) error,
	_ error,
) {
	localInsert, localInsertDone := newLocalInsert(localWorkerName)

	var distributeInsert func(opindex uint64, _ []StateMergeValue) error
	var distributeDone func()
	var distributeWait func() error

	switch i, d, w, err := distributeStateValuesFuncs(
		ctx,
		args.StateOwners,
		args.NewJobWorker,
		func(ctx context.Context, worker string, opindex uint64, values []StateMergeValue) error {
			if worker == localWorkerName {
				return localInsert(opindex, values)
			}

			return args.DistributeStateMergeValues(ctx, worker, opindex, values)
		},
	); {
	case err != nil:
		return nil, nil, nil, err
	default:
		distributeInsert = i
		distributeDone = d
		distributeWait = w
	}

	var pargs *stateValuesProcessorArgs
	var newf newStateValuesProcessorFunc

	switch a, n, err := args.NewStateValuesProcessorFuncs(); {
	case err != nil:
		return nil, nil, nil, err
	default:
		pargs = a
		newf = n
	}

	pargs.ProcessedOp = func(ctx context.Context, opindex uint64, op Operation, values []StateMergeValue) error {
		if err := args.SaveOperation(opindex, op); err != nil {
			return err
		}

		return distributeInsert(opindex, values)
	}

	switch i, d, w, err := newf(ctx, pargs); {
	case err != nil:
		return nil, nil, nil, err
	default:
		return i, d, func(f func(map[uint64]string) error) error {
			if err := FuncErrorLog(args.Log(), "process state values")(w(f)); err != nil {
				return err
			}

			distributeDone()

			if err := FuncErrorLog(args.Log(), "distribute values wait")(distributeWait()); err != nil {
				return err
			}

			localInsertDone()

			return nil
		}, nil
	}
}

func newWorkerMergerFuncsForProcessWorker(
	ctx context.Context,
	args *processWorkerArgs,
	getMergers phaseMergerGetMergers,
	getValues getValuesFunc,
) (
	prevDone func() error,
	_ workerMergerPhaseDoneFunc,
	_ workerMergerWaitFunc,
	_ error,
) {
	var wargs *workerMergerArgs
	var newf newWorkerMergerFunc

	switch a, n, err := args.NewWorkerMergerFuncs(); {
	case err != nil:
		return nil, nil, nil, err
	default:
		wargs = a
		newf = n
	}

	wargs.UpdateReasons = args.UpdateReasons
	wargs.BroadcastPhaseDone = args.BroadcastPhaseDone

	newWorkerPhaseMergerFuncs := wargs.NewWorkerPhaseMergerFuncs
	wargs.NewWorkerPhaseMergerFuncs = func(phase int) (
		*workerPhaseMergerArgs,
		newWorkerPhaseMergerFunc,
		error,
	) {
		wpargs, newf, err := newWorkerPhaseMergerFuncs(phase)
		if err != nil {
			return nil, nil, err
		}

		wpargs.NewPhaseMergerFuncs = func() (
			*PhaseMergerArgs,
			newPhaseMergerFunc,
			error,
		) {
			pargs, newf, err := newPhaseMergerFuncsForProcessWorkerFuncs(
				args,
				args.NewPhaseMergerFuncs,
				getMergers,
				getValues,
			)
			if err != nil {
				return nil, nil, err
			}

			return pargs, newf, nil
		}

		wpargs.IsLocalOp = args.isLocalOp
		wpargs.EnoughWorkers = args.EnoughWorkers

		wpargs.SetLogging(args.Logging)

		return wpargs, newf, nil
	}

	wargs.SetLogging(args.Logging)

	pool := map[ /* phase */ int]map[ /* worker */ string][2][]uint64{}

	var newLock sync.RWMutex
	var insert workerMergerPhaseDoneFunc
	var wait workerMergerWaitFunc

	return func() error {
			newLock.Lock()
			defer newLock.Unlock()

			defer clear(pool)

			switch i, w, err := newf(ctx, wargs); {
			case err != nil:
				return err
			default:
				insert = i
				wait = w
			}

			if len(pool) > 0 {
				if err := TraverseMap(pool, func(phase int, m map[string][2][]uint64) (bool, error) {
					for worker := range m {
						if err := insert(phase, worker, m[worker][0], m[worker][1]); err != nil {
							return false, err
						}
					}

					return true, nil
				}); err != nil {
					return err
				}
			}

			return nil
		},
		func(phase int, worker string, mergeOps, unmergeOps []uint64) error {
			newLock.RLock()
			defer newLock.RUnlock()

			if insert == nil {
				switch _, found := pool[phase]; {
				case !found:
					pool[phase] = map[string][2][]uint64{worker: {mergeOps, unmergeOps}}
				default:
					pool[phase][worker] = [2][]uint64{mergeOps, unmergeOps}
				}

				return nil
			}

			return insert(phase, worker, mergeOps, unmergeOps)
		},
		func(f workerMergerUnmergedOpsFunc) error {
			newLock.RLock()
			defer newLock.RUnlock()

			if wait == nil {
				if f == nil {
					return nil
				}

				return f(nil)
			}

			return wait(f)
		},
		nil
}

func distributeStateValuesFuncs(
	ctx context.Context,
	stateOwners stateOwnersFunc,
	newJobWorker func(context.Context) (JobWorker, error),
	broadcast func(_ context.Context, worker string, opindex uint64, _ []StateMergeValue) error,
) (
	insert func(opindex uint64, _ []StateMergeValue) error,
	done func(),
	wait func() error,
	_ error,
) {
	var jobWorker JobWorker

	switch i, err := newJobWorker(ctx); {
	case err != nil:
		return nil, nil, nil, err
	default:
		jobWorker = i
	}

	distribute := stateValuesByOwnerFunc(stateOwners)

	return func(opindex uint64, values []StateMergeValue) error {
			return jobWorker.NewJob(func(ctx context.Context) error {
				return distribute(ctx, values, func(worker string, filtered []StateMergeValue) error {
					return broadcast(ctx, worker, opindex, filtered)
				})
			})
		},
		jobWorker.JobDone,
		jobWorker.Wait,
		nil
}

func stateValuesByOwnerFunc(
	stateOwners stateOwnersFunc,
) func(context.Context, []StateMergeValue, func(worker string, _ []StateMergeValue) error) error {
	return func(ctx context.Context, values []StateMergeValue, f func(worker string, _ []StateMergeValue) error) error {
		m := map[string][]StateMergeValue{}
		defer clear(m)

		_ = util.TraverseSliceV1(values, func(_ int, value StateMergeValue) (bool, error) {
			switch owners, err := stateOwners(ctx, value.Key()); {
			case err != nil:
				return false, err
			default:
				_ = util.TraverseSliceV1(owners, func(_ int, owner string) (bool, error) {
					m[owner] = append(m[owner], value)

					return true, nil
				})

				return true, nil
			}
		})

		return TraverseMap(m, func(owner string, values []StateMergeValue) (bool, error) {
			return true, f(owner, values)
		})
	}
}

func serialStateOwnersFunc(workers []string, share uint8) (stateOwnersFunc, error) {
	w := uint64(len(workers))
	if w < 1 {
		return nil, errors.Errorf("empty workers")
	}

	n := uint64(share)
	if n < 1 {
		return nil, errors.Errorf("zero share")
	}

	if n > w {
		n = w
	}

	if w < 2 {
		return func(_ context.Context, stateKey string) (owners []string, _ error) {
			return workers, nil
		}, nil
	}

	last := util.NewLocked(uint64(0))

	return func(_ context.Context, stateKey string) (owners []string, _ error) {
		_, _ = last.Set(func(l uint64, _ bool) (uint64, error) {
			add, done := util.CompactAppendSlice[string](int(n))

			var i uint64

			for i = 0; i < n; i++ {
				add(workers[(l+i)%w])
			}

			owners = done()

			return l + i, nil
		})

		return owners, nil
	}, nil
}

func djb2StateOwnersFunc(workers []string, share uint8) (stateOwnersFunc, error) {
	w := uint64(len(workers))
	if w < 1 {
		return nil, errors.Errorf("empty workers")
	}

	n := uint64(share)
	if n < 1 {
		return nil, errors.Errorf("zero share")
	}

	if n > w {
		n = w
	}

	if w < 2 {
		return func(_ context.Context, stateKey string) (owners []string, _ error) {
			return workers, nil
		}, nil
	}

	seed := uint32(0)

	return func(_ context.Context, stateKey string) ([]string, error) {
		index := stringdjb2(seed, stateKey) % w

		add, done, _ := util.CompactAppendSliceV1[string](n)

		for i := uint64(0); i < n; i++ {
			add(workers[(index+i)%w])
		}

		var owners []string

		_ = done(func(s []string) error {
			owners = slices.Clone(s)

			return nil
		})

		return owners, nil
	}, nil
}

func newStateOwnersFunc(
	local string,
	workers []string,
	f stateOwnersFunc,
) (stateOwnersFunc, error) {
	if !slices.Contains(workers, local) {
		return nil, util.ErrNotFound.Errorf("local, %q not found in workers, %v", local, workers)
	}

	return func(ctx context.Context, stateKey string) (owners []string, _ error) {
			owners, err := f(ctx, stateKey)
			if err != nil {
				return nil, err
			}

			if i := slices.Index(owners, local); i >= 0 {
				owners = slices.Clone(owners)
				owners[i] = localWorkerName
			}

			return owners, nil
		},
		nil
}

func newPhaseMergerFuncsForProcessWorkerFuncs(
	args *processWorkerArgs,
	newPhaseMerger newPhaseMergerFuncs,
	getMergers phaseMergerGetMergers,
	getValues getValuesFunc,
) (
	pargs *PhaseMergerArgs,
	newf newPhaseMergerFunc,
	_ error,
) {
	switch a, n, err := newPhaseMerger(); {
	case err != nil:
		return nil, nil, err
	default:
		pargs = a
		newf = n
	}

	pargs.GetMergersFunc = getMergers
	pargs.GetValuesFunc = getValues

	extractKeys, _ := newExtractStateMergeValueKeysFunc(args.StateMergeValueKeysCacheSize)
	pargs.ExtractStateMergeValueKeysFunc = extractKeys

	pargs.SetLogging(args.Logging)

	return pargs, newf, nil
}

func newPhaseMergerFuncsForProcessWorker(
	ctx context.Context,
	args *processWorkerArgs,
	getMergers phaseMergerGetMergers,
	getValues getValuesFunc,
) (
	phaseMergerXMergeFunc,
	phaseMergerXMergeFunc,
	func(),
	phaseMergerWaitFunc,
	error,
) {
	switch pargs, newf, err := newPhaseMergerFuncsForProcessWorkerFuncs(
		args,
		args.NewPhaseMergerFuncs,
		getMergers,
		getValues,
	); {
	case err != nil:
		return nil, nil, nil, nil, err
	default:
		return newf(ctx, pargs)
	}
}

func newInsertWorkerOpFuncs(
	ctx context.Context,
	args *processWorkerArgs,
	addValuesPool func(opindex uint64, _ []StateMergeValue) bool,
	getMergers phaseMergerGetMergers,
	getValues getValuesFunc,
) (
	insertWorkerOpFunc,
	phaseMergerWaitFunc,
	error,
) {
	var merge phaseMergerXMergeFunc
	var mergeDone func()
	var mergeWait phaseMergerWaitFunc

	switch m, _, c, w, err := newPhaseMergerFuncsForProcessWorker(
		ctx,
		args,
		getMergers,
		getValues,
	); {
	case err != nil:
		return nil, nil, err
	default:
		merge = m
		mergeDone = c
		mergeWait = w
	}

	var checkWorker func(string, bool) (bool, bool)
	var closef func()

	switch i, _, c, err := newIsEnoughkWokerPhaseMergerFuncs(args.EnoughWorkers); {
	case err != nil:
		return nil, nil, err
	default:
		checkWorker = i
		closef = c
	}

	return func(worker string) (
			insertOpFunc,
			func(),
		) {
			return func(opindex uint64, values []StateMergeValue) error {
					if err := context.Cause(ctx); err != nil {
						return err
					}

					if !addValuesPool(opindex, values) {
						return nil
					}

					return merge(opindex)
				},
				func() {
					switch enough, added := checkWorker(worker, false); {
					case !added:
					case enough:
						mergeDone()
					}
				}
		},
		func(f phaseMergerWaitCallbackFunc) error {
			defer closef()

			return mergeWait(f)
		},
		nil
}

func newGetValuesFunc(valuesPool util.LockedMap[uint64, []StateMergeValue]) getValuesFunc {
	return func(opindex uint64) ([]StateMergeValue, error) {
		vs, found := valuesPool.Value(opindex)
		if !found {
			return nil, util.ErrNotFound.Errorf("values, %d", opindex)
		}

		return vs, nil
	}
}

func initialPhaseDoneFunc(
	ctx context.Context,
	args *processWorkerArgs,
	mergeInsert workerMergerPhaseDoneFunc,
	newlyMergedOps, newlyUnmergedOps []uint64,
	reasons map[uint64]string,
) (
	func() error,
	error,
) {
	if err := mergeInsert(0, localWorkerName, nil, nil); err != nil {
		return nil, err
	}

	delayed := NewContextDoneFunc(
		ctx,
		func(ctx context.Context) error {
			if err := args.BroadcastPhaseDone(ctx, 0, newlyMergedOps, newlyUnmergedOps); err != nil {
				return err
			}

			return args.UpdateReasons(ctx, -1, reasons)
		},
	)

	go func() {
		_ = delayed()
	}()

	return delayed, nil
}

func newIsLocalOpFunc(valuesPool util.LockedMap[uint64, []StateMergeValue]) isLocalOpFunc {
	return func(opindex uint64) bool {
		_, found := valuesPool.Value(opindex)

		return found
	}
}

func newStateOwnersWithLocalOpOwners(
	args *processWorkerArgs,
	f stateOwnersFunc,
) (
	_ stateOwnersFunc,
	closef func(),
) {
	stvcache := util.NewLFUGCache[ /* state key */ string, []string](args.StateOwnersCacheSize)

	var sg singleflight.Group

	return func(ctx context.Context, stateKey string) (owners []string, _ error) {
			switch i, err, _ := util.SingleflightDo(&sg, stateKey, func() ([]string, error) {
				if i, found := stvcache.Get(stateKey); found {
					return i, nil
				}

				switch i, err := f(ctx, stateKey); {
				case err != nil:
					return nil, err
				default:
					stvcache.Set(stateKey, i, 0)

					return i, nil
				}
			}); {
			case err != nil:
				return nil, err
			default:
				owners = i
			}

			return owners, nil
		},
		func() {
			stvcache.Close()
		}
}

func newBroadcastPhaseDoneFunc(
	args *processWorkerArgs,
) BroadcastPhaseDoneFunc {
	f := args.BroadcastPhaseDone

	if args.EnoughWorkers < 2 {
		return func(context.Context, int, []uint64, []uint64) error { return nil }
	}

	return f
}

func newReportResult(
	ctx context.Context,
	args *processWorkerArgs,
	mergersPool util.LockedMap[string, StateMerger],
) (
	func(unmergedOps []uint64) error,
	error,
) {
	var reportUnmerged func([]uint64) error
	var reportState func(string, base.StateValue) error
	var reportStateDone func()

	switch u, s, err := args.ReportResult(ctx); {
	case err != nil:
		return nil, err
	default:
		reportUnmerged = u

		switch s, d, err := s(); {
		case err != nil:
			return nil, err
		default:
			reportState = s
			reportStateDone = d
		}
	}

	return func(unmergedOps []uint64) error {
		var jw JobWorker

		switch j, err := NewBaseJobWorker(ctx, 2); {
		case err != nil:
			return err
		default:
			jw = j
		}

		u := slices.Clone(unmergedOps)

		if err := jw.NewJob(func(context.Context) error {
			return reportUnmerged(u)
		}); err != nil {
			return err
		}

		if err := jw.NewJob(func(context.Context) error {
			defer reportStateDone()

			return newCloseMergers(mergersPool, reportState)
		}); err != nil {
			return err
		}

		jw.JobDone()

		return jw.Wait()
	}, nil
}

func newCloseMergers(
	mergersPool util.LockedMap[string, StateMerger],
	f func(string, base.StateValue) error,
) error {
	var gerr error

	_ = mergersPool.Traverse(func(stateKey string, merger StateMerger) bool {
		switch i, err := merger.Close(); {
		case err != nil:
			gerr = err
		case i == nil:
		default:
			gerr = f(stateKey, i)
		}

		return gerr == nil
	})

	return gerr
}
