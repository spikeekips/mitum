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
		insert stateValuesProcessorInsertFunc,
		insertDone func(),
		wait waitWithReasonsFunc,
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

	insertOpFunc        func(opindex uint64, oph util.Hash, _ []StateMergeValue) error
	insertWorkerOpFuncs func(worker string) (insertOpFunc, func())

	addOpValuesFunc             func(opindex uint64, oph util.Hash, _ []StateMergeValue) bool
	stateOwnersFunc             func(_ context.Context, stateKey string) (owners []string, _ error)
	getValuesFunc               func(opindex uint64) ([]StateMergeValue, error)
	getOpHashFunc               func(opindex uint64) (util.Hash, error)
	filterOpsFunc               func([]uint64, []uint64) ([]uint64, []uint64)
	processWorkerInitialOpFuncs func() (
		insert stateValuesProcessorInsertFunc,
		insertDone func(),
	)
	distributeStateMergeValuesFunc  func(_ context.Context, worker string, opindex uint64, oph util.Hash, _ []StateMergeValue) error
	distributeStateMergeValuesFuncs func() (
		distributeStateMergeValuesFunc,
		func(context.Context) error, // NOTE send to all workers
		error,
	)
	reportStateFunc        func(_ context.Context, stateKey string, _ util.Hash) error
	reportStatesFuncs      func() (reportStateFunc, func(), error)
	saveOperationFunc      func(opindex uint64, _ Operation) error
	saveOperationFuncs     func() (saveOperationFunc, func(error) error, error)
	saveStateFunc          func(base.State) error
	saveStateFuncs         func() (saveStateFunc, func(error) error, error)
	saveReasonFunc         func(opindex uint64, reason string) error
	saveReasonsDoneFunc    func(unmergedOps []uint64, _ saveReasonFunc) error
	saveReasonsFuncs       func() (saveReasonFunc, func(error) error, error)
	processWorkerSaveFuncs func() (saveOperationFuncs, saveStateFuncs, saveReasonsFuncs, error)
)

type processWorkerArgs struct {
	*logging.Logging
	NewJobWorker                 func(context.Context) (JobWorker, error)
	NewPhaseMergerFuncs          newPhaseMergerFuncs
	GetStateFunc                 func(stateKey string) (base.State, bool, error)
	NewStateValuesProcessorFuncs newStateValuesProcessorFuncs
	NewWorkerMergerFuncs         newWorkerMergerFuncs
	StateOwners                  stateOwnersFunc // NOTE local is `localWorkerName`
	DistributeStateMergeValues   distributeStateMergeValuesFuncs
	StateMergeValueKeysCacheSize int
	BroadcastPhaseDone           BroadcastPhaseDoneFunc
	ReportFuncs                  func(context.Context) (
		workerMergerUnmergedOpsFunc,
		reportStatesFuncs,
		error,
	)
	SaveFuncs            processWorkerSaveFuncs
	EnoughWorkers        uint64 // NOTE include local worker
	StateOwnersCacheSize int
	filterOps            filterOpsFunc
	MaxOpsSize           uint64
	local                string // FIXME remove
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
		DistributeStateMergeValues: func() (
			distributeStateMergeValuesFunc,
			func(context.Context) error,
			error,
		) {
			return nil, nil, util.ErrNotImplemented.Errorf("DistributeStateMergeValues")
		},
		ReportFuncs: func(context.Context) (
			workerMergerUnmergedOpsFunc,
			reportStatesFuncs,
			error,
		) {
			return nil, nil, util.ErrNotImplemented.Errorf("ReportResult")
		},
		StateMergeValueKeysCacheSize: 1 << 9,
		BroadcastPhaseDone: func(context.Context, int, []uint64, []uint64) error {
			return util.ErrNotImplemented.Errorf("BroadcastPhaseDone")
		},
		StateOwnersCacheSize: 1 << 9,
		filterOps:            func([]uint64, []uint64) ([]uint64, []uint64) { return nil, nil },
		SaveFuncs: func() (
			saveOperationFuncs,
			saveStateFuncs,
			saveReasonsFuncs,
			error,
		) {
			return nil, nil, nil, util.ErrNotImplemented.Errorf("SaveFuncs")
		},
		MaxOpsSize: 1 << 9,
	}
}

func newProcessWorker(
	ctx context.Context,
	args *processWorkerArgs,
	height base.Height,
) (
	initialOps processWorkerInitialOpFuncs,
	insertWorkerOp insertWorkerOpFuncs,
	workerPhaseDone workerMergerPhaseDoneFunc,
	processWait workerMergerWaitFunc,
	_ error,
) {
	if args.EnoughWorkers < 1 {
		return nil, nil, nil, nil, util.ErrInvalid.Errorf("EnoughWorkers should be at least 1")
	}

	stateOwners, closeStateOwners := newStateOwnersWithLocalOpOwners(args, args.StateOwners)
	args.StateOwners = stateOwners
	args.BroadcastPhaseDone = newBroadcastPhaseDoneFunc(args)

	var valuesPoolAdd addOpValuesFunc
	var valuesPoolGetValues getValuesFunc
	var valuesPoolGetOpHash getOpHashFunc
	var valuesPoolDone func()

	switch a, v, h, d, err := newValuesPoolFuncs(args); {
	case err != nil:
		return nil, nil, nil, nil, err
	default:
		valuesPoolAdd = a
		valuesPoolGetValues = v
		valuesPoolGetOpHash = h
		valuesPoolDone = d
	}

	var mergersPoolGetMergers phaseMergerGetMergers
	var mergersPoolTraverse func(func(size uint64, stateKey string, merger StateMerger) bool)
	var mergersPoolDone func()

	switch g, t, d, err := newMergersPoolFuncs(args); {
	case err != nil:
		return nil, nil, nil, nil, err
	default:
		mergersPoolGetMergers = g
		mergersPoolTraverse = t
		mergersPoolDone = d
	}

	args.filterOps = newFilterOpsFunc(valuesPoolGetValues)

	pctx, pctxCancel := context.WithCancelCause(ctx)

	collectOps, _, collectOpsDone := newCollectXMergedOps()
	saveReasons, saveReasonsDone := newSaveReasonsFuncs(pctx)
	saveOps, saveOpsDone := newSaveOperationFuncs()

	var setStateOp phaseMergerSetStateOp
	var report func(unmergedOps []uint64, f workerMergerUnmergedOpsFunc) error
	var save func() error

	switch s, r, v, err := newReportResult(
		pctx,
		args,
		height,
		mergersPoolTraverse,
		valuesPoolGetOpHash,
		func(stateKey string) (util.Hash, bool, error) {
			i, found, err := args.GetStateFunc(stateKey)
			if err != nil || !found {
				return nil, found, err
			}

			return i.Hash(), true, nil
		},
		saveOpsDone,
		saveReasonsDone,
		args.SaveFuncs,
	); {
	case err != nil:
		return nil, nil, nil, nil, err
	default:
		setStateOp = s
		report = func(unmergedOps []uint64, f workerMergerUnmergedOpsFunc) error {
			collectOps(nil, unmergedOps)

			return collectOpsDone(func(unmergedOps []uint64) error {
				if f != nil {
					if err := f(unmergedOps); err != nil {
						return err
					}
				}

				return r(unmergedOps)
			})
		}
		save = v
	}

	var initialMergeWait phaseMergerWaitFunc

	switch i, c, err := newInsertWorkerOpFuncs(
		pctx,
		args,
		valuesPoolAdd,
		mergersPoolGetMergers,
		valuesPoolGetValues,
		setStateOp,
	); {
	case err != nil:
		return nil, nil, nil, nil, err
	default:
		insertWorkerOp = i
		initialMergeWait = c
	}

	var initialOpFuncs processWorkerInitialOpFuncs
	var processValuesWait waitWithReasonsFunc

	switch i, w, err := newStateValuesProcessWorker(
		pctx,
		args.NewStateValuesProcessorFuncs,
		args.StateOwners,
		insertWorkerOp,
		args.DistributeStateMergeValues,
		saveOps,
	); {
	case err != nil:
		return nil, nil, nil, nil, err
	default:
		initialOpFuncs = i
		processValuesWait = w
	}

	var mergeReady func() error
	var mergeWait workerMergerWaitFunc

	switch r, i, w, err := newWorkerMergerFuncsForProcessWorker(
		pctx,
		args,
		mergersPoolGetMergers,
		valuesPoolGetValues,
		setStateOp,
		saveReasons,
	); {
	case err != nil:
		return nil, nil, nil, nil, err
	default:
		mergeReady = r
		workerPhaseDone = i
		mergeWait = w
	}

	processWaitLog := FuncErrorLog(args.Log(), "process")
	initialMergeWaitLog := FuncErrorLog(args.Log(), "initial merge")
	mergeWaitLog := FuncErrorLog(args.Log(), "merge")

	return initialOpFuncs,
		insertWorkerOp,
		workerPhaseDone,
		func(f workerMergerUnmergedOpsFunc) error {
			defer func() {
				pctxCancel(nil)

				closeStateOwners()
				_ = collectOpsDone(nil)
				_ = saveOpsDone(nil)
				valuesPoolDone()
				mergersPoolDone()
				_ = mergeWait(nil)
			}()

			if err := processWaitLog(processValuesWait(func(reasons map[uint64]string) error {
				return saveReasons(-1, reasons)
			})); err != nil {
				return err
			}

			if err := initialMergeWaitLog(initialMergeWait(func(newlyMergedOps, newlyUnmergedOps []uint64, reasons map[uint64]string) error {
				if err := mergeReady(); err != nil {
					return err
				}

				if err := workerPhaseDone(0, localWorkerName, newlyMergedOps, newlyUnmergedOps); err != nil {
					return err
				}

				collectOps(newlyMergedOps, newlyUnmergedOps)

				return saveReasons(-1, reasons)
			})); err != nil {
				return err
			}

			if err := mergeWaitLog(mergeWait(func(unmergedOps []uint64) error {
				return report(unmergedOps, f)
			})); err != nil {
				return err
			}

			return save()
		},
		nil
}

func newWorkerMergerFuncsForProcessWorker(
	ctx context.Context,
	args *processWorkerArgs,
	getMergers phaseMergerGetMergers,
	getValues getValuesFunc,
	setStateOp phaseMergerSetStateOp,
	saveReasons setReasonsFunc,
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

	wargs.local = args.local
	wargs.SetReasons = saveReasons
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
				setStateOp,
			)
			if err != nil {
				return nil, nil, err
			}

			return pargs, newf, nil
		}

		wpargs.FilterMergeOps = args.filterOps
		wpargs.EnoughWorkers = args.EnoughWorkers

		wpargs.SetLogging(args.Logging)

		return wpargs, newf, nil
	}

	wargs.SetLogging(args.Logging)

	pool := util.NewSingleLockedMap[ /* phase */ int, map[ /* worker */ string][2][]uint64]()

	var newLock sync.RWMutex
	var insert workerMergerPhaseDoneFunc
	var wait workerMergerWaitFunc

	return func() error {
			newLock.Lock()
			defer newLock.Unlock()

			defer pool.Close()

			switch i, w, err := newf(ctx, wargs); {
			case err != nil:
				return err
			default:
				insert = i
				wait = w
			}

			if pool.Len() > 0 {
				var gerr error

				_ = pool.Traverse(func(phase int, m map[string][2][]uint64) bool {
					for worker := range m {
						if gerr = insert(phase, worker, m[worker][0], m[worker][1]); gerr != nil {
							return false
						}
					}

					return gerr == nil
				})
				if gerr != nil {
					return gerr
				}
			}

			return nil
		},
		func(phase int, worker string, mergeOps, unmergeOps []uint64) error {
			newLock.RLock()
			defer newLock.RUnlock()

			if insert == nil {
				if _, _, err := pool.Set(phase, func(prev map[string][2][]uint64, prevfound bool) (map[string][2][]uint64, error) {
					switch {
					case !prevfound:
						prev = map[string][2][]uint64{}
					default:
						if _, found := prev[worker]; found {
							return nil, util.ErrFound.Errorf("merge insert worker, %q", worker)
						}
					}

					prev[worker] = [2][]uint64{mergeOps, unmergeOps}

					return prev, nil
				}); err != nil {
					return err
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

func serialStateOwnersFunc(workers []string, share uint8) (stateOwnersFunc, error) { // FIXME share states from 0
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

	m := util.NewSingleLockedMap[string, []string]()

	var last uint64

	return func(_ context.Context, stateKey string) (owners []string, _ error) {
		_ = m.GetOrCreate(
			stateKey,
			func(i []string, _ bool) error {
				owners = i

				return nil
			},
			func() ([]string, error) {
				add, done := util.CompactAppendSlice[string](int(n))

				var i uint64

				for i = 0; i < n; i++ {
					add(workers[(last+i)%w])
				}

				last += i

				return done(), nil
			},
		)

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
	setStateOp phaseMergerSetStateOp,
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
	pargs.SetStateOp = setStateOp

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
	setStateOp phaseMergerSetStateOp,
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
		setStateOp,
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
	addOpValuesFunc func(opindex uint64, oph util.Hash, _ []StateMergeValue) bool,
	getMergers phaseMergerGetMergers,
	getValues getValuesFunc,
	setStateOp phaseMergerSetStateOp,
) (
	insertWorkerOpFuncs,
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
		setStateOp,
	); {
	case err != nil:
		return nil, nil, err
	default:
		merge = m
		mergeDone = c
		mergeWait = w
	}

	var checkWorker func(string, bool, func(bool, bool) error) error
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
			return func(opindex uint64, oph util.Hash, values []StateMergeValue) error {
					if err := context.Cause(ctx); err != nil {
						return err
					}

					if !addOpValuesFunc(opindex, oph, values) {
						return nil
					}

					return merge(opindex)
				},
				func() {
					_ = checkWorker(worker, false, func(enough, ok bool) error {
						if enough && ok {
							mergeDone()
						}

						return nil
					})
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

func newFilterOpsFunc(f getValuesFunc) filterOpsFunc {
	return func(m, u []uint64) ([]uint64, []uint64) {
		return util.FilterSlice(m, func(opindex uint64) bool {
				_, err := f(opindex)

				return err == nil
			}),
			util.FilterSlice(u, func(opindex uint64) bool {
				_, err := f(opindex)

				return err == nil
			})
	}
}

func newStateOwnersWithLocalOpOwners(
	args *processWorkerArgs,
	f stateOwnersFunc,
) (
	_ stateOwnersFunc,
	closef func(),
) {
	stvcache := util.NewLRUGCache[ /* state key */ string, []string](args.StateOwnersCacheSize)

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

func newStateOpsFuncs(args *processWorkerArgs) (
	phaseMergerSetStateOp,
	func(func(func(stateKey string) []uint64) error) error,
	error,
) {
	var m util.LockedMap[string, map[uint64]uint64]

	switch i, err := util.NewShardedMap[string, map[uint64]uint64](args.MaxOpsSize, nil); {
	case err != nil:
		return nil, nil, err
	default:
		m = i
	}

	filter := func(stateKey string) (ops []uint64) {
		switch i, found := m.Value(stateKey); {
		case !found, len(i) < 1:
			return nil
		default:
			add, done, _ := util.CompactAppendSliceV1[uint64](uint64(len(i)))

			_ = TraverseMap(i, func(opindex, count uint64) (bool, error) {
				if count > 0 {
					add(opindex)
				}

				return true, nil
			})

			_ = done(func(s []uint64) error {
				ops = slices.Clone(s)

				return nil
			})

			return ops
		}
	}

	return func(opindex uint64, stateKeys []string, x xMergeFlag) {
			util.TraverseSliceV1(stateKeys, func(_ int, stateKey string) (bool, error) {
				_, _, _ = m.Set(stateKey, func(prev map[uint64]uint64, found bool) (map[uint64]uint64, error) {
					if !found {
						prev = map[uint64]uint64{}
					}

					switch {
					case x == mergeFlag:
						prev[opindex]++
					case x == unmergeFlag:
						prev[opindex]--
					}

					return prev, nil
				})

				return true, nil
			})
		},
		func(f func(func(stateKey string) []uint64) error) error {
			defer m.Close()

			if f == nil {
				return nil
			}

			return f(filter)
		},
		nil
}

func newValuesPoolFuncs(args *processWorkerArgs) (
	addOpValuesFunc,
	getValuesFunc,
	getOpHashFunc,
	func(),
	error,
) {
	var valuesPool util.LockedMap[uint64, [2]interface{}]

	switch i, err := util.NewShardedMap[uint64, [2]interface{}](args.MaxOpsSize, nil); {
	case err != nil:
		return nil, nil, nil, nil, err
	default:
		valuesPool = i
	}

	return func(opindex uint64, oph util.Hash, values []StateMergeValue) bool {
			return valuesPool.SetValue(opindex, [2]interface{}{oph, values})
		},
		func(opindex uint64) ([]StateMergeValue, error) {
			switch i, found := valuesPool.Value(opindex); {
			case !found:
				return nil, util.ErrNotFound.Errorf("values, %d", opindex)
			default:
				return i[1].([]StateMergeValue), nil
			}
		},
		func(opindex uint64) (util.Hash, error) {
			switch i, found := valuesPool.Value(opindex); {
			case !found:
				return nil, util.ErrNotFound.Errorf("op hash, %d", opindex)
			default:
				return i[0].(util.Hash), nil
			}
		},
		valuesPool.Close,
		nil
}

func newMergersPoolFuncs(args *processWorkerArgs) (
	phaseMergerGetMergers,
	func(func(size uint64, stateKey string, merger StateMerger) bool),
	func(),
	error,
) {
	var mergersPool util.LockedMap[string, StateMerger]

	switch i, err := util.NewShardedMap[string, StateMerger](args.MaxOpsSize, nil); {
	case err != nil:
		return nil, nil, nil, err
	default:
		mergersPool = i
	}

	getMergers, closeGetMergers := newGetMergersFunc(mergersPool, args.GetStateFunc)

	return getMergers,
		func(f func(uint64, string, StateMerger) bool) {
			size := uint64(mergersPool.Len())

			_ = mergersPool.Traverse(func(stateKey string, merger StateMerger) bool {
				return f(size, stateKey, merger)
			})
		},
		func() {
			closeGetMergers()
			mergersPool.Close()
		},
		nil
}

func newSaveOperationFuncs() (
	saveOperationFunc,
	func(func(opindex uint64, _ Operation) error) error,
) {
	ops := util.NewSingleLockedMap[uint64, Operation]()

	return func(opindex uint64, op Operation) error {
			if !ops.SetValue(opindex, op) {
				return util.ErrFound.Errorf("save operaton, %d", opindex)
			}

			return nil
		},
		func(f func(uint64, Operation) error) error {
			defer ops.Close()

			if f == nil {
				return nil
			}

			var gerr error

			_ = ops.Traverse(func(opindex uint64, op Operation) bool {
				gerr = f(opindex, op)

				return gerr == nil
			})

			return gerr
		}
}

func newSaveReasonsFuncs(ctx context.Context) (
	setReasonsFunc,
	saveReasonsDoneFunc,
) {
	m := util.NewSingleLockedMap[uint64, [2]interface{}]()

	return func(phase int, reasons map[uint64]string) error {
			switch {
			case m.Len() < 1:
				_ = TraverseMap(reasons, func(opindex uint64, reason string) (bool, error) {
					_ = m.SetValue(opindex, [2]interface{}{phase, reason})

					return true, nil
				})
			default:
				_ = TraverseMap(reasons, func(opindex uint64, reason string) (bool, error) {
					_, _, _ = m.Set(opindex, func(prev [2]interface{}, found bool) (v [2]interface{}, _ error) {
						if found && phase <= prev[0].(int) && reason == prev[1].(string) {
							return v, util.ErrLockedSetIgnore
						}

						return [2]interface{}{phase, reason}, nil
					})

					return true, nil
				})
			}

			return nil
		},
		func(unmergedOps []uint64, f saveReasonFunc) error {
			defer m.Close()

			if m.Len() < 1 || len(unmergedOps) < 1 {
				return nil
			}

			var jw JobWorker

			switch i, err := NewBaseJobWorker(ctx, int64(m.Len())); {
			case err != nil:
				return err
			default:
				jw = i
			}

			var gerr error

			_ = m.Traverse(func(opindex uint64, v [2]interface{}) bool {
				gerr = jw.NewJob(func(context.Context) error {
					if slices.Index(unmergedOps, opindex) < 0 {
						return nil
					}

					return f(opindex, v[1].(string))
				})

				return gerr == nil
			})
			if gerr != nil {
				return gerr
			}

			jw.JobDone()

			return jw.Wait()
		}
}

func newReportResult(
	ctx context.Context,
	args *processWorkerArgs,
	height base.Height,
	mergersPoolTraverse func(func(size uint64, stateKey string, merger StateMerger) bool),
	getOpHash getOpHashFunc,
	getPrevState func(stateKey string) (util.Hash, bool, error),
	saveOpsDone func(func(opindex uint64, _ Operation) error) error,
	saveReasonsDone saveReasonsDoneFunc,
	saveFuncs processWorkerSaveFuncs,
) (
	phaseMergerSetStateOp,
	workerMergerUnmergedOpsFunc,
	func() error,
	error,
) {
	var setStateOp phaseMergerSetStateOp
	var setStateOpDone func(func(func(stateKey string) []uint64) error) error

	switch s, d, err := newStateOpsFuncs(args); {
	case err != nil:
		return nil, nil, nil, err
	default:
		setStateOp = s
		setStateOpDone = d
	}

	var reportUnmerged func([]uint64) error
	var reportState func(context.Context, string, util.Hash) error
	var reportStateDone func()

	switch u, s, err := args.ReportFuncs(ctx); {
	case err != nil:
		return nil, nil, nil, err
	default:
		reportUnmerged = u

		switch s, d, err := s(); {
		case err != nil:
			return nil, nil, nil, err
		default:
			reportState = s
			reportStateDone = d
		}
	}

	var statesPool util.LockedMap[ /* stateKey */ string, base.State]
	var statesPoolOnce sync.Once

	saveStatesPool := func(size uint64, st base.State) error {
		var gerr error

		statesPoolOnce.Do(func() {
			statesPool, gerr = util.NewLockedMap[string, base.State](size, nil)
		})

		if gerr != nil {
			return gerr
		}

		if !statesPool.SetValue(st.Key(), st) {
			return util.ErrFound.Errorf("save state, %q", st.Key())
		}

		return nil
	}

	var ofs saveOperationFuncs
	var sfs saveStateFuncs
	var rfs saveReasonsFuncs

	switch o, s, r, err := saveFuncs(); {
	case err != nil:
		return nil, nil, nil, err
	default:
		ofs = o
		sfs = s
		rfs = r
	}

	var unmergedOps []uint64

	return setStateOp,
		func(uops []uint64) error {
			defer reportStateDone()

			unmergedOps = slices.Clone(uops)

			var jw JobWorker

			switch j, err := args.NewJobWorker(ctx); {
			case err != nil:
				return err
			default:
				defer j.Close()

				jw = j
			}

			if err := jw.NewJob(func(context.Context) error {
				return reportUnmerged(slices.Clone(uops))
			}); err != nil {
				return err
			}

			return setStateOpDone(func(getStateOps func(stateKey string) []uint64) error {
				traverse := func(f func(stateKey string, merger StateMerger) (base.State, error)) error {
					var gerr error

					mergersPoolTraverse(func(size uint64, stateKey string, merger StateMerger) bool {
						gerr = jw.NewJob(func(context.Context) error {
							switch st, err := f(stateKey, merger); {
							case err != nil:
								return err
							case st == nil:
								return nil
							default:
								return saveStatesPool(size, st)
							}
						})

						return gerr == nil
					})

					return gerr
				}

				if err := newCloseMergers(
					ctx,
					height,
					traverse,
					getStateOps,
					getOpHash,
					getPrevState,
					reportState,
				); err != nil {
					return err
				}

				jw.JobDone()

				return jw.Wait()
			})
		},
		func() error {
			var jw JobWorker

			switch j, err := args.NewJobWorker(ctx); {
			case err != nil:
				return err
			default:
				defer j.Close()

				jw = j
			}

			if err := jw.NewJob(func(ctx context.Context) error {
				switch save, done, err := ofs(); {
				case err != nil:
					return done(err)
				default:
					if err := saveOpsDone(save); err != nil {
						return done(err)
					}

					return done(nil)
				}
			}); err != nil {
				return err
			}

			if err := jw.NewJob(func(ctx context.Context) error {
				switch save, done, err := rfs(); {
				case err != nil:
					return done(err)
				default:
					if err := saveReasonsDone(unmergedOps, save); err != nil {
						return done(err)
					}

					return done(nil)
				}
			}); err != nil {
				return err
			}

			if statesPool != nil {
				if err := jw.NewJob(func(ctx context.Context) error {
					switch save, done, err := sfs(); {
					case err != nil:
						return done(err)
					default:
						var gerr error

						_ = statesPool.Traverse(func(_ string, st base.State) bool {
							gerr = save(st)

							return gerr == nil
						})

						return done(gerr)
					}
				}); err != nil {
					return err
				}
			}

			jw.JobDone()

			return jw.Wait()
		},
		nil
}

func newCloseMergers(
	ctx context.Context,
	height base.Height,
	traverse func(func(stateKey string, merger StateMerger) (base.State, error)) error,
	getStateOp func(stateKey string) []uint64,
	getOpHash getOpHashFunc,
	getPrevState func(stateKey string) (util.Hash, bool, error),
	report func(context.Context, string, util.Hash) error,
) error {
	getStateOps := func(stateKey string) (ophs []util.Hash, _ error) {
		ops := getStateOp(stateKey)
		if len(ops) < 1 {
			return nil, nil
		}

		var add util.CompactAppendSliceAddFunc[util.Hash]
		var done util.CompactAppendSliceDoneFunc[util.Hash]

		switch a, d, err := util.CompactAppendSliceV1[util.Hash](uint64(len(ops))); {
		case err != nil:
			return nil, err
		default:
			add = a
			done = d
		}

		_ = util.TraverseSliceV1(ops, func(_ int, opindex uint64) (bool, error) {
			switch i, err := getOpHash(opindex); {
			case err != nil:
				return false, err
			default:
				add(i)
			}

			return true, nil
		})

		_ = done(func(s []util.Hash) error {
			ophs = slices.Clone(s)

			return nil
		})

		return ophs, nil
	}

	return traverse(func(stateKey string, merger StateMerger) (base.State, error) {
		var ophs []util.Hash

		switch i, err := getStateOps(stateKey); {
		case err != nil:
			return nil, err
		case len(i) < 1:
			return nil, nil
		default:
			ophs = i
		}

		var prevst util.Hash

		switch i, found, err := getPrevState(stateKey); {
		case err != nil:
			return nil, err
		case !found:
		default:
			prevst = i
		}

		switch v, err := merger.Close(); {
		case err != nil:
			return nil, err
		case v == nil:
			return nil, nil
		default:
			st := base.NewBaseState(height, stateKey, v, prevst, ophs)

			return st, report(ctx, stateKey, st.Hash())
		}
	})
}
