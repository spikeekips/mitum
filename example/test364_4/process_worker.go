package main

import (
	"context"
	"slices"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
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
		insert workerMergerInsertFunc,
		wait func() error,
		_ error,
	)
	newWorkerMergerFuncs func() (
		*workerMergerArgs,
		newWorkerMergerFunc,
		error,
	)
	saveOperationFunc func(opindex uint64, _ Operation) error
	stateOwnersFunc   func(_ context.Context, stateKey string) (owners []string, _ error)
)

type processWorkerArgs struct {
	*logging.Logging
	NewJobWorker                 func(context.Context) (JobWorker, error)
	NewStateValuesProcessorFuncs newStateValuesProcessorFuncs
	NewWorkerMergerFuncs         newWorkerMergerFuncs
	StateOwners                  stateOwnersFunc // NOTE local is `localWorkerName`
	DistributeStateMergeValues   func(_ context.Context, worker string, opindex uint64, _ []StateMergeValue) error
	UpdateReasons                UpdateReasonsFunc
	SaveOperation                saveOperationFunc
}

func newProcessWorkerArgs() *processWorkerArgs {
	return &processWorkerArgs{
		Logging: logging.NewLogging(func(zctx zerolog.Context) zerolog.Context {
			return zctx.Str("module", "process-worker")
		}),
		NewJobWorker: func(ctx context.Context) (JobWorker, error) {
			return NewBaseJobWorker(ctx, 1<<6)
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
		SaveOperation: func(uint64, Operation) error {
			return util.ErrNotImplemented.Errorf("SaveOperation")
		},
	}
}

func newProcessWorker(ctx context.Context, args *processWorkerArgs) (
	insertOps func(ops []uint64) error,
	mergeInsert workerMergerInsertFunc,
	processWait func() error,
	_ error,
) {
	pctx, pctxCancel := context.WithCancelCause(ctx)

	var distributeInsert func(opindex uint64, _ []StateMergeValue) error
	var distributeDone func()
	var distributeWait func() error

	switch i, d, w, err := distributeStateValuesFuncs(
		pctx,
		args.StateOwners,
		args.NewJobWorker,
		args.DistributeStateMergeValues,
	); {
	case err != nil:
		return nil, nil, nil, err
	default:
		distributeInsert = i
		distributeDone = d
		distributeWait = w
	}

	var processValuesInsert func(opindex uint64) error
	var processValuesDone func()
	var processValuesWait func(func(reasons map[uint64]string) error) error

	switch i, d, w, err := newStateValuesProcessorForProcessWorker(
		pctx,
		args.NewStateValuesProcessorFuncs,
		args.SaveOperation,
		distributeInsert,
	); {
	case err != nil:
		return nil, nil, nil, err
	default:
		processValuesInsert = i
		processValuesDone = d
		processValuesWait = w
	}

	var mergeWait func() error

	switch i, w, err := newWorkerMergerFuncsForProcessWorker(ctx, args.NewWorkerMergerFuncs); {
	case err != nil:
		return nil, nil, nil, err
	default:
		mergeInsert = i
		mergeWait = w
	}

	return func(ops []uint64) error {
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
				if err := processValuesInsert(ops[i]); err != nil {
					pctxCancel(err)

					return err
				}
			}

			processValuesDone()

			return nil
		},
		mergeInsert,
		func() error {
			defer func() {
				processValuesDone()

				_ = processValuesWait(nil)
			}()

			defer func() {
				distributeDone()

				_ = distributeWait()
			}()

			defer func() {
				_ = mergeWait()
			}()

			args.Log().Debug().Msg("> process state values wait")

			if err := processValuesWait(func(reasons map[uint64]string) error {
				return args.UpdateReasons(pctx, -1, reasons)
			}); err != nil {
				return err
			}

			args.Log().Debug().Msg("< process state values wait done")

			distributeDone()

			args.Log().Debug().Msg("> distribute wait")

			if err := distributeWait(); err != nil {
				return err
			}

			args.Log().Debug().Msg("< distribute wait done")

			args.Log().Debug().Msg("> merge wait")
			defer args.Log().Debug().Msg("< merge wait done")

			return mergeWait()
		},
		nil
}

func newStateValuesProcessorForProcessWorker(
	ctx context.Context,
	newProcessor newStateValuesProcessorFuncs,
	opf saveOperationFunc,
	distribute func(opindex uint64, _ []StateMergeValue) error,
) (
	insert func(opindex uint64) error,
	insertDone func(),
	wait func(func(reasons map[uint64]string) error) error,
	_ error,
) {
	var args *stateValuesProcessorArgs
	var newf newStateValuesProcessorFunc

	switch a, n, err := newProcessor(); {
	case err != nil:
		return nil, nil, nil, err
	default:
		args = a
		newf = n

		args.ProcessedOp = func(ctx context.Context, opindex uint64, op Operation, values []StateMergeValue) error {
			if err := opf(opindex, op); err != nil {
				return err
			}

			return distribute(opindex, values)
		}
	}

	return newf(ctx, args)
}

func newWorkerMergerFuncsForProcessWorker(
	ctx context.Context,
	newWorker newWorkerMergerFuncs,
) (
	insert workerMergerInsertFunc,
	wait func() error,
	_ error,
) {
	switch a, f, err := newWorker(); {
	case err != nil:
		return nil, nil, err
	default:
		return f(ctx, a)
	}
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
				owners[i] = localWorkerName
			}

			return owners, nil
		},
		nil
}
