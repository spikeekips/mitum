package main

import (
	"context"

	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/fixedtree"
	"github.com/spikeekips/mitum/util/logging"
)

type (
	managerWorkersDoneTreesFunc func() (opsTree, statesTree fixedtree.Tree, _ error)
	managerWorkersTreesFuncs    func(_ context.Context, workers []string, minEnough, maxFailed uint64, workID string, ophs [][2]util.Hash) (
		managerWorkersOperationsWorkerUnmergedFunc,
		managerWorkersWorkerStatesFunc,
		managerWorkersDoneTreesFunc,
		error,
	)
)

type managerWorkersTreesArgs struct {
	*logging.Logging
	NewCheckWorkersFunc          checkEnoughFailedWorkersFuncs
	NewWorkersOpsFunc            managerWorkersOperationsWorkersOpsFunc
	NewOperationsTreeWriterFuncs newOperationsTreeFuncs
	NewStatesTreeWriterFuncs     newStatesTreeFuncs
	DistributeWorkerOps          managerWorkerOperationsFunc
}

func newManagerWorkersTreesArgs() *managerWorkersTreesArgs {
	return &managerWorkersTreesArgs{
		Logging: logging.NewLogging(func(zctx zerolog.Context) zerolog.Context {
			return zctx.Str("module", "manager-workers-trees")
		}),
		NewCheckWorkersFunc: func(_, maxFailed uint64) (
			checkEnoughFailedWorkerFunc,
			func(),
			error,
		) {
			return nil, nil, util.ErrNotImplemented.Errorf("NewCheckWorkers")
		},
		NewWorkersOpsFunc: func([]string, [][2]util.Hash) (managerWorkersOperationsWorkerOpsFunc, error) {
			return nil, util.ErrNotImplemented.Errorf("NewWorkersOpsFunc")
		},
		NewOperationsTreeWriterFuncs: func(context.Context, [][2]util.Hash) (newOperationsTreeNotInStateFunc, errorFunc, newOperationsTreeNewTreeFunc, error) {
			return nil, nil, nil, util.ErrNotImplemented.Errorf("NewOperationsTreeWriterFuncs")
		},
		NewStatesTreeWriterFuncs: func(context.Context) (newStatesTreeAddNodeFunc, errorFunc, newStatesTreeNewTreeFunc, error) {
			return nil, nil, nil, util.ErrNotImplemented.Errorf("NewStatesTreeWriterFuncs")
		},
		DistributeWorkerOps: func(context.Context, string, func(managerWorkerOperationFunc) error) error {
			return util.ErrNotImplemented.Errorf("DistributeWorkerOps")
		},
	}
}

func newFuncManagerWorkersTrees(args *managerWorkersTreesArgs) managerWorkersTreesFuncs {
	oargs := newManagerWorkersOperationsArgs()
	oargs.NewCheckWorkersFunc = args.NewCheckWorkersFunc
	oargs.NewWorkersOpsFunc = args.NewWorkersOpsFunc
	oargs.NewTreeWriterFuncs = args.NewOperationsTreeWriterFuncs

	sargs := newManagerWorkersStatesArgs()
	sargs.NewCheckWorkersFunc = args.NewCheckWorkersFunc
	sargs.NewTreeWriterFuncs = args.NewStatesTreeWriterFuncs

	oargs.SetLogging(args.Logging)
	sargs.SetLogging(args.Logging)

	return func(
		ctx context.Context,
		workers []string,
		minEnough, maxFailed uint64,
		workID string,
		ophs [][2]util.Hash,
	) (
		managerWorkersOperationsWorkerUnmergedFunc,
		managerWorkersWorkerStatesFunc,
		managerWorkersDoneTreesFunc,
		error,
	) {
		wctx, wctxCancel := ContextWithCancelCause(ctx)

		var opsDistribute func(managerWorkerOperationsFunc) error
		var opsUnmerged managerWorkersOperationsWorkerUnmergedFunc
		var opsTreef managerWorkersTreeFunc

		switch d, u, t, err := newManagerWorkersOperations(wctx, oargs, workers, maxFailed, ophs); {
		case err != nil:
			return nil, nil, nil, err
		default:
			opsDistribute = d
			opsUnmerged = u
			opsTreef = t
		}

		var statesWorker managerWorkersWorkerStatesFunc
		var statesTreef managerWorkersTreeFunc

		switch s, t, err := newManagerWorkersStates(wctx, sargs, workers, minEnough, maxFailed); {
		case err != nil:
			return nil, nil, nil, err
		default:
			statesWorker = s
			statesTreef = t
		}

		go func() {
			_ = contextCauseError(wctx, ifErrErrorFunc(wctxCancel), func(context.Context) error {
				return opsDistribute(func(ctx context.Context, worker string, f func(managerWorkerOperationFunc) error) error {
					return args.DistributeWorkerOps(ctx, worker, f)
				})
			})
		}()

		return opsUnmerged, statesWorker,
			func() (opsTree, statesTree fixedtree.Tree, _ error) {
				return opsTree, statesTree, contextCauseError(wctx, ifErrErrorFunc(wctxCancel), func(context.Context) error {
					if err := opsTreef(func(tr fixedtree.Tree) error {
						opsTree = tr

						return nil
					}); err != nil {
						return err
					}

					if err := statesTreef(func(tr fixedtree.Tree) error {
						statesTree = tr

						return nil
					}); err != nil {
						return err
					}

					return nil
				})
			},
			nil
	}
}

func newArgsForManagerWorkersTrees(args *managerWorkersTreesArgs, workID string) (
	oargs *managerWorkersOperationsArgs,
	sargs *managerWorkersStatesArgs,
) {
	oargs = newManagerWorkersOperationsArgs()
	oargs.NewCheckWorkersFunc = args.NewCheckWorkersFunc
	oargs.NewWorkersOpsFunc = args.NewWorkersOpsFunc
	oargs.NewTreeWriterFuncs = args.NewOperationsTreeWriterFuncs

	sargs = newManagerWorkersStatesArgs()
	sargs.NewCheckWorkersFunc = args.NewCheckWorkersFunc
	sargs.NewTreeWriterFuncs = args.NewStatesTreeWriterFuncs

	oargs.SetLogging(args.Logging)
	sargs.SetLogging(args.Logging)

	return oargs, sargs
}
