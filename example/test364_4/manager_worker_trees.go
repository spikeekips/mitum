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
	managerWorkersTreesFuncs    func(_ context.Context, workers []string, opsShare, statesShare uint64, workID string, ophs [][2]util.Hash) (
		managerWorkersOperationsFunc,
		managerWorkerUnmergedFunc,
		managerWorkerStatesFunc,
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
		NewWorkersOpsFunc: func([]string, uint64, [][2]util.Hash) (managerWorkersOperationsWorkerOpsFunc, error) {
			return nil, util.ErrNotImplemented.Errorf("NewWorkersOpsFunc")
		},
		NewOperationsTreeWriterFuncs: func(context.Context, [][2]util.Hash) (newOperationsTreeNotInStateFunc, errorFunc, newOperationsTreeNewTreeFunc, error) {
			return nil, nil, nil, util.ErrNotImplemented.Errorf("NewOperationsTreeWriterFuncs")
		},
		NewStatesTreeWriterFuncs: func(context.Context) (newStatesTreeAddNodeFunc, errorFunc, newStatesTreeNewTreeFunc, error) {
			return nil, nil, nil, util.ErrNotImplemented.Errorf("NewStatesTreeWriterFuncs")
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
		opsShare, statesShare uint64,
		workID string,
		ophs [][2]util.Hash,
	) (
		managerWorkersOperationsFunc,
		managerWorkerUnmergedFunc,
		managerWorkerStatesFunc,
		managerWorkersDoneTreesFunc,
		error,
	) {
		var statesMinEnough, statesMaxFailed uint64

		switch e, f, _, err := minEnoughMaxFailed(uint64(len(workers)), statesShare); {
		case err != nil:
			return nil, nil, nil, nil, err
		default:
			statesMinEnough = e
			statesMaxFailed = f
		}

		wctx, wctxCancel := ContextWithCancelCause(ctx)

		var opsDistribute managerWorkersOperationsFunc
		var opsUnmerged managerWorkerUnmergedFunc
		var opsTreef managerWorkersTreeFunc

		switch d, u, t, err := newManagerWorkersOperations(wctx, oargs, workers, opsShare, ophs); {
		case err != nil:
			return nil, nil, nil, nil, err
		default:
			opsDistribute = d
			opsUnmerged = u
			opsTreef = t
		}

		var statesWorker managerWorkerStatesFunc
		var statesTreef managerWorkersTreeFunc

		switch s, t, err := newManagerWorkersStates(wctx, sargs, workers, statesMinEnough, statesMaxFailed); {
		case err != nil:
			return nil, nil, nil, nil, err
		default:
			statesWorker = s
			statesTreef = t
		}

		return opsDistribute, opsUnmerged, statesWorker,
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
