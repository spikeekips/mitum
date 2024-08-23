package main

import (
	"context"

	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/fixedtree"
	"github.com/spikeekips/mitum/util/logging"
)

type (
	managerWorkersTreeSaveFunc  func() (ops, states base.BlockMapItem, _ error)
	managerWorkersTreeRootsFunc func() (opsTreeRoot, statesTreeRoot util.Hash, save managerWorkersTreeSaveFunc, _ error)
	managerWorkersTreesFuncs    func(_ context.Context, workers []string, minEnough, maxFailed uint64, workID string, ophs [][2]util.Hash) (
		managerWorkersOperationsWorkerUnmergedFunc,
		managerWorkersWorkerStatesFunc,
		managerWorkersTreeRootsFunc,
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
	WriteBlockItemOperationsTree blockItemWriterNewWriteTreeFunc
	WriteBlockItemStatesTree     blockItemWriterNewWriteTreeFunc
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
		WriteBlockItemOperationsTree: func(context.Context, string, fixedtree.Tree) (base.BlockMapItem, error) {
			return nil, util.ErrNotImplemented.Errorf("WriteBlockItemOperationsTree")
		},
		WriteBlockItemStatesTree: func(context.Context, string, fixedtree.Tree) (base.BlockMapItem, error) {
			return nil, util.ErrNotImplemented.Errorf("WriteBlockItemStatesTree")
		},
	}
}

func newManagerWorkersTreesFuncs(args *managerWorkersTreesArgs) managerWorkersTreesFuncs {
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
		managerWorkersTreeRootsFunc,
		error,
	) {
		wctx, wctxCancel := ContextWithCancelCause(ctx)

		var opsDistribute func(managerWorkerOperationsFunc) error
		var opsUnmerged managerWorkersOperationsWorkerUnmergedFunc
		var opsTreef managerWorkersTreeFunc

		switch d, u, t, err := newManagerWorkersOperationsFuncs(wctx, oargs, workers, maxFailed, ophs); {
		case err != nil:
			return nil, nil, nil, err
		default:
			opsDistribute = d
			opsUnmerged = u
			opsTreef = t
		}

		var statesWorker managerWorkersWorkerStatesFunc
		var statesTreef managerWorkersTreeFunc

		switch s, t, err := newManagerWorkersStatesFuncs(wctx, sargs, workers, minEnough, maxFailed); {
		case err != nil:
			return nil, nil, nil, err
		default:
			statesWorker = s
			statesTreef = t
		}

		go func() {
			_ = contextCauseError(wctx, ifErrErrorFunc(wctxCancel), func() error {
				return opsDistribute(func(ctx context.Context, worker string, f func(managerWorkerOperationFunc) error) error {
					return args.DistributeWorkerOps(ctx, worker, f)
				})
			})
		}()

		return opsUnmerged, statesWorker,
			func() (opsTreeRoot, statesTreeRoot util.Hash, _ managerWorkersTreeSaveFunc, _ error) {
				var opsTree, statesTree fixedtree.Tree

				if err := contextCauseError(wctx, ifErrErrorFunc(wctxCancel), func() error {
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
				}); err != nil {
					return nil, nil, nil, err
				}

				opsTreeRoot = opsTree.Root()
				statesTreeRoot = statesTree.Root()

				return opsTreeRoot, statesTreeRoot, func() (base.BlockMapItem, base.BlockMapItem, error) {
					var opsB, statesB base.BlockMapItem

					closef := func(err error) error {
						wctxCancel(err)

						opsTree.Close()
						statesTree.Close()

						return err
					}

					if err := contextCauseError(wctx, closef, func() error {
						if opsTreeRoot == nil && statesTreeRoot == nil {
							return nil
						}

						var jw JobWorker

						switch i, err := NewBaseJobWorker(wctx, 2); {
						case err != nil:
							return err
						default:
							defer i.Close()

							jw = i
						}

						if err := jw.NewJob(func(ctx context.Context) error {
							b, err := args.WriteBlockItemOperationsTree(ctx, workID, opsTree)

							opsB = b

							return err
						}); err != nil {
							return err
						}

						if err := jw.NewJob(func(ctx context.Context) error {
							b, err := args.WriteBlockItemStatesTree(ctx, workID, statesTree)
							statesB = b

							return err
						}); err != nil {
							return err
						}

						jw.JobDone()

						return jw.Wait()
					}); err != nil {
						return nil, nil, err
					}

					return opsB, statesB, nil
				}, nil
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
