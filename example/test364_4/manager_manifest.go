package main

import (
	"context"
	"fmt"

	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/fixedtree"
	"github.com/spikeekips/mitum/util/logging"
)

type (
	managerManifestSaveFunc func() (
		opsTree, statesTree, proposal base.BlockMapItem,
		_ error,
	)
	managerManifestDoneFunc func(func(_ base.ProposalSignFact, opsTree, statesTree fixedtree.Tree) error) error
	managerManifestFuncs    func(_ context.Context, workers []string, minEnough, maxFailed uint64, workID string, _ base.ProposalSignFact) (
		managerWorkersOperationsWorkerUnmergedFunc,
		managerWorkersWorkerStatesFunc,
		managerManifestDoneFunc,
		error,
	)
)

type managerManifestArgs struct {
	*logging.Logging
	*managerWorkersTreesArgs
	WriteBlockItemProposalFunc blockItemWriterNewWriteProposalFunc
}

func newManagerManifestArgs(targs *managerWorkersTreesArgs) *managerManifestArgs {
	return &managerManifestArgs{
		Logging: logging.NewLogging(func(zctx zerolog.Context) zerolog.Context {
			return zctx.Str("module", "manager-manifest")
		}),
		managerWorkersTreesArgs: targs,
		WriteBlockItemProposalFunc: func(context.Context, string, base.ProposalSignFact) (base.BlockMapItem, error) {
			return nil, util.ErrNotImplemented.Errorf("WriteBlockItemProposalFunc")
		},
	}
}

func newFuncManagerManifest(args *managerManifestArgs) managerManifestFuncs {
	workersTreeFuncs := newFuncManagerWorkersTrees(args.managerWorkersTreesArgs)

	return func(
		ctx context.Context,
		workers []string,
		minEnough, maxFailed uint64,
		workID string,
		pr base.ProposalSignFact,
	) (
		managerWorkersOperationsWorkerUnmergedFunc,
		managerWorkersWorkerStatesFunc,
		managerManifestDoneFunc,
		error,
	) {
		switch w := uint64(len(workers)); {
		case w < 1:
			return nil, nil, nil, util.ErrNotFound.Errorf("empty workers")
		case maxFailed >= w:
			return nil, nil, nil, util.ErrInvalid.Errorf("invalid max failed; max failed=%d >= workers=%d", maxFailed, w)
		case minEnough > w:
			minEnough = w
		}

		var workersUnmerge managerWorkersOperationsWorkerUnmergedFunc
		var workersStates managerWorkersWorkerStatesFunc
		var workersTree managerWorkersDoneTreesFunc

		switch u, s, t, err := workersTreeFuncs(
			ctx,
			workers,
			minEnough, maxFailed,
			workID,
			pr.ProposalFact().Operations(),
		); {
		case err != nil:
			return nil, nil, nil, err
		default:
			workersUnmerge = u
			workersStates = s
			workersTree = t
		}

		return workersUnmerge, workersStates,
			func(f func(base.ProposalSignFact, fixedtree.Tree, fixedtree.Tree) error) error {
				var opsTree, statesTree fixedtree.Tree

				switch o, s, err := workersTree(); {
				case err != nil:
					return err
				default:
					opsTree = o
					statesTree = s
				}

				return f(pr, opsTree, statesTree)
			},
			nil
	}
}

func newWorkID(pr base.ProposalSignFact) string {
	return fmt.Sprintf("%21d-%21d-%s-%s",
		pr.Point().Height(),
		pr.Point().Round(),
		pr.Fact().Hash(),
		util.ULID(),
	)
}
