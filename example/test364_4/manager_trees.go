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
	managerTreesDoneFunc func(func(opsTree, statesTree fixedtree.Tree) error) error
	managerTreesFuncs    func(_ context.Context, workers []string, minEnough, maxFailed uint64, workID string, _ [][2]util.Hash) (
		managerWorkersOperationsWorkerUnmergedFunc,
		managerWorkersWorkerStatesFunc,
		managerTreesDoneFunc,
		error,
	)
)

type managerTreesArgs struct {
	*logging.Logging
}

func newManagerTreesArgs() *managerTreesArgs {
	return &managerTreesArgs{
		Logging: logging.NewLogging(func(zctx zerolog.Context) zerolog.Context {
			return zctx.Str("module", "manager-trees")
		}),
	}
}

func newFuncManagerTrees(
	args *managerTreesArgs,
	targs *managerWorkersTreesArgs,
) managerTreesFuncs {
	workersTreeFuncs := newFuncManagerWorkersTrees(targs)

	return func(
		ctx context.Context,
		workers []string,
		opsShare, statesShare uint64,
		workID string,
		ophs [][2]util.Hash,
	) (
		managerWorkersOperationsWorkerUnmergedFunc,
		managerWorkersWorkerStatesFunc,
		managerTreesDoneFunc,
		error,
	) {
		var workersUnmerge managerWorkersOperationsWorkerUnmergedFunc
		var workersStates managerWorkersWorkerStatesFunc
		var workersTree managerWorkersDoneTreesFunc

		switch u, s, t, err := workersTreeFuncs(
			ctx,
			workers,
			opsShare, statesShare,
			workID,
			ophs,
		); {
		case err != nil:
			return nil, nil, nil, err
		default:
			workersUnmerge = u
			workersStates = s
			workersTree = t
		}

		return workersUnmerge, workersStates,
			func(f func(fixedtree.Tree, fixedtree.Tree) error) error {
				var opsTree, statesTree fixedtree.Tree

				switch o, s, err := workersTree(); {
				case err != nil:
					return err
				default:
					opsTree = o
					statesTree = s
				}

				return f(opsTree, statesTree)
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
