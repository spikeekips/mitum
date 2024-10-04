package main

import (
	"context"
	"fmt"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/fixedtree"
)

type (
	managerTreesFunc     func(opsTree, statesTree fixedtree.Tree) error
	managerTreesDoneFunc func(managerTreesFunc) error
	managerTreesFuncs    func(_ context.Context, workers []string, minEnough, maxFailed uint64, workID string, _ [][2]util.Hash) (
		managerWorkersOperationsFunc,
		managerWorkerUnmergedFunc,
		managerWorkerStatesFunc,
		managerTreesDoneFunc,
		error,
	)
)

func newFuncManagerTrees(
	args *managerWorkersTreesArgs,
	newCheckWorkersFunc checkEnoughFailedWorkersFuncs,
) managerTreesFuncs {
	workersTreeFuncs := newFuncManagerWorkersTrees(args, newCheckWorkersFunc)

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
		managerTreesDoneFunc,
		error,
	) {
		var workersOps managerWorkersOperationsFunc
		var workersUnmerge managerWorkerUnmergedFunc
		var workersStates managerWorkerStatesFunc
		var workersTree managerWorkersDoneTreesFunc

		switch o, u, s, t, err := workersTreeFuncs(
			ctx,
			workers,
			opsShare, statesShare,
			workID,
			ophs,
		); {
		case err != nil:
			return nil, nil, nil, nil, err
		default:
			workersOps = o
			workersUnmerge = u
			workersStates = s
			workersTree = t
		}

		return workersOps, workersUnmerge, workersStates,
			func(f managerTreesFunc) error {
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
