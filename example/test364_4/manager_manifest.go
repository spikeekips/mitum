package main

import (
	"context"
	"fmt"

	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
)

type (
	managerManifestNewManifestFunc func(pr base.ProposalSignFact, opsTreeRoot, statesTreeRoot util.Hash) (base.Manifest, error)
	managerManifestSaveFunc        func() (
		opsTree, statesTree, proposal base.BlockMapItem,
		_ error,
	)
	managerManifestDoneFunc func() (base.Manifest, managerManifestSaveFunc, error)
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
	NewManifest                managerManifestNewManifestFunc
	WriteBlockItemProposalFunc blockItemWriterNewWriteProposalFunc
}

func newManagerManifestArgs(targs *managerWorkersTreesArgs) *managerManifestArgs {
	return &managerManifestArgs{
		Logging: logging.NewLogging(func(zctx zerolog.Context) zerolog.Context {
			return zctx.Str("module", "manager-manifest")
		}),
		managerWorkersTreesArgs: targs,
		NewManifest: func(base.ProposalSignFact, util.Hash, util.Hash) (base.Manifest, error) {
			return nil, util.ErrNotImplemented.Errorf("NewManifest")
		},
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
		var workersTree managerWorkersTreeRootsFunc

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
			func() (base.Manifest, managerManifestSaveFunc, error) {
				var opsTreeRoot, statesTreeRoot util.Hash
				var treeSave managerWorkersTreeSaveFunc

				switch o, s, f, err := workersTree(); {
				case err != nil:
					return nil, nil, err
				default:
					opsTreeRoot = o
					statesTreeRoot = s
					treeSave = f
				}

				switch m, err := args.NewManifest(pr, opsTreeRoot, statesTreeRoot); {
				case err != nil:
					return nil, nil, err
				default:
					return m, newFuncManagerManifestSave(
						ctx,
						workID,
						treeSave,
						pr,
						args.WriteBlockItemProposalFunc,
					), nil
				}
			},
			nil
	}
}

func newFuncManagerManifestSave(
	ctx context.Context,
	workID string,
	treeSave managerWorkersTreeSaveFunc,
	pr base.ProposalSignFact,
	writeBlockItemProposal blockItemWriterNewWriteProposalFunc,
) managerManifestSaveFunc {
	return func() (opsTree, statesTree, proposal base.BlockMapItem, _ error) {
		var jw JobWorker

		switch i, err := NewBaseJobWorker(ctx, 2); {
		case err != nil:
			return nil, nil, nil, err
		default:
			defer i.Close()

			jw = i
		}

		if err := jw.NewJob(func(ctx context.Context) error {
			switch o, s, err := treeSave(); {
			case err != nil:
				return err
			default:
				opsTree = o
				statesTree = s

				return nil
			}
		}); err != nil {
			return nil, nil, nil, err
		}

		if err := jw.NewJob(func(ctx context.Context) error {
			switch b, err := writeBlockItemProposal(ctx, workID, pr); {
			case err != nil:
				return err
			default:
				proposal = b

				return nil
			}
		}); err != nil {
			return nil, nil, nil, err
		}

		jw.JobDone()

		if err := jw.Wait(); err != nil {
			return nil, nil, nil, err
		}

		return opsTree, statesTree, proposal, nil
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
