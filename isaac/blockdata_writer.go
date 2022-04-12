package isaac

import (
	"context"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/tree"
)

type BlockDataWriter interface {
	SetProposal(context.Context, base.ProposalSignedFact) error
	SetOperationsSize(uint64)
	SetProcessResult(
		_ context.Context,
		index int,
		facthash util.Hash,
		instate bool,
		errorreason base.OperationProcessReasonError,
	) error
	SetStates(_ context.Context, index int, states []base.State, operation base.Operation) error
	Manifest(_ context.Context, previous base.Manifest) (base.Manifest, error)
	SetINITVoteproof(context.Context, base.INITVoteproof) error
	SetACCEPTVoteproof(context.Context, base.ACCEPTVoteproof) error
	Save(context.Context) error
	Cancel() error
}

type BlockDataFSWriter interface {
	SetProposal(context.Context, base.ProposalSignedFact) error
	SetOperation(context.Context, int, base.Operation) error
	SetOperationsTree(context.Context, tree.FixedTree) error
	SetState(context.Context, int, base.State) error
	SetStatesTree(context.Context, tree.FixedTree) error
	SetManifest(context.Context, base.Manifest) error
	SetINITVoteproof(context.Context, base.INITVoteproof) error
	SetACCEPTVoteproof(context.Context, base.ACCEPTVoteproof) error
	Save(context.Context) (base.BlockDataMap, error)
	Cancel() error
}
