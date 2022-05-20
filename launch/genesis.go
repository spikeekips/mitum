package launch

import (
	"context"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	isaacblock "github.com/spikeekips/mitum/isaac/block"
	isaacdatabase "github.com/spikeekips/mitum/isaac/database"
	isaacoperation "github.com/spikeekips/mitum/isaac/operation"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/logging"
)

type GenesisBlockGenerator struct {
	local    base.LocalNode
	enc      encoder.Encoder
	db       isaac.Database
	pool     isaac.VoteproofsPool
	proposal base.ProposalSignedFact
	ivp      base.INITVoteproof
	avp      base.ACCEPTVoteproof
	*logging.Logging
	dataroot  string
	networkID base.NetworkID
	ops       []base.Operation
}

// BLOCK set genesis design

func NewGenesisBlockGenerator(
	local base.LocalNode,
	networkID base.NetworkID,
	enc encoder.Encoder,
	db isaac.Database,
	pool isaac.VoteproofsPool,
	dataroot string,
) *GenesisBlockGenerator {
	return &GenesisBlockGenerator{
		Logging: logging.NewLogging(func(zctx zerolog.Context) zerolog.Context {
			return zctx.Str("module", "genesis-block-generator")
		}),
		local:     local,
		networkID: networkID,
		enc:       enc,
		db:        db,
		pool:      pool,
		dataroot:  dataroot,
	}
}

func (g *GenesisBlockGenerator) Generate() (base.BlockMap, error) {
	e := util.StringErrorFunc("failed to generate genesis block")

	if err := g.joinOperation(); err != nil {
		return nil, e(err, "")
	}

	if err := g.networkPolicyOperation(); err != nil {
		return nil, e(err, "")
	}

	if err := g.newProposal(nil); err != nil {
		return nil, e(err, "")
	}

	if err := g.process(); err != nil {
		return nil, e(err, "")
	}

	fsreader, err := isaacblock.NewLocalFSReaderFromHeight(g.dataroot, base.GenesisHeight, g.enc)
	if err != nil {
		return nil, e(err, "")
	}

	switch blockmap, found, err := fsreader.Map(); {
	case err != nil:
		return nil, e(err, "")
	case !found:
		return nil, errors.Errorf("blockmap not found")
	default:
		if err := blockmap.IsValid(g.networkID); err != nil {
			return nil, e(err, "")
		}

		g.Log().Info().Interface("blockmap", blockmap).Msg("genesis block generated")

		if err := g.closeDatabase(); err != nil {
			return nil, e(err, "")
		}

		return blockmap, nil
	}
}

func (g *GenesisBlockGenerator) joinOperation() error {
	e := util.StringErrorFunc("failed to make join operation")

	fact := isaacoperation.NewSuffrageGenesisJoinPermissionFact(g.local.Address(), g.local.Publickey(), g.networkID)
	if err := fact.IsValid(g.networkID); err != nil {
		return e(err, "")
	}

	op := isaacoperation.NewSuffrageGenesisJoin(fact)
	if err := op.Sign(g.local.Privatekey(), g.networkID); err != nil {
		return e(err, "")
	}

	g.ops = append(g.ops, op)

	g.Log().Debug().Interface("operation", op).Msg("genesis join operation created")

	return nil
}

func (g *GenesisBlockGenerator) networkPolicyOperation() error {
	e := util.StringErrorFunc("failed to make join operation")

	fact := isaacoperation.NewGenesisNetworkPolicyFact(isaac.DefaultNetworkPolicy())
	if err := fact.IsValid(nil); err != nil {
		return e(err, "")
	}

	op := isaacoperation.NewGenesisNetworkPolicy(fact)
	if err := op.Sign(g.local.Privatekey(), g.networkID); err != nil {
		return e(err, "")
	}

	g.ops = append(g.ops, op)

	g.Log().Debug().Interface("operation", op).Msg("genesis network policy operation created")

	return nil
}

func (g *GenesisBlockGenerator) newProposal(ops []util.Hash) error {
	e := util.StringErrorFunc("failed to make genesis proposal")

	nops := make([]util.Hash, len(ops)+len(g.ops))
	copy(nops[:len(ops)], ops)

	for i := range g.ops {
		nops[i+len(ops)] = g.ops[i].Fact().Hash()
	}

	fact := isaac.NewProposalFact(base.GenesisPoint, g.local.Address(), ops)
	signed := isaac.NewProposalSignedFact(fact)

	if err := signed.Sign(g.local.Privatekey(), g.networkID); err != nil {
		return e(err, "")
	}

	if err := signed.IsValid(g.networkID); err != nil {
		return e(err, "")
	}

	g.proposal = signed

	g.Log().Debug().Interface("proposal", signed).Msg("proposal created for genesis")

	return nil
}

func (g *GenesisBlockGenerator) initVoetproof() error {
	e := util.StringErrorFunc("failed to make genesis init voteproof")

	fact := isaac.NewINITBallotFact(base.GenesisPoint, nil, g.proposal.Fact().Hash())
	if err := fact.IsValid(nil); err != nil {
		return e(err, "")
	}

	sf := isaac.NewINITBallotSignedFact(g.local.Address(), fact)
	if err := sf.Sign(g.local.Privatekey(), g.networkID); err != nil {
		return e(err, "")
	}

	if err := sf.IsValid(g.networkID); err != nil {
		return e(err, "")
	}

	vp := isaac.NewINITVoteproof(fact.Point().Point)
	vp.SetResult(base.VoteResultMajority).
		SetMajority(fact).
		SetSignedFacts([]base.BallotSignedFact{sf}).
		SetThreshold(base.MaxThreshold).
		Finish()

	if err := vp.IsValid(g.networkID); err != nil {
		return e(err, "")
	}

	g.ivp = vp

	g.Log().Debug().Interface("init_voteproof", vp).Msg("init voteproof created for genesis")

	return nil
}

func (g *GenesisBlockGenerator) acceptVoteproof(proposal, newblock util.Hash) error {
	e := util.StringErrorFunc("failed to make genesis accept voteproof")

	fact := isaac.NewACCEPTBallotFact(base.GenesisPoint, proposal, newblock)
	if err := fact.IsValid(nil); err != nil {
		return e(err, "")
	}

	sf := isaac.NewACCEPTBallotSignedFact(g.local.Address(), fact)
	if err := sf.Sign(g.local.Privatekey(), g.networkID); err != nil {
		return e(err, "")
	}

	if err := sf.IsValid(g.networkID); err != nil {
		return e(err, "")
	}

	vp := isaac.NewACCEPTVoteproof(fact.Point().Point)
	vp.SetResult(base.VoteResultMajority).
		SetMajority(fact).
		SetSignedFacts([]base.BallotSignedFact{sf}).
		SetThreshold(base.MaxThreshold).
		Finish()

	if err := vp.IsValid(g.networkID); err != nil {
		return e(err, "")
	}

	g.avp = vp

	g.Log().Debug().Interface("init_voteproof", vp).Msg("init voteproof created for genesis")

	return nil
}

func (g *GenesisBlockGenerator) process() error {
	e := util.StringErrorFunc("failed to process")

	if err := g.initVoetproof(); err != nil {
		return e(err, "")
	}

	pp, err := g.newProposalProcessor()
	if err != nil {
		return e(err, "")
	}

	_ = pp.SetLogging(g.Logging)

	switch m, err := pp.Process(context.Background(), g.ivp); {
	case err != nil:
		return e(err, "")
	default:
		if err := m.IsValid(g.networkID); err != nil {
			return e(err, "")
		}

		g.Log().Info().Interface("manifest", m).Msg("genesis block generated")

		if err := g.acceptVoteproof(g.proposal.Fact().Hash(), m.Hash()); err != nil {
			return e(err, "")
		}
	}

	if err := pp.Save(context.Background(), g.avp); err != nil {
		return e(err, "")
	}

	return nil
}

func (g *GenesisBlockGenerator) closeDatabase() error {
	e := util.StringErrorFunc("failed to close database")

	if i, ok := g.db.(*isaacdatabase.Default); ok {
		if err := i.MergeAllPermanent(); err != nil {
			return e(err, "failed to merge temps")
		}
	}

	return nil
}

func (g *GenesisBlockGenerator) newProposalProcessor() (*isaac.DefaultProposalProcessor, error) {
	return isaac.NewDefaultProposalProcessor(
		g.proposal,
		nil,
		NewBlockWriterFunc(g.local, g.networkID, g.dataroot, g.enc, g.db),
		func(key string) (base.State, bool, error) {
			return nil, false, nil
		},
		func(_ context.Context, facthash util.Hash) (base.Operation, error) {
			for i := range g.ops {
				op := g.ops[i]
				if facthash.Equal(op.Fact().Hash()) {
					return op, nil
				}
			}

			return nil, util.ErrNotFound.Errorf("operation not found")
		},
		func(base.Height, hint.Hint) (base.OperationProcessor, bool, error) {
			return nil, false, nil
		},
		g.pool.SetLastVoteproofs,
	)
}

func NewBlockWriterFunc(
	local base.LocalNode,
	networkID base.NetworkID,
	dataroot string,
	enc encoder.Encoder,
	db isaac.Database,
) isaac.NewBlockWriterFunc {
	return func(proposal base.ProposalSignedFact, getStateFunc base.GetStateFunc) (isaac.BlockWriter, error) {
		e := util.StringErrorFunc("failed to crete BlockWriter")

		dbw, err := db.NewBlockWriteDatabase(proposal.Point().Height())
		if err != nil {
			return nil, e(err, "")
		}

		fswriter, err := isaacblock.NewLocalFSWriter(
			dataroot,
			proposal.Point().Height(),
			enc,
			local,
			networkID,
		)
		if err != nil {
			return nil, e(err, "")
		}

		return isaacblock.NewWriter(proposal, getStateFunc, dbw, db.MergeBlockWriteDatabase, fswriter), nil
	}
}
