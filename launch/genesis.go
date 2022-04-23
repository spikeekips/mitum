package launch

import (
	"context"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/isaac/blockdata"
	"github.com/spikeekips/mitum/isaac/database"
	isaacoperation "github.com/spikeekips/mitum/isaac/operation"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/logging"
)

type GenesisBlockGenerator struct {
	*logging.Logging
	local     base.LocalNode
	networkID base.NetworkID
	enc       encoder.Encoder
	db        *database.Default
	dataroot  string
	proposal  base.ProposalSignedFact
	joinOp    isaacoperation.SuffrageGenesisJoin
	ivp       base.INITVoteproof
	avp       base.ACCEPTVoteproof
}

func NewGenesisBlockGenerator(
	local base.LocalNode,
	networkID base.NetworkID,
	enc encoder.Encoder,
	db *database.Default,
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
		dataroot:  dataroot,
	}
}

func (g *GenesisBlockGenerator) Generate() (base.BlockDataMap, error) {
	e := util.StringErrorFunc("failed to generate genesis block")

	if err := g.joinOperation(); err != nil {
		return nil, e(err, "")
	}

	if err := g.newProposal(nil); err != nil {
		return nil, e(err, "")
	}

	if err := g.process(); err != nil {
		return nil, e(err, "")
	}

	fsreader, err := blockdata.NewLocalFSReader(g.dataroot, base.GenesisHeight, g.enc)
	if err != nil {
		return nil, e(err, "")
	}

	switch blockdatamap, found, err := fsreader.Map(); {
	case err != nil:
		return nil, e(err, "")
	case !found:
		return nil, errors.Errorf("blockdatamap not found")
	default:
		if err := blockdatamap.IsValid(g.networkID); err != nil {
			return nil, e(err, "")
		}

		g.Log().Info().Interface("blockdatamap", blockdatamap).Msg("genesis block generated")

		if err := g.closeDatabase(); err != nil {
			return nil, e(err, "")
		}

		return blockdatamap, nil
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

	g.joinOp = op

	g.Log().Debug().Interface("operation", op).Msg("genesis join operation created")

	return nil
}

func (g *GenesisBlockGenerator) newProposal(ops []util.Hash) error {
	e := util.StringErrorFunc("failed to make genesis proposal")

	ops = append(ops, g.joinOp.Fact().Hash())

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
		SetThreshold(base.Threshold(100)).
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
		SetThreshold(base.Threshold(100)).
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

	if err := g.db.MergeAllPermanent(); err != nil {
		return e(err, "failed to merge temps")
	}

	return nil
}

func (g *GenesisBlockGenerator) newProposalProcessor() (*isaac.DefaultProposalProcessor, error) {
	return isaac.NewDefaultProposalProcessor(
		g.proposal,
		nil,
		func(pr base.ProposalSignedFact, getStateFunc base.GetStateFunc) (isaac.BlockDataWriter, error) {
			e := util.StringErrorFunc("failed to crete BlockDataWriter")

			dbw, err := g.db.NewBlockWriteDatabase(pr.Point().Height())
			if err != nil {
				return nil, e(err, "")
			}

			fswriter, err := blockdata.NewLocalFSWriter(
				g.dataroot,
				pr.Point().Height(),
				g.enc,
				g.local,
				g.networkID,
			)
			if err != nil {
				return nil, e(err, "")
			}

			return blockdata.NewWriter(pr, getStateFunc, dbw, g.db.MergeBlockWriteDatabase, fswriter), nil
		},
		func(key string) (base.State, bool, error) {
			return nil, false, nil
		},
		func(_ context.Context, facthash util.Hash) (base.Operation, error) {
			switch {
			case facthash.Equal(g.joinOp.Fact().Hash()):
				return g.joinOp, nil
			default:
				return nil, util.NotFoundError.Errorf("operation not found")
			}
		},
		func(base.Height, hint.Hint) (base.OperationProcessor, bool, error) {
			return nil, false, nil
		},
	)
}