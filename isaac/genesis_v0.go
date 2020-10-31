package isaac

import (
	"context"

	"golang.org/x/xerrors"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/base/ballot"
	"github.com/spikeekips/mitum/base/block"
	"github.com/spikeekips/mitum/base/operation"
	"github.com/spikeekips/mitum/base/seal"
	"github.com/spikeekips/mitum/storage"
	"github.com/spikeekips/mitum/util/errors"
	"github.com/spikeekips/mitum/util/localtime"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/spikeekips/mitum/util/valuehash"
)

type GenesisBlockV0Generator struct {
	*logging.Logging
	local     *Local
	ballotbox *Ballotbox
	ops       []operation.Operation
	suffrage  base.Suffrage
}

func NewGenesisBlockV0Generator(local *Local, ops []operation.Operation) (*GenesisBlockV0Generator, error) {
	threshold, _ := base.NewThreshold(1, 100)

	suffrage := base.NewFixedSuffrage(local.Node().Address(), nil)
	if err := suffrage.Initialize(); err != nil {
		return nil, err
	}

	return &GenesisBlockV0Generator{
		Logging: logging.NewLogging(func(c logging.Context) logging.Emitter {
			return c.Str("module", "genesis-block-generator")
		}),
		local: local,
		ballotbox: NewBallotbox(
			func() []base.Address {
				return []base.Address{local.Node().Address()}
			},
			func() base.Threshold {
				return threshold
			},
		),
		ops:      ops,
		suffrage: suffrage,
	}, nil
}

func (gg *GenesisBlockV0Generator) Generate() (block.Block, error) {
	if err := gg.generatePreviousBlock(); err != nil {
		return nil, err
	}

	var ivp base.Voteproof
	if vp, err := gg.generateINITVoteproof(); err != nil {
		return nil, err
	} else {
		ivp = vp
	}

	var seals []operation.Seal
	if sls, err := gg.generateOperationSeal(); err != nil {
		return nil, err
	} else {
		seals = sls
	}

	var proposal ballot.Proposal
	if pr, err := gg.generateProposal(seals); err != nil {
		return nil, err
	} else {
		proposal = pr
	}

	pm := NewDefaultProposalProcessor(gg.local, gg.suffrage)
	_ = pm.SetLogger(gg.Log())

	if bk, err := pm.ProcessINIT(proposal.Hash(), ivp); err != nil {
		return nil, err
	} else if vp, err := gg.generateACCEPTVoteproof(bk, ivp); err != nil {
		return nil, err
	} else {
		return gg.store(pm, proposal, vp)
	}
}

func (gg *GenesisBlockV0Generator) store(
	pm *DefaultProposalProcessor,
	proposal ballot.Proposal,
	vp base.Voteproof,
) (block.Block, error) {
	var blk block.Block
	var bs storage.BlockStorage

	if st, err := pm.ProcessACCEPT(proposal.Hash(), vp); err != nil {
		return nil, err
	} else {
		bs = st
	}

	defer func() {
		_ = bs.Close()
	}()

	blk = bs.Block()

	if err := bs.Commit(context.Background()); err != nil {
		return nil, err
	} else if err := pm.Done(proposal.Hash()); err != nil {
		return nil, err
	} else {
		return blk, nil
	}
}

func (gg *GenesisBlockV0Generator) generateOperationSeal() ([]operation.Seal, error) {
	if len(gg.ops) < 1 {
		return nil, nil
	}

	var seals []operation.Seal
	if sl, err := operation.NewBaseSeal(
		gg.local.Node().Privatekey(),
		gg.ops,
		gg.local.Policy().NetworkID(),
	); err != nil {
		return nil, err
	} else if err := gg.local.Storage().NewSeals([]seal.Seal{sl}); err != nil {
		return nil, err
	} else {
		seals = append(seals, sl)
	}

	return seals, nil
}

func (gg *GenesisBlockV0Generator) generatePreviousBlock() error {
	// NOTE the privatekey of local node is melted into genesis previous block;
	// it means, genesis block contains who creates it.
	var genesisHash valuehash.Hash
	if sig, err := gg.local.Node().Privatekey().Sign(gg.local.Policy().NetworkID()); err != nil {
		return err
	} else {
		genesisHash = valuehash.NewBytes(sig.Bytes())
	}

	blk, err := block.NewBlockV0(
		block.NewSuffrageInfoV0(
			gg.local.Node().Address(),
			[]base.Node{gg.local.Node()},
		),
		base.PreGenesisHeight,
		base.Round(0),
		genesisHash,
		genesisHash,
		nil,
		nil,
		localtime.Now(),
	)
	if err != nil {
		return err
	}

	var bs storage.BlockStorage
	if st, err := gg.local.Storage().OpenBlockStorage(blk); err != nil {
		return err
	} else {
		bs = st
	}

	defer func() {
		_ = bs.Close()
	}()

	if err := bs.Commit(context.Background()); err != nil {
		return err
	} else if err := gg.local.BlockFS().AddAndCommit(blk); err != nil {
		err := errors.NewError("failed to commit to blockfs").Wrap(err)
		if err0 := bs.Cancel(); err0 != nil {
			return err.Wrap(err0)
		}

		return err
	}

	return nil
}

func (gg *GenesisBlockV0Generator) generateProposal(seals []operation.Seal) (ballot.Proposal, error) {
	sealHashes := make([]valuehash.Hash, len(seals))
	for i := range seals {
		sl := seals[i]
		sealHashes[i] = sl.Hash()
	}

	var proposal ballot.Proposal
	pr := ballot.NewProposalV0(
		gg.local.Node().Address(),
		base.Height(0),
		base.Round(0),
		sealHashes,
	)
	if err := SignSeal(&pr, gg.local); err != nil {
		return nil, err
	} else if err := gg.local.Storage().NewProposal(pr); err != nil {
		return nil, err
	} else {
		proposal = pr
	}

	return proposal, nil
}

func (gg *GenesisBlockV0Generator) generateINITVoteproof() (base.Voteproof, error) {
	var ib ballot.INITBallotV0
	if b, err := NewINITBallotV0Round0(gg.local); err != nil {
		return nil, err
	} else if err := SignSeal(&b, gg.local); err != nil {
		return nil, err
	} else {
		ib = b
	}

	var vp base.Voteproof
	if voteproof, err := gg.ballotbox.Vote(ib); err != nil {
		return nil, err
	} else {
		if !voteproof.IsFinished() {
			return nil, xerrors.Errorf("something wrong, INITVoteproof should be finished, but not")
		} else {
			vp = voteproof
		}
	}

	return vp, nil
}

func (gg *GenesisBlockV0Generator) generateACCEPTVoteproof(newBlock block.Block, ivp base.Voteproof) (
	base.Voteproof, error,
) {
	ab := NewACCEPTBallotV0(gg.local.Node().Address(), newBlock, ivp)
	if err := SignSeal(&ab, gg.local); err != nil {
		return nil, err
	}

	if voteproof, err := gg.ballotbox.Vote(ab); err != nil {
		return nil, err
	} else {
		if !voteproof.IsFinished() {
			return nil, xerrors.Errorf("something wrong, ACCEPTVoteproof should be finished, but not")
		}

		return voteproof, nil
	}
}
