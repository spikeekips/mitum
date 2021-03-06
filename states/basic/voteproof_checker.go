package basicstates

import (
	"golang.org/x/xerrors"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/base/ballot"
	"github.com/spikeekips/mitum/base/block"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/network"
	"github.com/spikeekips/mitum/storage"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
)

type VoteproofChecker struct {
	*logging.Logging
	storage   storage.Storage
	suffrage  base.Suffrage
	nodepool  *network.Nodepool
	lvp       base.Voteproof
	voteproof base.Voteproof
}

func NewVoteproofChecker(
	st storage.Storage,
	suffrage base.Suffrage,
	nodepool *network.Nodepool,
	lastVoteproof, voteproof base.Voteproof,
) *VoteproofChecker {
	return &VoteproofChecker{
		Logging: logging.NewLogging(func(c logging.Context) logging.Emitter {
			e := c.Str("module", "voteproof-validation-checker").
				Str("voteproof_id", voteproof.ID())

			if lastVoteproof != nil {
				e = e.Str("last_init_voteproof_id", lastVoteproof.ID())
			}

			return e
		}),
		storage:   st,
		suffrage:  suffrage,
		nodepool:  nodepool,
		lvp:       lastVoteproof,
		voteproof: voteproof,
	}
}

func (vc *VoteproofChecker) CheckPoint() (bool, error) {
	switch s := vc.voteproof.Stage(); s {
	case base.StageINIT:
		return vc.checkPointINITVoteproof()
	case base.StageACCEPT:
		return vc.checkPointACCEPTVoteproof()
	default:
		return false, xerrors.Errorf("unsupported voteproof stage, %v", s)
	}
}

func (vc *VoteproofChecker) checkPointINITVoteproof() (bool, error) {
	if vc.lvp == nil {
		return true, nil
	}

	if vc.voteproof.Stage() != base.StageINIT {
		return true, nil
	}

	if vc.voteproof.Height() == vc.lvp.Height() && vc.voteproof.Round() > vc.lvp.Round() {
		return true, nil
	}

	switch d := vc.voteproof.Height() - (vc.lvp.Height() + 1); {
	case d > 0:
		return false, SyncByVoteproofError.Errorf("height of init voteproof has higher than last voteproof; moves to syncing")
	case d < 0:
		return false, util.IgnoreError.Errorf("height of init voteproof has lower than last voteproof; ignore it")
	default:
		return true, nil
	}
}

func (vc *VoteproofChecker) checkPointACCEPTVoteproof() (bool, error) {
	if vc.voteproof.Stage() != base.StageACCEPT {
		return true, nil
	}

	if vc.lvp == nil {
		return true, nil
	}

	switch d := vc.voteproof.Height() - vc.lvp.Height(); {
	case d > 0:
		return false, SyncByVoteproofError.Errorf(
			"height of accept voteproof has higher than last voteproof; moves to syncing")
	case d < 0:
		return false, util.IgnoreError.Errorf("accept voteproof has lower than last voteproof; ignore it")
	case vc.lvp.Stage() == base.StageINIT:
		if vc.voteproof.Round() < vc.lvp.Round() {
			return false, util.IgnoreError.Errorf("lower round of accept voteproof with last init voteproof")
		}
	case vc.lvp.Stage() == base.StageACCEPT:
		if vc.voteproof.Round() <= vc.lvp.Round() {
			return false, util.IgnoreError.Errorf("same or lower round of accept voteproof with last accept voteproof")
		}
	}

	return true, nil
}

func (vc *VoteproofChecker) CheckINITVoteproofWithLocalBlock() (bool, error) {
	if vc.voteproof.Stage() != base.StageINIT {
		return true, nil
	}

	if err := CheckBlockWithINITVoteproof(vc.storage, vc.voteproof); err != nil {
		if xerrors.Is(err, util.IgnoreError) || xerrors.Is(err, storage.NotFoundError) {
			return true, nil
		}

		return false, SyncByVoteproofError.Wrap(err)
	}

	return true, nil
}

// CheckACCEPTVoteproofProposal checks proposal of accept voteproof. If proposal
// not found in local, request to the voted nodes.
func (vc *VoteproofChecker) CheckACCEPTVoteproofProposal() (bool, error) {
	if vc.voteproof.Stage() != base.StageACCEPT {
		return true, nil
	} else if vc.voteproof.Result() != base.VoteResultMajority {
		return true, nil
	}

	fact := vc.voteproof.Majority().(ballot.ACCEPTBallotFact)
	if found, err := vc.storage.HasSeal(fact.Proposal()); err != nil {
		return false, xerrors.Errorf("failed to check proposal of accept voteproof: %w", err)
	} else if found {
		return true, nil
	}

	var proposal ballot.Proposal
	for i := range vc.voteproof.Votes() {
		f := vc.voteproof.Votes()[i]
		if !f.Fact().Equal(fact.Hash()) {
			continue
		}

		if f.Node().Equal(vc.nodepool.Local().Address()) {
			continue
		}

		if node, found := vc.nodepool.Node(f.Node()); !found {
			vc.Log().Debug().Str("target_node", f.Node().String()).Msg("unknown node found in voteproof")

			continue
		} else if pr, err := isaac.RequestProposal(node, fact.Proposal()); err != nil {
			return false, xerrors.Errorf("failed to find proposal from accept voteproof: %w", err)
		} else {
			proposal = pr

			break
		}
	}

	if proposal == nil {
		return false, xerrors.Errorf("failed to find proposal from accept voteproof")
	}

	pvc := isaac.NewProposalValidationChecker(vc.storage, vc.suffrage, vc.nodepool, proposal, nil)
	checkers := []util.CheckerFunc{
		pvc.IsKnown,
		pvc.CheckSigning,
		pvc.SaveProposal,
	}

	if err := util.NewChecker("proposal-of-accept-voteproof-validation-checker", checkers).Check(); err != nil {
		switch {
		case xerrors.Is(err, util.IgnoreError):
		case xerrors.Is(err, isaac.KnownSealError):
		default:
			return false, xerrors.Errorf("failed to find proposal from accept voteproof: %w", err)
		}
	}

	return true, nil
}

func CheckBlockWithINITVoteproof(st storage.Storage, voteproof base.Voteproof) error {
	// check init ballot fact.PreviousBlock with local block
	fact, ok := voteproof.Majority().(ballot.INITBallotFact)
	if !ok {
		return xerrors.Errorf("needs INITTBallotFact: fact=%T", voteproof.Majority())
	}

	var m block.Manifest
	switch i, found, err := st.ManifestByHeight(voteproof.Height() - 1); {
	case err != nil:
		return err
	case !found:
		return storage.NotFoundError.Errorf("manifest, %v not found for checking init voteproof", voteproof.Height())
	default:
		m = i
	}

	if !fact.PreviousBlock().Equal(m.Hash()) {
		return xerrors.Errorf(
			"different block within previous block of init voteproof and local: previousBlock=%s local=%s",
			fact.PreviousBlock(), m.Hash(),
		)
	}

	return nil
}
