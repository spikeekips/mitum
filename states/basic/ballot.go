package basicstates

import (
	"golang.org/x/xerrors"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/base/ballot"
	"github.com/spikeekips/mitum/network"
	"github.com/spikeekips/mitum/storage"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/spikeekips/mitum/util/valuehash"
)

func NextINITBallotFromACCEPTVoteproof(
	st storage.Storage,
	blockFS *storage.BlockFS,
	local *network.LocalNode,
	voteproof base.Voteproof,
) (ballot.INITBallotV0, error) {
	if voteproof.Stage() != base.StageACCEPT {
		return ballot.INITBallotV0{}, xerrors.Errorf("not accept voteproof")
	} else if !voteproof.IsFinished() {
		return ballot.INITBallotV0{}, xerrors.Errorf("voteproof not yet finished")
	}

	var height base.Height
	var round base.Round
	var previousBlock valuehash.Hash

	// NOTE if voteproof drew, previous block should be get from local storage,
	// not from voteproof.
	if voteproof.Majority() == nil {
		height = voteproof.Height()
		round = voteproof.Round() + 1

		switch m, found, err := st.ManifestByHeight(height - 1); {
		case !found:
			return ballot.INITBallotV0{}, xerrors.Errorf("manfest, height=%d not found", height-1)
		case err != nil:
			return ballot.INITBallotV0{}, xerrors.Errorf("failed to get manifest: %w", err)
		default:
			previousBlock = m.Hash()
		}
	} else if i, ok := voteproof.Majority().(ballot.ACCEPTBallotFact); !ok {
		return ballot.INITBallotV0{}, xerrors.Errorf(
			"not ballot.ACCEPTBallotFact in voteproof.Majority(); %T", voteproof.Majority())
	} else { // NOTE agreed accept voteproof
		height = voteproof.Height() + 1
		round = base.Round(0)
		previousBlock = i.NewBlock()
	}

	var avp base.Voteproof
	if voteproof.Result() == base.VoteResultMajority {
		avp = voteproof
	} else {
		switch vp, err := blockFS.LoadACCEPTVoteproof(voteproof.Height() - 1); {
		case err != nil:
			return ballot.INITBallotV0{}, xerrors.Errorf("failed to get last voteproof: %w", err)
		case vp != nil:
			avp = vp
		}
	}

	return ballot.NewINITBallotV0(
		local.Address(),
		height,
		round,
		previousBlock,
		voteproof,
		avp,
	), nil
}

func NextINITBallotFromINITVoteproof(
	st storage.Storage,
	blockFS *storage.BlockFS,
	local *network.LocalNode,
	voteproof base.Voteproof,
) (ballot.INITBallotV0, error) {
	if voteproof.Stage() != base.StageINIT {
		return ballot.INITBallotV0{}, xerrors.Errorf("not init voteproof")
	} else if !voteproof.IsFinished() {
		return ballot.INITBallotV0{}, xerrors.Errorf("voteproof not yet finished")
	}

	var avp base.Voteproof
	switch vp, err := blockFS.LoadACCEPTVoteproof(voteproof.Height() - 1); {
	case err != nil:
		return ballot.INITBallotV0{}, xerrors.Errorf("failed to get last voteproof: %w", err)
	case vp != nil:
		avp = vp
	}

	var previousBlock valuehash.Hash
	switch m, found, err := st.ManifestByHeight(voteproof.Height() - 1); {
	case !found:
		return ballot.INITBallotV0{}, xerrors.Errorf("previous manfest, height=%d not found", voteproof.Height())
	case err != nil:
		return ballot.INITBallotV0{}, xerrors.Errorf("failed to get previous manifest: %w", err)
	default:
		previousBlock = m.Hash()
	}

	return ballot.NewINITBallotV0(
		local.Address(),
		voteproof.Height(),
		voteproof.Round()+1,
		previousBlock,
		voteproof,
		avp,
	), nil
}

type BallotChecker struct {
	*logging.Logging
	ballot ballot.Ballot
	lvp    base.Voteproof
}

func NewBallotChecker(blt ballot.Ballot, lvp base.Voteproof) *BallotChecker {
	return &BallotChecker{
		Logging: logging.NewLogging(func(c logging.Context) logging.Emitter {
			return c.Str("module", "ballot-checker-in-states")
		}),
		ballot: blt,
		lvp:    lvp,
	}
}

func (bc *BallotChecker) CheckWithLastVoteproof() (bool, error) {
	if bc.lvp == nil {
		return true, nil
	}

	bh := bc.ballot.Height()
	lh := bc.lvp.Height()

	if bh < lh {
		return false, xerrors.Errorf("lower height than last init voteproof")
	} else if bh > lh {
		return true, nil
	}

	if bc.ballot.Round() < bc.lvp.Round() {
		return false, xerrors.Errorf("lower round than last init voteproof")
	}

	return true, nil
}
