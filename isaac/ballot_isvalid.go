package isaac

import (
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
)

func IsValidBallotWithSuffrage(
	bl base.Ballot,
	suf base.Suffrage,
	isValidVoteproof func(base.Voteproof, base.Suffrage) error,
) error {
	e := util.ErrInvalid.Errorf("invalid ballot with suffrage")

	if !suf.ExistsPublickey(bl.SignFact().Node(), bl.SignFact().Signer()) {
		return e.Errorf("node not in suffrage")
	}

	if wbl, ok := bl.(BallotWithdraws); ok {
		withdraws := wbl.Withdraws()

		for i := range withdraws {
			if err := IsValidWithdrawWithSuffrage(bl.Point().Height(), withdraws[i], suf); err != nil {
				return e.Wrap(err)
			}
		}
	}

	if err := IsValidVoteproofWithSuffrage(bl.Voteproof(), suf); err != nil {
		return e.Wrap(err)
	}

	if isValidVoteproof != nil {
		if err := isValidVoteproof(bl.Voteproof(), suf); err != nil {
			return e.Wrap(err)
		}
	}

	return nil
}
