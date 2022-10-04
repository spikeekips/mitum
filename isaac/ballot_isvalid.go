package isaac

import (
	"github.com/spikeekips/mitum/base"
)

func IsValidBallotWithSuffrage(
	bl base.Ballot,
	suf base.Suffrage,
	isValidVoteproof func(base.Voteproof, base.Suffrage) error,
) (base.Suffrage, bool, error) {
	if !suf.ExistsPublickey(bl.SignFact().Node(), bl.SignFact().Signer()) {
		return nil, false, nil
	}

	if wbl, ok := bl.(ballotWithdraws); ok {
		withdraws := wbl.Withdraws()

		for i := range withdraws {
			if err := IsValidWithdrawWithSuffrage(bl.Point().Height(), withdraws[i], suf); err != nil {
				return nil, false, err
			}
		}
	}

	if err := IsValidVoteproofWithSuffrage(bl.Voteproof(), suf); err != nil {
		return nil, false, err
	}

	if isValidVoteproof != nil {
		if err := isValidVoteproof(bl.Voteproof(), suf); err != nil {
			return nil, false, err
		}
	}

	return suf, true, nil
}
