package isaac

import (
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
)

func IsValidBallotWithSuffrage(
	bl base.Ballot,
	suf base.Suffrage,
	isValidVoteproofWithSuffrage func(base.Voteproof, base.Suffrage) error,
) (base.Suffrage, bool, error) {
	if !suf.ExistsPublickey(bl.SignFact().Node(), bl.SignFact().Signer()) {
		return nil, false, nil
	}

	if isValidVoteproofWithSuffrage == nil {
		isValidVoteproofWithSuffrage = base.IsValidVoteproofWithSuffrage // revive:disable-line:modifies-parameter
	}

	if err := isValidVoteproofWithSuffrage(bl.Voteproof(), suf); err != nil {
		return nil, false, err
	}

	return suf, true, nil
}

func IsValidWithdrawWithSuffrage(
	height base.Height,
	withdraw SuffrageWithdrawOperation,
	suf base.Suffrage,
	lifespan base.Height,
) error {
	e := util.ErrInvalid.Errorf("invalid withdraw with suffrage")

	fact := withdraw.WithdrawFact()

	if height > fact.WithdrawStart()+lifespan {
		return e.Errorf("withdraw expired")
	}

	if !suf.Exists(fact.Node()) {
		return e.Errorf("unknown withdraw node found, %q", fact.Node())
	}

	signs := withdraw.NodeSigns()

	for i := range signs {
		sign := signs[i]

		if !suf.ExistsPublickey(sign.Node(), sign.Signer()) {
			return e.Errorf("unknown node signed, %q", sign.Node())
		}
	}

	return nil
}

func ValidateBallotBeforeVoting(
	bl base.Ballot,
	networkID base.NetworkID,
	getSuffrage GetSuffrageByBlockHeight,
	isValidVoteproofWithSuffrage func(base.Voteproof, base.Suffrage) error,
	getSuffrageWithdrawLifespan func() (base.Height, error),
) error {
	if err := bl.IsValid(networkID); err != nil {
		return err
	}

	var suf base.Suffrage

	switch i, found, err := getSuffrage(bl.Point().Height()); {
	case err != nil:
		return err
	case !found:
		return nil
	default:
		suf = i
	}

	switch i, found, err := IsValidBallotWithSuffrage(bl, suf, isValidVoteproofWithSuffrage); {
	case err != nil:
		return err
	case !found:
		return nil
	default:
		suf = i
	}

	switch wbl, ok := bl.(ballotWithdraws); {
	case !ok:
		return nil
	default:
		lifespan, err := getSuffrageWithdrawLifespan()
		if err != nil {
			return err
		}

		withdraws := wbl.Withdraws()

		for i := range withdraws {
			if err := IsValidWithdrawWithSuffrage(bl.Point().Height(), withdraws[i], suf, lifespan); err != nil {
				return err
			}
		}
	}

	return nil
}
