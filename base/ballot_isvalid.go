package base

import (
	"github.com/spikeekips/mitum/util"
)

func IsValidBallot(bl Ballot, networkID []byte) error {
	e := util.ErrInvalid.Errorf("ballot")

	if err := util.CheckIsValiders(networkID, false,
		bl.Voteproof(),
		bl.SignFact(),
	); err != nil {
		return e.Wrap(err)
	}

	return nil
}

func IsValidINITBallot(bl INITBallot, _ []byte) error {
	e := util.ErrInvalid.Errorf("init ballot")

	if _, err := util.AssertInterfaceValue[INITBallotSignFact](bl.SignFact()); err != nil {
		return e.Wrap(err)
	}

	switch bl.Voteproof().Point().Stage() { //nolint:exhaustive // state already checked
	case StageINIT:
		ivp, err := EnsureINITVoteproof(bl.Voteproof())
		if err != nil {
			return e.Wrap(err)
		}

		if err := isValidINITVoteproofInINITBallot(bl, ivp); err != nil {
			return e.Wrap(err)
		}
	case StageACCEPT:
		avp, err := EnsureACCEPTVoteproof(bl.Voteproof())
		if err != nil {
			return e.Wrap(err)
		}

		if err := isValidACCEPTVoteproofInINITBallot(bl, avp); err != nil {
			return e.Wrap(err)
		}
	}

	return nil
}

func isValidINITVoteproofInINITBallot(bl INITBallot, vp INITVoteproof) error {
	e := util.ErrInvalid.Errorf("init voteproof in init ballot")

	switch {
	case bl.Point().Round() == 0:
		return e.Errorf("init voteproof should not be in 0 round init ballot")
	case !vp.Point().Point.NextRound().Equal(bl.Point().Point):
		return e.Errorf(
			"next round not match; ballot(%q) == voteproof(%q)", bl.Point(), vp.Point())
	}

	return nil
}

func isValidACCEPTVoteproofInINITBallot(bl INITBallot, vp ACCEPTVoteproof) error {
	e := util.ErrInvalid.Errorf("accept voteproof in init ballot")

	switch {
	case vp.Result() == VoteResultMajority:
		fact := bl.BallotSignFact().BallotFact()
		if !fact.PreviousBlock().Equal(vp.BallotMajority().NewBlock()) {
			return e.Errorf("block does not match, ballot(%q) != voteproof(%q)",
				fact.PreviousBlock(),
				vp.BallotMajority().NewBlock(),
			)
		}
	default:
		if !vp.Point().Point.NextRound().Equal(bl.Point().Point) {
			return e.Errorf(
				"not majority, but next round not match; ballot(%q) == voteproof(%q)", bl.Point(), vp.Point())
		}
	}

	return nil
}

func isValidVoteproofInACCEPTBallot(bl Ballot, vp INITVoteproof) error {
	e := util.ErrInvalid.Errorf("init voteproof in accept ballot")
	if vp == nil {
		return e.Errorf("empty voteproof")
	}

	switch {
	case !bl.Point().Point.Equal(vp.Point().Point):
		return e.Errorf(
			"not same point; ballot(%q) == voteproof(%q)", bl.Point(), vp.Point())
	case vp.Result() != VoteResultMajority:
		return e.Errorf("init voteproof not majority result, %q", vp.Result())
	}

	return nil
}

func IsValidACCEPTBallot(bl ACCEPTBallot, _ []byte) error {
	e := util.ErrInvalid.Errorf("accept ballot")

	if _, err := util.AssertInterfaceValue[ACCEPTBallotSignFact](bl.SignFact()); err != nil {
		return e.Wrap(err)
	}

	switch ivp, err := util.AssertInterfaceValue[INITVoteproof](bl.Voteproof()); {
	case err != nil:
		return e.Errorf("not init voteproof in accept ballot, %T", bl.Voteproof())
	default:
		if err := isValidVoteproofInACCEPTBallot(bl, ivp); err != nil {
			return e.Wrap(err)
		}
	}

	return nil
}

func IsValidBallotFact(fact BallotFact) error {
	e := util.ErrInvalid.Errorf("ballot fact")

	if err := fact.Point().IsValid(nil); err != nil {
		return e.Wrap(err)
	}

	return nil
}

func IsValidINITBallotFact(fact INITBallotFact) error {
	e := util.ErrInvalid.Errorf("init ballot fact")

	if fact.Point().Stage() != StageINIT {
		return e.Errorf("invalid stage, %q", fact.Point().Stage())
	}

	if err := util.CheckIsValiders(nil, false,
		util.DummyIsValider(func(b []byte) error {
			if fact.Point().Point.Equal(GenesisPoint) {
				return nil
			}

			return fact.PreviousBlock().IsValid(b)
		}),
		fact.Proposal(),
	); err != nil {
		return e.Wrap(err)
	}

	return nil
}

func IsValidACCEPTBallotFact(fact ACCEPTBallotFact) error {
	e := util.ErrInvalid.Errorf("accept ballot fact")

	if fact.Point().Stage() != StageACCEPT {
		return e.Errorf("invalid stage, %q", fact.Point().Stage())
	}

	if err := util.CheckIsValiders(nil, false,
		fact.Proposal(),
		fact.NewBlock(),
	); err != nil {
		return e.Wrap(err)
	}

	return nil
}

func IsValidBallotSignFact(sf BallotSignFact, networkID []byte) error {
	e := util.ErrInvalid.Errorf("ballot signfact")

	if err := IsValidSignFact(sf, networkID); err != nil {
		return e.Wrap(err)
	}

	if err := util.CheckIsValiders(nil, false, sf.Node()); err != nil {
		return e.Wrap(err)
	}

	return nil
}

func IsValidINITBallotSignFact(sf BallotSignFact, networkID []byte) error {
	e := util.ErrInvalid.Errorf("init ballot signfact")

	if err := IsValidBallotSignFact(sf, networkID); err != nil {
		return e.Wrap(err)
	}

	if _, err := util.AssertInterfaceValue[INITBallotFact](sf.Fact()); err != nil {
		return e.Wrap(err)
	}

	return nil
}

func IsValidACCEPTBallotSignFact(sf BallotSignFact, networkID []byte) error {
	e := util.ErrInvalid.Errorf("accept ballot signfact")

	if err := IsValidBallotSignFact(sf, networkID); err != nil {
		return e.Wrap(err)
	}

	if _, err := util.AssertInterfaceValue[ACCEPTBallotFact](sf.Fact()); err != nil {
		return e.Wrap(err)
	}

	return nil
}
