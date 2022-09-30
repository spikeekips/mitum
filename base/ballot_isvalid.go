package base

import (
	"github.com/spikeekips/mitum/util"
)

func IsValidBallot(bl Ballot, networkID []byte) error {
	e := util.ErrInvalid.Errorf("invalid Ballot")

	if err := util.CheckIsValiders(networkID, false,
		bl.Voteproof(),
		bl.SignFact(),
	); err != nil {
		return e.Wrap(err)
	}

	return nil
}

func IsValidINITBallot(bl INITBallot, _ []byte) error {
	e := util.ErrInvalid.Errorf("invalid INITBallot")

	if _, ok := bl.SignFact().(INITBallotSignFact); !ok {
		return e.Errorf("INITBallotSignFact expected, not %T", bl.SignFact())
	}

	switch bl.Voteproof().Point().Stage() { //nolint:exhaustive // state already checked
	case StageINIT:
		ivp, err := EnsureINITVoteproof(bl.Voteproof())
		if err != nil {
			return e.Wrap(err)
		}

		if err := checkINITVoteproofInINITBallot(bl, ivp); err != nil {
			return e.Wrap(err)
		}
	case StageACCEPT:
		avp, err := EnsureACCEPTVoteproof(bl.Voteproof())
		if err != nil {
			return e.Wrap(err)
		}

		if err := checkACCEPTVoteproofInINITBallot(bl, avp); err != nil {
			return e.Wrap(err)
		}
	}

	return nil
}

func checkINITVoteproofInINITBallot(bl INITBallot, vp INITVoteproof) error {
	e := util.ErrInvalid.Errorf("invalid init voteproof in init ballot")

	switch {
	case bl.Point().Round() == 0:
		return e.Errorf("init voteproof should not be in 0 round init ballot")
	case !vp.Point().Point.NextRound().Equal(bl.Point().Point):
		return e.Errorf(
			"next round not match; ballot(%q) == voteproof(%q)", bl.Point(), vp.Point())
	}

	return nil
}

func checkACCEPTVoteproofInINITBallot(bl INITBallot, vp ACCEPTVoteproof) error {
	e := util.ErrInvalid.Errorf("invalid accept voteproof in init ballot")

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

func checkVoteproofInACCEPTBallot(bl Ballot, vp INITVoteproof) error {
	e := util.ErrInvalid.Errorf("invalid init voteproof in accept ballot")
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
	e := util.ErrInvalid.Errorf("invalid ACCEPTBallot")

	if _, ok := bl.SignFact().(ACCEPTBallotSignFact); !ok {
		return e.Errorf("ACCEPTBallotSignFact expected, not %T", bl.SignFact())
	}

	ivp, ok := bl.Voteproof().(INITVoteproof)
	if !ok {
		return e.Errorf("not init voteproof in accept ballot, %T", bl.Voteproof())
	}

	if err := checkVoteproofInACCEPTBallot(bl, ivp); err != nil {
		return e.Wrap(err)
	}

	return nil
}

func IsValidBallotFact(fact BallotFact) error {
	e := util.ErrInvalid.Errorf("invalid BallotFact")

	if err := fact.Point().IsValid(nil); err != nil {
		return e.Wrap(err)
	}

	return nil
}

func IsValidINITBallotFact(fact INITBallotFact) error {
	e := util.ErrInvalid.Errorf("invalid INITBallotFact")

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
	e := util.ErrInvalid.Errorf("invalid ACCEPTBallotFact")

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
	e := util.ErrInvalid.Errorf("invalid BallotSignFact")

	if err := IsValidSignFact(sf, networkID); err != nil {
		return e.Wrap(err)
	}

	if err := util.CheckIsValiders(nil, false, sf.Node()); err != nil {
		return e.Wrap(err)
	}

	return nil
}

func IsValidINITBallotSignFact(sf BallotSignFact, networkID []byte) error {
	e := util.ErrInvalid.Errorf("invalid INITBallotSignFact")

	if err := IsValidBallotSignFact(sf, networkID); err != nil {
		return e.Wrap(err)
	}

	if _, ok := sf.Fact().(INITBallotFact); !ok {
		return e.Errorf("not INITBallotFact, %T", sf.Fact())
	}

	return nil
}

func IsValidACCEPTBallotSignFact(sf BallotSignFact, networkID []byte) error {
	e := util.ErrInvalid.Errorf("invalid ACCEPTBallotSignFact")

	if err := IsValidBallotSignFact(sf, networkID); err != nil {
		return e.Wrap(err)
	}

	if _, ok := sf.Fact().(ACCEPTBallotFact); !ok {
		return e.Errorf("not ACCEPTBallotFact, %T", sf.Fact())
	}

	return nil
}
