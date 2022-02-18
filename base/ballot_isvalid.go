package base

import (
	"github.com/spikeekips/mitum/util"
)

func IsValidBallot(bl Ballot, networkID []byte) error {
	e := util.StringErrorFunc("invalid Ballot")

	if err := util.CheckIsValid(networkID, false,
		bl.Voteproof(),
		bl.SignedFact(),
	); err != nil {
		return e(err, "")
	}

	return nil
}

func IsValidINITBallot(bl INITBallot, networkID []byte) error {
	e := util.StringErrorFunc("invalid INITBallot")
	if err := IsValidBallot(bl, networkID); err != nil {
		return e(err, "")
	}

	if _, ok := bl.SignedFact().(INITBallotSignedFact); !ok {
		return e(util.InvalidError.Errorf("INITBallotSignedFact expected, not %T", bl.SignedFact()), "")
	}

	switch bl.Voteproof().Point().Stage() {
	case StageINIT:
		if err := checkINITVoteproofInINITBallot(bl, bl.Voteproof().(INITVoteproof)); err != nil {
			return e(err, "")
		}
	case StageACCEPT:
		if err := checkACCEPTVoteproofInINITBallot(bl, bl.Voteproof().(ACCEPTVoteproof)); err != nil {
			return e(err, "")
		}
	}

	return nil
}

func checkINITVoteproofInINITBallot(bl INITBallot, vp INITVoteproof) error {
	e := util.StringErrorFunc("invalid init voteproof in init ballot")

	switch {
	case bl.Point().Round() == 0:
		return e(util.InvalidError.Errorf("init voteproof should not be in 0 round init ballot"), "")
	case vp.Point().Point.NextRound() != bl.Point().Point:
		return e(util.InvalidError.Errorf(
			"wrong point of init voteproof; ballot(%q) == voteproof(%q)", bl.Point(), vp.Point()), "")
	case vp.Result() != VoteResultDraw:
		return e(util.InvalidError.Errorf("wrong vote result of init voteproof; %q", vp.Result()), "")
	}

	return nil
}

func checkACCEPTVoteproofInINITBallot(bl INITBallot, vp ACCEPTVoteproof) error {
	e := util.StringErrorFunc("invalid init voteproof in init ballot")

	switch {
	case vp.Result() == VoteResultMajority:
		fact := bl.BallotSignedFact().BallotFact()
		if !fact.PreviousBlock().Equal(vp.BallotMajority().NewBlock()) {
			return e(util.InvalidError.Errorf("block does not match, ballot(%q) != voteproof(%q)",
				fact.PreviousBlock(),
				vp.BallotMajority().NewBlock(),
			), "")
		}
	default:
		if vp.Point().Point.NextRound() != bl.Point().Point {
			return e(util.InvalidError.Errorf(
				"wrong point of accept voteproof; ballot(%q) == voteproof(%q)", bl.Point(), vp.Point()), "")
		}
	}

	return nil
}

func checkVoteproofInACCEPTBallot(bl Ballot, vp INITVoteproof) error {
	e := util.StringErrorFunc("invalid init voteproof in accept ballot")
	if vp == nil {
		return e(util.InvalidError.Errorf("empty voteproof"), "")
	}

	switch {
	case bl.Point().Point != vp.Point().Point:
		return e(util.InvalidError.Errorf(
			"wrong point of init voteproof; ballot(%q) == voteproof(%q)", bl.Point(), vp.Point()), "")
	case vp.Result() != VoteResultMajority:
		return e(util.InvalidError.Errorf("init voteproof not majority result, %q", vp.Result()), "")
	}

	return nil
}

func IsValidACCEPTBallot(bl ACCEPTBallot, networkID []byte) error {
	e := util.StringErrorFunc("invalid ACCEPTBallot")
	if err := IsValidBallot(bl, networkID); err != nil {
		return e(err, "")
	}

	if _, ok := bl.SignedFact().(ACCEPTBallotSignedFact); !ok {
		return util.InvalidError.Errorf("ACCEPTBallotSignedFact expected, not %T", bl.SignedFact())
	}

	ivp, ok := bl.Voteproof().(INITVoteproof)
	if !ok {
		return e(util.InvalidError.Errorf("not init voteproof in accept ballot, %T", bl.Voteproof()), "")
	}

	if err := checkVoteproofInACCEPTBallot(bl, ivp); err != nil {
		return e(err, "")
	}

	return nil
}

func IsValidBallotFact(fact BallotFact) error {
	e := util.StringErrorFunc("invalid BallotFact")
	if err := IsValidFact(fact, nil); err != nil {
		return e(err, "")
	}

	if err := fact.Point().IsValid(nil); err != nil {
		return e(err, "")
	}

	return nil
}

func IsValidINITBallotFact(fact INITBallotFact) error {
	e := util.StringErrorFunc("invalid INITBallotFact")
	if err := IsValidBallotFact(fact); err != nil {
		return e(err, "")
	}

	if err := util.CheckIsValid(nil, false, fact.PreviousBlock(), fact.Proposal()); err != nil {
		return e(err, "")
	}

	return nil
}

func IsValidACCEPTBallotFact(fact ACCEPTBallotFact) error {
	e := util.StringErrorFunc("invalid ACCEPTBallotFact")
	if err := IsValidBallotFact(fact); err != nil {
		return e(err, "")
	}

	if err := util.CheckIsValid(nil, false,
		fact.Proposal(),
		fact.NewBlock(),
	); err != nil {
		return e(err, "")
	}

	return nil
}

func IsValidBallotSignedFact(sf BallotSignedFact, networkID []byte) error {
	e := util.StringErrorFunc("invalid BallotSignedFact")
	if err := IsValidSignedFact(sf, networkID); err != nil {
		return e(err, "")
	}

	if err := util.CheckIsValid(nil, false,
		sf.Node(),
	); err != nil {
		return e(err, "")
	}

	return nil
}

func IsValidINITBallotSignedFact(sf BallotSignedFact, networkID []byte) error {
	e := util.StringErrorFunc("invalid INITBallotSignedFact")

	if err := IsValidBallotSignedFact(sf, networkID); err != nil {
		return e(err, "")
	}

	if _, ok := sf.Fact().(INITBallotFact); !ok {
		return e(util.InvalidError.Errorf("not INITBallotFact, %T", sf.Fact()), "")
	}

	return nil
}

func IsValidACCEPTBallotSignedFact(sf BallotSignedFact, networkID []byte) error {
	e := util.StringErrorFunc("invalid ACCEPTBallotSignedFact")

	if err := IsValidBallotSignedFact(sf, networkID); err != nil {
		return e(err, "")
	}

	if _, ok := sf.Fact().(ACCEPTBallotFact); !ok {
		return e(util.InvalidError.Errorf("not ACCEPTBallotFact, %T", sf.Fact()), "")
	}

	return nil
}
