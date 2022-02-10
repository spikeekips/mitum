package base

import (
	"github.com/spikeekips/mitum/util"
)

func IsValidBallot(bl Ballot, networkID []byte) error {
	e := util.StringErrorFunc("invalid Ballot")

	if err := util.CheckIsValid(networkID, false,
		bl.ACCEPTVoteproof(),
		bl.SignedFact(),
	); err != nil {
		return e(err, "")
	}

	if bl.INITVoteproof() != nil {
		if err := bl.INITVoteproof().IsValid(networkID); err != nil {
			return e(err, "invalid init voteproof")
		}
	}

	if err := checkACCEPTVoteproofInBallot(bl, bl.ACCEPTVoteproof()); err != nil {
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

	if err := checkINITVoteproofInINITBallot(bl, bl.INITVoteproof()); err != nil {
		return e(err, "")
	}

	if err := checkACCEPTVoteproofInINITBallot(bl, bl.ACCEPTVoteproof()); err != nil {
		return e(err, "")
	}

	return nil
}

func checkINITVoteproofInINITBallot(bl INITBallot, vp INITVoteproof) error {
	e := util.StringErrorFunc("invalid init voteproof in init ballot")
	switch {
	case bl.Point().Round() == 0:
		if vp != nil {
			return e(util.InvalidError.Errorf("init voteproof should be nil in 0 round init ballot"), "")
		}

		return nil
	case vp == nil:
		return e(util.InvalidError.Errorf("init voteproof should be not nil in none 0 round init ballot"), "")
	}

	blp := bl.Point()
	vpp := vp.Point()
	switch {
	case blp.Height() != vpp.Height():
		return e(util.InvalidError.Errorf(
			"wrong height of init voteproof; ballot(%d) == voteproof(%d)", blp.Height(), vpp.Height()), "")
	case blp.Round() != vpp.Round()+1:
		return e(util.InvalidError.Errorf(
			"wrong round of init voteproof; ballot(%d) == voteproof(%d) + 1", blp.Round(), vpp.Round()), "")
	}

	return nil
}

func checkINITVoteproofInNotINITBallot(bl Ballot, vp INITVoteproof) error {
	e := util.StringErrorFunc("invalid init voteproof in not init ballot")
	if vp == nil {
		return e(util.InvalidError.Errorf("empty init voteproof"), "")
	}

	blp := bl.Point()
	vpp := vp.Point()
	switch {
	case blp.Height() != vpp.Height():
		return e(util.InvalidError.Errorf(
			"wrong height of init voteproof; ballot(%d) == voteproof(%d)", blp.Height(), vpp.Height()), "")
	case blp.Round() != vpp.Round():
		return e(util.InvalidError.Errorf(
			"wrong round of init voteproof; ballot(%d) == voteproof(%d)", blp.Round(), vpp.Round()), "")
	}

	return nil
}

func checkACCEPTVoteproofInBallot(bl Ballot, vp ACCEPTVoteproof) error {
	e := util.StringErrorFunc("invalid init voteproof in ballot")

	if vp.Result() != VoteResultMajority {
		return e(util.InvalidError.Errorf("accept voteproof not majority result, %q", vp.Result()), "")
	}

	blp := bl.Point()
	vpp := vp.Point()
	switch {
	case blp.Height() != vpp.Height()+1:
		return e(util.InvalidError.Errorf(
			"wrong height of accept voteproof; ballot(%d) == voteproof(%d) + 1", blp.Height(), vpp.Height()), "")
	}

	return nil
}

func checkACCEPTVoteproofInINITBallot(bl INITBallot, vp ACCEPTVoteproof) error {
	e := util.StringErrorFunc("invalid init voteproof in init ballot")

	fact := bl.BallotSignedFact().BallotFact()
	if !fact.PreviousBlock().Equal(vp.BallotMajority().NewBlock()) {
		return e(util.InvalidError.Errorf("block does not match, ballot(%q) != voteproof(%q)",
			fact.PreviousBlock(),
			vp.BallotMajority().NewBlock(),
		), "")
	}

	return nil
}

func IsValidProposal(bl Proposal, networkID []byte) error {
	e := util.StringErrorFunc("invalid Proposal")
	if bl.INITVoteproof() != nil || bl.ACCEPTVoteproof() != nil {
		return e(util.InvalidError.Errorf("init and accept voteproof should be empty"), "")
	}

	if err := util.CheckIsValid(networkID, false,
		bl.SignedFact(),
	); err != nil {
		return e(err, "")
	}

	if _, ok := bl.SignedFact().(ProposalSignedFact); !ok {
		return util.InvalidError.Errorf("ProposalSignedFact expected, not %T", bl.SignedFact())
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

	if err := checkINITVoteproofInNotINITBallot(bl, bl.INITVoteproof()); err != nil {
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

func IsValidProposalFact(fact ProposalFact) error {
	e := util.StringErrorFunc("invalid ProposalFact")
	if err := IsValidBallotFact(fact); err != nil {
		return e(err, "")
	}

	ops := fact.Operations()
	if util.CheckSliceDuplicated(ops, func(i interface{}) string {
		j, ok := i.(util.Hash)
		if !ok {
			return ""
		}

		return j.String()
	}) {
		return util.InvalidError.Errorf("duplicated operation found")
	}

	vs := make([]util.IsValider, len(ops)+1)
	vs[0] = util.DummyIsValider(func([]byte) error {
		if fact.ProposedAt().IsZero() {
			return util.InvalidError.Errorf("empty ProposedAt")
		}

		return nil
	})

	for i := range ops {
		vs[i+1] = ops[i]
	}

	if err := util.CheckIsValid(nil, false, vs...); err != nil {
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

func IsValidProposalSignedFact(sf BallotSignedFact, networkID []byte) error {
	e := util.StringErrorFunc("invalid ProposalSignedFact")

	if err := IsValidBallotSignedFact(sf, networkID); err != nil {
		return e(err, "")
	}

	if _, ok := sf.Fact().(ProposalFact); !ok {
		return e(util.InvalidError.Errorf("not ProposalFact, %T", sf.Fact()), "")
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
