package base

import "github.com/spikeekips/mitum/util"

func IsValidBallot(bl Ballot, networkID []byte) error {
	e := util.StringErrorFunc("invalid Ballot")

	if err := util.CheckIsValid(networkID, false,
		bl.INITVoteproof(),
		bl.ACCEPTVoteproof(),
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
		return util.InvalidError.Errorf("INITBallotSignedFact expected, not %T", bl.SignedFact())
	}

	return nil
}

func IsValidProposal(bl Proposal, networkID []byte) error {
	e := util.StringErrorFunc("invalid Proposal")
	if err := IsValidBallot(bl, networkID); err != nil {
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

	return nil
}

func IsValidBallotFact(fact BallotFact) error {
	e := util.StringErrorFunc("invalid BallotFact")
	if err := IsValidFact(fact, nil); err != nil {
		return e(err, "")
	}

	if err := util.CheckIsValid(nil, false,
		fact.Stage(),
		fact.Point(),
	); err != nil {
		return e(err, "")
	}

	return nil
}

func IsValidINITBallotFact(fact INITBallotFact) error {
	e := util.StringErrorFunc("invalid INITBallotFact")
	if err := IsValidBallotFact(fact); err != nil {
		return e(err, "")
	}

	if fact.Stage() != StageINIT {
		return e(util.InvalidError.Errorf("invalid stage, %q", fact.Stage()), "")
	}

	if err := util.CheckIsValid(nil, false, fact.PreviousBlock()); err != nil {
		return e(err, "")
	}

	return nil
}

func IsValidProposalFact(fact ProposalFact) error {
	e := util.StringErrorFunc("invalid ProposalFact")
	if err := IsValidBallotFact(fact); err != nil {
		return e(err, "")
	}

	if fact.Stage() != StageProposal {
		return e(util.InvalidError.Errorf("invalid stage, %q", fact.Stage()), "")
	}

	ops := fact.Operations()
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

	if fact.Stage() != StageACCEPT {
		return e(util.InvalidError.Errorf("invalid stage, %q", fact.Stage()), "")
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
