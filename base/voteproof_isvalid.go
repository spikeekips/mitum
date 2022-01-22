package base

import "github.com/spikeekips/mitum/util"

func isValidVoteproof(vp Voteproof, networkID NetworkID) error {
	e := util.StringErrorFunc("invalid Voteproof")

	switch {
	case len(vp.ID()) < 1:
		return e(util.InvalidError.Errorf("empty id"), "")
	case !vp.Stage().CanVote():
		return e(util.InvalidError.Errorf("wrong stage, %q for Voteproof", vp.Stage()), "")
	case vp.Result() == VoteResultNotYet:
		return e(util.InvalidError.Errorf("not yet finished"), "")
	case vp.FinishedAt().IsZero():
		return e(util.InvalidError.Errorf("draw, but zero finished time"), "")
	case len(vp.SignedFacts()) < 1:
		return e(util.InvalidError.Errorf("empty signed facts"), "")
	}

	if err := util.CheckIsValid(networkID, false,
		vp.Majority(),
		vp.Point(),
		vp.Result(),
		vp.Stage(),
		// BLOCK uncomment checking SuffrageInfo
		// vp.Suffrage(),
	); err != nil {
		return e(err, "")
	}

	if vp.Result() != VoteResultDraw {
		if err := vp.Majority().IsValid(networkID); err != nil {
			return e(err, "invalid majority")
		}

		if err := isValidFactInVoteproof(vp, vp.Majority()); err != nil {
			return e(err, "invalid majority")
		}
	}

	var majority util.Hash
	if vp.Majority() != nil {
		majority = vp.Majority().Hash()
	}

	var foundMajority bool
	vs := vp.SignedFacts()
	bs := make([]util.IsValider, len(vs))
	for i := range vs {
		i := i
		bs[i] = util.DummyIsValider(func([]byte) error {
			if err := vs[i].IsValid(networkID); err != nil {
				return e(err, "")
			}

			return isValidSignedFactInVoteproof(vp, vs[i])
		})

		if majority != nil && !foundMajority && vs[i].Fact().(BallotFact).Hash().Equal(majority) {
			foundMajority = true
		}
	}

	if majority != nil && !foundMajority {
		return e(util.InvalidError.Errorf("majoirty not found in signed facts"), "")
	}

	if err := util.CheckIsValid(networkID, false, bs...); err != nil {
		return e(err, "invalid signed facts")
	}

	// NOTE check majority with SignedFacts
	if err := isValidSignedFactsInVoteproof(vp, vs); err != nil {
		return e(err, "")
	}

	return nil
}

func IsValidINITVoteproof(vp INITVoteproof, networkID NetworkID) error {
	e := util.StringErrorFunc("invalid INITVoteproof")

	if err := isValidVoteproof(vp, networkID); err != nil {
		return e(err, "")
	}

	if vp.Stage() != StageINIT {
		return e(util.InvalidError.Errorf("wrong stage for INITVoteproof, %q", vp.Stage()), "")
	}

	return nil
}

func IsValidACCEPTVoteproof(vp ACCEPTVoteproof, networkID NetworkID) error {
	e := util.StringErrorFunc("invalid ACCEPTVoteproof")

	if err := isValidVoteproof(vp, networkID); err != nil {
		return e(err, "")
	}

	if vp.Stage() != StageACCEPT {
		return e(util.InvalidError.Errorf("wrong stage for ACCEPTVoteproof, %q", vp.Stage()), "")
	}

	return nil
}

func isValidFactInVoteproof(vp Voteproof, fact BallotFact) error {
	e := util.StringErrorFunc("invalid fact in voteproof")

	// NOTE check point
	if vp.Point() != fact.Point() {
		return e(util.InvalidError.Errorf(
			"point does not match, voteproof(%q) != fact(%q)", vp.Point(), fact.Point()), "")
	}

	if vp.Stage() != fact.Stage() {
		return e(util.InvalidError.Errorf(
			"stage does not match, voteproof(%q) != fact(%q)", vp.Stage(), fact.Stage()), "")
	}

	return nil
}

func isValidSignedFactInVoteproof(vp Voteproof, sf BallotSignedFact) error {
	e := util.StringErrorFunc("invalid signed fact in voteproof")

	// BLOCK check node is in suffrage

	if err := isValidFactInVoteproof(vp, sf.Fact().(BallotFact)); err != nil {
		return e(err, "")
	}

	return nil
}

func isValidSignedFactsInVoteproof(Voteproof, []BallotSignedFact) error {
	// BLOCK SuffrageInfo.Threshold
	/*
		e := util.StringErrorFunc("invalid signed facts in voteproof")

		facts := make([]string, len(sfs))
		for i := range sfs {
			facts[i] = string(sfs[i].Fact().(BallotFact).Hash().Bytes())
		}
	*/

	return nil
}
