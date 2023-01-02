package base

import (
	"github.com/spikeekips/mitum/util"
)

func IsValidVoteproof(vp Voteproof, networkID NetworkID) error {
	e := util.StringErrorFunc("invalid Voteproof")

	switch {
	case len(vp.ID()) < 1:
		return e(util.ErrInvalid.Errorf("empty id"), "")
	case !vp.Point().Stage().CanVote():
		return e(util.ErrInvalid.Errorf("wrong stage, %q for Voteproof", vp.Point().Stage()), "")
	case vp.Result() == VoteResultNotYet:
		return e(util.ErrInvalid.Errorf("not yet finished"), "")
	case len(vp.SignFacts()) < 1:
		return e(util.ErrInvalid.Errorf("empty sign facts"), "")
	}

	// NOTE check duplicated sign node in SignFacts
	if err := isValidVoteproofDuplicatedSignNode(vp); err != nil {
		return e(err, "")
	}

	if err := util.CheckIsValiders(networkID, false,
		vp.Point(),
		vp.Result(),
		vp.Threshold(),
	); err != nil {
		return e(err, "")
	}

	if err := isValidVoteproofVoteResult(vp, networkID); err != nil {
		return e(err, "")
	}

	if err := isValidVoteproofSignFacts(vp, networkID); err != nil {
		return e(err, "")
	}

	return nil
}

func isValidVoteproofDuplicatedSignNode(vp Voteproof) error {
	facts := vp.SignFacts()

	if _, found := util.IsDuplicatedSlice(facts, func(fact BallotSignFact) (bool, string) {
		switch {
		case fact == nil, fact.Node() == nil:
			return true, ""
		default:
			return true, fact.Node().String()
		}
	}); found {
		return util.ErrInvalid.Errorf("duplicated node found in SignFacts of voteproof")
	}

	return nil
}

func isValidVoteproofVoteResult(vp Voteproof, networkID NetworkID) error {
	switch {
	case vp.Result() == VoteResultDraw:
		if vp.Majority() != nil {
			return util.ErrInvalid.Errorf("not empty majority for draw")
		}
	case vp.Majority() == nil:
		return util.ErrInvalid.Errorf("empty majority for majority")
	default:
		if err := vp.Majority().IsValid(networkID); err != nil {
			return util.ErrInvalid.Wrapf(err, "invalid majority")
		}

		if err := isValidFactInVoteproof(vp, vp.Majority()); err != nil {
			return util.ErrInvalid.Wrapf(err, "invalid majority")
		}
	}

	return nil
}

func isValidVoteproofSignFacts(vp Voteproof, networkID NetworkID) error {
	var majority util.Hash
	if vp.Majority() != nil {
		majority = vp.Majority().Hash()
	}

	vs := vp.SignFacts()
	bs := make([]util.IsValider, len(vs))

	for i := range vs {
		i := i
		bs[i] = util.DummyIsValider(func([]byte) error {
			if vs[i] == nil {
				return util.ErrInvalid.Errorf("nil sign fact found")
			}

			if err := vs[i].IsValid(networkID); err != nil {
				return err
			}

			return isValidSignFactInVoteproof(vp, vs[i])
		})
	}

	if err := util.CheckIsValiders(networkID, false, bs...); err != nil {
		return util.ErrInvalid.Wrapf(err, "invalid sign facts")
	}

	if majority != nil {
		var foundMajority bool

		for i := range vs {
			fact, ok := vs[i].Fact().(BallotFact)
			if !ok {
				return util.ErrInvalid.Errorf("invalid ballot fact")
			}

			if fact.Hash().Equal(majority) {
				foundMajority = true

				break
			}
		}

		if !foundMajority {
			return util.ErrInvalid.Errorf("majoirty not found in sign facts")
		}
	}

	return nil
}

func IsValidINITVoteproof(vp INITVoteproof, _ NetworkID) error {
	if vp.Point().Stage() != StageINIT {
		return util.ErrInvalid.Errorf("wrong stage in INITVoteproof, %q", vp.Point().Stage())
	}

	return nil
}

func IsValidACCEPTVoteproof(vp ACCEPTVoteproof, _ NetworkID) error {
	if vp.Point().Stage() != StageACCEPT {
		return util.ErrInvalid.Errorf("wrong stage for ACCEPTVoteproof, %q", vp.Point().Stage())
	}

	return nil
}

func isValidFactInVoteproof(vp Voteproof, fact BallotFact) error {
	e := util.StringErrorFunc("invalid fact in voteproof")

	// NOTE check point
	if !vp.Point().Equal(fact.Point()) {
		return e(util.ErrInvalid.Errorf(
			"point does not match, voteproof(%q) != fact(%q)", vp.Point(), fact.Point()), "")
	}

	return nil
}

func isValidSignFactInVoteproof(vp Voteproof, sf BallotSignFact) error {
	e := util.StringErrorFunc("invalid sign fact in voteproof")

	if err := isValidFactInVoteproof( //nolint:forcetypeassert // already checked
		vp, sf.Fact().(BallotFact)); err != nil {
		return e(err, "")
	}

	return nil
}

func IsValidVoteproofWithSuffrage(vp Voteproof, suf Suffrage, th Threshold) error {
	e := util.StringErrorFunc("invalid voteproof with suffrage")

	sfs := vp.SignFacts()

	for i := range sfs {
		n := sfs[i]

		switch {
		case !suf.Exists(n.Node()):
			return e(util.ErrInvalid.Errorf("unknown node found, %q", n.Node()), "")
		case !suf.ExistsPublickey(n.Node(), n.Signer()):
			return e(util.ErrInvalid.Errorf("wrong publickey"), "")
		}
	}

	if _, ok := vp.(StuckVoteproof); !ok {
		set, m := CountBallotSignFacts(sfs)
		result, majoritykey := th.VoteResult(uint(suf.Len()), set)

		switch {
		case result != vp.Result():
			return e(util.ErrInvalid.Errorf("wrong result; voteproof(%q) != %q", vp.Result(), result), "")
		case result == VoteResultDraw:
			if vp.Majority() != nil {
				return e(util.ErrInvalid.Errorf("not empty majority for draw"), "")
			}
		case result == VoteResultMajority:
			if vp.Majority() == nil {
				return e(util.ErrInvalid.Errorf("empty majority for majority"), "")
			}

			if !vp.Majority().Hash().Equal(m[majoritykey].Hash()) {
				return e(util.ErrInvalid.Errorf("wrong majority for majority"), "")
			}
		}
	}

	return nil
}
