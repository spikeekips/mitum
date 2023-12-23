package base

import (
	"github.com/spikeekips/mitum/util"
)

func IsValidVoteproof(vp Voteproof, networkID NetworkID) error {
	e := util.ErrInvalid.Errorf("invalid Voteproof")

	switch {
	case len(vp.ID()) < 1:
		return e.Errorf("empty id")
	case !vp.Point().Stage().CanVote():
		return e.Errorf("wrong stage, %q for Voteproof", vp.Point().Stage())
	case vp.Result() == VoteResultNotYet:
		return e.Errorf("not yet finished")
	case len(vp.SignFacts()) < 1:
		return e.Errorf("empty sign facts")
	}

	// NOTE check duplicated sign node in SignFacts
	if err := isValidVoteproofDuplicatedSignNode(vp); err != nil {
		return e.Wrap(err)
	}

	if err := util.CheckIsValiders(networkID, false,
		vp.Point(),
		vp.Result(),
		vp.Threshold(),
	); err != nil {
		return e.Wrap(err)
	}

	if err := isValidVoteproofVoteResult(vp, networkID); err != nil {
		return e.Wrap(err)
	}

	if err := isValidVoteproofSignFacts(vp, networkID); err != nil {
		return e.Wrap(err)
	}

	return nil
}

func isValidVoteproofDuplicatedSignNode(vp Voteproof) error {
	facts := vp.SignFacts()

	if util.IsDuplicatedSlice(facts, func(fact BallotSignFact) (bool, string) {
		switch {
		case fact == nil, fact.Node() == nil:
			return true, ""
		default:
			return true, fact.Node().String()
		}
	}) {
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
			return util.ErrInvalid.WithMessage(err, "invalid majority")
		}

		if err := isValidFactInVoteproof(vp, vp.Majority()); err != nil {
			return util.ErrInvalid.WithMessage(err, "invalid majority")
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
		return util.ErrInvalid.WithMessage(err, "invalid sign facts")
	}

	if majority != nil {
		var foundMajority bool

	end:
		for i := range vs {
			switch fact, err := util.AssertInterfaceValue[BallotFact](vs[i].Fact()); {
			case err != nil:
				return util.ErrInvalid.Wrap(err)
			case fact.Hash().Equal(majority):
				foundMajority = true

				break end
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
	e := util.ErrInvalid.Errorf("invalid fact in voteproof")

	// NOTE check point
	if !vp.Point().Equal(fact.Point()) {
		return e.Errorf(
			"point does not match, voteproof(%q) != fact(%q)", vp.Point(), fact.Point())
	}

	return nil
}

func isValidSignFactInVoteproof(vp Voteproof, sf BallotSignFact) error {
	e := util.ErrInvalid.Errorf("invalid sign fact in voteproof")

	if err := isValidFactInVoteproof( //nolint:forcetypeassert // already checked
		vp, sf.Fact().(BallotFact)); err != nil {
		return e.Wrap(err)
	}

	return nil
}

func IsValidVoteproofWithSuffrage(vp Voteproof, suf Suffrage, th Threshold) error {
	e := util.ErrInvalid.Errorf("invalid voteproof with suffrage")

	sfs := vp.SignFacts()

	for i := range sfs {
		n := sfs[i]

		switch {
		case !suf.Exists(n.Node()):
			return e.Errorf("unknown node found, %q", n.Node())
		case !suf.ExistsPublickey(n.Node(), n.Signer()):
			return e.Errorf("wrong publickey")
		}
	}

	if _, ok := vp.(StuckVoteproof); !ok {
		set, m := CountBallotSignFacts(sfs)
		defer clear(m)

		result, majoritykey := th.VoteResult(uint(suf.Len()), set)

		switch {
		case result != vp.Result():
			return e.Errorf("wrong result; voteproof(%q) != %q", vp.Result(), result)
		case result == VoteResultDraw:
			if vp.Majority() != nil {
				return e.Errorf("not empty majority for draw")
			}
		case result == VoteResultMajority:
			if vp.Majority() == nil {
				return e.Errorf("empty majority for majority")
			}

			if !vp.Majority().Hash().Equal(m[majoritykey].Hash()) {
				return e.Errorf("wrong majority for majority")
			}
		}
	}

	return nil
}
