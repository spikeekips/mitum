package base

import (
	"time"

	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
)

type Voteproof interface {
	hint.Hinter
	util.IsValider
	util.HashByter
	FinishedAt() time.Time // NOTE if zero, not yet finished
	Point() Point
	Result() VoteResult
	Stage() Stage
	Suffrage() SuffrageInfo
	Majority() BallotFact
	SignedFacts() []BallotSignedFact
	ID() string // NOTE ID is only unique in local machine
}

type INITVoteproof interface {
	Voteproof
	BallotMajority() INITBallotFact
	BallotSignedFacts() []INITBallotSignedFact
}

type ACCEPTVoteproof interface {
	Voteproof
	BallotMajority() ACCEPTBallotFact
	BallotSignedFacts() []ACCEPTBallotSignedFact
}

func isValidVoteproof(vp Voteproof, networkID NetworkID) error {
	e := util.StringErrorFunc("invalid Voteproof")

	switch {
	case len(vp.ID()) < 1:
		return e(util.InvalidError.Errorf("empty id"), "")
	case !vp.Stage().CanVote():
		return e(util.InvalidError.Errorf("wrong stage, %q for Voteproof", vp.Stage()), "")
	case vp.Result() == VoteResultNotYet:
		return e(util.InvalidError.Errorf("not yet finished"), "")
	case vp.Result() != VoteResultDraw && vp.FinishedAt().IsZero():
		return e(util.InvalidError.Errorf("draw, but zero finished time"), "")
	case len(vp.SignedFacts()) < 1:
		return e(util.InvalidError.Errorf("empty votes"), "")
	}

	if err := util.CheckIsValid(networkID, false,
		vp.Majority(),
		vp.Point(),
		vp.Result(),
		vp.Stage(),
		// BLOCK uncomment checking SuffrageInfo
		// vp.Suffrage(),
		util.DummyIsValider(func([]byte) error {
			if vp.FinishedAt().IsZero() {
				return util.InvalidError.Errorf("empty finishedAt in Voteproof")
			}

			return nil
		}),
	); err != nil {
		return e(err, "")
	}

	vs := vp.SignedFacts()
	bs := make([]util.IsValider, len(vs))
	for i := range vs {
		i := i
		bs[i] = util.DummyIsValider(func([]byte) error {
			return isValidSignedFactInVoteproof(vp, vs[i])
		})
	}

	if err := util.CheckIsValid(networkID, false, bs...); err != nil {
		return e(err, "invalid votes")
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

func isValidSignedFactInVoteproof(vp Voteproof, sf BallotSignedFact) error {
	e := util.StringErrorFunc("invalid singed fact in voteproof")

	// BLOCK check node is in suffrage

	fact := sf.Fact().(BallotFact)

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

func isValidSignedFactsInVoteproof(Voteproof, []BallotSignedFact) error {
	// BLOCK SuffrageInfo.Threshold
	/*
		e := util.StringErrorFunc("invalid singed facts in voteproof")

		facts := make([]string, len(sfs))
		for i := range sfs {
			facts[i] = string(sfs[i].Fact().(BallotFact).Hash().Bytes())
		}
	*/

	return nil
}
