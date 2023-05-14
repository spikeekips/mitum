package base

import (
	"time"

	"github.com/spikeekips/mitum/util"
)

type Voteproof interface {
	util.IsValider
	util.HashByter
	FinishedAt() time.Time // NOTE if zero, not yet finished
	Point() StagePoint
	Result() VoteResult
	Threshold() Threshold
	Majority() BallotFact
	SignFacts() []BallotSignFact
	ID() string // NOTE ID is only unique in local machine
}

type INITVoteproof interface {
	Voteproof
	BallotMajority() INITBallotFact
	BallotSignFacts() []INITBallotSignFact
}

type ACCEPTVoteproof interface {
	Voteproof
	BallotMajority() ACCEPTBallotFact
	BallotSignFacts() []ACCEPTBallotSignFact
}

type ExpelVoteproof interface {
	HasExpels
	IsExpelVoteproof() bool // NOTE should be true
}

type StuckVoteproof interface {
	HasExpels
	IsStuckVoteproof() bool // NOTE should be true
}

func EnsureINITVoteproof(vp Voteproof) (INITVoteproof, error) {
	e := util.StringError("invalid INITVoteproof")

	if vp.Point().Stage() != StageINIT {
		return nil, e.Wrap(util.ErrInvalid.Errorf("wrong point stage"))
	}

	i, ok := vp.(INITVoteproof)
	if !ok {
		return nil, e.Wrap(util.ErrInvalid.Errorf("expected INITVoteproof, but %T", vp))
	}

	return i, nil
}

func EnsureACCEPTVoteproof(vp Voteproof) (ACCEPTVoteproof, error) {
	e := util.StringError("invalid ACCEPTVoteproof")

	if vp.Point().Stage() != StageACCEPT {
		return nil, e.Wrap(util.ErrInvalid.Errorf("wrong point stage"))
	}

	i, ok := vp.(ACCEPTVoteproof)
	if !ok {
		return nil, e.Wrap(util.ErrInvalid.Errorf("expected ACCEPTVoteproof, but %T", vp))
	}

	return i, nil
}
