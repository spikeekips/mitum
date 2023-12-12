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

func EnsureINITVoteproof(vp Voteproof) (ivp INITVoteproof, _ error) {
	e := util.ErrInvalid.Errorf("invalid INITVoteproof")

	if vp.Point().Stage() != StageINIT {
		return nil, e.Errorf("wrong point stage")
	}

	if err := util.SetInterfaceValue(vp, &ivp); err != nil {
		return nil, e.Wrap(err)
	}

	return ivp, nil
}

func EnsureACCEPTVoteproof(vp Voteproof) (avp ACCEPTVoteproof, _ error) {
	e := util.ErrInvalid.Errorf("invalid ACCEPTVoteproof")

	if vp.Point().Stage() != StageACCEPT {
		return nil, e.Errorf("wrong point stage")
	}

	if err := util.SetInterfaceValue(vp, &avp); err != nil {
		return nil, e.Wrap(err)
	}

	return avp, nil
}
