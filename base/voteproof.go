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

type HasWithdrawVoteproof interface {
	Withdraws() []SuffrageWithdrawOperation
}

type WithdrawVoteproof interface {
	HasWithdrawVoteproof
	IsWithdrawVoteproof() bool // NOTE should be true
}

type StuckVoteproof interface {
	HasWithdrawVoteproof
	IsStuckVoteproof() bool // NOTE should be true
}

func EnsureINITVoteproof(vp Voteproof) (INITVoteproof, error) {
	e := util.StringErrorFunc("invalid INITVoteproof")

	if vp.Point().Stage() != StageINIT {
		return nil, e(util.ErrInvalid.Errorf("wrong point stage"), "")
	}

	i, ok := vp.(INITVoteproof)
	if !ok {
		return nil, e(util.ErrInvalid.Errorf("expected INITVoteproof, but %T", vp), "")
	}

	return i, nil
}

func EnsureACCEPTVoteproof(vp Voteproof) (ACCEPTVoteproof, error) {
	e := util.StringErrorFunc("invalid ACCEPTVoteproof")

	if vp.Point().Stage() != StageACCEPT {
		return nil, e(util.ErrInvalid.Errorf("wrong point stage"), "")
	}

	i, ok := vp.(ACCEPTVoteproof)
	if !ok {
		return nil, e(util.ErrInvalid.Errorf("expected ACCEPTVoteproof, but %T", vp), "")
	}

	return i, nil
}
