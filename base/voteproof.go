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
	Point() StagePoint
	Result() VoteResult
	Threshold() Threshold
	Majority() BallotFact
	SignedFacts() []BallotSignedFact // BLOCK Fact of BallotSignedFact can be duplicated within SignedFacts
	ID() string                      // NOTE ID is only unique in local machine
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
