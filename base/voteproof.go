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
