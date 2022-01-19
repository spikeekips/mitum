package base

import (
	"time"

	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
)

type BallotFact interface {
	Fact
	Point() Point
	Stage() Stage
}

type BallotSignedFact interface {
	hint.Hinter
	util.HashByter
	util.IsValider
	SignedFact
	Node() Address
}

type Ballot interface {
	SealBody
	Point() Point
	Stage() Stage
	SignedFact() BallotSignedFact
	INITVoteproof() INITVoteproof
	ACCEPTVoteproof() ACCEPTVoteproof
}

type INITBallotFact interface {
	BallotFact
	PreviousBlock() util.Hash
}

type ProposalFact interface {
	BallotFact
	Operations() []util.Hash
	ProposedAt() time.Time
}

type ACCEPTBallotFact interface {
	BallotFact
	Proposal() util.Hash // NOTE proposal fact hash
	NewBlock() util.Hash
}

type INITBallotSignedFact interface {
	BallotSignedFact
	BallotFact() INITBallotFact
}

type ProposalSignedFact interface {
	BallotSignedFact
	BallotFact() ProposalFact
	Proposer() Address
}

type ACCEPTBallotSignedFact interface {
	BallotSignedFact
	BallotFact() ACCEPTBallotFact
}

type INITBallot interface {
	Ballot
	BallotSignedFact() INITBallotSignedFact
}

type Proposal interface {
	Ballot
	BallotSignedFact() ProposalSignedFact
}

type ACCEPTBallot interface {
	Ballot
	BallotSignedFact() ACCEPTBallotSignedFact
}
