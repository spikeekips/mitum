package base

import (
	"github.com/spikeekips/mitum/util"
)

type BallotFact interface {
	Fact
	Point() StagePoint
}

type BallotSignFact interface {
	NodeSignFact
	Node() Address
	Signer() Publickey
}

type Ballot interface {
	util.IsValider
	util.HashByter
	Point() StagePoint
	SignFact() BallotSignFact
	Voteproof() Voteproof
}

type INITBallotFact interface {
	BallotFact
	PreviousBlock() util.Hash
	Proposal() util.Hash
}

type ACCEPTBallotFact interface {
	BallotFact
	Proposal() util.Hash // NOTE proposal fact hash
	NewBlock() util.Hash
}

type INITBallotSignFact interface {
	BallotSignFact
	BallotFact() INITBallotFact
}

type ACCEPTBallotSignFact interface {
	BallotSignFact
	BallotFact() ACCEPTBallotFact
}

type INITBallot interface {
	Ballot
	BallotSignFact() INITBallotSignFact
}

type ACCEPTBallot interface {
	Ballot
	BallotSignFact() ACCEPTBallotSignFact
}

func CountBallotSignFacts(sfs []BallotSignFact) (set []string, m map[string]BallotFact) {
	if len(sfs) < 1 {
		return set, m
	}

	set = make([]string, len(sfs))
	m = map[string]BallotFact{}

	for i := range sfs {
		sf := sfs[i]

		k := sf.Fact().Hash().String()
		m[k] = sf.Fact().(BallotFact) //nolint:forcetypeassert //...

		set[i] = k
	}

	return set, m
}
