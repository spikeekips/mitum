package base

import (
	"github.com/spikeekips/mitum/util"
)

type BallotFact interface {
	Fact
	Point() StagePoint
}

type BallotSignedFact interface {
	util.HashByter
	util.IsValider
	SignedFact
	Node() Address
	Signer() Publickey
}

type Ballot interface {
	SealBody
	Point() StagePoint
	SignedFact() BallotSignedFact
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

type INITBallotSignedFact interface {
	BallotSignedFact
	BallotFact() INITBallotFact
}

type ACCEPTBallotSignedFact interface {
	BallotSignedFact
	BallotFact() ACCEPTBallotFact
}

type INITBallot interface {
	Ballot
	BallotSignedFact() INITBallotSignedFact
}

type ACCEPTBallot interface {
	Ballot
	BallotSignedFact() ACCEPTBallotSignedFact
}

func CountBallotSignedFacts(allsfs []BallotSignedFact) (
	[]string,
	[]BallotSignedFact,
	map[string]BallotFact,
) {
	if len(allsfs) < 1 {
		return nil, nil, nil
	}

	set := make([]string, len(allsfs))
	sfs := make([]BallotSignedFact, len(allsfs))
	m := map[string]BallotFact{}

	for i := range allsfs {
		sf := allsfs[i]

		k := sf.Fact().Hash().String()
		if _, found := m[k]; !found {
			m[k] = sf.Fact().(BallotFact)
		}

		set[i] = k
		sfs[i] = sf
	}

	return set, sfs, m
}
