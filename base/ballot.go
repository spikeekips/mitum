package base

import (
	"github.com/pkg/errors"
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
	set []string,
	sfs []BallotSignedFact,
	m map[string]BallotFact,
	err error,
) {
	if len(allsfs) < 1 {
		return set, sfs, m, nil
	}

	set = make([]string, len(allsfs))
	sfs = make([]BallotSignedFact, len(allsfs))
	m = map[string]BallotFact{}

	for i := range allsfs {
		sf := allsfs[i]

		k := sf.Fact().Hash().String()
		if _, found := m[k]; !found {
			switch j, ok := sf.Fact().(BallotFact); {
			case !ok:
				return nil, nil, nil, errors.Errorf("expected BallotFact, but %T", sf.Fact())
			default:
				m[k] = j
			}
		}

		set[i] = k
		sfs[i] = sf
	}

	return set, sfs, m, nil
}
