//go:build test
// +build test

package base

import (
	"github.com/spikeekips/mitum/util/localtime"
	"github.com/stretchr/testify/assert"
)

func CompareBallotSignedFact(t *assert.Assertions, a, b BallotSignedFact) {
	CompareSignedFact(t, a, b)

	t.True(a.Hint().Equal(b.Hint()))
	t.True(a.Node().Equal(b.Node()))

	var af, bf BallotFact
	switch at := a.(type) {
	case INITBallotSignedFact:
		af = at.BallotFact()
		bf = b.(INITBallotSignedFact).BallotFact()
	case ProposalSignedFact:
		t.True(at.Proposer().Equal(b.(ProposalSignedFact).Proposer()))

		af = at.BallotFact()
		bf = b.(ProposalSignedFact).BallotFact()
	case ACCEPTBallotSignedFact:
		af = at.BallotFact()
		bf = b.(ACCEPTBallotSignedFact).BallotFact()
	}

	CompareBallotFact(t, af, bf)
}

func CompareBallotFact(t *assert.Assertions, a, b BallotFact) {
	CompareFact(t, a, b)
	t.Equal(a.Point(), b.Point())

	switch at := a.(type) {
	case INITBallotFact:
		compareINITBallotFact(t, at, b.(INITBallotFact))
	case ProposalFact:
		compareProposalFact(t, at, b.(ProposalFact))
	case ACCEPTBallotFact:
		compareACCEPTBallotFact(t, at, b.(ACCEPTBallotFact))
	}
}

func compareINITBallotFact(t *assert.Assertions, a, b INITBallotFact) {
	t.True(a.PreviousBlock().Equal(b.PreviousBlock()))
}

func compareProposalFact(t *assert.Assertions, a, b ProposalFact) {
	t.Equal(len(a.Operations()), len(b.Operations()))

	aop := a.Operations()
	bop := b.Operations()
	for i := range aop {
		t.True(aop[i].Equal(bop[i]))
	}
	t.Equal(localtime.NewTime(a.ProposedAt()).Normalize(), localtime.NewTime(b.ProposedAt()).Normalize())
}

func compareACCEPTBallotFact(t *assert.Assertions, a, b ACCEPTBallotFact) {
	t.True(a.Proposal().Equal(b.Proposal()))
	t.True(a.NewBlock().Equal(b.NewBlock()))
}

func CompareBallot(t *assert.Assertions, a, b Ballot) {
	t.Equal(a.HashBytes(), b.HashBytes())
	t.Equal(a.Point(), b.Point())

	CompareBallotSignedFact(t, a.SignedFact(), b.SignedFact())
	CompareVoteproof(t, a.INITVoteproof(), b.INITVoteproof())
	CompareVoteproof(t, a.ACCEPTVoteproof(), b.ACCEPTVoteproof())
}
