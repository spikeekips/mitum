//go:build test
// +build test

package base

import (
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func CompareBallotSignedFact(t *assert.Assertions, a, b BallotSignedFact) {
	switch {
	case a == nil && b == nil:
		return
	case a == nil || b == nil:
		t.NoError(errors.Errorf("empty"))

		return
	}

	CompareSignedFact(t, a, b)

	t.True(a.Hint().Equal(b.Hint()))
	t.True(a.Node().Equal(b.Node()))

	var af, bf BallotFact
	switch at := a.(type) {
	case INITBallotSignedFact:
		af = at.BallotFact()
		bf = b.(INITBallotSignedFact).BallotFact()
	case ACCEPTBallotSignedFact:
		af = at.BallotFact()
		bf = b.(ACCEPTBallotSignedFact).BallotFact()
	}

	CompareBallotFact(t, af, bf)
}

func CompareBallotFact(t *assert.Assertions, a, b BallotFact) {
	switch {
	case a == nil && b == nil:
		return
	case a == nil || b == nil:
		t.NoError(errors.Errorf("empty"))

		return
	}

	CompareFact(t, a, b)
	t.Equal(a.Point(), b.Point())

	switch at := a.(type) {
	case INITBallotFact:
		compareINITBallotFact(t, at, b.(INITBallotFact))
	case ACCEPTBallotFact:
		compareACCEPTBallotFact(t, at, b.(ACCEPTBallotFact))
	}
}

func compareINITBallotFact(t *assert.Assertions, a, b INITBallotFact) {
	t.True(a.Hash().Equal(b.Hash()))
	t.True(a.PreviousBlock().Equal(b.PreviousBlock()))
	t.True(a.Proposal().Equal(b.Proposal()))
}

func compareACCEPTBallotFact(t *assert.Assertions, a, b ACCEPTBallotFact) {
	t.True(a.Proposal().Equal(b.Proposal()))
	t.True(a.NewBlock().Equal(b.NewBlock()))
}

func CompareBallot(t *assert.Assertions, a, b Ballot) {
	t.Equal(a.HashBytes(), b.HashBytes())
	t.Equal(a.Point(), b.Point())

	CompareBallotSignedFact(t, a.SignedFact(), b.SignedFact())
	CompareVoteproof(t, a.Voteproof(), b.Voteproof())
}
