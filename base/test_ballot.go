//go:build test
// +build test

package base

import (
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/stretchr/testify/assert"
)

func EqualBallotSignedFact(t *assert.Assertions, a, b BallotSignedFact) {
	switch {
	case a == nil && b == nil:
		return
	case a == nil || b == nil:
		t.NoError(errors.Errorf("empty"))

		return
	}

	EqualSignedFact(t, a, b)

	aht := a.(hint.Hinter).Hint()
	bht := b.(hint.Hinter).Hint()
	t.True(aht.Equal(bht), "Hint does not match")

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

	EqualBallotFact(t, af, bf)
}

func EqualBallotFact(t *assert.Assertions, a, b BallotFact) {
	switch {
	case a == nil && b == nil:
		return
	case a == nil || b == nil:
		t.NoError(errors.Errorf("empty"))

		return
	}

	EqualFact(t, a, b)
	t.Equal(a.Point(), b.Point())

	switch at := a.(type) {
	case INITBallotFact:
		equalINITBallotFact(t, at, b.(INITBallotFact))
	case ACCEPTBallotFact:
		equalACCEPTBallotFact(t, at, b.(ACCEPTBallotFact))
	}
}

func equalINITBallotFact(t *assert.Assertions, a, b INITBallotFact) {
	t.True(a.Hash().Equal(b.Hash()))
	t.True(a.PreviousBlock().Equal(b.PreviousBlock()))
	t.True(a.Proposal().Equal(b.Proposal()))
}

func equalACCEPTBallotFact(t *assert.Assertions, a, b ACCEPTBallotFact) {
	t.True(a.Proposal().Equal(b.Proposal()))
	t.True(a.NewBlock().Equal(b.NewBlock()))
}

func EqualBallot(t *assert.Assertions, a, b Ballot) {
	t.Equal(a.HashBytes(), b.HashBytes())
	t.Equal(a.Point(), b.Point())

	EqualBallotSignedFact(t, a.SignedFact(), b.SignedFact())
	EqualVoteproof(t, a.Voteproof(), b.Voteproof())
}
