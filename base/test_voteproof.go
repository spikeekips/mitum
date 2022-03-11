//go:build test
// +build test

package base

import (
	"github.com/spikeekips/mitum/util/localtime"
	"github.com/stretchr/testify/assert"
)

func EqualVoteproof(t *assert.Assertions, a, b Voteproof) {
	if a == nil {
		t.Equal(a, b)
		return
	}

	t.True(a.Hint().Equal(b.Hint()))
	t.Equal(a.HashBytes(), b.HashBytes())
	t.True(localtime.New(a.FinishedAt()).Equal(localtime.New(b.FinishedAt())))
	t.Equal(a.Point(), b.Point())
	t.Equal(a.Result(), b.Result())

	EqualBallotFact(t, a.Majority(), b.Majority())

	t.Equal(len(a.SignedFacts()), len(b.SignedFacts()))

	as := a.SignedFacts()
	bs := b.SignedFacts()
	for i := range as {
		EqualBallotSignedFact(t, as[i], bs[i])
	}

	t.Equal(a.Threshold(), b.Threshold())
}
