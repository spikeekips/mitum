//go:build test
// +build test

package base

import (
	"github.com/spikeekips/mitum/util/localtime"
	"github.com/stretchr/testify/assert"
)

func CompareVoteproof(t *assert.Assertions, a, b Voteproof) {
	if a == nil {
		t.Equal(a, b)
		return
	}

	t.True(a.Hint().Equal(b.Hint()))
	t.Equal(a.HashBytes(), b.HashBytes())
	t.True(localtime.NewTime(a.FinishedAt()).Equal(localtime.NewTime(b.FinishedAt())))
	t.Equal(a.Point(), b.Point())
	t.Equal(a.Stage(), b.Stage())
	t.Equal(a.Result(), b.Result())

	CompareBallotFact(t, a.Majority(), b.Majority())

	t.Equal(len(a.SignedFacts()), len(b.SignedFacts()))

	as := a.SignedFacts()
	bs := b.SignedFacts()
	for i := range as {
		CompareBallotSignedFact(t, as[i], bs[i])
	}

	CompareSuffrageInfo(t, a.Suffrage(), b.Suffrage())
}
