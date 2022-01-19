//go:build test
// +build test

package base

import (
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/localtime"
	"github.com/stretchr/testify/assert"
)

var (
	dummyINITVoteproofHint   = hint.MustNewHint("dummy-init-voteproof-v3.3.3")
	dummyACCEPTVoteproofHint = hint.MustNewHint("dummy-accept-voteproof-v3.3.3")
)

type DummyINITVoteproof struct {
	BaseINITVoteproof
}

func NewDummyINITVoteproof(p Point) DummyINITVoteproof {
	return DummyINITVoteproof{
		BaseINITVoteproof: NewBaseINITVoteproof(dummyINITVoteproofHint, p),
	}
}

type DummyACCEPTVoteproof struct {
	BaseACCEPTVoteproof
}

func NewDummyACCEPTVoteproof(p Point) DummyACCEPTVoteproof {
	return DummyACCEPTVoteproof{
		BaseACCEPTVoteproof: NewBaseACCEPTVoteproof(dummyACCEPTVoteproofHint, p),
	}
}

func CompareVoteproof(t *assert.Assertions, a, b Voteproof) {
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

	// BLOCK compare SuffrageInfo
}
