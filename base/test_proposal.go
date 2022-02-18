//go:build test
// +build test

package base

import (
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util/localtime"
	"github.com/stretchr/testify/assert"
)

func CompareProposalSignedFact(t *assert.Assertions, a, b ProposalSignedFact) {
	switch {
	case a == nil && b == nil:
		return
	case a == nil || b == nil:
		t.NoError(errors.Errorf("empty"))

		return
	}

	CompareSignedFact(t, a, b)

	t.True(a.Hint().Equal(b.Hint()))

	CompareProposalFact(t, a.ProposalFact(), b.ProposalFact())
}

func CompareProposalFact(t *assert.Assertions, a, b ProposalFact) {
	switch {
	case a == nil && b == nil:
		return
	case a == nil || b == nil:
		t.NoError(errors.Errorf("empty"))

		return
	}

	CompareFact(t, a, b)
	t.Equal(a.Point(), b.Point())
	t.True(a.Proposer().Equal(b.Proposer()))

	t.Equal(len(a.Operations()), len(b.Operations()))

	aop := a.Operations()
	bop := b.Operations()
	for i := range aop {
		t.True(aop[i].Equal(bop[i]))
	}
	t.True(localtime.Equal(a.ProposedAt(), b.ProposedAt()))
}
