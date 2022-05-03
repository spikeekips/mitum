package base

import (
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/stretchr/testify/assert"
)

func EqualSuffrageInfo(t *assert.Assertions, a, b SuffrageInfo) {
	switch {
	case a == nil && b == nil:
		return
	case a == nil || b == nil:
		t.NoError(errors.Errorf("empty"))

		return
	}

	aht := a.(hint.Hinter).Hint()
	bht := b.(hint.Hinter).Hint()

	t.True(aht.Equal(bht))

	t.True(a.State().Equal(b.State()), "state: %q != %q", a.State(), b.State())
	t.Equal(a.Height(), b.Height(), "height: %d != %d", a.Height(), b.Height())
	t.Equal(len(a.Suffrage()), len(b.Suffrage()), "suffrage: %d != %d", len(a.Suffrage()), len(b.Suffrage()))
	t.Equal(len(a.Candidates()), len(b.Candidates()), "candidates: %d != %d", len(a.Candidates()), len(b.Candidates()))

	asufs := a.Suffrage()
	bsufs := b.Suffrage()
	for i := range asufs {
		t.True(IsEqualNode(asufs[i], bsufs[i]))
	}

	acans := a.Candidates()
	bcans := b.Candidates()
	for i := range asufs {
		EqualSuffrageCandidate(t, acans[i], bcans[i])
	}
}

func EqualSuffrageCandidate(t *assert.Assertions, a, b SuffrageCandidate) {
	switch {
	case a == nil && b == nil:
		return
	case a == nil || b == nil:
		t.NoError(errors.Errorf("empty"))

		return
	}

	aht := a.(hint.Hinter).Hint()
	bht := b.(hint.Hinter).Hint()

	t.True(aht.Equal(bht))

	t.True(IsEqualNode(a, b))
	t.Equal(a.Start(), b.Start())
	t.Equal(a.Deadline(), b.Deadline())
}
