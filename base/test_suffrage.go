package base

import (
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/stretchr/testify/assert"
)

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
