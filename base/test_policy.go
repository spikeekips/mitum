//go:build test
// +build test

package base

import (
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/stretchr/testify/assert"
)

func EqualNetworkPolicy(t *assert.Assertions, a, b NetworkPolicy) {
	switch {
	case a == nil && b == nil:
		return
	case a == nil || b == nil:
		t.NoError(errors.Errorf("empty"))

		return
	}

	aht := a.(hint.Hinter).Hint()
	bht := b.(hint.Hinter).Hint()
	t.True(aht.Equal(bht), "Hint does not match")

	t.Equal(a.HashBytes(), b.HashBytes())
}
