//go:build test
// +build test

package base

import (
	"github.com/pkg/errors"
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

	t.True(a.Hint().Equal(b.Hint()))

	t.Equal(a.HashBytes(), b.HashBytes())
}
