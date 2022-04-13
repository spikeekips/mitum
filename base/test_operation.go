//go:build test
// +build test

package base

import (
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func EqualOperation(t *assert.Assertions, a, b Operation) {
	switch {
	case a == nil && b == nil:
		return
	case a == nil || b == nil:
		t.NoError(errors.Errorf("empty"))

		return
	}

	EqualSignedFact(t, a, b)

	t.True(a.Hint().Equal(b.Hint()))

	t.Equal(a.HashBytes(), b.HashBytes())
}
