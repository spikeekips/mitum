//go:build test
// +build test

package base

import (
	"reflect"

	"github.com/stretchr/testify/assert"
)

func EqualSeal(t *assert.Assertions, a, b Seal) {
	t.True(a.Hint().Equal(b.Hint()))
	EqualSigned(t, a.Signed(), b.Signed())

	abs := a.Body()
	bbs := b.Body()
	t.Equal(len(abs), len(bbs))

	for i := range abs {
		EqualSealBody(t, abs[i], bbs[i])
	}
}

func EqualSealBody(t *assert.Assertions, a, b SealBody) {
	t.Equal(reflect.TypeOf(a), reflect.TypeOf(b))
	t.Equal(a.HashBytes(), b.HashBytes())
}
