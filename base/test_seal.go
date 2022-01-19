//go:build test
// +build test

package base

import (
	"reflect"

	"github.com/stretchr/testify/assert"
)

func CompareSeal(t *assert.Assertions, a, b Seal) {
	t.True(a.Hint().Equal(b.Hint()))
	CompareSigned(t, a.Signed(), b.Signed())

	abs := a.Body()
	bbs := b.Body()
	t.Equal(len(abs), len(bbs))

	for i := range abs {
		CompareSealBody(t, abs[i], bbs[i])
	}
}

func CompareSealBody(t *assert.Assertions, a, b SealBody) {
	t.Equal(reflect.TypeOf(a), reflect.TypeOf(b))
	t.Equal(a.HashBytes(), b.HashBytes())
}
