//go:build test
// +build test

package base

import "github.com/stretchr/testify/assert"

func EqualFact(t *assert.Assertions, a, b Fact) {
	t.True(a.Hint().Equal(b.Hint()))
	t.True(a.Hash().Equal(b.Hash()))
	t.Equal(a.Token(), b.Token())
}

func EqualSignedFact(t *assert.Assertions, a, b SignedFact) {
	t.Equal(a.HashBytes(), b.HashBytes())

	EqualFact(t, a.Fact(), b.Fact())
	EqualSigneds(t, a.Signed(), b.Signed())
}
