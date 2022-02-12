//go:build test
// +build test

package base

import "github.com/stretchr/testify/assert"

func CompareFact(t *assert.Assertions, a, b Fact) {
	t.True(a.Hint().Equal(b.Hint()))
	t.True(a.Hash().Equal(b.Hash()))
	t.Equal(a.Token(), b.Token())
}

func CompareSignedFact(t *assert.Assertions, a, b SignedFact) {
	t.Equal(a.HashBytes(), b.HashBytes())

	CompareFact(t, a.Fact(), b.Fact())
	CompareSigneds(t, a.Signed(), b.Signed())
}
