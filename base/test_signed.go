//go:build test
// +build test

package base

import (
	"bytes"

	"github.com/spikeekips/mitum/util"
	"github.com/stretchr/testify/assert"
)

func EqualSigneds(t *assert.Assertions, a, b []Signed) {
	t.Equal(len(a), len(b))

	for i := range a {
		EqualSigned(t, a[i], b[i])
	}
}

func EqualSigned(t *assert.Assertions, a, b Signed) {
	t.True(bytes.Equal(a.Bytes(), b.Bytes()))
	t.True(a.Signer().Equal(b.Signer()))
	t.True(a.Signature().Equal(b.Signature()))
	t.True(util.TimeEqual(a.SignedAt(), b.SignedAt()))
}
