//go:build test
// +build test

package base

import (
	"bytes"

	"github.com/spikeekips/mitum/util/localtime"
	"github.com/stretchr/testify/assert"
)

func CompareSigneds(t *assert.Assertions, a, b []Signed) {
	t.Equal(len(a), len(b))

	for i := range a {
		CompareSigned(t, a[i], b[i])
	}
}

func CompareSigned(t *assert.Assertions, a, b Signed) {
	t.True(bytes.Equal(a.Bytes(), b.Bytes()))
	t.True(a.Signer().Equal(b.Signer()))
	t.True(a.Signature().Equal(b.Signature()))
	t.True(
		localtime.NewTime(a.SignedAt()).Normalize().Equal(
			localtime.NewTime(b.SignedAt()).Normalize(),
		),
	)
}
