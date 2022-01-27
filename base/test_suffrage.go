//go:build test
// +build test

package base

import (
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/assert"
)

var DummySuffrageInfoHint = hint.MustNewHint("dummy-suffrage-info-v0.0.1")

type DummySuffrageInfo struct {
	h util.Hash
	t Threshold
	n []Address
}

func (suf DummySuffrageInfo) HashBytes() []byte {
	bs := make([]util.Byter, len(suf.n)+2)
	bs[0] = suf.h
	bs[1] = suf.t
	for i := range suf.n {
		bs[i+2] = suf.n[i]
	}

	return util.ConcatByters(bs...)
}

func (suf DummySuffrageInfo) Hint() hint.Hint {
	return DummySuffrageInfoHint
}

func (suf DummySuffrageInfo) Hash() util.Hash {
	return suf.h
}

func (suf *DummySuffrageInfo) SetHash(h util.Hash) *DummySuffrageInfo {
	suf.h = h

	return suf
}

func (suf DummySuffrageInfo) Threshold() Threshold {
	return suf.t
}

func (suf *DummySuffrageInfo) SetThreshold(t Threshold) *DummySuffrageInfo {
	suf.t = t

	return suf
}

func (suf DummySuffrageInfo) Nodes() []Address {
	return suf.n
}

func (suf *DummySuffrageInfo) SetNodes(n []Address) *DummySuffrageInfo {
	suf.n = n

	return suf
}

func (suf DummySuffrageInfo) IsValid([]byte) error {
	if err := IsValidSuffrageInfo(suf); err != nil {
		return errors.Wrap(err, "")
	}

	return nil
}

func (suf DummySuffrageInfo) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(map[string]interface{}{
		hint.HintedJSONTag: suf.Hint(),
		"hash":             suf.h,
		"threshold":        suf.t,
		"nodes":            suf.n,
	})
}

type dummySuffrageInfoJSONUnmarshaler struct {
	H valuehash.Bytes `json:"hash"`
	T Threshold       `json:"threshold"`
	N []string        `json:"nodes"`
}

func (suf *DummySuffrageInfo) DecodeJSON(b []byte, enc *jsonenc.Encoder) error {
	e := util.StringErrorFunc("failed to decode DummySuffrageInfo")

	var u dummySuffrageInfoJSONUnmarshaler
	if err := enc.Unmarshal(b, &u); err != nil {
		return e(err, "")
	}

	ns := make([]Address, len(u.N))
	for i := range u.N {
		switch j, err := DecodeAddressFromString(u.N[i], enc); {
		case err != nil:
			return e(err, "failed to decode address")
		default:
			ns[i] = j
		}
	}

	suf.h = u.H
	suf.t = u.T
	suf.n = ns

	return nil
}

func CompareSuffrageInfo(t *assert.Assertions, a, b SuffrageInfo) {
	if a == nil {
		t.Equal(a, b)
		return
	}

	t.True(a.Hint().Equal(b.Hint()))
	t.True(a.Hash().Equal(b.Hash()))
	t.Equal(a.Threshold(), b.Threshold())
	t.Equal(len(a.Nodes()), len(b.Nodes()))

	an := a.Nodes()
	bn := b.Nodes()
	for i := range an {
		t.True(an[i].Equal(bn[i]))
	}
}
