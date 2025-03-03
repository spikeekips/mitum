package dop5

import (
	"testing"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
)

func TestBaseStateEncode(tt *testing.T) {
	t := new(encoder.BaseTestEncode)

	enc := jsonenc.NewEncoder()

	t.Encode = func() (interface{}, []byte) {
		sv := NewAccountSV(base.NewDummyPublickey(util.UUID().String()), 1)
		st := NewBaseState(base.Height(33), util.UUID().String(), sv, valuehash.RandomSHA256(), nil)

		b, err := enc.Marshal(st)
		t.NoError(err)

		return st, b
	}
	t.Decode = func(b []byte) interface{} {
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: base.DummyPublickeyHint, Instance: base.DummyPublickey{}}))
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: AccountSVHint, Instance: &AccountSV{}}))
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: BaseStateHint, Instance: &baseState{}}))

		hinter, err := enc.Decode(b)
		t.NoError(err)

		return hinter.(*baseState)
	}
	t.Compare = func(a interface{}, b interface{}) {
		as := a.(base.State)
		bs := b.(base.State)

		t.NoError(as.IsValid(nil))
		t.NoError(bs.IsValid(nil))

		t.True(base.IsEqualState(as, bs))
	}

	suite.Run(tt, t)
}
