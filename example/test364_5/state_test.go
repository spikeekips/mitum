package dop5

import (
	"context"
	"testing"

	"github.com/spikeekips/mitum/base"
	jsonenc "github.com/spikeekips/mitum/example/test364_5/encoder/json"
	dhash "github.com/spikeekips/mitum/example/test364_5/hash"
	"github.com/spikeekips/mitum/util"
	mencoder "github.com/spikeekips/mitum/util/encoder"
	"github.com/stretchr/testify/suite"
)

func TestBaseStateEncode(tt *testing.T) {
	t := new(mencoder.BaseTestEncode)

	enc := jsonenc.NewEncoder(context.Background(), 1<<9)
	defer enc.Close()

	t.Encode = func() (interface{}, []byte) {
		sv := NewAccountSV(base.NewDummyPublickey(util.UUID().String()), 1)
		st := NewBaseState(base.Height(33), util.UUID().String(), sv, dhash.RandomHex64(), nil)

		b, err := enc.Marshal(st)
		t.NoError(err)

		return st, b
	}
	t.Decode = func(b []byte) interface{} {
		t.NoError(jsonenc.AddDecode(enc, base.DummyPublickeyHint, base.DummyPublickey{}, DecodeJSONDummyPublickey))
		t.NoError(jsonenc.AddDecode(enc, AccountSVHint, &AccountSV{}, AccountSVDecodeJSON))
		t.NoError(jsonenc.AddDecode(enc, BaseStateHint, &BaseState{}, BaseStateDecodeJSON))

		hinter, err := enc.Unmarshal(b)
		t.NoError(err)

		return hinter.(*BaseState)
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
