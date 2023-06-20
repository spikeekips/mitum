package isaacnetwork

import (
	"testing"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/network/quicstream"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/stretchr/testify/suite"
)

type baseTestCAHandoverHeader struct {
	encoder.BaseTestEncode
	enc  *jsonenc.Encoder
	newf func(quicstream.ConnInfo, base.Address) interface{}
}

func (t *baseTestCAHandoverHeader) SetupSuite() {
	t.enc = jsonenc.NewEncoder()
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: base.StringAddressHint, Instance: base.StringAddress{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: StartHandoverHeaderHint, Instance: StartHandoverHeader{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: CheckHandoverHeaderHint, Instance: CheckHandoverHeader{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: AskHandoverHeaderHint, Instance: AskHandoverHeader{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: CheckHandoverXHeaderHint, Instance: CheckHandoverXHeader{}}))
}

func (t *baseTestCAHandoverHeader) SetupTest() {
	ci, err := quicstream.NewConnInfoFromFullString("1.2.3.4:4321#tls_insecure")
	t.NoError(err)

	t.Encode = func() (interface{}, []byte) {
		h := t.newf(ci, base.RandomAddress(""))
		t.NoError(h.(util.IsValider).IsValid(nil))

		b, err := util.MarshalJSON(h)
		t.NoError(err)

		t.T().Log("marshaled:", string(b))

		return h, b
	}
}

func TestStartHandoverHeaderEncode(tt *testing.T) {
	t := new(baseTestCAHandoverHeader)
	t.SetT(tt)

	t.newf = func(ci quicstream.ConnInfo, address base.Address) interface{} {
		return NewStartHandoverHeader(ci, address)
	}

	t.Decode = func(b []byte) interface{} {
		var u StartHandoverHeader
		t.NoError(encoder.Decode(t.enc, b, &u))

		return u
	}
	t.Compare = func(a interface{}, b interface{}) {
		ah := a.(StartHandoverHeader)
		bh := b.(StartHandoverHeader)

		t.Equal(ah.Hint(), bh.Hint())
		t.Equal(ah.ConnInfo().String(), bh.ConnInfo().String())
		t.True(ah.Address().Equal(bh.Address()))
	}

	suite.Run(tt, t)
}

func TestCheckHandoverHeaderEncode(tt *testing.T) {
	t := new(baseTestCAHandoverHeader)
	t.SetT(tt)

	t.newf = func(ci quicstream.ConnInfo, address base.Address) interface{} {
		return NewCheckHandoverHeader(ci, address)
	}

	t.Decode = func(b []byte) interface{} {
		var u CheckHandoverHeader
		t.NoError(encoder.Decode(t.enc, b, &u))

		return u
	}
	t.Compare = func(a interface{}, b interface{}) {
		ah := a.(CheckHandoverHeader)
		bh := b.(CheckHandoverHeader)

		t.Equal(ah.Hint(), bh.Hint())
		t.Equal(ah.ConnInfo().String(), bh.ConnInfo().String())
		t.True(ah.Address().Equal(bh.Address()))
	}

	suite.Run(tt, t)
}

func TestAskHandoverHeaderEncode(tt *testing.T) {
	t := new(baseTestCAHandoverHeader)
	t.SetT(tt)

	t.newf = func(ci quicstream.ConnInfo, address base.Address) interface{} {
		return NewAskHandoverHeader(ci, address)
	}

	t.Decode = func(b []byte) interface{} {
		var u AskHandoverHeader
		t.NoError(encoder.Decode(t.enc, b, &u))

		return u
	}
	t.Compare = func(a interface{}, b interface{}) {
		ah := a.(AskHandoverHeader)
		bh := b.(AskHandoverHeader)

		t.Equal(ah.Hint(), bh.Hint())
		t.Equal(ah.ConnInfo().String(), bh.ConnInfo().String())
		t.True(ah.Address().Equal(bh.Address()))
	}

	suite.Run(tt, t)
}

func TestAskHandoverResponseHeaderEncode(tt *testing.T) {
	t := new(encoder.BaseTestEncode)
	t.SetT(tt)

	enc := jsonenc.NewEncoder()
	t.NoError(enc.Add(encoder.DecodeDetail{Hint: AskHandoverResponseHeaderHint, Instance: AskHandoverResponseHeader{}}))

	t.Encode = func() (interface{}, []byte) {
		h := NewAskHandoverResponseHeader(true, errors.Errorf("hehehe"), util.UUID().String())
		t.NoError(h.IsValid(nil))

		b, err := util.MarshalJSON(h)
		t.NoError(err)

		t.T().Log("marshaled:", string(b))

		return h, b
	}
	t.Decode = func(b []byte) interface{} {
		var u AskHandoverResponseHeader
		t.NoError(encoder.Decode(enc, b, &u))

		return u
	}
	t.Compare = func(a interface{}, b interface{}) {
		ah := a.(AskHandoverResponseHeader)
		bh := b.(AskHandoverResponseHeader)

		t.Equal(ah.Hint(), bh.Hint())
		t.Equal(ah.ID(), bh.ID())
		t.Equal(ah.OK(), bh.OK())
		t.Equal(ah.Err().Error(), bh.Err().Error())
	}

	suite.Run(tt, t)
}

func TestCancelHandoverHeaderEncode(tt *testing.T) {
	t := new(encoder.BaseTestEncode)
	t.SetT(tt)

	enc := jsonenc.NewEncoder()
	t.NoError(enc.Add(encoder.DecodeDetail{Hint: CancelHandoverHeaderHint, Instance: CancelHandoverHeader{}}))

	t.Encode = func() (interface{}, []byte) {
		h := NewCancelHandoverHeader()
		t.NoError(h.IsValid(nil))

		b, err := util.MarshalJSON(h)
		t.NoError(err)

		t.T().Log("marshaled:", string(b))

		return h, b
	}
	t.Decode = func(b []byte) interface{} {
		var u CancelHandoverHeader
		t.NoError(encoder.Decode(enc, b, &u))

		return u
	}
	t.Compare = func(a interface{}, b interface{}) {
		ah := a.(CancelHandoverHeader)
		bh := b.(CancelHandoverHeader)

		t.Equal(ah.Hint(), bh.Hint())
	}

	suite.Run(tt, t)
}

func TestHandoverMessageHeaderEncode(tt *testing.T) {
	t := new(encoder.BaseTestEncode)
	t.SetT(tt)

	enc := jsonenc.NewEncoder()
	t.NoError(enc.Add(encoder.DecodeDetail{Hint: HandoverMessageHeaderHint, Instance: HandoverMessageHeader{}}))

	t.Encode = func() (interface{}, []byte) {
		h := NewHandoverMessageHeader()
		t.NoError(h.IsValid(nil))

		b, err := util.MarshalJSON(h)
		t.NoError(err)

		t.T().Log("marshaled:", string(b))

		return h, b
	}
	t.Decode = func(b []byte) interface{} {
		var u HandoverMessageHeader
		t.NoError(encoder.Decode(enc, b, &u))

		return u
	}
	t.Compare = func(a interface{}, b interface{}) {
		ah := a.(HandoverMessageHeader)
		bh := b.(HandoverMessageHeader)

		t.Equal(ah.Hint(), bh.Hint())
	}

	suite.Run(tt, t)
}

func TestCheckHandoverXHeaderEncode(tt *testing.T) {
	t := new(baseTestCAHandoverHeader)
	t.SetT(tt)

	t.newf = func(_ quicstream.ConnInfo, address base.Address) interface{} {
		return NewCheckHandoverXHeader(address)
	}

	t.Decode = func(b []byte) interface{} {
		var u CheckHandoverXHeader
		t.NoError(encoder.Decode(t.enc, b, &u))

		return u
	}
	t.Compare = func(a interface{}, b interface{}) {
		ah := a.(CheckHandoverXHeader)
		bh := b.(CheckHandoverXHeader)

		t.Equal(ah.Hint(), bh.Hint())
		t.True(ah.Address().Equal(bh.Address()))
	}

	suite.Run(tt, t)
}
