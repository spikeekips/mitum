package base

import (
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/localtime"
	"github.com/stretchr/testify/suite"
)

type testSign struct {
	suite.Suite
}

func (t *testSign) TestNew() {
	s := NewBaseSign(
		NewMPrivatekey().Publickey(),
		Signature([]byte("showme")),
		localtime.UTCNow(),
	)
	t.NoError(s.IsValid(nil))

	_ = (interface{})(s).(Sign)
}

func (t *testSign) TestEmptySigner() {
	s := NewBaseSign(
		nil,
		Signature([]byte("showme")),
		localtime.UTCNow(),
	)

	err := s.IsValid(nil)
	t.Error(err)
	t.True(errors.Is(err, util.ErrInvalid))
	t.ErrorContains(err, "invalid BaseSign")
}

func (t *testSign) TestEmptySignature() {
	s := NewBaseSign(
		NewMPrivatekey().Publickey(),
		nil,
		localtime.UTCNow(),
	)

	err := s.IsValid(nil)
	t.Error(err)
	t.True(errors.Is(err, util.ErrInvalid))
	t.ErrorContains(err, "empty signature")
}

func (t *testSign) TestZeroSignedAt() {
	s := NewBaseSign(
		NewMPrivatekey().Publickey(),
		Signature([]byte("showme")),
		time.Time{},
	)

	err := s.IsValid(nil)
	t.Error(err)
	t.True(errors.Is(err, util.ErrInvalid))
	t.ErrorContains(err, "empty signedAt")
}

func (t *testSign) TestSignedAndVerify() {
	priv := NewMPrivatekey()
	input := util.UUID().Bytes()

	s, err := NewBaseSignFromBytes(priv, nil, input)
	t.NoError(err)
	t.NoError(s.IsValid(nil))

	t.NoError(s.Verify(nil, input))

	err = s.Verify(nil, util.UUID().Bytes())
	t.Error(err)
	t.True(errors.Is(err, ErrSignatureVerification))
}

func TestSigns(t *testing.T) {
	suite.Run(t, new(testSign))
}

func TestBaseSignedJSON(tt *testing.T) {
	priv := NewMPrivatekey()
	input := util.UUID().Bytes()

	t := new(encoder.BaseTestEncode)
	enc := jsonenc.NewEncoder()

	t.Encode = func() (interface{}, []byte) {
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: MPublickeyHint, Instance: MPublickey{}}))

		s, err := NewBaseSignFromBytes(priv, nil, input)
		t.NoError(err)

		b, err := enc.Marshal(s)
		t.NoError(err)

		return s, b
	}
	t.Decode = func(b []byte) interface{} {
		var u BaseSign
		t.NoError(u.DecodeJSON(b, enc))

		t.NoError(u.Verify(nil, input))

		return u
	}
	t.Compare = func(a, b interface{}) {
		as, ok := a.(BaseSign)
		t.True(ok)
		bs, ok := b.(BaseSign)
		t.True(ok)

		t.NoError(bs.IsValid(nil))

		EqualSign(t.Assert(), as, bs)
	}

	suite.Run(tt, t)
}

type testNodeSign struct {
	suite.Suite
}

func (t *testNodeSign) TestNew() {
	s := NewBaseNodeSign(
		RandomAddress(""),
		NewMPrivatekey().Publickey(),
		Signature([]byte("showme")),
		localtime.UTCNow(),
	)
	t.NoError(s.IsValid(nil))

	_ = (interface{})(s).(NodeSign)
}

func (t *testNodeSign) TestSignedAndVerify() {
	priv := NewMPrivatekey()
	input := util.UUID().Bytes()
	node := RandomAddress("")

	s, err := BaseNodeSignFromBytes(node, priv, nil, input)
	t.NoError(err)
	t.NoError(s.IsValid(nil))

	t.NoError(s.Verify(nil, input))

	err = s.BaseSign.Verify(nil, input)
	t.Error(err)
	t.True(errors.Is(err, ErrSignatureVerification))

	err = s.Verify(nil, util.UUID().Bytes())
	t.Error(err)
	t.True(errors.Is(err, ErrSignatureVerification))
}

func TestNodeSign(t *testing.T) {
	suite.Run(t, new(testNodeSign))
}

func TestBaseNodeSignJSON(tt *testing.T) {
	priv := NewMPrivatekey()
	input := util.UUID().Bytes()

	t := new(encoder.BaseTestEncode)
	enc := jsonenc.NewEncoder()

	t.Encode = func() (interface{}, []byte) {
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: MPublickeyHint, Instance: MPublickey{}}))
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: StringAddressHint, Instance: StringAddress{}}))

		s, err := BaseNodeSignFromBytes(RandomAddress(""), priv, nil, input)
		t.NoError(err)

		b, err := enc.Marshal(s)
		t.NoError(err)

		return s, b
	}
	t.Decode = func(b []byte) interface{} {
		var u BaseNodeSign
		t.NoError(u.DecodeJSON(b, enc))

		t.NoError(u.Verify(nil, input))

		return u
	}
	t.Compare = func(a, b interface{}) {
		as, ok := a.(BaseNodeSign)
		t.True(ok)
		bs, ok := b.(BaseNodeSign)
		t.True(ok)

		t.NoError(bs.IsValid(nil))

		EqualSign(t.Assert(), as, bs)
	}

	suite.Run(tt, t)
}
