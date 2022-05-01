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

type testSigned struct {
	suite.Suite
}

func (t *testSigned) TestNew() {
	s := NewBaseSigned(
		NewMPrivatekey().Publickey(),
		Signature([]byte("showme")),
		localtime.UTCNow(),
	)
	t.NoError(s.IsValid(nil))

	_ = (interface{})(s).(Signed)
}

func (t *testSigned) TestEmptySigner() {
	s := NewBaseSigned(
		nil,
		Signature([]byte("showme")),
		localtime.UTCNow(),
	)

	err := s.IsValid(nil)
	t.Error(err)
	t.True(errors.Is(err, util.InvalidError))
	t.ErrorContains(err, "invalid BaseSign")
}

func (t *testSigned) TestEmptySignature() {
	s := NewBaseSigned(
		NewMPrivatekey().Publickey(),
		nil,
		localtime.UTCNow(),
	)

	err := s.IsValid(nil)
	t.Error(err)
	t.True(errors.Is(err, util.InvalidError))
	t.ErrorContains(err, "empty signature")
}

func (t *testSigned) TestZeroSignedAt() {
	s := NewBaseSigned(
		NewMPrivatekey().Publickey(),
		Signature([]byte("showme")),
		time.Time{},
	)

	err := s.IsValid(nil)
	t.Error(err)
	t.True(errors.Is(err, util.InvalidError))
	t.ErrorContains(err, "empty signedAt")
}

func (t *testSigned) TestSignedAndVerify() {
	priv := NewMPrivatekey()
	input := util.UUID().Bytes()

	s, err := BaseSignedFromBytes(priv, nil, input)
	t.NoError(err)
	t.NoError(s.IsValid(nil))

	t.NoError(s.Verify(nil, input))

	err = s.Verify(nil, util.UUID().Bytes())
	t.Error(err)
	t.True(errors.Is(err, SignatureVerificationError))
}

func TestSigned(t *testing.T) {
	suite.Run(t, new(testSigned))
}

func TestBaseSignedJSON(tt *testing.T) {
	priv := NewMPrivatekey()
	input := util.UUID().Bytes()

	t := new(encoder.BaseTestEncode)
	enc := jsonenc.NewEncoder()

	t.Encode = func() (interface{}, []byte) {
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: MPublickeyHint, Instance: MPublickey{}}))

		s, err := BaseSignedFromBytes(priv, nil, input)
		t.NoError(err)

		b, err := enc.Marshal(s)
		t.NoError(err)

		return s, b
	}
	t.Decode = func(b []byte) interface{} {
		var u BaseSigned
		t.NoError(u.DecodeJSON(b, enc))

		t.NoError(u.Verify(nil, input))

		return u
	}
	t.Compare = func(a, b interface{}) {
		as, ok := a.(BaseSigned)
		t.True(ok)
		bs, ok := b.(BaseSigned)
		t.True(ok)

		t.NoError(bs.IsValid(nil))

		EqualSigned(t.Assert(), as, bs)
	}

	suite.Run(tt, t)
}

type testNodeSigned struct {
	suite.Suite
}

func (t *testNodeSigned) TestNew() {
	s := NewBaseNodeSigned(
		RandomAddress(""),
		NewMPrivatekey().Publickey(),
		Signature([]byte("showme")),
		localtime.UTCNow(),
	)
	t.NoError(s.IsValid(nil))

	_ = (interface{})(s).(NodeSigned)
}

func (t *testNodeSigned) TestSignedAndVerify() {
	priv := NewMPrivatekey()
	input := util.UUID().Bytes()
	node := RandomAddress("")

	s, err := BaseNodeSignedFromBytes(node, priv, nil, input)
	t.NoError(err)
	t.NoError(s.IsValid(nil))

	t.NoError(s.Verify(nil, input))

	err = s.BaseSigned.Verify(nil, input)
	t.Error(err)
	t.True(errors.Is(err, SignatureVerificationError))

	err = s.Verify(nil, util.UUID().Bytes())
	t.Error(err)
	t.True(errors.Is(err, SignatureVerificationError))
}

func TestNodeSigned(t *testing.T) {
	suite.Run(t, new(testNodeSigned))
}

func TestBaseNodeSignedJSON(tt *testing.T) {
	priv := NewMPrivatekey()
	input := util.UUID().Bytes()

	t := new(encoder.BaseTestEncode)
	enc := jsonenc.NewEncoder()

	t.Encode = func() (interface{}, []byte) {
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: MPublickeyHint, Instance: MPublickey{}}))
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: StringAddressHint, Instance: StringAddress{}}))

		s, err := BaseNodeSignedFromBytes(RandomAddress(""), priv, nil, input)
		t.NoError(err)

		b, err := enc.Marshal(s)
		t.NoError(err)

		return s, b
	}
	t.Decode = func(b []byte) interface{} {
		var u BaseNodeSigned
		t.NoError(u.DecodeJSON(b, enc))

		t.NoError(u.Verify(nil, input))

		return u
	}
	t.Compare = func(a, b interface{}) {
		as, ok := a.(BaseNodeSigned)
		t.True(ok)
		bs, ok := b.(BaseNodeSigned)
		t.True(ok)

		t.NoError(bs.IsValid(nil))

		EqualSigned(t.Assert(), as, bs)
	}

	suite.Run(tt, t)
}
