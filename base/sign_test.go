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
		localtime.Now(),
	)
	t.NoError(s.IsValid(nil))

	_ = (interface{})(s).(Sign)
}

func (t *testSign) TestEmptySigner() {
	s := NewBaseSign(
		nil,
		Signature([]byte("showme")),
		localtime.Now(),
	)

	err := s.IsValid(nil)
	t.Error(err)
	t.True(errors.Is(err, util.InvalidError))
	t.Contains(err.Error(), "invalid BaseSign")
}

func (t *testSign) TestEmptySignature() {
	s := NewBaseSign(
		NewMPrivatekey().Publickey(),
		nil,
		localtime.Now(),
	)

	err := s.IsValid(nil)
	t.Error(err)
	t.True(errors.Is(err, util.InvalidError))
	t.Contains(err.Error(), "empty signature")
}

func (t *testSign) TestZeroSignedAt() {
	s := NewBaseSign(
		NewMPrivatekey().Publickey(),
		Signature([]byte("showme")),
		time.Time{},
	)

	err := s.IsValid(nil)
	t.Error(err)
	t.True(errors.Is(err, util.InvalidError))
	t.Contains(err.Error(), "empty signedAt")
}

func (t *testSign) TestSignAndVerify() {
	priv := NewMPrivatekey()
	input := util.UUID().Bytes()

	sig, err := priv.Sign(input)
	t.NoError(err)

	t.NoError(priv.Publickey().Verify(input, sig))

	s := NewBaseSign(
		priv.Publickey(),
		sig,
		localtime.Now(),
	)
	t.NoError(s.IsValid(nil))

	t.NoError(s.Verify(input))
	err = s.Verify(util.UUID().Bytes())
	t.Error(err)
	t.True(errors.Is(err, SignatureVerificationError))
}

func TestSign(t *testing.T) {
	suite.Run(t, new(testSign))
}

func testBaseSign() *encoder.BaseTestEncode {
	t := new(encoder.BaseTestEncode)
	t.Compare = func(a, b interface{}) {
		as, ok := a.(BaseSign)
		t.True(ok)
		bs, ok := b.(BaseSign)
		t.True(ok)

		t.NoError(bs.IsValid(nil))

		t.True(as.signer.Equal(bs.signer))
		t.True(as.signature.Equal(bs.signature))
		t.True(as.signedAt.Equal(bs.signedAt))
	}

	return t
}

func TestBaseSignJSON(tt *testing.T) {
	priv := NewMPrivatekey()
	input := util.UUID().Bytes()

	t := testBaseSign()
	enc := jsonenc.NewEncoder()

	t.Encode = func() (interface{}, []byte) {
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: MPublickeyHint, Instance: MPublickey{}}))
		sig, err := priv.Sign(input)
		t.NoError(err)

		s := NewBaseSign(
			priv.Publickey(),
			sig,
			localtime.Now(),
		)

		b, err := enc.Marshal(s)
		t.NoError(err)

		return s, b
	}
	t.Decode = func(b []byte) interface{} {
		var u BaseSign
		t.NoError(u.DecodeJSON(b, enc))

		t.NoError(u.Verify(input))

		return u
	}

	suite.Run(tt, t)
}
