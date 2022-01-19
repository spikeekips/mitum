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
		localtime.Now(),
	)
	t.NoError(s.IsValid(nil))

	_ = (interface{})(s).(Signed)
}

func (t *testSigned) TestEmptySigner() {
	s := NewBaseSigned(
		nil,
		Signature([]byte("showme")),
		localtime.Now(),
	)

	err := s.IsValid(nil)
	t.Error(err)
	t.True(errors.Is(err, util.InvalidError))
	t.Contains(err.Error(), "invalid BaseSign")
}

func (t *testSigned) TestEmptySignature() {
	s := NewBaseSigned(
		NewMPrivatekey().Publickey(),
		nil,
		localtime.Now(),
	)

	err := s.IsValid(nil)
	t.Error(err)
	t.True(errors.Is(err, util.InvalidError))
	t.Contains(err.Error(), "empty signature")
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
	t.Contains(err.Error(), "empty signedAt")
}

func (t *testSigned) TestSignedAndVerify() {
	priv := NewMPrivatekey()
	input := util.UUID().Bytes()

	sig, err := priv.Sign(input)
	t.NoError(err)

	t.NoError(priv.Publickey().Verify(input, sig))

	s := NewBaseSigned(
		priv.Publickey(),
		sig,
		localtime.Now(),
	)
	t.NoError(s.IsValid(nil))

	t.NoError(s.Verify(nil, input))
	err = s.Verify(nil, util.UUID().Bytes())
	t.Error(err)
	t.True(errors.Is(err, SignatureVerificationError))
}

func TestSigned(t *testing.T) {
	suite.Run(t, new(testSigned))
}

func testBaseSigned() *encoder.BaseTestEncode {
	t := new(encoder.BaseTestEncode)
	t.Compare = func(a, b interface{}) {
		as, ok := a.(BaseSigned)
		t.True(ok)
		bs, ok := b.(BaseSigned)
		t.True(ok)

		t.NoError(bs.IsValid(nil))

		CompareSigned(t.Assert(), as, bs)
	}

	return t
}

func TestBaseSignedJSON(tt *testing.T) {
	priv := NewMPrivatekey()
	input := util.UUID().Bytes()

	t := testBaseSigned()
	enc := jsonenc.NewEncoder()

	t.Encode = func() (interface{}, []byte) {
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: MPublickeyHint, Instance: MPublickey{}}))
		sig, err := priv.Sign(input)
		t.NoError(err)

		s := NewBaseSigned(
			priv.Publickey(),
			sig,
			localtime.Now(),
		)

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

	suite.Run(tt, t)
}
