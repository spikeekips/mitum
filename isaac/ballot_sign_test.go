package isaac

import (
	"testing"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
)

type testBaseBallotSignFact struct {
	suite.Suite
	priv      base.Privatekey
	networkID base.NetworkID
	signfact  func() base.BallotSignFact
	wrongfact func() base.BallotFact
}

func (t *testBaseBallotSignFact) SetupTest() {
	t.priv = base.NewMPrivatekey()
	t.networkID = base.NetworkID(util.UUID().Bytes())
}

func (t *testBaseBallotSignFact) TestNew() {
	sb := t.signfact()

	_ = (interface{})(sb).(base.BallotSignFact)

	t.NoError(sb.IsValid(t.networkID))
}

func (t *testBaseBallotSignFact) TestEmptySigns() {
	sb := t.signfact()

	switch u := sb.(type) {
	case INITBallotSignFact:
		u.sign = base.BaseSign{}
		sb = u
	case ACCEPTBallotSignFact:
		u.sign = base.BaseSign{}
		sb = u
	}

	err := sb.IsValid(t.networkID)
	t.Error(err)
	t.True(errors.Is(err, util.ErrInvalid))
}

func (t *testBaseBallotSignFact) TestWrongFact() {
	sb := t.signfact()
	switch u := sb.(type) {
	case INITBallotSignFact:
		u.fact = t.wrongfact()
		t.NoError(u.Sign(t.priv, t.networkID))
		sb = u
	case ACCEPTBallotSignFact:
		u.fact = t.wrongfact()
		t.NoError(u.Sign(t.priv, t.networkID))
		sb = u
	}

	err := sb.IsValid(t.networkID)
	t.Error(err)
	t.True(errors.Is(err, util.ErrInvalid))
}

func TestINITBallotSignFact(tt *testing.T) {
	t := new(testBaseBallotSignFact)
	t.signfact = func() base.BallotSignFact {
		fact := NewINITBallotFact(base.RawPoint(33, 44), valuehash.RandomSHA256(), valuehash.RandomSHA256(), nil)

		sb := NewINITBallotSignFact(base.RandomAddress(""), fact)
		t.NoError(sb.Sign(t.priv, t.networkID))

		_ = (interface{})(sb).(base.INITBallotSignFact)

		return sb
	}
	t.wrongfact = func() base.BallotFact {
		return NewACCEPTBallotFact(base.RawPoint(33, 44), valuehash.RandomSHA256(), valuehash.RandomSHA256())
	}

	suite.Run(tt, t)
}

func TestACCEPTBallotSignFact(tt *testing.T) {
	t := new(testBaseBallotSignFact)
	t.signfact = func() base.BallotSignFact {
		fact := NewACCEPTBallotFact(base.RawPoint(33, 44), valuehash.RandomSHA256(), valuehash.RandomSHA256())

		sb := NewACCEPTBallotSignFact(base.RandomAddress(""), fact)
		t.NoError(sb.Sign(t.priv, t.networkID))

		_ = (interface{})(sb).(base.ACCEPTBallotSignFact)
		return sb
	}
	t.wrongfact = func() base.BallotFact {
		return NewINITBallotFact(base.RawPoint(33, 44), valuehash.RandomSHA256(), valuehash.RandomSHA256(), nil)
	}

	suite.Run(tt, t)
}

func TestINITBallotSignFactJSON(tt *testing.T) {
	t := new(encoder.BaseTestEncode)

	enc := jsonenc.NewEncoder()
	priv := base.NewMPrivatekey()
	networkID := base.NetworkID(util.UUID().Bytes())

	t.Encode = func() (interface{}, []byte) {
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: base.StringAddressHint, Instance: base.StringAddress{}}))
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: base.MPublickeyHint, Instance: base.MPublickey{}}))
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: INITBallotFactHint, Instance: INITBallotFact{}}))
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: INITBallotSignFactHint, Instance: INITBallotSignFact{}}))

		fact := NewINITBallotFact(base.RawPoint(33, 44), valuehash.RandomSHA256(), valuehash.RandomSHA256(), nil)
		sb := NewINITBallotSignFact(base.RandomAddress(""), fact)
		t.NoError(sb.Sign(priv, networkID))
		t.NoError(sb.IsValid(networkID))

		b, err := enc.Marshal(&sb)
		t.NoError(err)

		return sb, b
	}
	t.Decode = func(b []byte) interface{} {
		i, err := enc.Decode(b)
		t.NoError(err)

		sb, ok := i.(INITBallotSignFact)
		t.True(ok)
		t.NoError(sb.IsValid(networkID))

		return i
	}
	t.Compare = func(a, b interface{}) {
		as, ok := a.(INITBallotSignFact)
		t.True(ok)
		bs, ok := b.(INITBallotSignFact)
		t.True(ok)

		base.EqualBallotSignFact(t.Assert(), as, bs)
	}

	suite.Run(tt, t)
}

func TestACCEPTBallotSignFactJSON(tt *testing.T) {
	t := new(encoder.BaseTestEncode)

	enc := jsonenc.NewEncoder()
	priv := base.NewMPrivatekey()
	networkID := base.NetworkID(util.UUID().Bytes())

	t.Encode = func() (interface{}, []byte) {
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: base.StringAddressHint, Instance: base.StringAddress{}}))
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: base.MPublickeyHint, Instance: base.MPublickey{}}))
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: ACCEPTBallotFactHint, Instance: ACCEPTBallotFact{}}))
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: ACCEPTBallotSignFactHint, Instance: ACCEPTBallotSignFact{}}))

		fact := NewACCEPTBallotFact(base.RawPoint(33, 44), valuehash.RandomSHA256(), valuehash.RandomSHA256())
		sb := NewACCEPTBallotSignFact(base.RandomAddress(""), fact)
		t.NoError(sb.Sign(priv, networkID))
		t.NoError(sb.IsValid(networkID))

		b, err := enc.Marshal(&sb)
		t.NoError(err)

		return sb, b
	}
	t.Decode = func(b []byte) interface{} {
		i, err := enc.Decode(b)
		t.NoError(err)

		sb, ok := i.(ACCEPTBallotSignFact)
		t.True(ok)
		t.NoError(sb.IsValid(networkID))

		return i
	}
	t.Compare = func(a, b interface{}) {
		as, ok := a.(ACCEPTBallotSignFact)
		t.True(ok)
		bs, ok := b.(ACCEPTBallotSignFact)
		t.True(ok)

		base.EqualBallotSignFact(t.Assert(), as, bs)
	}

	suite.Run(tt, t)
}
