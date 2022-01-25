package states

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

type testBaseBallotSignedFact struct {
	suite.Suite
	priv       base.Privatekey
	networkID  base.NetworkID
	signedfact func() base.BallotSignedFact
	wrongfact  func() base.BallotFact
}

func (t *testBaseBallotSignedFact) SetupTest() {
	t.priv = base.NewMPrivatekey()
	t.networkID = base.NetworkID(util.UUID().Bytes())
}

func (t *testBaseBallotSignedFact) TestNew() {
	sb := t.signedfact()

	_ = (interface{})(sb).(base.BallotSignedFact)

	t.NoError(sb.IsValid(t.networkID))
}

func (t *testBaseBallotSignedFact) TestEmptySigned() {
	sb := t.signedfact()

	switch u := sb.(type) {
	case INITBallotSignedFact:
		u.signed = base.BaseSigned{}
		sb = u
	case ProposalSignedFact:
		u.signed = base.BaseSigned{}
		sb = u
	case ACCEPTBallotSignedFact:
		u.signed = base.BaseSigned{}
		sb = u
	}

	err := sb.IsValid(t.networkID)
	t.Error(err)
	t.True(errors.Is(err, util.InvalidError))
}

func (t *testBaseBallotSignedFact) TestWrongFact() {
	sb := t.signedfact()
	switch u := sb.(type) {
	case INITBallotSignedFact:
		u.fact = t.wrongfact()
		t.NoError(u.Sign(t.priv, t.networkID))
		sb = u
	case ProposalSignedFact:
		u.fact = t.wrongfact()
		t.NoError(u.Sign(t.priv, t.networkID))
		sb = u
	case ACCEPTBallotSignedFact:
		u.fact = t.wrongfact()
		t.NoError(u.Sign(t.priv, t.networkID))
		sb = u
	}

	err := sb.IsValid(t.networkID)
	t.Error(err)
	t.True(errors.Is(err, util.InvalidError))
}

func TestINITBallotSignedFact(tt *testing.T) {
	t := new(testBaseBallotSignedFact)
	t.signedfact = func() base.BallotSignedFact {
		fact := NewINITBallotFact(base.NewPoint(base.Height(33), base.Round(44)), valuehash.RandomSHA256())

		sb := NewINITBallotSignedFact(base.RandomAddress(""), fact)
		t.NoError(sb.Sign(t.priv, t.networkID))

		_ = (interface{})(sb).(base.INITBallotSignedFact)

		return sb
	}
	t.wrongfact = func() base.BallotFact {
		return NewACCEPTBallotFact(base.NewPoint(base.Height(33), base.Round(44)), valuehash.RandomSHA256(), valuehash.RandomSHA256())
	}

	suite.Run(tt, t)
}

func TestProposalBallotSignedFact(tt *testing.T) {
	t := new(testBaseBallotSignedFact)
	t.signedfact = func() base.BallotSignedFact {
		fact := NewProposalFact(base.NewPoint(base.Height(33), base.Round(44)),
			[]util.Hash{valuehash.RandomSHA256()},
		)

		sb := NewProposalSignedFact(
			base.RandomAddress(""),
			fact,
		)
		t.NoError(sb.Sign(t.priv, t.networkID))

		_ = (interface{})(sb).(base.ProposalSignedFact)
		return sb
	}
	t.wrongfact = func() base.BallotFact {
		return NewINITBallotFact(base.NewPoint(base.Height(33), base.Round(44)), valuehash.RandomSHA256())
	}

	suite.Run(tt, t)
}

func TestACCEPTBallotSignedFact(tt *testing.T) {
	t := new(testBaseBallotSignedFact)
	t.signedfact = func() base.BallotSignedFact {
		fact := NewACCEPTBallotFact(base.NewPoint(base.Height(33), base.Round(44)), valuehash.RandomSHA256(), valuehash.RandomSHA256())

		sb := NewACCEPTBallotSignedFact(base.RandomAddress(""), fact)
		t.NoError(sb.Sign(t.priv, t.networkID))

		_ = (interface{})(sb).(base.ACCEPTBallotSignedFact)
		return sb
	}
	t.wrongfact = func() base.BallotFact {
		return NewINITBallotFact(base.NewPoint(base.Height(33), base.Round(44)), valuehash.RandomSHA256())
	}

	suite.Run(tt, t)
}

func TestINITBallotSignedFactEncode(tt *testing.T) {
	t := new(encoder.BaseTestEncode)

	enc := jsonenc.NewEncoder()
	priv := base.NewMPrivatekey()
	networkID := base.NetworkID(util.UUID().Bytes())

	t.Encode = func() (interface{}, []byte) {
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: base.StringAddressHint, Instance: base.StringAddress{}}))
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: base.MPublickeyHint, Instance: base.MPublickey{}}))
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: INITBallotFactHint, Instance: INITBallotFact{}}))
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: INITBallotSignedFactHint, Instance: INITBallotSignedFact{}}))

		fact := NewINITBallotFact(base.NewPoint(base.Height(33), base.Round(44)), valuehash.RandomSHA256())
		sb := NewINITBallotSignedFact(base.RandomAddress(""), fact)
		t.NoError(sb.Sign(priv, networkID))
		t.NoError(sb.IsValid(networkID))

		b, err := enc.Marshal(sb)
		t.NoError(err)

		return sb, b
	}
	t.Decode = func(b []byte) interface{} {
		i, err := enc.Decode(b)
		t.NoError(err)

		sb, ok := i.(INITBallotSignedFact)
		t.True(ok)
		t.NoError(sb.IsValid(networkID))

		return i
	}
	t.Compare = func(a, b interface{}) {
		as, ok := a.(INITBallotSignedFact)
		t.True(ok)
		bs, ok := b.(INITBallotSignedFact)
		t.True(ok)

		base.CompareBallotSignedFact(t.Assert(), as, bs)
	}

	suite.Run(tt, t)
}

func TestProposalBallotSignedFactEncode(tt *testing.T) {
	t := new(encoder.BaseTestEncode)

	enc := jsonenc.NewEncoder()
	priv := base.NewMPrivatekey()
	networkID := base.NetworkID(util.UUID().Bytes())

	t.Encode = func() (interface{}, []byte) {
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: base.StringAddressHint, Instance: base.StringAddress{}}))
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: base.MPublickeyHint, Instance: base.MPublickey{}}))
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: ProposalFactHint, Instance: ProposalFact{}}))
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: ProposalSignedFactHint, Instance: ProposalSignedFact{}}))

		fact := NewProposalFact(base.NewPoint(base.Height(33), base.Round(44)),
			[]util.Hash{valuehash.RandomSHA256()},
		)
		sb := NewProposalSignedFact(
			base.RandomAddress(""),
			fact,
		)
		t.NoError(sb.Sign(priv, networkID))
		t.NoError(sb.IsValid(networkID))

		b, err := enc.Marshal(sb)
		t.NoError(err)

		return sb, b
	}
	t.Decode = func(b []byte) interface{} {
		i, err := enc.Decode(b)
		t.NoError(err)

		sb, ok := i.(ProposalSignedFact)
		t.True(ok)
		t.NoError(sb.IsValid(networkID))

		return i
	}
	t.Compare = func(a, b interface{}) {
		as, ok := a.(ProposalSignedFact)
		t.True(ok)
		bs, ok := b.(ProposalSignedFact)
		t.True(ok)

		base.CompareBallotSignedFact(t.Assert(), as, bs)
	}

	suite.Run(tt, t)
}

func TestACCEPTBallotSignedFactEncode(tt *testing.T) {
	t := new(encoder.BaseTestEncode)

	enc := jsonenc.NewEncoder()
	priv := base.NewMPrivatekey()
	networkID := base.NetworkID(util.UUID().Bytes())

	t.Encode = func() (interface{}, []byte) {
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: base.StringAddressHint, Instance: base.StringAddress{}}))
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: base.MPublickeyHint, Instance: base.MPublickey{}}))
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: ACCEPTBallotFactHint, Instance: ACCEPTBallotFact{}}))
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: ACCEPTBallotSignedFactHint, Instance: ACCEPTBallotSignedFact{}}))

		fact := NewACCEPTBallotFact(base.NewPoint(base.Height(33), base.Round(44)), valuehash.RandomSHA256(), valuehash.RandomSHA256())
		sb := NewACCEPTBallotSignedFact(base.RandomAddress(""), fact)
		t.NoError(sb.Sign(priv, networkID))
		t.NoError(sb.IsValid(networkID))

		b, err := enc.Marshal(sb)
		t.NoError(err)

		return sb, b
	}
	t.Decode = func(b []byte) interface{} {
		i, err := enc.Decode(b)
		t.NoError(err)

		sb, ok := i.(ACCEPTBallotSignedFact)
		t.True(ok)
		t.NoError(sb.IsValid(networkID))

		return i
	}
	t.Compare = func(a, b interface{}) {
		as, ok := a.(ACCEPTBallotSignedFact)
		t.True(ok)
		bs, ok := b.(ACCEPTBallotSignedFact)
		t.True(ok)

		base.CompareBallotSignedFact(t.Assert(), as, bs)
	}

	suite.Run(tt, t)
}
