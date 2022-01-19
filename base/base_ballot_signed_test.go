package base

import (
	"testing"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
)

var (
	dummyINITBallotSignedFactHint   = hint.MustNewHint("dummy-init-signed-ballot-fact-v1.2.3")
	dummyProposalSignedFactHint     = hint.MustNewHint("dummy-proposal-signed-ballot-fact-v1.2.3")
	dummyACCEPTBallotSignedFactHint = hint.MustNewHint("dummy-accept-signed-ballot-fact-v1.2.3")
)

type (
	dummyINITBallotSignedFact   struct{ BaseINITBallotSignedFact }
	dummyProposalSignedFact     struct{ BaseProposalSignedFact }
	dummyACCEPTBallotSignedFact struct{ BaseACCEPTBallotSignedFact }
)

type testBaseBallotSignedFact struct {
	suite.Suite
	priv       Privatekey
	networkID  NetworkID
	signedfact func() BallotSignedFact
	wrongfact  func() BallotFact
}

func (t *testBaseBallotSignedFact) SetupTest() {
	t.priv = NewMPrivatekey()
	t.networkID = NetworkID(util.UUID().Bytes())
}

func (t *testBaseBallotSignedFact) TestNew() {
	sb := t.signedfact()

	_ = (interface{})(sb).(BallotSignedFact)

	t.NoError(sb.IsValid(t.networkID))
}

func (t *testBaseBallotSignedFact) TestEmptySigned() {
	sb := t.signedfact()

	switch u := sb.(type) {
	case dummyINITBallotSignedFact:
		u.signed = BaseSigned{}
		sb = u
	case dummyProposalSignedFact:
		u.signed = BaseSigned{}
		sb = u
	case dummyACCEPTBallotSignedFact:
		u.signed = BaseSigned{}
		sb = u
	}

	err := sb.IsValid(t.networkID)
	t.Error(err)
	t.True(errors.Is(err, util.InvalidError))
}

func (t *testBaseBallotSignedFact) TestWrongFact() {
	sb := t.signedfact()
	switch u := sb.(type) {
	case dummyINITBallotSignedFact:
		u.fact = t.wrongfact()
		t.NoError(u.Sign(t.priv, t.networkID))
		sb = u
	case dummyProposalSignedFact:
		u.fact = t.wrongfact()
		t.NoError(u.Sign(t.priv, t.networkID))
		sb = u
	case dummyACCEPTBallotSignedFact:
		u.fact = t.wrongfact()
		t.NoError(u.Sign(t.priv, t.networkID))
		sb = u
	}

	err := sb.IsValid(t.networkID)
	t.Error(err)
	t.True(errors.Is(err, util.InvalidError))
}

func TestBaseINITBallotSignedFact(tt *testing.T) {
	t := new(testBaseBallotSignedFact)
	t.signedfact = func() BallotSignedFact {
		fact := newDummyINITBallotFact(NewPoint(Height(33), Round(44)),
			valuehash.RandomSHA256(),
			util.UUID().String())

		sb := dummyINITBallotSignedFact{
			BaseINITBallotSignedFact: NewBaseINITBallotSignedFact(
				dummyINITBallotSignedFactHint,
				RandomAddress(""),
				fact,
			),
		}
		t.NoError(sb.Sign(t.priv, t.networkID))

		_ = (interface{})(sb).(INITBallotSignedFact)

		return sb
	}
	t.wrongfact = func() BallotFact {
		return newDummyACCEPTBallotFact(NewPoint(Height(33), Round(44)),
			valuehash.RandomSHA256(),
			valuehash.RandomSHA256(),
			util.UUID().String())
	}

	suite.Run(tt, t)
}

func TestBaseProposalBallotSignedFact(tt *testing.T) {
	t := new(testBaseBallotSignedFact)
	t.signedfact = func() BallotSignedFact {
		fact := newDummyProposalFact(NewPoint(Height(33), Round(44)),
			[]util.Hash{valuehash.RandomSHA256()},
			util.UUID().String())

		sb := dummyProposalSignedFact{
			BaseProposalSignedFact: NewBaseProposalSignedFact(
				dummyProposalSignedFactHint,
				RandomAddress(""),
				fact,
			),
		}
		t.NoError(sb.Sign(t.priv, t.networkID))

		_ = (interface{})(sb).(ProposalSignedFact)
		return sb
	}
	t.wrongfact = func() BallotFact {
		return newDummyINITBallotFact(NewPoint(Height(33), Round(44)),
			valuehash.RandomSHA256(),
			util.UUID().String())
	}

	suite.Run(tt, t)
}

func TestBaseACCEPTBallotSignedFact(tt *testing.T) {
	t := new(testBaseBallotSignedFact)
	t.signedfact = func() BallotSignedFact {
		fact := newDummyACCEPTBallotFact(NewPoint(Height(33), Round(44)),
			valuehash.RandomSHA256(),
			valuehash.RandomSHA256(),
			util.UUID().String())

		sb := dummyACCEPTBallotSignedFact{
			BaseACCEPTBallotSignedFact: NewBaseACCEPTBallotSignedFact(
				dummyACCEPTBallotSignedFactHint,
				RandomAddress(""),
				fact,
			),
		}
		t.NoError(sb.Sign(t.priv, t.networkID))

		_ = (interface{})(sb).(ACCEPTBallotSignedFact)
		return sb
	}
	t.wrongfact = func() BallotFact {
		return newDummyINITBallotFact(NewPoint(Height(33), Round(44)),
			valuehash.RandomSHA256(),
			util.UUID().String())
	}

	suite.Run(tt, t)
}

func TestDummyINITBallotSignedFactEncode(tt *testing.T) {
	t := new(encoder.BaseTestEncode)

	enc := jsonenc.NewEncoder()
	priv := NewMPrivatekey()
	networkID := NetworkID(util.UUID().Bytes())

	t.Encode = func() (interface{}, []byte) {
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: StringAddressHint, Instance: StringAddress{}}))
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: MPublickeyHint, Instance: MPublickey{}}))
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: dummyINITBallotFactHint, Instance: dummyINITBallotFact{}}))
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: dummyINITBallotSignedFactHint, Instance: dummyINITBallotSignedFact{}}))

		fact := newDummyINITBallotFact(NewPoint(Height(33), Round(44)),
			valuehash.RandomSHA256(),
			util.UUID().String())
		sb := dummyINITBallotSignedFact{
			BaseINITBallotSignedFact: NewBaseINITBallotSignedFact(
				dummyINITBallotSignedFactHint,
				RandomAddress(""),
				fact,
			),
		}
		t.NoError(sb.Sign(priv, networkID))
		t.NoError(sb.IsValid(networkID))

		b, err := enc.Marshal(sb)
		t.NoError(err)

		return sb, b
	}
	t.Decode = func(b []byte) interface{} {
		i, err := enc.Decode(b)
		t.NoError(err)

		sb, ok := i.(dummyINITBallotSignedFact)
		t.True(ok)
		t.NoError(sb.IsValid(networkID))

		return i
	}
	t.Compare = func(a, b interface{}) {
		as, ok := a.(dummyINITBallotSignedFact)
		t.True(ok)
		bs, ok := b.(dummyINITBallotSignedFact)
		t.True(ok)

		CompareBallotSignedFact(t.Assert(), as, bs)
	}

	suite.Run(tt, t)
}

func TestDummyProposalBallotSignedFactEncode(tt *testing.T) {
	t := new(encoder.BaseTestEncode)

	enc := jsonenc.NewEncoder()
	priv := NewMPrivatekey()
	networkID := NetworkID(util.UUID().Bytes())

	t.Encode = func() (interface{}, []byte) {
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: StringAddressHint, Instance: StringAddress{}}))
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: MPublickeyHint, Instance: MPublickey{}}))
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: dummyProposalFactHint, Instance: dummyProposalFact{}}))
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: dummyProposalSignedFactHint, Instance: dummyProposalSignedFact{}}))

		fact := newDummyProposalFact(NewPoint(Height(33), Round(44)),
			[]util.Hash{valuehash.RandomSHA256()},
			util.UUID().String())
		sb := dummyProposalSignedFact{
			BaseProposalSignedFact: NewBaseProposalSignedFact(
				dummyProposalSignedFactHint,
				RandomAddress(""),
				fact,
			),
		}
		t.NoError(sb.Sign(priv, networkID))
		t.NoError(sb.IsValid(networkID))

		b, err := enc.Marshal(sb)
		t.NoError(err)

		return sb, b
	}
	t.Decode = func(b []byte) interface{} {
		i, err := enc.Decode(b)
		t.NoError(err)

		sb, ok := i.(dummyProposalSignedFact)
		t.True(ok)
		t.NoError(sb.IsValid(networkID))

		return i
	}
	t.Compare = func(a, b interface{}) {
		as, ok := a.(dummyProposalSignedFact)
		t.True(ok)
		bs, ok := b.(dummyProposalSignedFact)
		t.True(ok)

		CompareBallotSignedFact(t.Assert(), as, bs)
	}

	suite.Run(tt, t)
}

func TestDummyACCEPTBallotSignedFactEncode(tt *testing.T) {
	t := new(encoder.BaseTestEncode)

	enc := jsonenc.NewEncoder()
	priv := NewMPrivatekey()
	networkID := NetworkID(util.UUID().Bytes())

	t.Encode = func() (interface{}, []byte) {
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: StringAddressHint, Instance: StringAddress{}}))
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: MPublickeyHint, Instance: MPublickey{}}))
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: dummyACCEPTBallotFactHint, Instance: dummyACCEPTBallotFact{}}))
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: dummyACCEPTBallotSignedFactHint, Instance: dummyACCEPTBallotSignedFact{}}))

		fact := newDummyACCEPTBallotFact(NewPoint(Height(33), Round(44)),
			valuehash.RandomSHA256(),
			valuehash.RandomSHA256(),
			util.UUID().String())
		sb := dummyACCEPTBallotSignedFact{
			BaseACCEPTBallotSignedFact: NewBaseACCEPTBallotSignedFact(
				dummyACCEPTBallotSignedFactHint,
				RandomAddress(""),
				fact,
			),
		}
		t.NoError(sb.Sign(priv, networkID))
		t.NoError(sb.IsValid(networkID))

		b, err := enc.Marshal(sb)
		t.NoError(err)

		return sb, b
	}
	t.Decode = func(b []byte) interface{} {
		i, err := enc.Decode(b)
		t.NoError(err)

		sb, ok := i.(dummyACCEPTBallotSignedFact)
		t.True(ok)
		t.NoError(sb.IsValid(networkID))

		return i
	}
	t.Compare = func(a, b interface{}) {
		as, ok := a.(dummyACCEPTBallotSignedFact)
		t.True(ok)
		bs, ok := b.(dummyACCEPTBallotSignedFact)
		t.True(ok)

		CompareBallotSignedFact(t.Assert(), as, bs)
	}

	suite.Run(tt, t)
}
