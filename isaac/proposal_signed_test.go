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

type testProposalSignedFact struct {
	suite.Suite
	priv      base.Privatekey
	networkID base.NetworkID
}

func (t *testProposalSignedFact) SetupTest() {
	t.priv = base.NewMPrivatekey()
	t.networkID = base.NetworkID(util.UUID().Bytes())
}

func (t *testProposalSignedFact) signedFact() ProposalSignedFact {
	fact := NewProposalFact(
		base.RawPoint(33, 44),
		base.RandomAddress("pr"),
		[]util.Hash{valuehash.RandomSHA256()},
	)

	sf := NewProposalSignedFact(
		fact,
	)
	t.NoError(sf.Sign(t.priv, t.networkID))

	return sf
}

func (t *testProposalSignedFact) TestNew() {
	sf := t.signedFact()

	_ = (interface{})(sf).(base.ProposalSignedFact)

	t.NoError(sf.IsValid(t.networkID))
}

func (t *testProposalSignedFact) TestEmptySigned() {
	sf := t.signedFact()

	sf.signed = base.BaseSigned{}

	err := sf.IsValid(t.networkID)
	t.Error(err)
	t.True(errors.Is(err, util.InvalidError))
}

func (t *testProposalSignedFact) TestWrongFact() {
	sf := t.signedFact()

	sf.fact = NewProposalFact(
		base.RawPoint(33, 44),
		base.RandomAddress("pr"),
		[]util.Hash{valuehash.RandomSHA256()},
	)

	err := sf.IsValid(t.networkID)
	t.Error(err)
	t.True(errors.Is(err, util.InvalidError))
}

func TestProposalSignedFact(tt *testing.T) {
	suite.Run(tt, new(testProposalSignedFact))
}

func TestProposalSignedFactEncode(tt *testing.T) {
	t := new(encoder.BaseTestEncode)

	enc := jsonenc.NewEncoder()
	priv := base.NewMPrivatekey()
	networkID := base.NetworkID(util.UUID().Bytes())

	t.Encode = func() (interface{}, []byte) {
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: base.StringAddressHint, Instance: base.StringAddress{}}))
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: base.MPublickeyHint, Instance: base.MPublickey{}}))
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: ProposalFactHint, Instance: ProposalFact{}}))
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: ProposalSignedFactHint, Instance: ProposalSignedFact{}}))

		fact := NewProposalFact(base.RawPoint(33, 44),
			base.RandomAddress("pr"),
			[]util.Hash{valuehash.RandomSHA256()},
		)
		sf := NewProposalSignedFact(
			fact,
		)
		t.NoError(sf.Sign(priv, networkID))
		t.NoError(sf.IsValid(networkID))

		b, err := enc.Marshal(sf)
		t.NoError(err)

		return sf, b
	}
	t.Decode = func(b []byte) interface{} {
		i, err := enc.Decode(b)
		t.NoError(err)

		sf, ok := i.(ProposalSignedFact)
		t.True(ok)
		t.NoError(sf.IsValid(networkID))

		return i
	}
	t.Compare = func(a, b interface{}) {
		as, ok := a.(ProposalSignedFact)
		t.True(ok)
		bs, ok := b.(ProposalSignedFact)
		t.True(ok)

		base.CompareProposalSignedFact(t.Assert(), as, bs)
	}

	suite.Run(tt, t)
}
