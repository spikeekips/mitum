package isaac

import (
	"testing"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
)

type testProposalSignFact struct {
	suite.Suite
	priv      base.Privatekey
	networkID base.NetworkID
}

func (t *testProposalSignFact) SetupTest() {
	t.priv = base.NewMPrivatekey()
	t.networkID = base.NetworkID(util.UUID().Bytes())
}

func (t *testProposalSignFact) signfact() ProposalSignFact {
	fact := NewProposalFact(
		base.RawPoint(33, 44),
		base.RandomAddress("pr"),
		valuehash.RandomSHA256(),
		[][2]util.Hash{{valuehash.RandomSHA256(), valuehash.RandomSHA256()}},
	)

	sf := NewProposalSignFact(
		fact,
	)
	t.NoError(sf.Sign(t.priv, t.networkID))

	return sf
}

func (t *testProposalSignFact) TestNew() {
	sf := t.signfact()

	_ = (interface{})(sf).(base.ProposalSignFact)

	t.NoError(sf.IsValid(t.networkID))
}

func (t *testProposalSignFact) TestEmptySigns() {
	sf := t.signfact()

	sf.sign = base.BaseSign{}

	err := sf.IsValid(t.networkID)
	t.Error(err)
	t.ErrorIs(err, util.ErrInvalid)
}

func (t *testProposalSignFact) TestWrongFact() {
	sf := t.signfact()

	sf.fact = NewProposalFact(
		base.RawPoint(33, 44),
		base.RandomAddress("pr"),
		valuehash.RandomSHA256(),
		[][2]util.Hash{{valuehash.RandomSHA256(), valuehash.RandomSHA256()}},
	)

	err := sf.IsValid(t.networkID)
	t.Error(err)
	t.ErrorIs(err, util.ErrInvalid)
}

func TestProposalSignFact(tt *testing.T) {
	suite.Run(tt, new(testProposalSignFact))
}

func TestProposalSignFactJSON(tt *testing.T) {
	t := new(encoder.BaseTestEncode)

	enc := jsonenc.NewEncoder()
	priv := base.NewMPrivatekey()
	networkID := base.NetworkID(util.UUID().Bytes())

	t.Encode = func() (interface{}, []byte) {
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: base.StringAddressHint, Instance: base.StringAddress{}}))
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: base.MPublickeyHint, Instance: &base.MPublickey{}}))
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: ProposalFactHint, Instance: ProposalFact{}}))
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: ProposalSignFactHint, Instance: ProposalSignFact{}}))

		fact := NewProposalFact(base.RawPoint(33, 44),
			base.RandomAddress("pr"),
			valuehash.RandomSHA256(),
			[][2]util.Hash{{valuehash.RandomSHA256(), valuehash.RandomSHA256()}},
		)
		sf := NewProposalSignFact(
			fact,
		)
		t.NoError(sf.Sign(priv, networkID))
		t.NoError(sf.IsValid(networkID))

		b, err := enc.Marshal(&sf)
		t.NoError(err)

		return sf, b
	}
	t.Decode = func(b []byte) interface{} {
		i, err := enc.Decode(b)
		t.NoError(err)

		sf, ok := i.(ProposalSignFact)
		t.True(ok)
		t.NoError(sf.IsValid(networkID))

		return i
	}
	t.Compare = func(a, b interface{}) {
		as, ok := a.(ProposalSignFact)
		t.True(ok)
		bs, ok := b.(ProposalSignFact)
		t.True(ok)

		base.EqualProposalSignFact(t.Assert(), as, bs)
	}

	suite.Run(tt, t)
}
