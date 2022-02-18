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

type testProposalFact struct {
	suite.Suite
}

func (t *testProposalFact) proposal() ProposalFact {
	pr := NewProposalFact(
		base.RawPoint(33, 44),
		base.RandomAddress("pr"),
		[]util.Hash{valuehash.RandomSHA256()},
	)
	_ = (interface{})(pr).(base.ProposalFact)

	return pr
}

func (t *testProposalFact) TestNew() {
	pr := t.proposal()
	t.NoError(pr.IsValid(nil))

	_ = (interface{})(pr).(base.ProposalFact)
}

func (t *testProposalFact) TestEmptyHash() {
	pr := t.proposal()
	pr.h = nil

	err := pr.IsValid(nil)
	t.Error(err)
	t.True(errors.Is(err, util.InvalidError))
}

func (t *testProposalFact) TestWrongHash() {
	pr := t.proposal()
	pr.h = valuehash.RandomSHA256()

	err := pr.IsValid(nil)
	t.Error(err)
	t.True(errors.Is(err, util.InvalidError))
}

func (t *testProposalFact) TestWrongPoint() {
	pr := t.proposal()
	pr.point = base.ZeroPoint

	err := pr.IsValid(nil)
	t.Error(err)
	t.True(errors.Is(err, util.InvalidError))
}

func (t *testProposalFact) TestDuplicatedOperations() {
	op := valuehash.RandomSHA256()
	pr := NewProposalFact(
		base.RawPoint(33, 44),
		base.RandomAddress("pr"),
		[]util.Hash{
			valuehash.RandomSHA256(),
			op,
			valuehash.RandomSHA256(),
			op,
		})

	err := pr.IsValid(nil)
	t.Error(err)
	t.True(errors.Is(err, util.InvalidError))
	t.Contains(err.Error(), "duplicated operation found")
}

type testProposalFactEncode struct {
	encoder.BaseTestEncode
	enc *jsonenc.Encoder
}

func (t *testProposalFactEncode) SetupTest() {
	t.enc = jsonenc.NewEncoder()

	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: base.StringAddressHint, Instance: base.StringAddress{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: ProposalFactHint, Instance: ProposalFact{}}))
}

func TestProposalFactJSON(tt *testing.T) {
	t := new(testProposalFactEncode)

	t.Encode = func() (interface{}, []byte) {
		pr := NewProposalFact(base.RawPoint(33, 44),
			base.RandomAddress("pr"),
			[]util.Hash{valuehash.RandomSHA256()})

		b, err := t.enc.Marshal(pr)
		t.NoError(err)

		return pr, b
	}
	t.Decode = func(b []byte) interface{} {
		i, err := t.enc.Decode(b)
		t.NoError(err)

		_, ok := i.(ProposalFact)
		t.True(ok)

		return i
	}
	t.Compare = func(a, b interface{}) {
		af, ok := a.(ProposalFact)
		t.True(ok)
		bf, ok := b.(ProposalFact)
		t.True(ok)

		t.NoError(bf.IsValid(nil))

		base.CompareProposalFact(t.Assert(), af, bf)
	}

	suite.Run(tt, t)
}
