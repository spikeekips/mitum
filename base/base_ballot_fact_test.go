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
	dummyINITBallotFactHint   = hint.MustNewHint("dummy-init-ballot-fact-v1.2.3")
	dummyProposalFactHint     = hint.MustNewHint("dummy-proposal-fact-v1.2.3")
	dummyACCEPTBallotFactHint = hint.MustNewHint("dummy-accept-ballot-fact-v1.2.3")
)

type dummyINITBallotFact struct {
	BaseINITBallotFact
	A string
}

func newDummyINITBallotFact(point Point, previousBlock util.Hash, a string) dummyINITBallotFact {
	return dummyINITBallotFact{
		BaseINITBallotFact: NewBaseINITBallotFact(
			dummyINITBallotFactHint,
			point,
			previousBlock,
		),
		A: a,
	}
}

func (fact dummyINITBallotFact) MarshalJSON() ([]byte, error) {
	b, err := util.MarshalJSON(fact.BaseINITBallotFact)
	if err != nil {
		panic(err)
	}

	var m map[string]interface{}
	if err := util.UnmarshalJSON(b, &m); err != nil {
		panic(err)
	}

	m["A"] = fact.A
	return util.MarshalJSON(m)
}

func (fact *dummyINITBallotFact) DecodeJSON(b []byte, enc *jsonenc.Encoder) error {
	var m struct {
		A string
	}
	if err := util.UnmarshalJSON(b, &m); err != nil {
		panic(err)
	}
	fact.A = m.A

	return fact.BaseINITBallotFact.DecodeJSON(b, enc)
}

type dummyProposalFact struct {
	BaseProposalFact
	A string
}

func newDummyProposalFact(point Point, operations []util.Hash, a string) dummyProposalFact {
	return dummyProposalFact{
		BaseProposalFact: NewBaseProposalFact(
			dummyProposalFactHint,
			point,
			operations,
		),
		A: a,
	}
}

type dummyACCEPTBallotFact struct {
	BaseACCEPTBallotFact
	A string
}

func newDummyACCEPTBallotFact(point Point, proposal, newBlock util.Hash, a string) dummyACCEPTBallotFact {
	return dummyACCEPTBallotFact{
		BaseACCEPTBallotFact: NewBaseACCEPTBallotFact(
			dummyACCEPTBallotFactHint,
			point,
			proposal,
			newBlock,
		),
		A: a,
	}
}

type testBaseBallotFact struct {
	suite.Suite
	ballot func() BallotFact
}

func (t *testBaseBallotFact) setHash(bl BallotFact, h util.Hash) BallotFact {
	switch t := bl.(type) {
	case dummyINITBallotFact:
		t.h = h
		return t
	case dummyProposalFact:
		t.h = h
		return t
	case dummyACCEPTBallotFact:
		t.h = h
		return t
	default:
		panic("unknown BallotFact")
	}
}

func (t *testBaseBallotFact) setWrongStage(bl BallotFact) BallotFact {
	switch t := bl.(type) {
	case dummyINITBallotFact:
		t.stage = StageProposal
		return t
	case dummyProposalFact:
		t.stage = StageINIT
		return t
	case dummyACCEPTBallotFact:
		t.stage = StageProposal
		return t
	default:
		panic("unknown BallotFact")
	}
}

func (t *testBaseBallotFact) TestNew() {
	bl := t.ballot()
	t.NoError(bl.IsValid(nil))

	_ = (interface{})(bl).(BallotFact)
}

func (t *testBaseBallotFact) TestEmptyHash() {
	bl := t.ballot()
	bl = t.setHash(bl, nil)

	err := bl.IsValid(nil)
	t.Error(err)
	t.True(errors.Is(err, util.InvalidError))
}

func (t *testBaseBallotFact) TestWrongHash() {
	bl := t.ballot()
	bl = t.setHash(bl, valuehash.RandomSHA256())

	err := bl.IsValid(nil)
	t.Error(err)
	t.True(errors.Is(err, util.InvalidError))
}

func (t *testBaseBallotFact) TestWrongStage() {
	bl := t.ballot()
	bl = t.setWrongStage(bl)

	err := bl.IsValid(nil)
	t.Error(err)
	t.True(errors.Is(err, util.InvalidError))
}

func TestBaseINITBallotFact(tt *testing.T) {
	t := new(testBaseBallotFact)
	t.ballot = func() BallotFact {
		bl := newDummyINITBallotFact(NewPoint(Height(33), Round(44)),
			valuehash.RandomSHA256(), util.UUID().String())
		_ = (interface{})(bl).(INITBallotFact)

		return bl
	}

	suite.Run(tt, t)
}

func TestBaseProposalBallotFact(tt *testing.T) {
	t := new(testBaseBallotFact)
	t.ballot = func() BallotFact {
		bl := newDummyProposalFact(NewPoint(Height(33), Round(44)),
			[]util.Hash{valuehash.RandomSHA256()}, util.UUID().String())
		_ = (interface{})(bl).(ProposalFact)

		return bl
	}

	suite.Run(tt, t)
}

func TestBaseACCEPTBallotFact(tt *testing.T) {
	t := new(testBaseBallotFact)
	t.ballot = func() BallotFact {
		bl := newDummyACCEPTBallotFact(NewPoint(Height(33), Round(44)),
			valuehash.RandomSHA256(),
			valuehash.RandomSHA256(),
			util.UUID().String())
		_ = (interface{})(bl).(ACCEPTBallotFact)

		return bl
	}

	suite.Run(tt, t)
}

type baseTestDummyBallotFactEncode struct {
	*encoder.BaseTestEncode
	enc     encoder.Encoder
	priv    Privatekey
	compare func(a, b BallotFact)
}

func (t *baseTestDummyBallotFactEncode) SetupTest() {
	t.enc = jsonenc.NewEncoder()

	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: StringAddressHint, Instance: StringAddress{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: dummyINITBallotFactHint, Instance: dummyINITBallotFact{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: dummyProposalFactHint, Instance: dummyProposalFact{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: dummyACCEPTBallotFactHint, Instance: dummyACCEPTBallotFact{}}))
}

func testDummyBallotFactEncode() *baseTestDummyBallotFactEncode {
	t := new(baseTestDummyBallotFactEncode)
	t.BaseTestEncode = new(encoder.BaseTestEncode)

	t.priv = NewMPrivatekey()
	t.Compare = func(a, b interface{}) {
		af, ok := a.(BallotFact)
		t.True(ok)
		bf, ok := b.(BallotFact)
		t.True(ok)

		t.NoError(bf.IsValid(nil))

		CompareBallotFact(t.Assert(), af, bf)

		if t.compare != nil {
			t.compare(af, bf)
		}
	}

	return t
}

func TestINITBallotFactJSON(tt *testing.T) {
	t := testDummyBallotFactEncode()

	t.Encode = func() (interface{}, []byte) {
		bl := newDummyINITBallotFact(NewPoint(Height(33), Round(44)),
			valuehash.RandomSHA256(), util.UUID().String())

		b, err := t.enc.Marshal(bl)
		t.NoError(err)

		return bl, b
	}
	t.Decode = func(b []byte) interface{} {
		i, err := t.enc.Decode(b)
		t.NoError(err)

		_, ok := i.(dummyINITBallotFact)
		t.True(ok)

		return i
	}

	suite.Run(tt, t)
}

func TestProposalBallotFactJSON(tt *testing.T) {
	t := testDummyBallotFactEncode()

	t.Encode = func() (interface{}, []byte) {
		bl := newDummyProposalFact(NewPoint(Height(33), Round(44)),
			[]util.Hash{valuehash.RandomSHA256()}, util.UUID().String())

		b, err := t.enc.Marshal(bl)
		t.NoError(err)

		return bl, b
	}
	t.Decode = func(b []byte) interface{} {
		i, err := t.enc.Decode(b)
		t.NoError(err)

		_, ok := i.(dummyProposalFact)
		t.True(ok)

		return i
	}

	suite.Run(tt, t)
}

func TestACCEPTBallotFactJSON(tt *testing.T) {
	t := testDummyBallotFactEncode()

	t.Encode = func() (interface{}, []byte) {
		bl := newDummyACCEPTBallotFact(NewPoint(Height(33), Round(44)),
			valuehash.RandomSHA256(),
			valuehash.RandomSHA256(),
			util.UUID().String())

		b, err := t.enc.Marshal(bl)
		t.NoError(err)

		return bl, b
	}
	t.Decode = func(b []byte) interface{} {
		i, err := t.enc.Decode(b)
		t.NoError(err)

		_, ok := i.(dummyACCEPTBallotFact)
		t.True(ok)

		return i
	}

	suite.Run(tt, t)
}
