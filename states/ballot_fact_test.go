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

type testBaseBallotFact struct {
	suite.Suite
	ballot func() base.BallotFact
}

func (t *testBaseBallotFact) setHash(bl base.BallotFact, h util.Hash) base.BallotFact {
	switch t := bl.(type) {
	case INITBallotFact:
		t.h = h
		return t
	case ProposalFact:
		t.h = h
		return t
	case ACCEPTBallotFact:
		t.h = h
		return t
	default:
		panic("unknown BallotFact")
	}
}

func (t *testBaseBallotFact) setWrongStage(bl base.BallotFact) base.BallotFact {
	switch y := bl.(type) {
	case INITBallotFact:
		y.point = base.NewStagePoint(y.point.Point, base.StageProposal)
		return y
	case ProposalFact:
		y.point = base.NewStagePoint(y.point.Point, base.StageINIT)
		return y
	case ACCEPTBallotFact:
		y.point = base.NewStagePoint(y.point.Point, base.StageProposal)
		return y
	default:
		panic("unknown BallotFact")
	}
}

func (t *testBaseBallotFact) TestNew() {
	bl := t.ballot()
	t.NoError(bl.IsValid(nil))

	_ = (interface{})(bl).(base.BallotFact)
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

func TestBasicINITBallotFact(tt *testing.T) {
	t := new(testBaseBallotFact)
	t.ballot = func() base.BallotFact {
		bl := NewINITBallotFact(base.NewPoint(base.Height(33), base.Round(44)), valuehash.RandomSHA256())
		_ = (interface{})(bl).(base.INITBallotFact)

		return bl
	}

	suite.Run(tt, t)
}

func TestBasicProposalBallotFact(tt *testing.T) {
	t := new(testBaseBallotFact)
	t.ballot = func() base.BallotFact {
		bl := NewProposalFact(base.NewPoint(base.Height(33), base.Round(44)),
			[]util.Hash{valuehash.RandomSHA256()})
		_ = (interface{})(bl).(base.ProposalFact)

		return bl
	}

	suite.Run(tt, t)
}

func TestBasicACCEPTBallotFact(tt *testing.T) {
	t := new(testBaseBallotFact)
	t.ballot = func() base.BallotFact {
		bl := NewACCEPTBallotFact(base.NewPoint(base.Height(33), base.Round(44)),
			valuehash.RandomSHA256(),
			valuehash.RandomSHA256(),
		)
		_ = (interface{})(bl).(base.ACCEPTBallotFact)

		return bl
	}

	suite.Run(tt, t)
}

type testProposalBallotFact struct {
	suite.Suite
}

func (t *testProposalBallotFact) TestDuplicatedOperations() {
	op := valuehash.RandomSHA256()
	bl := NewProposalFact(base.NewPoint(base.Height(33), base.Round(44)),
		[]util.Hash{
			valuehash.RandomSHA256(),
			op,
			valuehash.RandomSHA256(),
			op,
		})

	err := bl.IsValid(nil)
	t.Error(err)
	t.True(errors.Is(err, util.InvalidError))
	t.Contains(err.Error(), "duplicated operation found")
}

func TestProposalBallotFact(t *testing.T) {
	suite.Run(t, new(testProposalBallotFact))
}

type baseTestBallotFactEncode struct {
	*encoder.BaseTestEncode
	enc     encoder.Encoder
	priv    base.Privatekey
	compare func(a, b base.BallotFact)
}

func (t *baseTestBallotFactEncode) SetupTest() {
	t.enc = jsonenc.NewEncoder()

	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: base.StringAddressHint, Instance: base.StringAddress{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: INITBallotFactHint, Instance: INITBallotFact{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: ProposalFactHint, Instance: ProposalFact{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: ACCEPTBallotFactHint, Instance: ACCEPTBallotFact{}}))
}

func testBallotFactEncode() *baseTestBallotFactEncode {
	t := new(baseTestBallotFactEncode)
	t.BaseTestEncode = new(encoder.BaseTestEncode)

	t.priv = base.NewMPrivatekey()
	t.Compare = func(a, b interface{}) {
		af, ok := a.(base.BallotFact)
		t.True(ok)
		bf, ok := b.(base.BallotFact)
		t.True(ok)

		t.NoError(bf.IsValid(nil))

		base.CompareBallotFact(t.Assert(), af, bf)

		if t.compare != nil {
			t.compare(af, bf)
		}
	}

	return t
}

func TestINITBallotFactJSON(tt *testing.T) {
	t := testBallotFactEncode()

	t.Encode = func() (interface{}, []byte) {
		bl := NewINITBallotFact(base.NewPoint(base.Height(33), base.Round(44)), valuehash.RandomSHA256())

		b, err := t.enc.Marshal(bl)
		t.NoError(err)

		return bl, b
	}
	t.Decode = func(b []byte) interface{} {
		i, err := t.enc.Decode(b)
		t.NoError(err)

		_, ok := i.(INITBallotFact)
		t.True(ok)

		return i
	}

	suite.Run(tt, t)
}

func TestProposalBallotFactJSON(tt *testing.T) {
	t := testBallotFactEncode()

	t.Encode = func() (interface{}, []byte) {
		bl := NewProposalFact(base.NewPoint(base.Height(33), base.Round(44)),
			[]util.Hash{valuehash.RandomSHA256()})

		b, err := t.enc.Marshal(bl)
		t.NoError(err)

		return bl, b
	}
	t.Decode = func(b []byte) interface{} {
		i, err := t.enc.Decode(b)
		t.NoError(err)

		_, ok := i.(ProposalFact)
		t.True(ok)

		return i
	}

	suite.Run(tt, t)
}

func TestACCEPTBallotFactJSON(tt *testing.T) {
	t := testBallotFactEncode()

	t.Encode = func() (interface{}, []byte) {
		bl := NewACCEPTBallotFact(base.NewPoint(base.Height(33), base.Round(44)),
			valuehash.RandomSHA256(),
			valuehash.RandomSHA256(),
		)

		b, err := t.enc.Marshal(bl)
		t.NoError(err)

		return bl, b
	}
	t.Decode = func(b []byte) interface{} {
		i, err := t.enc.Decode(b)
		t.NoError(err)

		_, ok := i.(ACCEPTBallotFact)
		t.True(ok)

		return i
	}

	suite.Run(tt, t)
}