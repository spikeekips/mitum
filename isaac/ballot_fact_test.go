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

type testBaseBallotFact struct {
	suite.Suite
	ballot func(base.Point, []SuffrageWithdrawFact) base.BallotFact
}

func (t *testBaseBallotFact) setHash(bl base.BallotFact, h util.Hash) base.BallotFact {
	switch t := bl.(type) {
	case INITBallotFact:
		t.SetHash(h)
		return t
	case ACCEPTBallotFact:
		t.SetHash(h)
		return t
	default:
		panic("unknown BallotFact")
	}
}

func (t *testBaseBallotFact) setWrongStage(bl base.BallotFact) base.BallotFact {
	switch y := bl.(type) {
	case INITBallotFact:
		y.point = base.NewStagePoint(y.point.Point, base.StageACCEPT)
		return y
	case ACCEPTBallotFact:
		y.point = base.NewStagePoint(y.point.Point, base.StageINIT)
		return y
	default:
		panic("unknown BallotFact")
	}
}

func (t *testBaseBallotFact) TestNew() {
	bl := t.ballot(base.RawPoint(33, 44), nil)
	t.NoError(bl.IsValid(nil))

	_ = (interface{})(bl).(base.BallotFact)
}

func (t *testBaseBallotFact) TestInValid() {
	t.Run("EmptyHash", func() {
		bl := t.ballot(base.RawPoint(33, 44), nil)
		bl = t.setHash(bl, nil)

		err := bl.IsValid(nil)
		t.Error(err)
		t.True(errors.Is(err, util.ErrInvalid))
	})

	t.Run("WrongHash", func() {
		bl := t.ballot(base.RawPoint(33, 44), nil)
		bl = t.setHash(bl, valuehash.RandomSHA256())

		err := bl.IsValid(nil)
		t.Error(err)
		t.True(errors.Is(err, util.ErrInvalid))
	})

	t.Run("WrongStage", func() {
		bl := t.ballot(base.RawPoint(33, 44), nil)
		bl = t.setWrongStage(bl)

		err := bl.IsValid(nil)
		t.Error(err)
		t.True(errors.Is(err, util.ErrInvalid))
	})

	t.Run("empty withdraw facts", func() {
		bl := t.ballot(base.RawPoint(33, 44), nil)
		t.NoError(bl.IsValid(nil))
	})

	t.Run("wrong start height withdraw fact", func() {
		withdrawfacts := make([]SuffrageWithdrawFact, 3)
		for i := range withdrawfacts[:2] {
			withdrawfacts[i] = NewSuffrageWithdrawFact(base.RandomAddress(""), base.Height(31), base.Height(44), util.UUID().String())
		}

		withdrawfacts[2] = NewSuffrageWithdrawFact(base.RandomAddress(""), base.Height(31), base.Height(44), util.UUID().String())

		bl := t.ballot(base.RawPoint(33, 44), withdrawfacts)
		t.NoError(bl.IsValid(nil))
	})

	t.Run("withdraw facts", func() {
		withdrawfacts := make([]SuffrageWithdrawFact, 3)
		for i := range withdrawfacts {
			withdrawfacts[i] = NewSuffrageWithdrawFact(base.RandomAddress(""), base.Height(31), base.Height(44), util.UUID().String())
		}

		bl := t.ballot(base.RawPoint(33, 44), withdrawfacts)
		t.NoError(bl.IsValid(nil))
	})

	t.Run("duplicated withdraw node", func() {
		withdrawfacts := make([]SuffrageWithdrawFact, 3)
		for i := range withdrawfacts[:len(withdrawfacts)-1] {
			withdrawfacts[i] = NewSuffrageWithdrawFact(base.RandomAddress(""), base.Height(31), base.Height(44), util.UUID().String())
		}
		withdrawfacts[len(withdrawfacts)-1] = NewSuffrageWithdrawFact(withdrawfacts[len(withdrawfacts)-2].Node(), base.Height(33), base.Height(44), util.UUID().String())

		bl := t.ballot(base.RawPoint(33, 44), withdrawfacts)
		err := bl.IsValid(nil)
		t.Error(err)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "duplicated withdraw node found")
	})
}

func TestBasicINITBallotFact(tt *testing.T) {
	t := new(testBaseBallotFact)
	t.ballot = func(point base.Point, withdrawfacts []SuffrageWithdrawFact) base.BallotFact {
		wfacts := make([]base.SuffrageWithdrawFact, len(withdrawfacts))
		for i := range wfacts {
			wfacts[i] = withdrawfacts[i]
		}

		bl := NewINITBallotFact(point, valuehash.RandomSHA256(), valuehash.RandomSHA256(), wfacts)
		_ = (interface{})(bl).(base.INITBallotFact)

		return bl
	}

	suite.Run(tt, t)
}

func TestBasicACCEPTBallotFact(tt *testing.T) {
	t := new(testBaseBallotFact)
	t.ballot = func(point base.Point, withdrawfacts []SuffrageWithdrawFact) base.BallotFact {
		wfacts := make([]base.SuffrageWithdrawFact, len(withdrawfacts))
		for i := range wfacts {
			wfacts[i] = withdrawfacts[i]
		}

		bl := NewACCEPTBallotFact(point,
			valuehash.RandomSHA256(),
			valuehash.RandomSHA256(),
			wfacts,
		)
		_ = (interface{})(bl).(base.ACCEPTBallotFact)

		return bl
	}

	suite.Run(tt, t)
}

type baseTestBallotFactEncode struct {
	encoder.BaseTestEncode
	enc     encoder.Encoder
	priv    base.Privatekey
	compare func(a, b base.BallotFact)
}

func (t *baseTestBallotFactEncode) SetupTest() {
	t.enc = jsonenc.NewEncoder()

	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: base.StringAddressHint, Instance: base.StringAddress{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: SuffrageWithdrawFactHint, Instance: SuffrageWithdrawFact{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: INITBallotFactHint, Instance: INITBallotFact{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: ACCEPTBallotFactHint, Instance: ACCEPTBallotFact{}}))
}

func testBallotFactEncode() *baseTestBallotFactEncode {
	t := new(baseTestBallotFactEncode)

	t.priv = base.NewMPrivatekey()
	t.Compare = func(a, b interface{}) {
		af, ok := a.(base.BallotFact)
		t.True(ok)
		bf, ok := b.(base.BallotFact)
		t.True(ok)

		t.NoError(bf.IsValid(nil))

		base.EqualBallotFact(t.Assert(), af, bf)

		if t.compare != nil {
			t.compare(af, bf)
		}
	}

	return t
}

func TestINITBallotFactJSON(tt *testing.T) {
	t := testBallotFactEncode()

	point := base.RawPoint(33, 44)

	withdrawfacts := make([]base.SuffrageWithdrawFact, 3)
	for i := range withdrawfacts {
		withdrawfacts[i] = NewSuffrageWithdrawFact(base.RandomAddress(""), point.Height()-1, point.Height()+1, util.UUID().String())
	}

	t.Encode = func() (interface{}, []byte) {
		bl := NewINITBallotFact(point, valuehash.RandomSHA256(), valuehash.RandomSHA256(), withdrawfacts)

		b, err := t.enc.Marshal(&bl)
		t.NoError(err)

		t.T().Log("marshaled:", string(b))

		return bl, b
	}
	t.Decode = func(b []byte) interface{} {
		i, err := t.enc.Decode(b)
		t.NoError(err)

		_, ok := i.(INITBallotFact)
		t.True(ok)

		return i
	}

	t.compare = func(a, b base.BallotFact) {
		bb, ok := b.(INITBallotFact)
		t.True(ok)

		bwfs := bb.WithdrawFacts()
		t.Equal(len(withdrawfacts), len(bwfs))

		for i := range withdrawfacts {
			af := withdrawfacts[i]
			bf := bwfs[i]

			t.True(af.Hash().Equal(bf.Hash()))
			t.True(af.Node().Equal(bf.Node()))
			t.Equal(af.WithdrawStart(), bf.WithdrawStart())
		}
	}

	suite.Run(tt, t)
}

func TestACCEPTBallotFactJSON(tt *testing.T) {
	t := testBallotFactEncode()

	point := base.RawPoint(33, 44)

	withdrawfacts := make([]base.SuffrageWithdrawFact, 3)
	for i := range withdrawfacts {
		withdrawfacts[i] = NewSuffrageWithdrawFact(base.RandomAddress(""), point.Height()-1, point.Height()+1, util.UUID().String())
	}

	t.Encode = func() (interface{}, []byte) {
		bl := NewACCEPTBallotFact(point,
			valuehash.RandomSHA256(),
			valuehash.RandomSHA256(),
			withdrawfacts,
		)

		b, err := t.enc.Marshal(&bl)
		t.NoError(err)

		t.T().Log("marshaled:", string(b))

		return bl, b
	}
	t.Decode = func(b []byte) interface{} {
		i, err := t.enc.Decode(b)
		t.NoError(err)

		_, ok := i.(ACCEPTBallotFact)
		t.True(ok)

		return i
	}

	t.compare = func(a, b base.BallotFact) {
		bb, ok := b.(ACCEPTBallotFact)
		t.True(ok)

		bwfs := bb.WithdrawFacts()
		t.Equal(len(withdrawfacts), len(bwfs))

		for i := range withdrawfacts {
			af := withdrawfacts[i]
			bf := bwfs[i]

			t.True(af.Hash().Equal(bf.Hash()))
			t.True(af.Node().Equal(bf.Node()))
			t.Equal(af.WithdrawStart(), bf.WithdrawStart())
		}
	}

	suite.Run(tt, t)
}
