package isaac

import (
	"fmt"
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
	ballot func(base.Point, []util.Hash) base.BallotFact
}

func (t *testBaseBallotFact) setHash(bl base.BallotFact, h util.Hash) base.BallotFact {
	switch t := bl.(type) {
	case INITBallotFact:
		t.SetHash(h)
		return t
	case ACCEPTBallotFact:
		t.SetHash(h)
		return t
	case SuffrageConfirmBallotFact:
		t.SetHash(h)
		return t
	default:
		panic(fmt.Errorf("unknown BallotFact, %T", t))
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
	case SuffrageConfirmBallotFact:
		y.point = base.NewStagePoint(y.point.Point, base.StageACCEPT)
		return y
	default:
		panic(fmt.Errorf("unknown BallotFact, %T", t))
	}
}

func (t *testBaseBallotFact) TestNew() {
	bl := t.ballot(base.RawPoint(33, 44), nil)
	switch bl.(type) {
	case INITBallotFact, ACCEPTBallotFact:
	default:
		return
	}

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

	t.Run("empty expel facts", func() {
		bl := t.ballot(base.RawPoint(33, 44), nil)

		switch bl.(type) {
		case INITBallotFact, ACCEPTBallotFact:
		default:
			return
		}

		t.NoError(bl.IsValid(nil))
	})

	t.Run("expel facts", func() {
		expelfacts := make([]util.Hash, 3)
		for i := range expelfacts {
			expelfacts[i] = valuehash.RandomSHA256()
		}

		bl := t.ballot(base.RawPoint(33, 44), expelfacts)
		t.NoError(bl.IsValid(nil))
	})
}

func TestINITBallotFact(tt *testing.T) {
	t := new(testBaseBallotFact)
	t.ballot = func(point base.Point, expelfacts []util.Hash) base.BallotFact {
		bl := NewINITBallotFact(point, valuehash.RandomSHA256(), valuehash.RandomSHA256(), expelfacts)
		_ = (interface{})(bl).(base.INITBallotFact)

		return bl
	}

	suite.Run(tt, t)
}

func TestACCEPTBallotFact(tt *testing.T) {
	t := new(testBaseBallotFact)
	t.ballot = func(point base.Point, expelfacts []util.Hash) base.BallotFact {
		bl := NewACCEPTBallotFact(point,
			valuehash.RandomSHA256(),
			valuehash.RandomSHA256(),
			expelfacts,
		)
		_ = (interface{})(bl).(base.ACCEPTBallotFact)

		return bl
	}

	suite.Run(tt, t)
}

type testSuffrageConfirmBallotFact struct {
	testBaseBallotFact
}

func (t *testSuffrageConfirmBallotFact) SetupSuite() {
	t.ballot = func(point base.Point, expelfacts []util.Hash) base.BallotFact {
		bl := NewSuffrageConfirmBallotFact(point, valuehash.RandomSHA256(), valuehash.RandomSHA256(), expelfacts)
		_ = (interface{})(bl).(base.INITBallotFact)

		return bl
	}
}

func (t *testSuffrageConfirmBallotFact) TestNew() {
	point := base.RawPoint(33, 44)

	expelfacts := make([]util.Hash, 3)
	for i := range expelfacts {
		expelfacts[i] = valuehash.RandomSHA256()
	}

	bl := t.ballot(point, expelfacts)
	t.NoError(bl.IsValid(nil))

	_ = (interface{})(bl).(base.BallotFact)
}

func (t *testSuffrageConfirmBallotFact) TestIsValid() {
	t.Run("empty expelfacts", func() {
		point := base.RawPoint(33, 44)

		bl := t.ballot(point, nil)

		switch bl.(type) {
		case INITBallotFact, ACCEPTBallotFact:
		default:
			return
		}

		err := bl.IsValid(nil)
		t.Error(err)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "empty expel facts")
	})
}

func TestSuffrageConfirmBallotFact(t *testing.T) {
	suite.Run(t, new(testSuffrageConfirmBallotFact))
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
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: SuffrageExpelFactHint, Instance: SuffrageExpelFact{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: INITBallotFactHint, Instance: INITBallotFact{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: ACCEPTBallotFactHint, Instance: ACCEPTBallotFact{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: SuffrageConfirmBallotFactHint, Instance: SuffrageConfirmBallotFact{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: EmptyProposalINITBallotFactHint, Instance: EmptyProposalINITBallotFact{}}))
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

	expelfacts := make([]util.Hash, 3)
	for i := range expelfacts {
		expelfacts[i] = valuehash.RandomSHA256()
	}

	t.Encode = func() (interface{}, []byte) {
		bl := NewINITBallotFact(point, valuehash.RandomSHA256(), valuehash.RandomSHA256(), expelfacts)

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
		ab, ok := a.(INITBallotFact)
		t.True(ok)
		bb, ok := b.(INITBallotFact)
		t.True(ok)

		t.True(ab.Proposal().Equal(bb.Proposal()))
		t.True(ab.PreviousBlock().Equal(bb.PreviousBlock()))

		bwfs := bb.ExpelFacts()
		t.Equal(len(expelfacts), len(bwfs))

		for i := range expelfacts {
			af := expelfacts[i]
			bf := bwfs[i]

			t.True(af.Equal(bf))
		}
	}

	suite.Run(tt, t)
}

func TestEmptyProposalINITBallotFactJSON(tt *testing.T) {
	t := testBallotFactEncode()

	point := base.RawPoint(33, 44)

	t.Encode = func() (interface{}, []byte) {
		bl := NewEmptyProposalINITBallotFact(point, valuehash.RandomSHA256(), valuehash.RandomSHA256())

		b, err := t.enc.Marshal(&bl)
		t.NoError(err)

		t.T().Log("marshaled:", string(b))

		return bl, b
	}
	t.Decode = func(b []byte) interface{} {
		i, err := t.enc.Decode(b)
		t.NoError(err)

		_, ok := i.(EmptyProposalINITBallotFact)
		t.True(ok)

		return i
	}

	t.compare = func(a, b base.BallotFact) {
		ab, ok := a.(EmptyProposalINITBallotFact)
		t.True(ok)
		bb, ok := b.(EmptyProposalINITBallotFact)
		t.True(ok)

		t.True(ab.Proposal().Equal(bb.Proposal()))
		t.True(ab.PreviousBlock().Equal(bb.PreviousBlock()))
	}

	suite.Run(tt, t)
}

func TestACCEPTBallotFactJSON(tt *testing.T) {
	t := testBallotFactEncode()

	point := base.RawPoint(33, 44)

	expelfacts := make([]util.Hash, 3)
	for i := range expelfacts {
		expelfacts[i] = valuehash.RandomSHA256()
	}

	t.Encode = func() (interface{}, []byte) {
		bl := NewACCEPTBallotFact(point,
			valuehash.RandomSHA256(),
			valuehash.RandomSHA256(),
			expelfacts,
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

		bwfs := bb.ExpelFacts()
		t.Equal(len(expelfacts), len(bwfs))

		for i := range expelfacts {
			af := expelfacts[i]
			bf := bwfs[i]

			t.True(af.Equal(bf))
		}
	}

	suite.Run(tt, t)
}

func TestSuffrageConfirmBallotFactJSON(tt *testing.T) {
	t := testBallotFactEncode()

	point := base.RawPoint(33, 44)

	t.Encode = func() (interface{}, []byte) {
		bl := NewSuffrageConfirmBallotFact(point, valuehash.RandomSHA256(), valuehash.RandomSHA256(), []util.Hash{valuehash.RandomSHA256(), valuehash.RandomSHA256()})

		b, err := t.enc.Marshal(&bl)
		t.NoError(err)

		t.T().Log("marshaled:", string(b))

		return bl, b
	}
	t.Decode = func(b []byte) interface{} {
		i, err := t.enc.Decode(b)
		t.NoError(err)

		_, ok := i.(SuffrageConfirmBallotFact)
		t.True(ok)

		return i
	}

	t.Compare = func(a, b interface{}) {
		af, ok := a.(base.BallotFact)
		t.True(ok)
		bf, ok := b.(base.BallotFact)
		t.True(ok)

		base.EqualFact(t.Assert(), af, bf)
		t.Equal(af.Point(), bf.Point())

		t.NoError(bf.IsValid(nil))

		ab, ok := b.(SuffrageConfirmBallotFact)
		t.True(ok)
		bb, ok := b.(SuffrageConfirmBallotFact)
		t.True(ok)

		abf := ab.ExpelFacts()
		bbf := bb.ExpelFacts()

		t.Equal(len(abf), len(bbf))

		for i := range abf {
			t.True(abf[i].Equal(bbf[i]))
		}
	}

	suite.Run(tt, t)
}
