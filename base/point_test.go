package base

import (
	"testing"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/stretchr/testify/suite"
)

type testHeight struct {
	suite.Suite
}

func (t *testHeight) TestNew() {
	h10 := Height(10)
	t.Equal(int64(10), int64(h10))
}

func (t *testHeight) TestInt64() {
	h10 := Height(10)
	t.Equal(int64(10), h10.Int64())
}

func (t *testHeight) TestInvalid() {
	h10 := Height(10)
	t.NoError(h10.IsValid(nil))

	hu1 := NilHeight
	err := hu1.IsValid(nil)
	t.True(errors.Is(err, util.InvalidError))
}

func TestHeight(t *testing.T) {
	suite.Run(t, new(testHeight))
}

type testPoint struct {
	suite.Suite
}

func (t *testPoint) TestCompare() {
	cases := []struct {
		name     string
		ah       int64
		ar       uint64
		bh       int64
		br       uint64
		expected int
	}{
		{name: "higher", ah: 33, ar: 0, bh: 34, br: 0, expected: -1},
		{name: "lower", ah: 33, ar: 0, bh: 32, br: 0, expected: 1},
		{name: "same", ah: 33, ar: 0, bh: 33, br: 0, expected: 0},
		{name: "higher round", ah: 33, ar: 0, bh: 33, br: 1, expected: -1},
		{name: "lower round", ah: 33, ar: 1, bh: 33, br: 0, expected: 1},
	}

	for i, c := range cases {
		i := i
		c := c
		t.Run(
			c.name,
			func() {
				ap := NewPoint(Height(c.ah), Round(c.ar))
				bp := NewPoint(Height(c.bh), Round(c.br))

				r := ap.Compare(bp)
				t.Equal(c.expected, r, "%d: %v; %q, %q", i, c.name, ap, bp)
			},
		)
	}
}

func TestPoint(t *testing.T) {
	suite.Run(t, new(testPoint))
}

type testStagePoint struct {
	suite.Suite
}

func (t *testStagePoint) TestCompare() {
	cases := []struct {
		name     string
		ah       int64
		ar       uint64
		as       Stage
		bh       int64
		br       uint64
		bs       Stage
		expected int
	}{
		{name: "higher stage", ah: 33, ar: 0, as: StageINIT, bh: 33, br: 0, bs: StageACCEPT, expected: -1},
		{name: "lower stage", ah: 33, ar: 0, as: StageACCEPT, bh: 33, br: 0, bs: StageINIT, expected: 1},
		{name: "same stage", ah: 33, ar: 0, as: StageACCEPT, bh: 33, br: 0, bs: StageACCEPT, expected: 0},
		{name: "higher height", ah: 33, ar: 0, as: StageACCEPT, bh: 34, br: 0, bs: StageACCEPT, expected: -1},
		{name: "lower height", ah: 33, ar: 0, as: StageACCEPT, bh: 32, br: 0, bs: StageACCEPT, expected: 1},
		{name: "higher round", ah: 33, ar: 0, as: StageACCEPT, bh: 33, br: 1, bs: StageACCEPT, expected: -1},
		{name: "lower round", ah: 33, ar: 1, as: StageACCEPT, bh: 33, br: 0, bs: StageACCEPT, expected: 1},
		{name: "unknown stage", ah: 33, ar: 1, as: StageACCEPT, bh: 33, br: 0, bs: Stage("what?"), expected: 1},
	}

	for i, c := range cases {
		i := i
		c := c
		t.Run(
			c.name,
			func() {
				ap := NewStagePoint(NewPoint(Height(c.ah), Round(c.ar)), c.as)
				bp := NewStagePoint(NewPoint(Height(c.bh), Round(c.br)), c.bs)

				r := ap.Compare(bp)
				t.Equal(c.expected, r, "%d: %v; %q, %q", i, c.name, ap, bp)
			},
		)
	}
}

func TestStagePoint(t *testing.T) {
	suite.Run(t, new(testStagePoint))
}

type testPointEncode struct {
	*encoder.BaseTestEncode
}

func TestPointEncode(tt *testing.T) {
	t := new(testPointEncode)
	t.BaseTestEncode = new(encoder.BaseTestEncode)

	t.Encode = func() (interface{}, []byte) {
		p := NewPoint(Height(33), Round(44))

		b, err := util.MarshalJSON(p)
		t.NoError(err)

		return p, b
	}

	t.Decode = func(b []byte) interface{} {
		var u Point
		t.NoError(util.UnmarshalJSON(b, &u))

		return u
	}

	suite.Run(tt, t)
}

type testStagePointEncode struct {
	*encoder.BaseTestEncode
}

func TestStagePointEncode(tt *testing.T) {
	t := new(testStagePointEncode)
	t.BaseTestEncode = new(encoder.BaseTestEncode)

	t.Encode = func() (interface{}, []byte) {
		p := NewStagePoint(NewPoint(Height(33), Round(44)), StageINIT)

		b, err := util.MarshalJSON(p)
		t.NoError(err)

		return p, b
	}

	t.Decode = func(b []byte) interface{} {
		var u StagePoint
		t.NoError(util.UnmarshalJSON(b, &u))

		return u
	}

	suite.Run(tt, t)
}
