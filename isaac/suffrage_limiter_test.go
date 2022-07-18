package isaac

import (
	"testing"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

func TestFixedSuffrageCandidateLimiterRuleJSON(tt *testing.T) {
	t := new(encoder.BaseTestEncode)

	enc := jsonenc.NewEncoder()

	t.Encode = func() (interface{}, []byte) {
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: FixedSuffrageCandidateLimiterRuleHint, Instance: FixedSuffrageCandidateLimiterRule{}}))

		l := NewFixedSuffrageCandidateLimiterRule(33)

		b, err := util.MarshalJSON(&l)
		t.NoError(err)

		t.T().Log("marshaled:", string(b))

		return l, b
	}

	t.Decode = func(b []byte) interface{} {
		i, err := enc.Decode(b)
		t.NoError(err)

		_ = i.(base.SuffrageCandidateLimiterRule)

		u, ok := i.(FixedSuffrageCandidateLimiterRule)
		t.True(ok)

		return u
	}
	t.Compare = func(a, b interface{}) {
		av := a.(FixedSuffrageCandidateLimiterRule)
		bv := b.(FixedSuffrageCandidateLimiterRule)

		t.True(av.Hint().Equal(bv.Hint()))
		t.Equal(av.Limit(), bv.Limit())
	}

	suite.Run(tt, t)
}

func TestMajoritySuffrageCandidateLimiterRuleJSON(tt *testing.T) {
	t := new(encoder.BaseTestEncode)

	enc := jsonenc.NewEncoder()

	t.Encode = func() (interface{}, []byte) {
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: MajoritySuffrageCandidateLimiterRuleHint, Instance: MajoritySuffrageCandidateLimiterRule{}}))

		l := NewMajoritySuffrageCandidateLimiterRule(0.5, 3, 33)

		b, err := util.MarshalJSON(&l)
		t.NoError(err)

		t.T().Log("marshaled:", string(b))

		return l, b
	}

	t.Decode = func(b []byte) interface{} {
		i, err := enc.Decode(b)
		t.NoError(err)

		_ = i.(base.SuffrageCandidateLimiterRule)

		u, ok := i.(MajoritySuffrageCandidateLimiterRule)
		t.True(ok)

		return u
	}
	t.Compare = func(a, b interface{}) {
		av := a.(MajoritySuffrageCandidateLimiterRule)
		bv := b.(MajoritySuffrageCandidateLimiterRule)

		t.True(av.Hint().Equal(bv.Hint()))
		t.Equal(av.Ratio(), bv.Ratio())
		t.Equal(av.Min(), bv.Min())
		t.Equal(av.Max(), bv.Max())
	}

	suite.Run(tt, t)
}

func TestNewCandidatesOfMajoritySuffrageCandidateLimiterRule(t *testing.T) {
	for i := uint64(0); i < 1<<14; i++ {
		if i%1000 == 0 {
			t.Log(">", i)
		}

		n, err := NewCandidatesOfMajoritySuffrageCandidateLimiterRule(0.5, func() (uint64, error) { return i, nil })
		assert.NoError(t, err, "%d: %v", i, err)
		assert.True(t, n > 0, "%d: %d", i, n)
	}
}

func TestMajoritySuffrageCandidateLimiter(tt *testing.T) {
	t := new(suite.Suite)
	t.SetT(tt)

	t.Run("min", func() {
		rule := NewMajoritySuffrageCandidateLimiterRule(0, 3, 4)

		f := NewMajoritySuffrageCandidateLimiter(rule, func() (uint64, error) { return 46, nil })
		i, err := f()
		t.NoError(err)
		t.Equal(uint64(3), i)
	})

	t.Run("no min", func() {
		rule := NewMajoritySuffrageCandidateLimiterRule(0, 0, 4)

		f := NewMajoritySuffrageCandidateLimiter(rule, func() (uint64, error) { return 46, nil })
		i, err := f()
		t.NoError(err)
		t.Equal(uint64(0), i)
	})

	t.Run("max", func() {
		rule := NewMajoritySuffrageCandidateLimiterRule(1, 3, 4)

		f := NewMajoritySuffrageCandidateLimiter(rule, func() (uint64, error) { return 46, nil })
		i, err := f()
		t.NoError(err)
		t.Equal(uint64(4), i)
	})

	t.Run("no max", func() {
		rule := NewMajoritySuffrageCandidateLimiterRule(1, 3, 0)

		f := NewMajoritySuffrageCandidateLimiter(rule, func() (uint64, error) { return 46, nil })
		i, err := f()
		t.NoError(err)
		t.Equal(uint64(46), i)
	})
}

func TestNewCandidatesOfMajoritySuffrageCandidateLimiterRuleInvalidRatio(tt *testing.T) {
	t := new(suite.Suite)
	t.SetT(tt)

	t.Run("1", func() {
		a, err := NewCandidatesOfMajoritySuffrageCandidateLimiterRule(0.2, func() (uint64, error) { return 1, nil })
		t.NoError(err)
		t.True(a > 0)
	})

	t.Run("under zero", func() {
		_, err := NewCandidatesOfMajoritySuffrageCandidateLimiterRule(-0.2, func() (uint64, error) { return 1, nil })
		t.Error(err)
		t.ErrorContains(err, "invalid ratio")
	})

	t.Run("over one", func() {
		_, err := NewCandidatesOfMajoritySuffrageCandidateLimiterRule(1.1, func() (uint64, error) { return 1, nil })
		t.Error(err)
		t.ErrorContains(err, "invalid ratio")
	})
}

func TestNewCandidatesOfMajoritySuffrageCandidateLimiterRuleCases(tt *testing.T) {
	t := new(suite.Suite)
	t.SetT(tt)

	cases := []struct {
		name     string
		s        uint64
		ratio    float64
		expected uint64
	}{
		{name: "0: under 3", s: 0, ratio: 0.2, expected: 1},
		{name: "1: under 3", s: 1, ratio: 0.2, expected: 1},
		{name: "2: under 3", s: 2, ratio: 0.2, expected: 1},
		{name: "3: under 3", s: 3, ratio: 0.2, expected: 1},
		{name: "4: over 3", s: 4, ratio: 0.2, expected: 1},
		{name: "4: f < 2", s: 4, ratio: 0.2, expected: 1},
		{name: "5: f < 2", s: 5, ratio: 0.2, expected: 2},
		{name: "6: f < 2", s: 6, ratio: 0.2, expected: 2},
		{name: "7: f == 2", s: 7, ratio: 0.2, expected: 1},
		{name: "45", s: 45, ratio: 0.2, expected: 10},
		{name: "0", s: 0, ratio: 0.2, expected: 1},
		{name: "45 zero ratio", s: 45, ratio: 0, expected: 1},
		{name: "46 zero ratio", s: 46, ratio: 0, expected: 0},
		{name: "11 one ratio", s: 11, ratio: 1, expected: 12},
		{name: "40 one ratio", s: 40, ratio: 1, expected: 40},
	}

	for i, c := range cases {
		t.Run(c.name, func() {
			a, err := NewCandidatesOfMajoritySuffrageCandidateLimiterRule(c.ratio, func() (uint64, error) { return c.s, nil })
			t.NoError(err)

			t.Equal(c.expected, a, "%d: %v; expected=%d new=%d", i, c.name, c.expected, a)
		})
	}
}
