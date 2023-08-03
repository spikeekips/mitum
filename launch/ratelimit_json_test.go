package launch

import (
	"net"
	"testing"
	"time"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/network/quicstream"
	"github.com/spikeekips/mitum/util/encoder"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/stretchr/testify/suite"
	"golang.org/x/time/rate"
)

func TestRateLimiterRuleEncode(tt *testing.T) {
	t := new(suite.Suite)
	t.SetT(tt)

	cases := []struct {
		name   string
		a      string
		result RateLimiterRule
		err    string
	}{
		{name: "example", a: "33/3s", result: RateLimiterRule{Limit: rate.Every(time.Second * 3), Burst: 33}},
		{name: "nolimit", a: "nolimit", result: RateLimiterRule{Limit: rate.Inf, Burst: 0}},
		{name: "limit all", a: "0", result: RateLimiterRule{Limit: 0, Burst: 0}},
		{name: "empty", a: "  ", err: "empty"},
		{name: "missing burst", a: "/3s", err: "burst"},
		{name: "missing duration", a: "3/", err: "duration"},
		{name: "missing sep", a: "33s", err: "invalid format"},
	}

	for i, c := range cases {
		i := i
		c := c

		t.Run(c.name, func() {
			var r RateLimiterRule
			err := r.UnmarshalText([]byte(c.a))

			if len(c.err) > 0 {
				t.Error(err, "%d: %v", i, c.name)
				t.ErrorContains(err, c.err, "%d: %v", i, c.name)

				return
			}

			t.Equal(c.result, r, "%d: %v", i, c.name)
		})
	}
}

func TestNetRateLimiterRuleSetEncode(tt *testing.T) {
	t := new(encoder.BaseTestEncode)

	enc := jsonenc.NewEncoder()

	newipnet := func() *net.IPNet {
		addr := quicstream.RandomUDPAddr()

		ipnet := &net.IPNet{IP: addr.IP, Mask: net.CIDRMask(24, 32)}
		_, i, _ := net.ParseCIDR(ipnet.String())

		return i
	}

	t.Encode = func() (interface{}, []byte) {
		rs := NewNetRateLimiterRuleSet()
		rs.
			Add(
				newipnet(),
				map[string]RateLimiterRule{
					"a": {Limit: rate.Every(time.Second * 33), Burst: 44},
					"b": {Limit: rate.Inf, Burst: 0},
					"c": {Limit: 0, Burst: 0},
				},
			).
			Add(
				newipnet(),
				map[string]RateLimiterRule{
					"d": {Limit: rate.Every(time.Second * 55), Burst: 66},
					"e": {Limit: rate.Inf, Burst: 0},
					"f": {Limit: 0, Burst: 0},
				},
			)

		b, err := enc.Marshal(rs)
		t.NoError(err)

		t.T().Log("marshaled:", string(b))

		return rs, b
	}

	t.Decode = func(b []byte) interface{} {
		var rs NetRateLimiterRuleSet
		t.NoError(enc.Unmarshal(b, &rs))

		return rs
	}

	t.Compare = func(a interface{}, b interface{}) {
		ars, ok := a.(NetRateLimiterRuleSet)
		t.True(ok)
		brs, ok := b.(NetRateLimiterRuleSet)
		t.True(ok)

		t.Equal(len(ars.ipnets), len(brs.ipnets))
		for i := range ars.ipnets {
			t.Equal(ars.ipnets[i].String(), brs.ipnets[i].String())
		}

		t.Equal(len(ars.rules), len(brs.rules))
		for i := range ars.rules {
			am := ars.rules[i]
			bm := brs.rules[i]

			t.Equal(len(am), len(bm))

			for j := range am {
				t.Equal(am[j], bm[j])
			}
		}
	}

	suite.Run(tt, t)
}

func TestNodeRateLimiterRuleSetEncode(tt *testing.T) {
	t := new(encoder.BaseTestEncode)
	t.SetT(tt)

	enc := jsonenc.NewEncoder()

	t.Encode = func() (interface{}, []byte) {
		rs := NewNodeRateLimiterRuleSet(
			map[string]map[string]RateLimiterRule{
				base.RandomAddress("").String(): {
					"a": {Limit: rate.Every(time.Second * 33), Burst: 44},
					"b": {Limit: rate.Inf, Burst: 0},
					"c": {Limit: 0, Burst: 0},
				},
				base.RandomAddress("").String(): {
					"d": {Limit: rate.Every(time.Second * 55), Burst: 66},
					"e": {Limit: rate.Inf, Burst: 0},
					"f": {Limit: 0, Burst: 0},
				},
			},
		)

		b, err := enc.Marshal(rs)
		t.NoError(err)

		t.T().Log("marshaled:", string(b))

		return rs, b
	}

	t.Decode = func(b []byte) interface{} {
		var rs NodeRateLimiterRuleSet
		t.NoError(enc.Unmarshal(b, &rs))

		return rs
	}

	t.Compare = func(a interface{}, b interface{}) {
		ars, ok := a.(NodeRateLimiterRuleSet)
		t.True(ok)
		brs, ok := b.(NodeRateLimiterRuleSet)
		t.True(ok)

		t.Equal(len(ars.rules), len(brs.rules))
		for i := range ars.rules {
			am := ars.rules[i]
			bm := brs.rules[i]

			t.Equal(len(am), len(bm))

			for j := range am {
				t.Equal(am[j], bm[j])
			}
		}
	}

	suite.Run(tt, t)
}

func TestSuffrageRateLimiterRuleSetEncode(tt *testing.T) {
	t := new(encoder.BaseTestEncode)
	t.SetT(tt)

	enc := jsonenc.NewEncoder()

	t.Encode = func() (interface{}, []byte) {
		rs := NewSuffrageRateLimiterRuleSet(
			map[string]RateLimiterRule{
				"a": {Limit: rate.Every(time.Second * 33), Burst: 44},
				"b": {Limit: rate.Inf, Burst: 0},
				"c": {Limit: 0, Burst: 0},
				"d": {Limit: rate.Every(time.Second * 55), Burst: 66},
				"e": {Limit: rate.Inf, Burst: 0},
				"f": {Limit: 0, Burst: 0},
			},
			RateLimiterRule{Limit: rate.Every(time.Second * 33), Burst: 44},
		)

		b, err := enc.Marshal(rs)
		t.NoError(err)

		t.T().Log("marshaled:", string(b))

		return rs, b
	}

	t.Decode = func(b []byte) interface{} {
		var rs *SuffrageRateLimiterRuleSet
		t.NoError(enc.Unmarshal(b, &rs))

		return rs
	}

	t.Compare = func(a interface{}, b interface{}) {
		ars, ok := a.(*SuffrageRateLimiterRuleSet)
		t.True(ok, "%T", a)
		brs, ok := b.(*SuffrageRateLimiterRuleSet)
		t.True(ok, "%T", b)

		t.Equal(len(ars.rules), len(brs.rules))
		for i := range ars.rules {
			t.Equal(ars.rules[i], brs.rules[i])
		}
	}

	suite.Run(tt, t)
}
