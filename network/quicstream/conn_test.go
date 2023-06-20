package quicstream

import (
	"testing"

	"github.com/spikeekips/mitum/network"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/stretchr/testify/suite"
)

func TestBaseConnInfoEncode(t *testing.T) {
	tt := new(encoder.BaseTestEncode)

	tt.Encode = func() (interface{}, []byte) {
		ci, err := NewConnInfoFromFullString("1.2.3.4:4321#tls_insecure")
		tt.NoError(err)

		b, err := util.MarshalJSON(ci)
		tt.NoError(err)

		tt.T().Log("marshaled:", string(b))

		return ci, b
	}
	tt.Decode = func(b []byte) interface{} {
		var u ConnInfo
		tt.NoError(util.UnmarshalJSON(b, &u))

		return u
	}
	tt.Compare = func(a interface{}, b interface{}) {
		ap := a.(ConnInfo)
		bp := b.(ConnInfo)

		tt.Equal(ap.String(), bp.String())
	}

	suite.Run(t, tt)
}

func TestEqualConnInfo(tt *testing.T) {
	t := new(suite.Suite)
	t.SetT(tt)

	cases := []struct {
		name   string
		a      string
		b      string
		result bool
	}{
		{name: "simple", a: "localhost:1", b: "localhost:1", result: true},
		{name: "named vs resolved", a: "localhost:1", b: "127.0.0.1:1", result: true},
		{name: "same tls_insecure", a: "localhost:1#tls_insecure", b: "localhost:1#tls_insecure", result: true},
		{name: "a empty tls_insecure", a: "localhost:1", b: "localhost:1#tls_insecure", result: true},
		{name: "b empty tls_insecure", a: "localhost:1#tls_insecure", b: "localhost:1", result: true},
	}

	for i, c := range cases {
		i := i
		c := c
		t.Run(c.name, func() {
			a, err := NewConnInfoFromFullString(c.a)
			t.NoError(err)
			b, err := NewConnInfoFromFullString(c.b)
			t.NoError(err)

			t.Equal(c.result, network.EqualConnInfo(a, b), "%d: %v", i, c.name)
		})
	}
}
