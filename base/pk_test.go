package base

import (
	"testing"

	"github.com/spikeekips/mitum/util/encoder"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/stretchr/testify/suite"
)

func TestDecodePrivatekeyFromString(tt *testing.T) {
	t := new(suite.Suite)
	t.SetT(tt)

	cases := []struct {
		name     string
		s        string
		expected string
		err      string
	}{
		{name: "ok", s: "58d2671582e7866ab98bc0024c4c474b81b2f0846f06c0696ebd50a9dd3127e0mpr", expected: "58d2671582e7866ab98bc0024c4c474b81b2f0846f06c0696ebd50a9dd3127e0mpr"},
		{name: "unknown hint", s: "58d2671582e7866ab98bc0024c4c474b81b2f0846f06c0696ebd50a9dd3127e0unk", err: "find decoder by type"},
		{name: "publickey", s: "0298d7f011bcf398780caca0ea975744bd74fc0c59eca14ac5910ede19d398e3eempu", err: "expected base.Privatekey"},
		{name: "String() not match", s: "0b4d4c59b46e472b860be9a7d65d112cmpr", err: "unknown key format"},
	}

	enc := jsonenc.NewEncoder()

	t.NoError(enc.Add(encoder.DecodeDetail{Hint: MPublickeyHint, Instance: &MPublickey{}}))
	t.NoError(enc.Add(encoder.DecodeDetail{Hint: MPrivatekeyHint, Instance: &MPrivatekey{}}))

	for i, c := range cases {
		i := i
		c := c

		t.Run(c.name, func() {
			u, err := DecodePrivatekeyFromString(c.s, enc)
			if len(c.err) > 0 {
				t.ErrorContains(err, c.err, "%d: %v", i, c.name)

				return
			}

			if err != nil {
				t.NoError(err, "%d: %v", i, c.name)

				return
			}

			t.Equal(c.expected, u.String(), "%d: %v", i, c.name)
		})
	}
}

func TestDecodePublickeyFromString(tt *testing.T) {
	t := new(suite.Suite)
	t.SetT(tt)

	cases := []struct {
		name     string
		s        string
		expected string
		err      string
	}{
		{name: "ok", s: "0298d7f011bcf398780caca0ea975744bd74fc0c59eca14ac5910ede19d398e3eempu", expected: "0298d7f011bcf398780caca0ea975744bd74fc0c59eca14ac5910ede19d398e3eempu"},
		{name: "unknown hint", s: "0298d7f011bcf398780caca0ea975744bd74fc0c59eca14ac5910ede19d398e3eeunk", err: "find decoder by type"},
		{name: "privatekey", s: "58d2671582e7866ab98bc0024c4c474b81b2f0846f06c0696ebd50a9dd3127e0mpr", err: "expected base.Publickey"},
		{name: "String() not match", s: "0b4d4c59b46e472b860be9a7d65d112cmpu", err: "malformed"},
	}

	enc := jsonenc.NewEncoder()

	t.NoError(enc.Add(encoder.DecodeDetail{Hint: MPublickeyHint, Instance: &MPublickey{}}))
	t.NoError(enc.Add(encoder.DecodeDetail{Hint: MPrivatekeyHint, Instance: &MPrivatekey{}}))

	for i, c := range cases {
		i := i
		c := c

		t.Run(c.name, func() {
			u, err := DecodePublickeyFromString(c.s, enc)
			if len(c.err) > 0 {
				t.ErrorContains(err, c.err, "%d: %v", i, c.name)

				return
			}

			if err != nil {
				t.NoError(err, "%d: %v", i, c.name)

				return
			}

			t.Equal(c.expected, u.String(), "%d: %v", i, c.name)
		})
	}
}
