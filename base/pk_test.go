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
		{name: "ok", s: "4L7bTPQHphG6TP6VPAnTUgS9w31eSqxFFHkUVW5saS84mpr", expected: "4L7bTPQHphG6TP6VPAnTUgS9w31eSqxFFHkUVW5saS84mpr"},
		{name: "unknown hint", s: "4L7bTPQHphG6TP6VPAnTUgS9w31eSqxFFHkUVW5saS84unk", err: "find decoder by type"},
		{name: "publickey", s: "hCCF87ntRpCwwY1uAwb4AMZdf1JYXRD4VeT8WpchE4dJmpu", err: "not Privatekey"},
		{name: "String() not match", s: "4L7bTPQHphG6TP6VPAnTUgS9w31eSqxFFHkUVW5saS84YYYYYYYYYYYYmpr", err: "unknown key format"},
	}

	enc := jsonenc.NewEncoder()

	t.NoError(enc.Add(encoder.DecodeDetail{Hint: MPublickeyHint, Instance: MPublickey{}}))
	t.NoError(enc.Add(encoder.DecodeDetail{Hint: MPrivatekeyHint, Instance: MPrivatekey{}}))

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
		{name: "ok", s: "hCCF87ntRpCwwY1uAwb4AMZdf1JYXRD4VeT8WpchE4dJmpu", expected: "hCCF87ntRpCwwY1uAwb4AMZdf1JYXRD4VeT8WpchE4dJmpu"},
		{name: "unknown hint", s: "4L7bTPQHphG6TP6VPAnTUgS9w31eSqxFFHkUVW5saS84unk", err: "find decoder by type"},
		{name: "privatekey", s: "4L7bTPQHphG6TP6VPAnTUgS9w31eSqxFFHkUVW5saS84mpr", err: "not Publickey"},
		{name: "String() not match", s: "hCCF87ntRpCwwY1uAwb4AMZdf1JYXRD4VeT8WpchE4dJYYYYYmpu", err: "malformed"},
	}

	enc := jsonenc.NewEncoder()

	t.NoError(enc.Add(encoder.DecodeDetail{Hint: MPublickeyHint, Instance: MPublickey{}}))
	t.NoError(enc.Add(encoder.DecodeDetail{Hint: MPrivatekeyHint, Instance: MPrivatekey{}}))

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
