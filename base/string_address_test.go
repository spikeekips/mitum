package base

import (
	"strings"
	"testing"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/stretchr/testify/suite"
)

type testStringAddress struct {
	suite.Suite
}

func (t *testStringAddress) TestNew() {
	ad := NewStringAddress("abc")
	err := ad.IsValid(nil)
	t.NoError(err)

	_ = (interface{})(ad).(Address)
	t.Implements((*Address)(nil), ad)
}

func (t *testStringAddress) TestEmpty() {
	{ // empty
		ad := NewStringAddress("")
		err := ad.IsValid(nil)
		t.Error(err)
		t.True(errors.Is(err, util.InvalidError))
		t.Contains(err.Error(), "too short")
	}

	{ // short
		ad := NewStringAddress(strings.Repeat("a", MinAddressSize-AddressTypeSize-1))
		err := ad.IsValid(nil)
		t.Error(err)
		t.True(errors.Is(err, util.InvalidError))
		t.Contains(err.Error(), "too short")
	}

	{ // long
		ad := NewStringAddress(strings.Repeat("a", MaxAddressSize) + "a")
		err := ad.IsValid(nil)
		t.Error(err)
		t.True(errors.Is(err, util.InvalidError))
		t.Contains(err.Error(), "too long")
	}
}

func (t *testStringAddress) TestStringWithHint() {
	ht := hint.MustNewHint("xyz-v1.2.3")
	ad := NewStringAddressWithHint(ht, "abc")
	err := ad.IsValid(nil)
	t.NoError(err)

	t.True(ht.Equal(ad.Hint()))
	t.True(strings.HasSuffix(ad.String(), ht.Type().String()))
}

func (t *testStringAddress) TestStringWithType() {
	ad := NewStringAddress("abc")
	err := ad.IsValid(nil)
	t.NoError(err)

	t.True(strings.HasSuffix(ad.String(), StringAddressHint.Type().String()))
}

func (t *testStringAddress) TestWrongHint() {
	ad := NewStringAddress("abc")
	ad.BaseHinter = ad.SetHint(hint.MustNewHint("aaa-v0.0.1")).(hint.BaseHinter)
	err := ad.IsValid(nil)
	t.NotNil(err)
	t.True(errors.Is(err, util.InvalidError))
	t.Contains(err.Error(), "wrong type of string address")
}

func (t *testStringAddress) TestParse() {
	t.Run("valid", func() {
		ad := NewStringAddress("abc")
		t.NoError(ad.IsValid(nil))

		uad, err := ParseStringAddress(ad.String())
		t.NoError(err)

		t.True(ad.Equal(uad))
	})

	t.Run("wrong type", func() {
		ad := NewStringAddress("abc")
		t.NoError(ad.IsValid(nil))

		_, err := ParseStringAddress(ad.s[:len(ad.s)-AddressTypeSize] + "000")
		t.NotNil(err)
		t.True(errors.Is(err, util.InvalidError))
		t.Contains(err.Error(), "wrong hint type in string address")
	})
}

type wrongHintedAddress struct {
	Address
	ht hint.Hint
}

func (k wrongHintedAddress) Hint() hint.Hint {
	return k.ht
}

func (t *testStringAddress) TestEqual() {
	a := NewStringAddress("abc")
	b := NewStringAddress("abc")
	t.True(a.Equal(b))

	t.False(a.Equal(wrongHintedAddress{Address: a, ht: hint.MustNewHint("wrong-v0.0.1")}))
	t.True(a.Equal(wrongHintedAddress{Address: a, ht: hint.MustNewHint(StringAddressHint.Type().String() + "-v0.0.1")}))
}

func (t *testStringAddress) TestFormat() {
	uuidString := util.UUID().String()

	cases := []struct {
		name     string
		s        string
		expected string
		err      string
	}{
		{name: "uuid", s: uuidString, expected: uuidString + StringAddressHint.Type().String()},
		{name: "blank first", s: " showme", err: "has blank"},
		{name: "blank inside", s: "sh owme", err: "has blank"},
		{name: "blank inside #1", s: "sh\towme", err: "has blank"},
		{name: "blank ends", s: "showme ", err: "has blank"},
		{name: "blank ends, tab", s: "showme\t", err: "has blank"},
		{name: "has underscore", s: "showm_e", expected: "showm_e" + StringAddressHint.Type().String()},
		{name: "has plus sign", s: "showm+e", err: "invalid string address string"},
		{name: "has at sign", s: "showm@e", expected: "showm@e" + StringAddressHint.Type().String()},
		{name: "has dot", s: "showm.e", expected: "showm.e" + StringAddressHint.Type().String()},
		{name: "has dot #1", s: "showme.", err: "invalid string address string"},
	}

	for i, c := range cases {
		i := i
		c := c
		t.Run(
			c.name,
			func() {
				r := NewStringAddress(c.s)
				err := r.IsValid(nil)
				if err != nil {
					if len(c.err) < 1 {
						t.NoError(err, "%d: %v", i, c.name)
					} else {
						t.Contains(err.Error(), c.err, "%d: %v; %v != %v", i, c.name, c.err, err)
					}
				} else if len(c.err) > 0 {
					t.NoError(errors.Errorf(c.err), "%d: %v", i, c.name)
				} else {
					t.Equal(c.expected, r.String(), "%d: %v; %v != %v", i, c.name, c.expected, r.String())
				}
			},
		)
	}
}

func TestStringAddress(t *testing.T) {
	suite.Run(t, new(testStringAddress))
}

type baseTestStringAddressEncode struct {
	suite.Suite
	enc     encoder.Encoder
	encode  func() (Address, []byte)
	decode  func([]byte) (Address, error)
	compare func(Address, Address)
}

func (t *baseTestStringAddressEncode) TestDecode() {
	ad, b := t.encode()

	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: ad.Hint(), Instance: ad}))

	uad, err := t.decode(b)
	if err != nil {
		return
	}

	if t.compare != nil {
		t.compare(ad, uad)

		return
	}

	t.True(ad.Hint().Equal(uad.Hint()))
	t.True(ad.Equal(uad))
}

func TestStringAddressDecodeAddressFromString(t *testing.T) {
	s := new(baseTestStringAddressEncode)
	s.enc = jsonenc.NewEncoder()
	s.encode = func() (Address, []byte) {
		ad := NewStringAddress(util.UUID().String())

		return ad, []byte(ad.String())
	}
	s.decode = func(b []byte) (Address, error) {
		i, err := DecodeAddressFromString(string(b), s.enc)
		s.NoError(err)

		uad, ok := i.(StringAddress)
		s.True(ok)

		return uad, nil
	}

	suite.Run(t, s)
}

func TestStringAddressJSONDecoder(t *testing.T) {
	s := new(baseTestStringAddressEncode)
	s.enc = jsonenc.NewEncoder()
	s.encode = func() (Address, []byte) {
		ad := NewStringAddress(util.UUID().String())
		b, err := s.enc.Marshal(struct {
			A StringAddress
		}{A: ad})
		s.NoError(err)

		return ad, b
	}
	s.decode = func(b []byte) (Address, error) {
		var u struct {
			A AddressDecoder
		}
		s.NoError(s.enc.Unmarshal(b, &u))

		i, err := u.A.Decode(s.enc)
		s.NoError(err)

		uad, ok := i.(StringAddress)
		s.True(ok)

		return uad, nil
	}

	suite.Run(t, s)
}

func TestStringAddressDecodeAddressFromStringHinted(t *testing.T) {
	ht := hint.MustNewHint("abc-v0.0.1")

	s := new(baseTestStringAddressEncode)
	s.enc = jsonenc.NewEncoder()
	s.encode = func() (Address, []byte) {
		ad := NewStringAddressWithHint(ht, util.UUID().String())

		return ad, []byte(ad.String())
	}
	s.decode = func(b []byte) (Address, error) {
		i, err := DecodeAddressFromString(string(b), s.enc)
		s.NoError(err)

		uad, ok := i.(StringAddress)
		s.True(ok)

		return uad, nil
	}

	suite.Run(t, s)
}

func TestStringAddressJSONHinted(t *testing.T) {
	ht := hint.MustNewHint("abc-v0.0.1")

	s := new(baseTestStringAddressEncode)
	s.enc = jsonenc.NewEncoder()
	s.encode = func() (Address, []byte) {
		ad := NewStringAddressWithHint(ht, util.UUID().String())
		b, err := s.enc.Marshal(struct {
			A StringAddress
		}{A: ad})
		s.NoError(err)

		return ad, b
	}
	s.decode = func(b []byte) (Address, error) {
		var u struct {
			A AddressDecoder
		}
		s.NoError(s.enc.Unmarshal(b, &u))

		i, err := u.A.Decode(s.enc)
		s.NoError(err)

		uad, ok := i.(StringAddress)
		s.True(ok)

		return uad, nil
	}

	suite.Run(t, s)
}

func TestNilStringAddressJSONHinted(t *testing.T) {
	s := new(baseTestStringAddressEncode)
	s.enc = jsonenc.NewEncoder()
	s.encode = func() (Address, []byte) {
		return NewStringAddress(util.UUID().String()), nil
	}
	s.decode = func(b []byte) (Address, error) {
		var u struct {
			A AddressDecoder
		}
		s.NoError(s.enc.Unmarshal(b, &u))

		return u.A.Decode(s.enc)
	}
	s.compare = func(a, b Address) {
		s.NotNil(a)
		s.Nil(b)
	}

	suite.Run(t, s)
}
