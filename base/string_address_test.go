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
		t.ErrorContains(err, "too short")
	}

	{ // short
		ad := NewStringAddress(strings.Repeat("a", MinAddressSize-AddressTypeSize-1))
		err := ad.IsValid(nil)
		t.Error(err)
		t.True(errors.Is(err, util.InvalidError))
		t.ErrorContains(err, "too short")
	}

	{ // long
		ad := NewStringAddress(strings.Repeat("a", MaxAddressSize) + "a")
		err := ad.IsValid(nil)
		t.Error(err)
		t.True(errors.Is(err, util.InvalidError))
		t.ErrorContains(err, "too long")
	}
}

func (t *testStringAddress) TestStringWithHint() {
	ht := hint.MustNewHint("xyz-v1.2.3")
	ad := NewBaseStringAddressWithHint(ht, "abc")
	err := ad.IsValid(ad.Hint().Type().Bytes())
	t.NoError(err)

	t.True(ht.Equal(ad.Hint()))
	t.True(strings.HasSuffix(ad.String(), ht.Type().String()))
}

func (t *testStringAddress) TestStringWithType() {
	ad := NewStringAddress("abc")
	err := ad.IsValid(ad.Hint().Type().Bytes())
	t.NoError(err)

	t.True(strings.HasSuffix(ad.String(), StringAddressHint.Type().String()))
}

func (t *testStringAddress) TestWrongHint() {
	ad := NewStringAddress("abc")
	ad.BaseHinter = ad.SetHint(hint.MustNewHint("aaa-v0.0.1")).(hint.BaseHinter)
	err := ad.IsValid(ad.Hint().Type().Bytes())
	t.NotNil(err)
	t.True(errors.Is(err, util.InvalidError))
	t.ErrorContains(err, "wrong hint in StringAddress")
}

func (t *testStringAddress) TestParse() {
	t.Run("valid", func() {
		ad := NewStringAddress("abc")
		t.NoError(ad.IsValid(ad.Hint().Type().Bytes()))

		uad, err := ParseStringAddress(ad.String())
		t.NoError(err)

		t.True(ad.Equal(uad))
	})

	t.Run("wrong type", func() {
		ad := NewStringAddress("abc")
		t.NoError(ad.IsValid(ad.Hint().Type().Bytes()))

		_, err := ParseStringAddress(ad.s[:len(ad.s)-AddressTypeSize] + "000")
		t.NotNil(err)
		t.True(errors.Is(err, util.InvalidError))
		t.ErrorContains(err, "wrong hint type in StringAddress")
	})
}

func (t *testStringAddress) TestEqual() {
	a := NewStringAddress("abc")
	b := NewStringAddress("abc")
	t.True(a.Equal(b))

	other := NewBaseStringAddressWithHint(hint.MustNewHint("wrong-v0.0.1"), "abc")
	t.False(a.Equal(other))

	other = NewBaseStringAddressWithHint(hint.MustNewHint(StringAddressHint.Type().String()+"-v0.0.1"), "abc")
	t.True(a.Equal(other))
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
				err := r.IsValid(r.Hint().Type().Bytes())
				if err != nil {
					if len(c.err) < 1 {
						t.NoError(err, "%d: %v", i, c.name)
					} else {
						t.ErrorContains(err, c.err, "%d: %v; %v != %v", i, c.name, c.err, err)
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
	encoder.BaseTestEncode
	enc     encoder.Encoder
	compare func(Address, Address)
}

func (t *baseTestStringAddressEncode) SetupTest() {
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: StringAddressHint, Instance: StringAddress{}}))
}

func (t *baseTestStringAddressEncode) Compare(a, b interface{}) {
	ad := a.(Address)
	uad := b.(Address)

	if t.compare != nil {
		t.compare(ad, uad)

		return
	}

	aht := ad.(hint.Hinter).Hint()
	uht := uad.(hint.Hinter).Hint()
	t.True(aht.Equal(uht), "Hint does not match")

	t.True(ad.Equal(uad))
}

func TestStringAddressDecodeAddressFromString(tt *testing.T) {
	t := new(baseTestStringAddressEncode)

	t.enc = jsonenc.NewEncoder()
	t.Encode = func() (interface{}, []byte) {
		ad := NewStringAddress(util.UUID().String())

		b, err := t.enc.Marshal(ad)
		t.NoError(err)

		return ad, b
	}
	t.Decode = func(b []byte) interface{} {
		var s string
		t.NoError(t.enc.Unmarshal(b, &s))

		i, err := DecodeAddress(s, t.enc)
		t.NoError(err)

		uad, ok := i.(StringAddress)
		t.True(ok)

		return uad
	}

	suite.Run(tt, t)
}

func TestStringAddressJSON(tt *testing.T) {
	t := new(baseTestStringAddressEncode)
	t.enc = jsonenc.NewEncoder()
	t.Encode = func() (interface{}, []byte) {
		ad := NewStringAddress(util.UUID().String())
		b, err := t.enc.Marshal(struct {
			A StringAddress
		}{A: ad})
		t.NoError(err)

		return ad, b
	}
	t.Decode = func(b []byte) interface{} {
		var u struct {
			A string
		}
		t.NoError(t.enc.Unmarshal(b, &u))

		i, err := DecodeAddress(u.A, t.enc)
		t.NoError(err)

		uad, ok := i.(StringAddress)
		t.True(ok)

		return uad
	}

	suite.Run(tt, t)
}

func TestNilStringAddressJSONHinted(tt *testing.T) {
	t := new(baseTestStringAddressEncode)
	t.enc = jsonenc.NewEncoder()
	t.Encode = func() (interface{}, []byte) {
		return NewStringAddress(util.UUID().String()), nil
	}
	t.Decode = func(b []byte) interface{} {
		var u struct {
			A string
		}
		t.NoError(t.enc.Unmarshal(b, &u))

		i, err := DecodeAddress(u.A, t.enc)
		t.NoError(err)

		return i
	}
	t.compare = func(a, b Address) {
		t.NotNil(a)
		t.Nil(b)
	}

	suite.Run(tt, t)
}
