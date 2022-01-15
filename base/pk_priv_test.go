package base

import (
	"errors"
	"testing"

	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/stretchr/testify/suite"
)

type testMPrivatekey struct {
	suite.Suite
}

func (t *testMPrivatekey) TestNew() {
	priv := NewMPrivatekey()

	t.NoError(priv.IsValid(nil))

	_ = (interface{})(priv).(Privatekey)
	t.Implements((*Privatekey)(nil), priv)
}

func (t *testMPrivatekey) TestFromSeedStatic() {
	seed := "L1bQZCcDZKy342x8xjK9Hk935Nttm2jkApVVS2mn4Nqyxvu7nyGC"
	priv, err := NewMPrivatekeyFromSeed(seed)
	t.NoError(err)

	t.Equal("KzBYiN3Qr1JuYNf7Eyc67PAC5bzBazopzwAQDVZj4jmya7sWTbCDmpr", priv.String())
	t.Equal("oxkQTcfKzrC67GE8ChZmZw8SBBBYefMp5859R2AZ8bB9mpu", priv.Publickey().String())
}

func (t *testMPrivatekey) TestConflicts() {
	created := map[string]struct{}{}

	for i := 0; i < 400; i++ {
		if i%200 == 0 {
			t.T().Log("generated:", i)
		}

		priv := NewMPrivatekey()
		upriv, err := ParseMPrivatekey(priv.String())
		t.NoError(err)
		t.True(priv.Equal(upriv))

		upub, err := ParseMPublickey(priv.Publickey().String())
		t.NoError(err)
		t.True(priv.Publickey().Equal(upub))

		_, found := created[priv.String()]
		t.False(found)

		if found {
			break
		}

		created[priv.String()] = struct{}{}
	}
}

func (t *testMPrivatekey) TestConflictsSeed() {
	created := map[string]struct{}{}

	for i := 0; i < 400; i++ {
		if i%200 == 0 {
			t.T().Log("generated:", i)
		}

		priv, err := NewMPrivatekeyFromSeed(util.UUID().String())
		t.NoError(err)

		upriv, err := ParseMPrivatekey(priv.String())
		t.NoError(err)
		t.True(priv.Equal(upriv))

		upub, err := ParseMPublickey(priv.Publickey().String())
		t.NoError(err)
		t.True(priv.Publickey().Equal(upub))

		_, found := created[priv.String()]
		t.False(found)

		if found {
			break
		}

		created[priv.String()] = struct{}{}
	}
}

func (t *testMPrivatekey) TestFromSeedButTooShort() {
	seed := util.UUID().String()[:MinSeedSize-1]

	_, err := NewMPrivatekeyFromSeed(seed)
	t.True(errors.Is(err, util.InvalidError))
	t.Contains(err.Error(), "too short")
}

func (t *testMPrivatekey) TestParseMPrivatekey() {
	priv := NewMPrivatekey()
	parsed, err := ParseMPrivatekey(priv.String())
	t.NoError(err)

	t.True(priv.Equal(parsed))
}

func (t *testMPrivatekey) TestParseMPrivatekeyButEmpty() {
	_, err := ParseMPrivatekey("")
	t.True(errors.Is(err, util.InvalidError))
	t.Contains(err.Error(), "unknown privatekey string")

	_, err = ParseMPrivatekey(MPrivatekeyHint.Type().String())
	t.True(errors.Is(err, util.InvalidError))
	t.Contains(err.Error(), "invalid privatekey string")

	_, err = ParseMPrivatekey(util.UUID().String() + MPrivatekeyHint.Type().String())
	t.True(errors.Is(err, util.InvalidError))
	t.Contains(err.Error(), "malformed private key")
}

func (t *testMPrivatekey) TestFromSeed() {
	seed := util.UUID().String() + util.UUID().String()

	priva, err := NewMPrivatekeyFromSeed(seed)
	t.NoError(err)

	for i := 0; i < 400; i++ {
		if i%200 == 0 {
			t.T().Log("generated:", i)
		}

		b, err := NewMPrivatekeyFromSeed(seed)
		t.NoError(err)

		t.True(priva.Equal(b))
	}
}

type wrongHintedKey struct {
	PKKey
	ht hint.Hint
}

func (k wrongHintedKey) Hint() hint.Hint {
	return k.ht
}

func (t *testMPrivatekey) TestEqual() {
	priv := NewMPrivatekey()
	b := NewMPrivatekey()

	t.True(priv.Equal(priv))
	t.False(priv.Equal(b))
	t.True(b.Equal(b))
	t.False(priv.Equal(nil))
	t.False(b.Equal(nil))
	t.False(priv.Equal(wrongHintedKey{PKKey: priv, ht: hint.MustNewHint("wrong-v0.0.1")}))
	t.True(priv.Equal(wrongHintedKey{PKKey: priv, ht: hint.MustNewHint(MPrivatekeyHint.Type().String() + "-v0.0.1")}))
}

func TestMPrivatekey(t *testing.T) {
	suite.Run(t, new(testMPrivatekey))
}

type baseTestMPKKeyEncode struct {
	encoder.BaseTestEncode
	enc     encoder.Encoder
	compare func(PKKey, PKKey)
}

func (t *baseTestMPKKeyEncode) Compare(a, b interface{}) {
	ak := a.(PKKey)
	uak := b.(PKKey)

	if t.compare != nil {
		t.compare(ak, uak)

		return
	}

	t.True(ak.Hint().Equal(uak.Hint()))
	t.True(ak.Equal(uak))
	t.Equal(ak.String(), uak.String())
}

func (t *baseTestMPKKeyEncode) SetupSuite() {
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: MPrivatekeyHint, Instance: MPrivatekey{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: MPublickeyHint, Instance: MPublickey{}}))
}

func testMPrivatekeyEncode() *baseTestMPKKeyEncode {
	t := new(baseTestMPKKeyEncode)
	t.compare = func(a, b PKKey) {
		_, ok := a.(MPrivatekey)
		t.True(ok)
		_, ok = b.(MPrivatekey)
		t.True(ok)
	}

	return t
}

func TestMPrivatekeyDecoderJSON(tt *testing.T) {
	t := testMPrivatekeyEncode()
	t.enc = jsonenc.NewEncoder()
	t.Encode = func() (interface{}, []byte) {
		k := NewMPrivatekey()
		b, err := t.enc.Marshal(k)
		t.NoError(err)

		return k, b
	}
	t.Decode = func(b []byte) interface{} {
		var d PrivatekeyDecoder
		t.NoError(t.enc.Unmarshal(b, &d))
		uk, err := d.Decode(t.enc)
		t.NoError(err)

		return uk
	}

	suite.Run(tt, t)
}

func TestNilMPrivatekeyDecoderJSON(tt *testing.T) {
	t := testMPrivatekeyEncode()
	t.enc = jsonenc.NewEncoder()
	t.Encode = func() (interface{}, []byte) {
		b, err := t.enc.Marshal(nil)
		t.NoError(err)

		return nil, b
	}
	t.Decode = func(b []byte) interface{} {
		var d PrivatekeyDecoder
		t.NoError(t.enc.Unmarshal(b, &d))
		uk, err := d.Decode(t.enc)
		t.NoError(err)

		return uk
	}
	t.compare = func(a, b PKKey) {
		t.Nil(a)
		t.Nil(b)
	}

	suite.Run(tt, t)
}
