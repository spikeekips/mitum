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

	for i := 0; i < 400; i++ {
		if i%200 == 0 {
			t.T().Log("generated:", i)
		}

		priv, err := NewMPrivatekeyFromSeed(seed)
		t.NoError(err)

		t.Equal("6yixbhwXXf9hpTnwgHJimSACCtLiTJuNRbvfaAhijzP1mpr", priv.String())
		t.Equal("mkGgtfftZn6jY19bnJbYmKy171HA5WFCoacHeqMwNNUumpu", priv.Publickey().String())
	}
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
	seed := util.UUID().String()[:PrivatekeyMinSeedSize-1]

	_, err := NewMPrivatekeyFromSeed(seed)
	t.True(errors.Is(err, util.ErrInvalid))
	t.ErrorContains(err, "too short")
}

func (t *testMPrivatekey) TestParseMPrivatekey() {
	priv := NewMPrivatekey()
	parsed, err := ParseMPrivatekey(priv.String())
	t.NoError(err)

	t.True(priv.Equal(parsed))
}

func (t *testMPrivatekey) TestParseMPrivatekeyButEmpty() {
	t.Run("empty", func() {
		_, err := ParseMPrivatekey("")
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "unknown privatekey string")
	})

	t.Run("empty body", func() {
		_, err := ParseMPrivatekey(MPrivatekeyHint.Type().String())
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "invalid privatekey string")
	})

	t.Run("random", func() {
		_, err := ParseMPrivatekey(util.UUID().String() + MPrivatekeyHint.Type().String())
		t.Error(err)

		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "malformed private key")
	})
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
		t.True(priva.Equal(b), priva.String(), b.String())
	}
}

func (t *testMPrivatekey) TestEqual() {
	priv := NewMPrivatekey()
	b := NewMPrivatekey()

	t.True(priv.Equal(priv))
	t.False(priv.Equal(b))
	t.True(b.Equal(b))
	t.False(priv.Equal(nil))
	t.False(b.Equal(nil))

	npriv := priv
	npriv.BaseHinter = npriv.BaseHinter.SetHint(hint.MustNewHint("wrong-v0.0.1")).(hint.BaseHinter)
	npriv = npriv.ensure()
	t.False(priv.Equal(npriv))

	npriv.BaseHinter = npriv.BaseHinter.SetHint(hint.MustNewHint(MPrivatekeyHint.Type().String() + "-v0.0.1")).(hint.BaseHinter)
	npriv = npriv.ensure()
	t.True(priv.Equal(npriv))
}

func TestMPrivatekey(t *testing.T) {
	suite.Run(t, new(testMPrivatekey))
}

type baseTestMPKKeyEncode struct {
	encoder.BaseTestEncode
	setupsuite func()
	enc        encoder.Encoder
	compare    func(PKKey, PKKey)
}

func (t *baseTestMPKKeyEncode) Compare(a, b interface{}) {
	ak := a.(PKKey)
	uak := b.(PKKey)

	if t.compare != nil {
		t.compare(ak, uak)

		return
	}

	aht := ak.(hint.Hinter).Hint()
	uht := uak.(hint.Hinter).Hint()

	t.True(aht.Equal(uht))
	t.True(ak.Equal(uak))
	t.Equal(ak.String(), uak.String())
}

func (t *baseTestMPKKeyEncode) SetupSuite() {
	if t.setupsuite != nil {
		t.setupsuite()

		return
	}

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

func TestMPrivatekeyJSON(tt *testing.T) {
	t := testMPrivatekeyEncode()
	t.enc = jsonenc.NewEncoder()

	orig := NewMPrivatekey().String()

	t.Encode = func() (interface{}, []byte) {
		k, err := DecodePrivatekeyFromString(orig, t.enc)
		t.NoError(err)

		b, err := t.enc.Marshal(k)
		t.NoError(err)

		return k, b
	}
	t.Decode = func(b []byte) interface{} {
		var s string
		t.NoError(t.enc.Unmarshal(b, &s))

		{
			_, err := DecodePrivatekeyFromString(" "+s, t.enc)
			t.Error(err)
			t.ErrorContains(err, "malformed private key")
		}

		uk, err := DecodePrivatekeyFromString(s, t.enc)
		t.NoError(err)

		t.Equal(orig, uk.String())

		return uk
	}

	suite.Run(tt, t)
}

func TestNilMPrivatekeyJSON(tt *testing.T) {
	t := testMPrivatekeyEncode()
	t.enc = jsonenc.NewEncoder()
	t.Encode = func() (interface{}, []byte) {
		b, err := t.enc.Marshal(nil)
		t.NoError(err)

		t.T().Log("marshaled:", string(b))

		return nil, b
	}

	t.Decode = func(b []byte) interface{} {
		var s string
		t.NoError(t.enc.Unmarshal(b, &s))

		_, err := DecodePrivatekeyFromString(s, t.enc)

		t.Error(err)
		t.ErrorContains(err, "nil")

		return nil
	}

	suite.Run(tt, t)
}
