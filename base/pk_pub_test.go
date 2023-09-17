package base

import (
	"errors"
	"testing"

	"github.com/spikeekips/mitum/util"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/stretchr/testify/suite"
)

type testMPublickey struct {
	suite.Suite
}

func (t *testMPublickey) TestNew() {
	priv := NewMPrivatekey()
	t.NoError(priv.IsValid(nil))

	pub := priv.Publickey()

	_ = (interface{})(pub).(Publickey)

	t.Implements((*Publickey)(nil), pub)
}

func (t *testMPublickey) TestParseMPublickey() {
	priv := NewMPrivatekey()
	pub := priv.Publickey()

	parsed, err := ParseMPublickey(pub.String())
	t.NoError(err)

	t.True(pub.Equal(parsed))
}

func (t *testMPublickey) TestInvalid() {
	priv := NewMPrivatekey()
	pub := priv.Publickey().(MPublickey)

	{ // empty *btcec.PublicKey
		n := pub
		n.k = nil
		err := n.IsValid(nil)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "empty btc publickey")
	}

	{ // empty *btcec.PublicKey
		n := pub
		n.s = ""
		err := n.IsValid(nil)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "empty publickey string")
	}

	{ // empty *btcec.PublicKey
		n := pub
		n.b = nil
		err := n.IsValid(nil)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "empty publickey []byte")
	}
}

func (t *testMPublickey) TestEqual() {
	priv := NewMPrivatekey()
	pub := priv.Publickey().(MPublickey)

	privb := NewMPrivatekey()
	pubb := privb.Publickey().(MPublickey)

	t.True(pub.Equal(pub))
	t.False(pub.Equal(pubb))
	t.True(pubb.Equal(pubb))
	t.False(pub.Equal(nil))
	t.False(pubb.Equal(nil))

	npub := pub
	npub.BaseHinter = npub.BaseHinter.SetHint(hint.MustNewHint("wrong-v0.0.1")).(hint.BaseHinter)
	npub = npub.ensure()
	t.False(pub.Equal(npub))

	npub.BaseHinter = npub.BaseHinter.SetHint(hint.MustNewHint(MPublickeyHint.Type().String() + "-v0.0.1")).(hint.BaseHinter)
	npub = npub.ensure()
	t.True(pub.Equal(npub))
}

func (t *testMPublickey) TestSign() {
	priv := NewMPrivatekey()

	input := []byte("makeme")

	sig, err := priv.Sign(input)
	t.NoError(err)
	t.NotNil(sig)

	t.NoError(priv.Publickey().Verify(input, sig))

	{ // different input
		err = priv.Publickey().Verify([]byte("findme"), sig)
		t.Error(err)
		t.True(errors.Is(err, ErrSignatureVerification))
	}

	{ // wrong signature
		sig, err := priv.Sign([]byte("findme"))
		t.NoError(err)
		t.NotNil(sig)

		err = priv.Publickey().Verify(input, sig)
		t.Error(err)
		t.True(errors.Is(err, ErrSignatureVerification))
	}

	{ // different publickey
		err = NewMPrivatekey().Publickey().Verify(input, sig)
		t.Error(err)
		t.True(errors.Is(err, ErrSignatureVerification))
	}
}

func TestMPublickey(t *testing.T) {
	suite.Run(t, new(testMPublickey))
}

type basetestMPublickeyEncode struct {
	baseTestMPKKeyEncode
	priv  Privatekey
	input []byte
	sig   Signature
}

func (t *basetestMPublickeyEncode) SetupTest() {
	t.priv = NewMPrivatekey()
	t.input = util.UUID().Bytes()

	i, err := t.priv.Sign(t.input)
	t.NoError(err)

	t.sig = i
}

func testMPublickeyEncode() *basetestMPublickeyEncode {
	t := new(basetestMPublickeyEncode)
	t.compare = func(a, b PKKey) {
		_, ok := a.(Publickey)
		t.True(ok)
		upub, ok := b.(Publickey)
		t.True(ok)

		t.NoError(upub.Verify(t.input, t.sig))
	}

	return t
}

func TestMPublickeyJSON(tt *testing.T) {
	t := testMPublickeyEncode()
	t.enc = jsonenc.NewEncoder()
	t.Encode = func() (interface{}, []byte) {
		k := t.priv.Publickey()
		b, err := t.enc.Marshal(k)
		t.NoError(err)

		return k, b
	}
	t.Decode = func(b []byte) interface{} {
		var s string
		t.NoError(t.enc.Unmarshal(b, &s))

		{
			_, err := DecodePublickeyFromString(" "+s, t.enc)
			t.Error(err)
			t.ErrorContains(err, "malformed public key")
		}

		uk, err := DecodePublickeyFromString(s, t.enc)
		t.NoError(err)

		return uk
	}

	suite.Run(tt, t)
}

func TestNilMPublickeyJSON(tt *testing.T) {
	t := testMPublickeyEncode()
	t.enc = jsonenc.NewEncoder()
	t.Encode = func() (interface{}, []byte) {
		b, err := t.enc.Marshal(nil)
		t.NoError(err)

		return nil, b
	}
	t.Decode = func(b []byte) interface{} {
		var s string
		t.NoError(t.enc.Unmarshal(b, &s))

		uk, err := DecodePublickeyFromString(s, t.enc)
		t.NoError(err)

		return uk
	}
	t.compare = func(a, b PKKey) {
		t.Nil(a)
		t.Nil(b)
	}

	suite.Run(tt, t)
}
