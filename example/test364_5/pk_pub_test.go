package dop5

import (
	"testing"

	"github.com/spikeekips/mitum/base"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
	"github.com/stretchr/testify/suite"
)

type testLazyMPublickey struct {
	dutil.BaseSuite
}

func (t *testLazyMPublickey) TestMethods() {
	pub := base.NewMPrivatekey().Publickey()
	lpub := NewLazyMPublickey(pub.String())

	t.Run("String()", func() { t.Equal(pub.String(), lpub.String()) })
	t.Run("Bytes()", func() { t.Equal(pub.Bytes(), lpub.Bytes()) })

	t.Run("IsValid()", func() { t.NoError(lpub.IsValid(nil)) })
	t.Run("IsValid(); empty", func() {
		err := NewLazyMPublickey("").IsValid(nil)
		t.ErrorIs(err, util.ErrInvalid)
	})

	t.Run("Equal()", func() { t.True(pub.Equal(lpub)) })
}

func (t *testLazyMPublickey) TestVerify() {
	priv := base.NewMPrivatekey()
	pub := priv.Publickey()

	lpub := NewLazyMPublickey(pub.String())

	input := []byte("makeme")

	sig, err := priv.Sign(input)
	t.NoError(err)

	t.Run("ok", func() {
		t.NoError(pub.Verify(input, sig))
		t.NoError(lpub.Verify(input, sig))
	})

	t.Run("wrong", func() {
		err := lpub.Verify([]byte("showme"), sig)
		t.ErrorIs(err, base.ErrSignatureVerification)
	})
}

func (t *testLazyMPublickey) TestEncode() {
	pub := base.NewMPrivatekey().Publickey()
	lpub := NewLazyMPublickey(pub.String())

	ba, err := util.MarshalJSON(pub)
	t.NoError(err)
	bb, err := util.MarshalJSON(lpub)
	t.NoError(err)

	t.T().Log("marshaled:", string(bb))

	t.Equal(ba, bb)
}

func TestLazyMPublickey(t *testing.T) {
	suite.Run(t, new(testLazyMPublickey))
}
