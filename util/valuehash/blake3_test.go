package valuehash

import (
	"testing"

	"github.com/spikeekips/mitum/util"
	"github.com/stretchr/testify/suite"
)

type testBlake3256 struct {
	suite.Suite
}

func (t *testBlake3256) TestNew() {
	h := NewBlake3256(nil)
	t.Implements((*util.Hash)(nil), h)

	initial := h.Bytes()

	b := []byte("showme")
	h = NewBlake3256(b)

	t.T().Log(h.String())
	t.Equal("3eabc450d5d854f35b9207d42f80ac4edad842d0979551c9cfd7f1749642ee92", h.String())

	t.NotEqual(initial, h.Bytes())

	newS512 := NewBlake3256(b)

	t.Equal(h.Bytes(), newS512.Bytes())
}

func TestBlake3256(t *testing.T) {
	suite.Run(t, new(testBlake3256))
}
