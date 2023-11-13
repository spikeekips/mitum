package valuehash

import (
	"testing"

	"github.com/spikeekips/mitum/util"
	"github.com/stretchr/testify/suite"
)

type testKeccak512 struct {
	suite.Suite
}

func (t *testKeccak512) TestNew() {
	h := NewSHA512(nil)
	t.Implements((*util.Hash)(nil), h)

	initial := h.Bytes()

	b := []byte("showme")
	h = NewSHA512(b)

	t.T().Log(h.String())
	t.Equal("3fe7ce3697e71e9f3b89695cbda3f8fbcecde125b7dbe5a87e22760e4d81bf9df0de06a64c61bec1bea520b34a15d91663955990ebbeb296f6a0046afdaf3de7", h.String())

	t.NotEqual(initial, h.Bytes())

	newS512 := NewSHA512(b)

	t.Equal(h.Bytes(), newS512.Bytes())
}

func TestKeccak512(t *testing.T) {
	suite.Run(t, new(testKeccak512))
}

type testKeccak256 struct {
	suite.Suite
}

func (t *testKeccak256) TestNew() {
	h := NewSHA256(nil)
	t.Implements((*util.Hash)(nil), h)

	initial := h.Bytes()

	b := []byte("showme")
	h = NewSHA256(b)

	t.T().Log(h.String())
	t.Equal("4bff5792ba96a36b04dfc48838746d5a92a9d76b7a9ed2d55bd64b4c44be0ef0", h.String())

	t.NotEqual(initial, h.Bytes())

	newS256 := NewSHA256(b)

	t.Equal(h.Bytes(), newS256.Bytes())
}

func TestKeccak256(t *testing.T) {
	suite.Run(t, new(testKeccak256))
}
