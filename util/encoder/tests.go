package encoder

import (
	"github.com/spikeekips/mitum/util/hint"
	"github.com/stretchr/testify/suite"
)

type BaseTestEncode struct {
	suite.Suite
	Encode  func() (interface{}, []byte)
	Decode  func([]byte) interface{}
	Compare func(interface{}, interface{})
}

func (t *BaseTestEncode) TestDecode() {
	i, b := t.Encode()

	u := t.Decode(b)

	if t.Compare != nil {
		t.Compare(i, u)
	}

	if i == nil || u == nil {
		return
	}

	ih, iok := i.(hint.Hinter)
	uh, uok := u.(hint.Hinter)

	t.Equal(iok, uok)

	if !iok {
		return
	}

	t.True(ih.Hint().Equal(uh.Hint()))
}
