//go:build test
// +build test

package encoder

import (
	"github.com/spikeekips/mitum/util/hint"
	"github.com/stretchr/testify/suite"
)

type BaseTest struct {
	MarshalFunc func(interface{}) ([]byte, error)
}

func (t *BaseTest) StringMarshal(i interface{}) string {
	b, err := t.MarshalFunc(i)
	if err != nil {
		return "<failed to marshal>"
	}

	return string(b)
}

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

	if !iok || !uok {
		return
	}

	t.True(ih.Hint().Equal(uh.Hint()))
}
