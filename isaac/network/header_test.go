package isaacnetwork

import (
	"testing"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util/encoder"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/stretchr/testify/suite"
)

type testResponseHeader struct {
	suite.Suite
}

func (t *testResponseHeader) TestResponseHeader() {
	tt := new(encoder.BaseTestEncode)
	enc := jsonenc.NewEncoder()

	hints := []encoder.DecodeDetail{
		{Hint: ResponseHeaderHint, Instance: ResponseHeader{}},
	}
	for i := range hints {
		t.NoError(enc.Add(hints[i]))
	}

	tt.Encode = func() (interface{}, []byte) {
		h := NewResponseHeader(true, errors.Errorf("kekeke"))

		b, err := enc.Marshal(h)
		t.NoError(err)

		t.T().Log("marshaled:", string(b))

		return h, b
	}
	tt.Decode = func(b []byte) interface{} {
		hinter, err := enc.Decode(b)
		t.NoError(err)

		i, ok := hinter.(ResponseHeader)
		t.True(ok)

		return i
	}
	tt.Compare = func(a, b interface{}) {
		ah, ok := a.(ResponseHeader)
		t.True(ok)
		bh, ok := b.(ResponseHeader)
		t.True(ok)

		t.NoError(bh.IsValid(nil))
		t.Equal(ah.Err().Error(), bh.Err().Error())
		t.Equal(ah.OK(), bh.OK())
	}

	suite.Run(t.T(), tt)
}

func TestResponseHeader(t *testing.T) {
	suite.Run(t, new(testResponseHeader))
}
