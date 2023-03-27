package quicstream

import (
	"testing"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/stretchr/testify/suite"
)

func TestBaseRequestHeader(tt *testing.T) {
	ht := hint.MustNewHint("quicstream-test-request-header-v0.0.1")

	t := new(encoder.BaseTestEncode)

	t.Encode = func() (interface{}, []byte) {
		h := NewBaseRequestHeader(ht, util.UUID().Bytes())

		b, err := util.MarshalJSON(h.JSONMarshaler())
		t.NoError(err)

		t.T().Log("marshaled:", string(b))

		return h, b
	}
	t.Decode = func(b []byte) interface{} {
		var u BaseRequestHeader
		t.NoError(util.UnmarshalJSON(b, &u))

		return u
	}
	t.Compare = func(a interface{}, b interface{}) {
		ap := a.(BaseRequestHeader)
		bp := b.(BaseRequestHeader)

		t.Empty(bp.Handler())
		t.Equal(ap.Hint(), bp.Hint())
	}

	suite.Run(tt, t)
}

func TestBaseResponseHeader(tt *testing.T) {
	t := new(encoder.BaseTestEncode)

	t.Encode = func() (interface{}, []byte) {
		h := NewDefaultResponseHeader(true, errors.Errorf("showme"))

		b, err := util.MarshalJSON(h.JSONMarshaler())
		t.NoError(err)

		t.T().Log("marshaled:", string(b))

		return h, b
	}
	t.Decode = func(b []byte) interface{} {
		var u DefaultResponseHeader
		t.NoError(util.UnmarshalJSON(b, &u))

		return u
	}
	t.Compare = func(a interface{}, b interface{}) {
		ap := a.(DefaultResponseHeader)
		bp := b.(DefaultResponseHeader)

		t.Equal(ap.Hint(), bp.Hint())
		t.Equal(ap.OK(), bp.OK())
		t.Equal(ap.Err().Error(), bp.Err().Error())
	}

	suite.Run(tt, t)
}
