package quicstreamheader

import (
	"testing"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/network/quicstream"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/stretchr/testify/suite"
)

func TestBaseRequestHeader(tt *testing.T) {
	ht := hint.MustNewHint("quicstream-test-request-header-v0.0.1")

	t := new(encoder.BaseTestEncode)

	t.Encode = func() (interface{}, []byte) {
		prefix := quicstream.HashPrefix(quicstream.HandlerName(util.UUID().String()))

		h := NewBaseRequestHeader(ht, prefix)

		_, ok := (interface{})(h).(RequestHeader)
		t.True(ok)

		t.NoError(h.IsValid(nil))

		b, err := util.MarshalJSON(h.JSONMarshaler())
		t.NoError(err)

		t.T().Log("marshaled:", string(b))

		return h, b
	}
	t.Decode = func(b []byte) interface{} {
		var u BaseRequestHeader
		t.NoError(util.UnmarshalJSON(b, &u))

		t.NoError(u.IsValid(nil))

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

func TestDefaultResponseHeader(tt *testing.T) {
	t := new(encoder.BaseTestEncode)

	t.Encode = func() (interface{}, []byte) {
		h := NewDefaultResponseHeader(true, errors.Errorf("showme"))

		t.NoError(h.IsValid(nil))

		b, err := util.MarshalJSON(h.JSONMarshaler())
		t.NoError(err)

		_, ok := (interface{})(h).(ResponseHeader)
		t.True(ok)

		t.T().Log("marshaled:", string(b))

		return h, b
	}
	t.Decode = func(b []byte) interface{} {
		var u DefaultResponseHeader
		t.NoError(util.UnmarshalJSON(b, &u))

		t.NoError(u.IsValid(nil))

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
