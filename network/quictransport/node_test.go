package quictransport

import (
	"testing"

	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/stretchr/testify/suite"
)

func TestBaseConnInfoEncode(t *testing.T) {
	tt := new(encoder.BaseTestEncode)

	tt.Encode = func() (interface{}, []byte) {
		ci, err := NewBaseConnInfoFromString("1.2.3.4:4321#insecure")
		tt.NoError(err)

		b, err := util.MarshalJSON(ci)
		tt.NoError(err)

		tt.T().Log("marshaled:", string(b))

		return ci, b
	}
	tt.Decode = func(b []byte) interface{} {
		var u BaseConnInfo
		tt.NoError(util.UnmarshalJSON(b, &u))

		return u
	}
	tt.Compare = func(a interface{}, b interface{}) {
		ap := a.(BaseConnInfo)
		bp := b.(BaseConnInfo)

		tt.Equal(ap.String(), bp.String())
	}

	suite.Run(t, tt)
}
