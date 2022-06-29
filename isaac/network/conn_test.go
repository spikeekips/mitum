package isaacnetwork

import (
	"testing"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/network/quicstream"
	"github.com/spikeekips/mitum/util/encoder"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/stretchr/testify/suite"
)

func TestBaseNodeConnInfoEncode(t *testing.T) {
	tt := new(encoder.BaseTestEncode)

	enc := jsonenc.NewEncoder()

	tt.Encode = func() (interface{}, []byte) {
		tt.NoError(enc.Add(encoder.DecodeDetail{Hint: base.StringAddressHint, Instance: base.StringAddress{}}))
		tt.NoError(enc.Add(encoder.DecodeDetail{Hint: base.MPublickeyHint, Instance: base.MPublickey{}}))
		tt.NoError(enc.Add(encoder.DecodeDetail{Hint: BaseNodeConnInfoHint, Instance: BaseNodeConnInfo{}}))

		node := base.RandomNode()
		ci, err := quicstream.NewBaseConnInfoFromString("1.2.3.4:4321#tls_insecure")
		tt.NoError(err)

		nc := NewBaseNodeConnInfo(node, ci)
		_ = (interface{})(nc).(isaac.NodeConnInfo)

		b, err := enc.Marshal(nc)
		tt.NoError(err)

		tt.T().Log("marshaled:", string(b))

		return nc, b
	}
	tt.Decode = func(b []byte) interface{} {
		var u BaseNodeConnInfo

		tt.NoError(encoder.Decode(enc, b, &u))

		return u
	}
	tt.Compare = func(a interface{}, b interface{}) {
		ap := a.(BaseNodeConnInfo)
		bp := b.(BaseNodeConnInfo)

		tt.True(base.IsEqualNode(ap, bp))
		tt.Equal(ap.String(), bp.String())
	}

	suite.Run(t, tt)
}
