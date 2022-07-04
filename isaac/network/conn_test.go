package isaacnetwork

import (
	"net"
	"testing"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/network"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/stretchr/testify/suite"
)

type testNodeConnInfo struct {
	isaac.BaseTestBallots
}

func (t *testNodeConnInfo) TestNew() {
	node := base.NewBaseNode(isaac.NodeHint, base.NewMPrivatekey().Publickey(), base.RandomAddress("local-"))

	nci := NewNodeConnInfo(node, "127.0.0.1:1234", true)

	t.NoError(nci.IsValid(nil))

	_ = (interface{})(nci).(base.Node)
	_ = (interface{})(nci).(network.ConnInfo)
}

func (t *testNodeConnInfo) TestInvalid() {
	t.Run("wrong ip", func() {
		node := base.NewBaseNode(isaac.NodeHint, base.NewMPrivatekey().Publickey(), base.RandomAddress("local-"))

		nci := NewNodeConnInfo(node, "1.2.3.500:1234", true)

		t.NoError(nci.IsValid(nil))

		_, err := nci.UDPConnInfo()
		t.Error(err)

		var dnserr *net.DNSError
		t.True(errors.As(err, &dnserr))
	})

	t.Run("dns error", func() {
		node := base.NewBaseNode(isaac.NodeHint, base.NewMPrivatekey().Publickey(), base.RandomAddress("local-"))

		nci := NewNodeConnInfo(node, "a.b.c.d:1234", true)

		t.NoError(nci.IsValid(nil))

		_, err := nci.UDPConnInfo()
		t.Error(err)

		var dnserr *net.DNSError
		t.True(errors.As(err, &dnserr))
	})

	t.Run("empty host", func() {
		node := base.NewBaseNode(isaac.NodeHint, base.NewMPrivatekey().Publickey(), base.RandomAddress("local-"))

		nci := NewNodeConnInfo(node, ":1234", true)

		err := nci.IsValid(nil)
		t.Error(err)
		t.True(errors.Is(err, util.ErrInvalid))
	})

	t.Run("empty port", func() {
		node := base.NewBaseNode(isaac.NodeHint, base.NewMPrivatekey().Publickey(), base.RandomAddress("local-"))

		nci := NewNodeConnInfo(node, "a.b.c.d", true)

		err := nci.IsValid(nil)
		t.Error(err)
		t.True(errors.Is(err, util.ErrInvalid))
	})
}

func TestNodeConnInfo(t *testing.T) {
	suite.Run(t, new(testNodeConnInfo))
}

func TestNodeConnInfoEncode(t *testing.T) {
	tt := new(encoder.BaseTestEncode)

	enc := jsonenc.NewEncoder()

	tt.Encode = func() (interface{}, []byte) {
		tt.NoError(enc.Add(encoder.DecodeDetail{Hint: base.StringAddressHint, Instance: base.StringAddress{}}))
		tt.NoError(enc.Add(encoder.DecodeDetail{Hint: base.MPublickeyHint, Instance: base.MPublickey{}}))
		tt.NoError(enc.Add(encoder.DecodeDetail{Hint: NodeConnInfoHint, Instance: NodeConnInfo{}}))

		node := base.RandomNode()

		nc := NewNodeConnInfo(node, "1.2.3.4:4321", true)
		_ = (interface{})(nc).(isaac.NodeConnInfo)

		b, err := enc.Marshal(nc)
		tt.NoError(err)

		tt.T().Log("marshaled:", string(b))

		return nc, b
	}
	tt.Decode = func(b []byte) interface{} {
		var u NodeConnInfo

		tt.NoError(encoder.Decode(enc, b, &u))

		return u
	}
	tt.Compare = func(a interface{}, b interface{}) {
		ap := a.(NodeConnInfo)
		bp := b.(NodeConnInfo)

		tt.True(base.IsEqualNode(ap, bp))

		tt.Equal(ap.String(), bp.String())
	}

	suite.Run(t, tt)
}
