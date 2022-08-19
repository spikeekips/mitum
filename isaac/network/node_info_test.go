package isaacnetwork

import (
	"testing"
	"time"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	isaacstates "github.com/spikeekips/mitum/isaac/states"
	"github.com/spikeekips/mitum/network/quicstream"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
)

type testNodeInfo struct {
	suite.Suite
}

func TestNodeInfo(t *testing.T) {
	suite.Run(t, new(testNodeInfo))
}

func TestNodeInfoEncode(tt *testing.T) {
	t := new(encoder.BaseTestEncode)
	t.SetT(tt)

	enc := jsonenc.NewEncoder()

	hints := []encoder.DecodeDetail{
		{Hint: base.StringAddressHint, Instance: base.StringAddress{}},
		{Hint: base.MPublickeyHint, Instance: base.MPublickey{}},
		{Hint: base.DummyNodeHint, Instance: base.BaseNode{}},
		{Hint: base.DummyManifestHint, Instance: base.DummyManifest{}},
		{Hint: isaac.FixedSuffrageCandidateLimiterRuleHint, Instance: isaac.FixedSuffrageCandidateLimiterRule{}},
		{Hint: isaac.NetworkPolicyHint, Instance: isaac.NetworkPolicy{}},
		{Hint: isaac.LocalParamsHint, Instance: isaac.LocalParams{}},
		{Hint: NodeInfoHint, Instance: NodeInfo{}},
	}
	for i := range hints {
		t.NoError(enc.Add(hints[i]))
	}

	networkID := util.UUID().Bytes()

	t.Encode = func() (interface{}, []byte) {
		info := NewNodeInfoUpdater(networkID, base.RandomNode(), util.MustNewVersion("v1.2.3"))
		info.startedAt = time.Now().Add(((time.Hour + time.Nanosecond*33) * -1))
		info.SetLastManifest(base.NewDummyManifest(base.Height(33), valuehash.RandomSHA256()))
		info.SetSuffrageHeight(base.Height(44))
		info.SetNetworkPolicy(isaac.DefaultNetworkPolicy())
		info.SetLocalParams(isaac.DefaultLocalParams(networkID))

		ci, err := quicstream.NewUDPConnInfoFromStringAddress("1.2.3.4:4321", true)
		t.NoError(err)
		info.SetConnInfo(ci.String())

		info.SetConsensusNodes([]base.Node{
			base.RandomNode(),
			base.RandomNode(),
			base.RandomNode(),
		})
		info.SetConsensusState(isaacstates.StateBroken)

		n := info.NodeInfo()

		b, err := enc.Marshal(n)
		t.NoError(err)

		t.T().Log("marshaled:", string(b))

		return n, b
	}
	t.Decode = func(b []byte) interface{} {
		hinter, err := enc.Decode(b)
		t.NoError(err)

		i, ok := hinter.(NodeInfo)
		t.True(ok)

		return i
	}
	t.Compare = func(a, b interface{}) {
		ah, ok := a.(NodeInfo)
		t.True(ok)
		bh, ok := b.(NodeInfo)
		t.True(ok)

		t.NoError(bh.IsValid(networkID))

		t.True(ah.Hint().Equal(bh.Hint()))
		t.True(ah.address.Equal(bh.address))
		t.True(ah.publickey.Equal(bh.publickey))
		t.Equal(ah.consensusState, bh.consensusState)
		base.EqualManifest(t.Assert(), ah.lastManifest, bh.lastManifest)
		t.Equal(ah.suffrageHeight, bh.suffrageHeight)
		base.EqualNetworkPolicy(t.Assert(), ah.networkPolicy, bh.networkPolicy)
		base.EqualLocalParams(t.Assert(), ah.localParams, bh.localParams)
		t.Equal(ah.connInfo, bh.connInfo)
		t.Equal(len(ah.consensusNodes), len(bh.consensusNodes))
		for i := range ah.consensusNodes {
			ac := ah.consensusNodes[i]
			bc := bh.consensusNodes[i]
			base.IsEqualNode(ac, bc)
		}

		t.Equal(ah.version, bh.version)
		t.Equal(ah.uptime.Round(time.Millisecond).String(), bh.uptime.Round(time.Millisecond).String())
	}

	suite.Run(tt, t)
}
