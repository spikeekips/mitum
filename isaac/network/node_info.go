package isaacnetwork

import (
	"bytes"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	isaacstates "github.com/spikeekips/mitum/isaac/states"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/localtime"
)

var NodeInfoHint = hint.MustNewHint("node-info-v0.0.1")

type NodeInfo struct {
	networkID      base.NetworkID
	address        base.Address
	publickey      base.Publickey
	lastManifest   base.Manifest
	networkPolicy  base.NetworkPolicy
	localParams    *isaac.Params
	connInfo       string
	consensusState isaacstates.StateType
	consensusNodes []base.Node
	hint.BaseHinter
	version        util.Version
	suffrageHeight base.Height
	uptime         time.Duration
}

func (info NodeInfo) IsValid(networkID base.NetworkID) error {
	e := util.ErrInvalid.Errorf("invalid NodeInfo")

	if err := util.CheckIsValiders(networkID, false,
		info.consensusState,
		info.lastManifest,
		info.suffrageHeight,
		info.networkPolicy,
		info.localParams,
		util.DummyIsValider(func([]byte) error {
			if len(info.connInfo) < 1 {
				return errors.Errorf("empty conn info")
			}

			return nil
		}),
		info.version,
		util.DummyIsValider(func([]byte) error {
			if info.uptime < 1 {
				return errors.Errorf("empty started at")
			}

			return nil
		}),
	); err != nil {
		return e.Wrap(err)
	}

	if err := util.CheckIsValiderSlice(nil, false, info.consensusNodes); err != nil {
		return e.Wrap(err)
	}

	return nil
}

func (info NodeInfo) Address() base.Address {
	return info.address
}

func (info NodeInfo) Publickey() base.Publickey {
	return info.publickey
}

func (info NodeInfo) LastManifest() base.Manifest {
	return info.lastManifest
}

func (info NodeInfo) SuffrageHeight() base.Height {
	return info.suffrageHeight
}

func (info NodeInfo) NetworkPolicy() base.NetworkPolicy {
	return info.networkPolicy
}

func (info NodeInfo) LocalParams() *isaac.Params {
	return info.localParams
}

func (info NodeInfo) ConnInfo() string {
	return info.connInfo
}

func (info NodeInfo) ConsensusNodes() []base.Node {
	return info.consensusNodes
}

func (info NodeInfo) Version() util.Version {
	return info.version
}

func (info NodeInfo) ConsensusState() string {
	return info.consensusState.String()
}

type NodeInfoUpdater struct {
	startedAt time.Time
	id        string
	n         NodeInfo
	sync.RWMutex
}

func NewNodeInfoUpdater(networkID base.NetworkID, local base.Node, version util.Version) *NodeInfoUpdater {
	return &NodeInfoUpdater{
		n: NodeInfo{
			BaseHinter:     hint.NewBaseHinter(NodeInfoHint),
			networkID:      networkID,
			address:        local.Address(),
			publickey:      local.Publickey(),
			suffrageHeight: base.NilHeight,
			version:        version,
		},
		startedAt: localtime.Now().UTC(),
	}
}

func (info *NodeInfoUpdater) StartedAt() time.Time {
	return info.startedAt
}

func (info *NodeInfoUpdater) ID() string {
	info.RLock()
	defer info.RUnlock()

	return info.id
}

func (info *NodeInfoUpdater) NodeInfo() NodeInfo {
	info.RLock()
	defer info.RUnlock()

	n := info.n
	n.uptime = localtime.Now().UTC().Sub(info.startedAt)

	return n
}

func (info *NodeInfoUpdater) SetConsensusState(s isaacstates.StateType) bool {
	info.Lock()
	defer info.Unlock()

	if info.n.consensusState == s {
		return false
	}

	info.n.consensusState = s
	info.id = util.UUID().String()

	return true
}

func (info *NodeInfoUpdater) SetLastManifest(m base.Manifest) bool {
	info.Lock()
	defer info.Unlock()

	switch {
	case info.n.lastManifest == nil, m == nil:
	case info.n.lastManifest.Hash().Equal(m.Hash()):
		return false
	}

	info.n.lastManifest = m
	info.id = util.UUID().String()

	return true
}

func (info *NodeInfoUpdater) SetSuffrageHeight(h base.Height) bool {
	info.Lock()
	defer info.Unlock()

	if info.n.suffrageHeight == h {
		return false
	}

	info.n.suffrageHeight = h
	info.id = util.UUID().String()

	return true
}

func (info *NodeInfoUpdater) SetNetworkPolicy(p base.NetworkPolicy) bool {
	info.Lock()
	defer info.Unlock()

	switch {
	case info.n.networkPolicy == nil, p == nil:
	case bytes.Equal(info.n.networkPolicy.HashBytes(), p.HashBytes()):
		return false
	}

	info.n.networkPolicy = p
	info.id = util.UUID().String()

	return true
}

func (info *NodeInfoUpdater) SetLocalParams(p *isaac.Params) bool {
	info.Lock()
	defer info.Unlock()

	switch {
	case info.n.localParams == nil, p == nil:
	case info.n.localParams.ID() == p.ID():
		return false
	}

	info.n.localParams = p
	info.n.networkID = p.NetworkID()
	info.id = util.UUID().String()

	return true
}

func (info *NodeInfoUpdater) SetConnInfo(c string) bool {
	info.Lock()
	defer info.Unlock()

	if info.n.connInfo == c {
		return false
	}

	info.n.connInfo = c
	info.id = util.UUID().String()

	return true
}

func (info *NodeInfoUpdater) SetConsensusNodes(nodes []base.Node) bool {
	info.Lock()
	defer info.Unlock()

	info.n.consensusNodes = nodes
	info.id = util.UUID().String()

	return true
}
