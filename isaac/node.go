package isaac

import (
	"sync"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/base/key"
	"github.com/spikeekips/mitum/network"
)

type LocalNode struct {
	sync.RWMutex
	base.BaseNodeV0
	privatekey key.Privatekey
	channel    network.NetworkChannel
}

func NewLocalNode(address base.Address, privatekey key.Privatekey) *LocalNode {
	return &LocalNode{
		BaseNodeV0: base.NewBaseNodeV0(address, privatekey.Publickey()),
		privatekey: privatekey,
	}
}

func (ln *LocalNode) Publickey() key.Publickey {
	ln.RLock()
	defer ln.RUnlock()

	return ln.BaseNodeV0.Publickey()
}

func (ln *LocalNode) Privatekey() key.Privatekey {
	ln.RLock()
	defer ln.RUnlock()

	return ln.privatekey
}

func (ln *LocalNode) Channel() network.NetworkChannel {
	ln.RLock()
	defer ln.RUnlock()

	return ln.channel
}

func (ln *LocalNode) SetChannel(channel network.NetworkChannel) *LocalNode {
	ln.Lock()
	defer ln.Unlock()

	ln.channel = channel

	return ln
}

type RemoteNode struct {
	sync.RWMutex
	base.BaseNodeV0
	channel network.NetworkChannel
}

func NewRemoteNode(address base.Address, publickey key.Publickey) *RemoteNode {
	return &RemoteNode{
		BaseNodeV0: base.NewBaseNodeV0(address, publickey),
	}
}

func (ln *RemoteNode) Channel() network.NetworkChannel {
	ln.RLock()
	defer ln.RUnlock()

	return ln.channel
}

func (ln *RemoteNode) SetChannel(channel network.NetworkChannel) *RemoteNode {
	ln.Lock()
	defer ln.Unlock()

	ln.channel = channel

	return ln
}
