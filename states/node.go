package states

import "github.com/spikeekips/mitum/base"

type LocalNode struct {
	addr base.Address
	priv base.Privatekey
}

func NewLocalNode(priv base.Privatekey, addr base.Address) *LocalNode {
	return &LocalNode{priv: priv, addr: addr}
}

func (n *LocalNode) Address() base.Address {
	return n.addr
}

func (n *LocalNode) Privatekey() base.Privatekey {
	return n.priv
}

func (n *LocalNode) Publickey() base.Publickey {
	if n.priv == nil {
		return nil
	}

	return n.priv.Publickey()
}

type RemoteNode struct {
	addr base.Address
	pub  base.Publickey
}

func NewRemoteNode(pub base.Publickey, addr base.Address) *RemoteNode {
	return &RemoteNode{pub: pub, addr: addr}
}

func (n *RemoteNode) Address() base.Address {
	return n.addr
}

func (n *RemoteNode) Publickey() base.Publickey {
	return n.pub
}
