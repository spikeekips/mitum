package isaac

import (
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
)

var NodeHint = hint.MustNewHint("node-v0.0.1")

func NewNode(pub base.Publickey, addr base.Address) base.BaseNode {
	return base.NewBaseNode(NodeHint, pub, addr)
}

type LocalNode struct {
	priv base.Privatekey
	base.BaseNode
}

func NewLocalNode(priv base.Privatekey, addr base.Address) LocalNode {
	return LocalNode{
		BaseNode: base.NewBaseNode(NodeHint, priv.Publickey(), addr),
		priv:     priv,
	}
}

func (n LocalNode) IsValid([]byte) error {
	if err := util.CheckIsValiders(nil, false, n.BaseNode, n.priv); err != nil {
		return errors.Wrap(err, "invalid LocalNode")
	}

	return nil
}

func (n LocalNode) Privatekey() base.Privatekey {
	return n.priv
}

func (n LocalNode) Base() base.BaseNode {
	return n.BaseNode
}
