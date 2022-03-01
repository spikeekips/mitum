package isaac

import (
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/hint"
)

var NodeHint = hint.MustNewHint("node-v0.0.1")

type LocalNode struct {
	addr base.Address
	priv base.Privatekey
}

func NewLocalNode(priv base.Privatekey, addr base.Address) LocalNode {
	return LocalNode{priv: priv, addr: addr}
}

func (n LocalNode) IsValid([]byte) error {
	if err := util.CheckIsValid(nil, false, n.addr, n.priv); err != nil {
		return errors.Wrap(err, "invalid LocalNode")
	}

	return nil
}

func (n LocalNode) Hint() hint.Hint {
	return NodeHint
}

func (n LocalNode) Address() base.Address {
	return n.addr
}

func (n LocalNode) Privatekey() base.Privatekey {
	return n.priv
}

func (n LocalNode) Publickey() base.Publickey {
	if n.priv == nil {
		return nil
	}

	return n.priv.Publickey()
}

func (n LocalNode) HashBytes() []byte {
	return util.ConcatByters(n.addr, n.Publickey())
}

func (n LocalNode) Remote() RemoteNode {
	return NewRemoteNode(n.priv.Publickey(), n.addr)
}

func (n LocalNode) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(n.Remote())
}

type RemoteNode struct {
	hint.BaseHinter
	addr base.Address
	pub  base.Publickey
}

func NewRemoteNode(pub base.Publickey, addr base.Address) RemoteNode {
	return RemoteNode{
		BaseHinter: hint.NewBaseHinter(NodeHint),
		pub:        pub, addr: addr,
	}
}

func (n RemoteNode) IsValid([]byte) error {
	if err := util.CheckIsValid(nil, false, n.addr, n.pub); err != nil {
		return errors.Wrap(err, "invalid RemoteNode")
	}

	return nil
}

func (n RemoteNode) Address() base.Address {
	return n.addr
}

func (n RemoteNode) Publickey() base.Publickey {
	return n.pub
}

func (n RemoteNode) HashBytes() []byte {
	return util.ConcatByters(n.addr, n.pub)
}

type remoteNodeJSONMarshaler struct {
	hint.BaseHinter
	Addr base.Address   `json:"address"`
	Pub  base.Publickey `json:"publickey"`
}

func (n RemoteNode) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(remoteNodeJSONMarshaler{
		BaseHinter: n.BaseHinter,
		Addr:       n.addr,
		Pub:        n.pub,
	})
}

type remoteNodeJSONUnmarshaler struct {
	Addr string `json:"address"`
	Pub  string `json:"publickey"`
}

func (n *RemoteNode) DecodeJSON(b []byte, enc *jsonenc.Encoder) error {
	e := util.StringErrorFunc("failed to decode RemoteNode")

	var u remoteNodeJSONUnmarshaler
	if err := enc.Unmarshal(b, &u); err != nil {
		return e(err, "")
	}

	switch i, err := base.DecodeAddressFromString(u.Addr, enc); {
	case err != nil:
		return e(err, "failed to decode node address")
	default:
		n.addr = i
	}

	switch i, err := base.DecodePublickeyFromString(u.Pub, enc); {
	case err != nil:
		return e(err, "failed to decode node publickey")
	default:
		n.pub = i
	}

	return nil
}
