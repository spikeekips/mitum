package base

import (
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/hint"
)

type BaseNode struct {
	util.IsValider
	addr Address
	pub  Publickey
	hint.BaseHinter
}

func NewBaseNode(ht hint.Hint, pub Publickey, addr Address) BaseNode {
	return BaseNode{
		BaseHinter: hint.NewBaseHinter(ht),
		addr:       addr,
		pub:        pub,
	}
}

func (n BaseNode) IsValid([]byte) error {
	if err := util.CheckIsValiders(nil, false, n.addr, n.pub); err != nil {
		return errors.Wrap(err, "invalid RemoteNode")
	}

	return nil
}

func (n BaseNode) Address() Address {
	return n.addr
}

func (n BaseNode) Publickey() Publickey {
	return n.pub
}

func (n BaseNode) HashBytes() []byte {
	return util.ConcatByters(n.addr, n.pub)
}

type BaseNodeJSONMarshaler struct {
	Address   Address   `json:"address"`
	Publickey Publickey `json:"publickey"`
}

func (n BaseNode) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(struct {
		BaseNodeJSONMarshaler
		hint.BaseHinter
	}{
		BaseHinter: n.BaseHinter,
		BaseNodeJSONMarshaler: BaseNodeJSONMarshaler{
			Address:   n.addr,
			Publickey: n.pub,
		},
	})
}

type BaseNodeJSONUnmarshaler struct {
	Address   string `json:"address"`
	Publickey string `json:"publickey"`
}

func (n *BaseNode) DecodeJSON(b []byte, enc *jsonenc.Encoder) error {
	e := util.StringError("decode BaseNode")

	var u BaseNodeJSONUnmarshaler
	if err := enc.Unmarshal(b, &u); err != nil {
		return e.Wrap(err)
	}

	switch i, err := DecodeAddress(u.Address, enc); {
	case err != nil:
		return e.WithMessage(err, "decode node address")
	default:
		n.addr = i
	}

	switch i, err := DecodePublickeyFromString(u.Publickey, enc); {
	case err != nil:
		return e.WithMessage(err, "node publickey")
	default:
		n.pub = i
	}

	return nil
}

type BaseLocalNode struct {
	priv Privatekey
	BaseNode
}

func NewBaseLocalNode(ht hint.Hint, priv Privatekey, addr Address) BaseLocalNode {
	return BaseLocalNode{
		BaseNode: NewBaseNode(ht, priv.Publickey(), addr),
		priv:     priv,
	}
}

func (n BaseLocalNode) IsValid([]byte) error {
	if err := util.CheckIsValiders(nil, false, n.BaseNode, n.priv); err != nil {
		return errors.Wrap(err, "invalid LocalNode")
	}

	return nil
}

func (n BaseLocalNode) Privatekey() Privatekey {
	return n.priv
}

func (n BaseLocalNode) Base() BaseNode {
	return n.BaseNode
}
