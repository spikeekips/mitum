package base

import (
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/hint"
)

type BaseNode struct {
	util.DefaultJSONMarshaled // BLOCK apply all json struct
	hint.BaseHinter
	addr Address
	pub  Publickey
}

func NewBaseNode(ht hint.Hint, pub Publickey, addr Address) BaseNode {
	return BaseNode{
		BaseHinter: hint.NewBaseHinter(ht),
		addr:       addr,
		pub:        pub,
	}
}

func (n BaseNode) IsValid([]byte) error {
	if err := util.CheckIsValid(nil, false, n.addr, n.pub); err != nil {
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
	hint.BaseHinter
	Addr Address   `json:"address"`
	Pub  Publickey `json:"publickey"`
}

func (n BaseNode) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(BaseNodeJSONMarshaler{
		BaseHinter: n.BaseHinter,
		Addr:       n.addr,
		Pub:        n.pub,
	})
}

type BaseNodeJSONUnmarshaler struct {
	Addr string `json:"address"`
	Pub  string `json:"publickey"`
}

func (n *BaseNode) DecodeJSON(b []byte, enc *jsonenc.Encoder) error {
	e := util.StringErrorFunc("failed to decode RemoteNode")

	var u BaseNodeJSONUnmarshaler
	if err := enc.Unmarshal(b, &u); err != nil {
		return e(err, "")
	}

	switch i, err := DecodeAddress(u.Addr, enc); {
	case err != nil:
		return e(err, "failed to decode node address")
	default:
		n.addr = i
	}

	switch i, err := DecodePublickeyFromString(u.Pub, enc); {
	case err != nil:
		return e(err, "failed to decode node publickey")
	default:
		n.pub = i
	}

	return nil
}
