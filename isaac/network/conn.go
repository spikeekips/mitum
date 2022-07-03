package isaacnetwork

import (
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/network/quicmemberlist"
	"github.com/spikeekips/mitum/util"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/hint"
)

var BaseNodeConnInfoHint = hint.MustNewHint("node-conninfo-v0.0.1")

type NodeConnInfo struct {
	quicmemberlist.NamedConnInfo
	base.BaseNode
}

func NewNodeConnInfo(node base.BaseNode, addr string, tlsinsecure bool) NodeConnInfo {
	node.BaseHinter = node.BaseHinter.SetHint(BaseNodeConnInfoHint).(hint.BaseHinter) //nolint:forcetypeassert //...

	return NodeConnInfo{
		BaseNode:      node,
		NamedConnInfo: quicmemberlist.NewNamedConnInfo(addr, tlsinsecure),
	}
}

func (n NodeConnInfo) IsValid([]byte) error {
	e := util.ErrInvalid.Errorf("invalid BaseNodeConnInfo")

	if err := n.BaseNode.BaseHinter.IsValid(BaseNodeConnInfoHint.Type().Bytes()); err != nil {
		return e.Wrap(err)
	}

	if err := n.NamedConnInfo.IsValid(nil); err != nil {
		return e.Wrap(err)
	}

	return nil
}

type connInfoJSONMarshaler struct {
	ConnInfo quicmemberlist.NamedConnInfo `json:"conn_info"`
}

func (n NodeConnInfo) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(struct {
		connInfoJSONMarshaler
		base.BaseNodeJSONMarshaler
	}{
		BaseNodeJSONMarshaler: base.BaseNodeJSONMarshaler{
			Address:    n.BaseNode.Address(),
			Publickey:  n.BaseNode.Publickey(),
			BaseHinter: n.BaseHinter,
		},
		connInfoJSONMarshaler: connInfoJSONMarshaler{
			ConnInfo: n.NamedConnInfo,
		},
	})
}

func (n *NodeConnInfo) DecodeJSON(b []byte, enc *jsonenc.Encoder) error {
	e := util.StringErrorFunc("failed to decode BaseNodeConnInfo")

	if err := n.BaseNode.DecodeJSON(b, enc); err != nil {
		return e(err, "")
	}

	var u connInfoJSONMarshaler

	if err := util.UnmarshalJSON(b, &u); err != nil {
		return e(err, "")
	}

	n.NamedConnInfo = u.ConnInfo

	return nil
}
