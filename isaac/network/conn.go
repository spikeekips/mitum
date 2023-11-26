package isaacnetwork

import (
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/network/quicmemberlist"
	"github.com/spikeekips/mitum/network/quicstream"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/hint"
)

var NodeConnInfoHint = hint.MustNewHint("node-conninfo-v0.0.1")

type NodeConnInfo struct {
	quicmemberlist.NamedConnInfo
	base.BaseNode
}

func NewNodeConnInfo(node base.BaseNode, addr string, tlsinsecure bool) (nci NodeConnInfo, _ error) {
	node.BaseHinter = node.BaseHinter.SetHint(NodeConnInfoHint).(hint.BaseHinter) //nolint:forcetypeassert //...

	ci, err := quicmemberlist.NewNamedConnInfo(addr, tlsinsecure)
	if err != nil {
		return nci, err
	}

	return NodeConnInfo{
		BaseNode:      node,
		NamedConnInfo: ci,
	}, nil
}

func MustNodeConnInfo(node base.BaseNode, addr string, tlsinsecure bool) NodeConnInfo {
	ci, err := NewNodeConnInfo(node, addr, tlsinsecure)
	if err != nil {
		panic(err)
	}

	return ci
}

func NewNodeConnInfoFromConnInfo(node base.BaseNode, ci quicstream.ConnInfo) NodeConnInfo {
	node.BaseHinter = node.BaseHinter.SetHint(NodeConnInfoHint).(hint.BaseHinter) //nolint:forcetypeassert //...

	return NodeConnInfo{
		BaseNode:      node,
		NamedConnInfo: quicmemberlist.NewNamedConnInfoFromConnInfo(ci),
	}
}

func NewNodeConnInfoFromMemberlistNode(member quicmemberlist.Member) NodeConnInfo {
	node := isaac.NewNode(member.Publickey(), member.Address())
	node.BaseHinter = node.BaseHinter.SetHint(NodeConnInfoHint).(hint.BaseHinter) //nolint:forcetypeassert //...

	return NodeConnInfo{
		BaseNode:      node,
		NamedConnInfo: member.Publish(),
	}
}

func (n NodeConnInfo) IsValid([]byte) error {
	e := util.ErrInvalid.Errorf("invalid NodeConnInfo")

	if err := n.BaseNode.BaseHinter.IsValid(NodeConnInfoHint.Type().Bytes()); err != nil {
		return e.Wrap(err)
	}

	if err := n.BaseNode.IsValid(nil); err != nil {
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
		hint.BaseHinter
	}{
		BaseHinter: n.BaseHinter,
		BaseNodeJSONMarshaler: base.BaseNodeJSONMarshaler{
			Address:   n.BaseNode.Address(),
			Publickey: n.BaseNode.Publickey(),
		},
		connInfoJSONMarshaler: connInfoJSONMarshaler{
			ConnInfo: n.NamedConnInfo,
		},
	})
}

func (n *NodeConnInfo) DecodeJSON(b []byte, enc encoder.Encoder) error {
	e := util.StringError("decode NodeConnInfo")

	if err := n.BaseNode.DecodeJSON(b, enc); err != nil {
		return e.Wrap(err)
	}

	var u connInfoJSONMarshaler

	if err := util.UnmarshalJSON(b, &u); err != nil {
		return e.Wrap(err)
	}

	n.NamedConnInfo = u.ConnInfo

	return nil
}
