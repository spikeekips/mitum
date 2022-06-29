package isaacnetwork

import (
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/network/quicmemberlist"
	"github.com/spikeekips/mitum/network/quicstream"
	"github.com/spikeekips/mitum/util"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/hint"
)

var BaseNodeConnInfoHint = hint.MustNewHint("node-conninfo-v0.0.1")

type BaseNodeConnInfo struct {
	quicstream.BaseConnInfo
	base.BaseNode
}

func NewBaseNodeConnInfo(node base.BaseNode, ci quicstream.BaseConnInfo) BaseNodeConnInfo {
	node.BaseHinter = node.BaseHinter.SetHint(BaseNodeConnInfoHint).(hint.BaseHinter) //nolint:forcetypeassert //...

	return BaseNodeConnInfo{
		BaseNode:     node,
		BaseConnInfo: ci,
	}
}

func NewBaseNodeConnInfoFromQuicmemberlistNode(node quicmemberlist.Node) BaseNodeConnInfo {
	return NewBaseNodeConnInfo(
		isaac.NewNode(node.Publickey(), node.Address()),
		quicstream.NewBaseConnInfo(node.UDPAddr(), node.TLSInsecure()),
	)
}

func (n BaseNodeConnInfo) IsValid([]byte) error {
	e := util.ErrInvalid.Errorf("invalid BaseNodeConnInfo")

	if err := n.BaseNode.BaseHinter.IsValid(BaseNodeConnInfoHint.Type().Bytes()); err != nil {
		return e.Wrap(err)
	}

	if err := n.BaseConnInfo.IsValid(nil); err != nil {
		return e.Wrap(err)
	}

	return nil
}

func (n BaseNodeConnInfo) HashBytes() []byte {
	return util.ConcatBytesSlice(
		n.BaseNode.HashBytes(),
		[]byte(n.BaseConnInfo.String()),
	)
}

type baseConnInfoJSONMarshaler struct {
	ConnInfo string `json:"conn_info"`
}

func (n BaseNodeConnInfo) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(struct {
		baseConnInfoJSONMarshaler
		base.BaseNodeJSONMarshaler
	}{
		BaseNodeJSONMarshaler: base.BaseNodeJSONMarshaler{
			Address:    n.BaseNode.Address(),
			Publickey:  n.BaseNode.Publickey(),
			BaseHinter: n.BaseHinter,
		},
		baseConnInfoJSONMarshaler: baseConnInfoJSONMarshaler{
			ConnInfo: n.BaseConnInfo.String(),
		},
	})
}

func (n *BaseNodeConnInfo) DecodeJSON(b []byte, enc *jsonenc.Encoder) error {
	e := util.StringErrorFunc("failed to decode BaseNodeConnInfo")

	if err := n.BaseNode.DecodeJSON(b, enc); err != nil {
		return e(err, "")
	}

	var u baseConnInfoJSONMarshaler

	if err := util.UnmarshalJSON(b, &u); err != nil {
		return e(err, "")
	}

	if err := n.BaseConnInfo.UnmarshalText([]byte(u.ConnInfo)); err != nil {
		return e(err, "")
	}

	return nil
}
