package quictransport

import (
	"encoding/json"
	"net"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/network"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/localtime"
)

var (
	NodeHint     = hint.MustNewHint("memberlist-node-v0.0.1")
	NodeMetaHint = hint.MustNewHint("memberlist-node-meta-v0.0.1")
)

type ConnInfo interface {
	Addr() net.Addr
	UDPAddr() *net.UDPAddr
	TLSInsecure() bool
}

type Node interface {
	ConnInfo
	Name() string
	Node() base.Address
	JoinedAt() time.Time
	Meta() NodeMeta
	MetaBytes() []byte
}

type BaseNode struct {
	joinedAt time.Time
	addr     *net.UDPAddr
	name     string
	metab    []byte
	hint.BaseHinter
	meta NodeMeta
}

func NewNode(name string, addr *net.UDPAddr, meta NodeMeta) (BaseNode, error) {
	metab, err := util.MarshalJSON(meta)
	if err != nil {
		return BaseNode{}, errors.Wrap(err, "failed to create Node")
	}

	return BaseNode{
		BaseHinter: hint.NewBaseHinter(NodeHint),
		name:       name,
		addr:       addr,
		joinedAt:   localtime.UTCNow(),
		meta:       meta,
		metab:      metab,
	}, nil
}

func newNodeFromMemberlist(node *memberlist.Node, enc encoder.Encoder) (BaseNode, error) {
	e := util.StringErrorFunc("failed to make Node from memberlist.Node")

	var meta NodeMeta

	if err := encoder.Decode(enc, node.Meta, &meta); err != nil {
		return BaseNode{}, e(err, "failed to decode NodeMeta")
	}

	addr, _ := convertNetAddr(node)

	return NewNode(node.Name, addr.(*net.UDPAddr), meta) //nolint:forcetypeassert // ...
}

func (n BaseNode) Name() string {
	return n.name
}

func (n BaseNode) Node() base.Address {
	return n.meta.Node()
}

func (n BaseNode) Addr() net.Addr {
	return n.addr
}

func (n BaseNode) UDPAddr() *net.UDPAddr {
	return n.addr
}

func (n BaseNode) TLSInsecure() bool {
	return n.meta.TLSInsecure()
}

func (n BaseNode) JoinedAt() time.Time {
	return n.joinedAt
}

func (n BaseNode) Meta() NodeMeta {
	return n.meta
}

func (n BaseNode) MetaBytes() []byte {
	return n.metab
}

func (n BaseNode) MarshalZerologObject(e *zerolog.Event) {
	e.
		Str("name", n.name).
		Stringer("node", n.meta.Node()).
		Stringer("address", n.addr).
		Bool("tls_insecure", n.meta.TLSInsecure()).
		Time("joined_at", n.joinedAt)
}

func (n BaseNode) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Name     string
		Address  string
		JoinedAt time.Time `json:"joined_at"`
		Meta     NodeMeta
	}{
		Name:     n.name,
		Address:  n.addr.String(),
		JoinedAt: n.joinedAt,
		Meta:     n.meta,
	})
}

func (n *BaseNode) DecodeJSON(b []byte, enc *jsonenc.Encoder) error {
	var u struct {
		Name     string
		Address  string
		JoinedAt time.Time `json:"joined_at"`
		Meta     json.RawMessage
	}

	e := util.StringErrorFunc("failed to unmarshal Node")
	if err := json.Unmarshal(b, &u); err != nil {
		return e(err, "")
	}

	var meta NodeMeta
	if err := meta.DecodeJSON(u.Meta, enc); err != nil {
		return e(err, "failed to decode NodeMeta")
	}

	addr, err := net.ResolveUDPAddr("udp", u.Address)
	if err != nil {
		return e(err, "")
	}

	n.name = u.Name
	n.addr = addr
	n.joinedAt = u.JoinedAt
	n.meta = meta

	return nil
}

type NodeMeta struct {
	node base.Address
	hint.BaseHinter
	tlsinsecure bool
}

func NewNodeMeta(node base.Address, tlsinsecure bool) NodeMeta {
	return NodeMeta{
		BaseHinter:  hint.NewBaseHinter(NodeMetaHint),
		node:        node,
		tlsinsecure: tlsinsecure,
	}
}

func (n NodeMeta) Node() base.Address {
	return n.node
}

func (n NodeMeta) TLSInsecure() bool {
	return n.tlsinsecure
}

type nodeMetaJSONMmarshaler struct {
	Node base.Address `json:"node"`
	hint.BaseHinter
	TLSInsecure bool `json:"tls_insecure"`
}

func (n NodeMeta) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(nodeMetaJSONMmarshaler{
		BaseHinter:  n.BaseHinter,
		Node:        n.node,
		TLSInsecure: n.tlsinsecure,
	})
}

type nodeMetaJSONUnmarshaler struct {
	Node        string `json:"node"`
	TLSInsecure bool   `json:"tls_insecure"`
}

func (n *NodeMeta) DecodeJSON(b []byte, enc *jsonenc.Encoder) error {
	e := util.StringErrorFunc("failed to decode NodeMta")

	var u nodeMetaJSONUnmarshaler
	if err := enc.Unmarshal(b, &u); err != nil {
		return e(err, "")
	}

	switch i, err := base.DecodeAddress(u.Node, enc); {
	case err != nil:
		return e(err, "failed to decode node")
	default:
		n.node = i
	}

	n.tlsinsecure = u.TLSInsecure

	return nil
}

type BaseConnInfo struct {
	addr        *net.UDPAddr
	tlsinsecure bool
}

func NewBaseConnInfo(addr *net.UDPAddr, tlsinsecure bool) BaseConnInfo {
	return BaseConnInfo{addr: addr, tlsinsecure: tlsinsecure}
}

func NewBaseConnInfoFromString(s string) (BaseConnInfo, error) {
	as, tlsinsecure := network.ParseInsecure(s)

	return NewBaseConnInfoFromStringAddress(as, tlsinsecure)
}

func NewBaseConnInfoFromStringAddress(s string, tlsinsecure bool) (BaseConnInfo, error) {
	addr, err := net.ResolveUDPAddr("udp", s)
	if err != nil {
		return BaseConnInfo{}, util.ErrInvalid.Wrapf(err, "failed to parse BaseConnInfo")
	}

	return NewBaseConnInfo(addr, tlsinsecure), nil
}

func (c BaseConnInfo) Addr() net.Addr {
	return c.addr
}

func (c BaseConnInfo) UDPAddr() *net.UDPAddr {
	return c.addr
}

func (c BaseConnInfo) TLSInsecure() bool {
	return c.tlsinsecure
}

func (c BaseConnInfo) String() string {
	var addr string
	if c.addr != nil {
		addr = c.addr.String()
	}

	return network.ConnInfoToString(addr, c.tlsinsecure)
}

func (c BaseConnInfo) MarshalText() ([]byte, error) {
	return []byte(c.String()), nil
}

func (c *BaseConnInfo) UnmarshalText(b []byte) error {
	ci, err := NewBaseConnInfoFromString(string(b))
	if err != nil {
		return errors.Wrap(err, "failed to unmarshal BaseConnInfo")
	}

	*c = ci

	return nil
}

func (c BaseConnInfo) MarshalZerologObject(e *zerolog.Event) {
	e.
		Stringer("address", c.addr).
		Bool("tls_insecure", c.tlsinsecure)
}
