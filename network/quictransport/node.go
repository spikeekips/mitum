package quictransport

import (
	"encoding/json"
	"net"
	"strings"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
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
	// BLOCK add IsValid()?
	Address() *net.UDPAddr
	Insecure() bool
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
	hint.BaseHinter
	name     string
	addr     *net.UDPAddr
	joinedAt time.Time
	meta     NodeMeta
	metab    []byte
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
	switch hinter, err := enc.Decode(node.Meta); {
	case err != nil:
		return BaseNode{}, e(err, "failed to decode NodeMeta")
	default:
		i, ok := hinter.(NodeMeta)
		if !ok {
			return BaseNode{}, e(err, "failed to decode NodeMeta; not NodeMeta, %T", hinter)
		}

		meta = i
	}

	addr, _ := convertNetAddr(node)

	return NewNode(node.Name, addr.(*net.UDPAddr), meta)
}

func (n BaseNode) Name() string {
	return n.name
}

func (n BaseNode) Node() base.Address {
	return n.meta.Node()
}

func (n BaseNode) Address() *net.UDPAddr {
	return n.addr
}

func (n BaseNode) Insecure() bool {
	return n.meta.Insecure()
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
		Bool("insecure", n.meta.Insecure()).
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
	hint.BaseHinter
	node     base.Address
	insecure bool
}

func NewNodeMeta(node base.Address, insecure bool) NodeMeta {
	return NodeMeta{
		BaseHinter: hint.NewBaseHinter(NodeMetaHint),
		node:       node,
		insecure:   insecure,
	}
}

func (n NodeMeta) Node() base.Address {
	return n.node
}

func (n NodeMeta) Insecure() bool {
	return n.insecure
}

type nodeMetaJSONMmarshaler struct {
	hint.BaseHinter
	Node     base.Address `json:"node"`
	Insecure bool         `json:"insecure"`
}

func (n NodeMeta) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(nodeMetaJSONMmarshaler{
		BaseHinter: n.BaseHinter,
		Node:       n.node,
		Insecure:   n.insecure,
	})
}

type nodeMetaJSONUnmarshaler struct {
	Node     string `json:"node"`
	Insecure bool   `json:"insecure"`
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

	n.insecure = u.Insecure

	return nil
}

type BaseConnInfo struct {
	addr     *net.UDPAddr
	insecure bool
}

func NewBaseConnInfo(addr *net.UDPAddr, insecure bool) BaseConnInfo {
	return BaseConnInfo{addr: addr, insecure: insecure}
}

func NewBaseConnInfoFromString(s string) (BaseConnInfo, error) {
	var as string
	var insecure bool
	switch i := strings.Index(s, "#"); {
	case i < 0:
		as = s
	default:
		as = s[:i]
		if len(s[i:]) > 0 {
			insecure = strings.ToLower(s[i+1:]) == "insecure"
		}
	}

	addr, err := net.ResolveUDPAddr("udp", as)
	if err != nil {
		return BaseConnInfo{}, errors.Wrap(err, "failed to parse net.UDPAddr")
	}

	return NewBaseConnInfo(addr, insecure), nil
}

func (c BaseConnInfo) Address() *net.UDPAddr {
	return c.addr
}

func (c BaseConnInfo) Insecure() bool {
	return c.insecure
}

func (c BaseConnInfo) String() string {
	insecure := ""
	if c.insecure {
		insecure = "#insecure"
	}

	return c.addr.String() + insecure
}

func (c BaseConnInfo) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(struct {
		Addr     *net.UDPAddr
		Insecure bool
	}{
		Addr:     c.addr,
		Insecure: c.insecure,
	})
}

func (c BaseConnInfo) MarshalZerologObject(e *zerolog.Event) {
	e.
		Stringer("address", c.addr).
		Bool("insecure", c.insecure)
}
