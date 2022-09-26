package quicmemberlist

import (
	"encoding/json"
	"net"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/network"
	"github.com/spikeekips/mitum/network/quicstream"
	"github.com/spikeekips/mitum/util"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/localtime"
)

var NodeHint = hint.MustNewHint("memberlist-node-v0.0.1")

type Node interface {
	util.IsValider
	UDPAddr() *net.UDPAddr
	UDPConnInfo() quicstream.UDPConnInfo
	Name() string
	Address() base.Address
	Publickey() base.Publickey
	Publish() NamedConnInfo
	JoinedAt() time.Time
	MetaBytes() []byte
	HashBytes() []byte
}

type BaseNode struct {
	joinedAt time.Time
	addr     *net.UDPAddr
	name     string
	meta     nodeMeta
	metab    []byte
	publish  NamedConnInfo
	hint.BaseHinter
}

func NewNode(
	name string,
	addr *net.UDPAddr,
	address base.Address,
	publickey base.Publickey,
	publish string,
	tlsinsecure bool,
) (BaseNode, error) {
	return newNodeWithMeta(name, addr, newNodeMeta(address, publickey, publish, tlsinsecure))
}

func newNodeFromMemberlist(node *memberlist.Node, enc *jsonenc.Encoder) (BaseNode, error) {
	e := util.StringErrorFunc("failed to make Node from memberlist.Node")

	var meta nodeMeta

	if err := meta.DecodeJSON(node.Meta, enc); err != nil {
		return BaseNode{}, e(err, "failed to decode NodeMeta")
	}

	addr, _ := convertNetAddr(node)

	return newNodeWithMeta(node.Name, addr.(*net.UDPAddr), meta) //nolint:forcetypeassert // ...
}

func newNodeWithMeta(name string, addr *net.UDPAddr, meta nodeMeta) (BaseNode, error) {
	metab, err := util.MarshalJSON(meta)
	if err != nil {
		return BaseNode{}, errors.WithMessage(err, "failed to create Node")
	}

	return BaseNode{
		BaseHinter: hint.NewBaseHinter(NodeHint),
		name:       name,
		addr:       addr,
		joinedAt:   localtime.UTCNow(),
		meta:       meta,
		metab:      metab,
		publish:    NewNamedConnInfo(meta.publish, meta.tlsinsecure),
	}, nil
}

func (n BaseNode) IsValid([]byte) error {
	e := util.ErrInvalid.Errorf("invalid BaseNode")

	if err := n.BaseHinter.IsValid(NodeHint.Type().Bytes()); err != nil {
		return e.Wrap(err)
	}

	if err := util.CheckIsValiders(nil, false,
		util.DummyIsValider(func([]byte) error {
			if n.addr.IP.IsUnspecified() {
				return errors.Errorf("empty udp addr")
			}

			return nil
		}),
		util.DummyIsValider(func([]byte) error {
			if n.joinedAt.IsZero() {
				return errors.Errorf("empty joined at time")
			}

			return nil
		}),
		util.DummyIsValider(func([]byte) error {
			if len(n.name) < 1 {
				return errors.Errorf("empty name")
			}

			return nil
		}),
		util.DummyIsValider(func([]byte) error {
			if len(n.metab) < 1 {
				return errors.Errorf("empty meta")
			}

			return nil
		}),
		n.meta,
	); err != nil {
		return e.Wrap(err)
	}

	return nil
}

func (n BaseNode) String() string {
	return n.publish.String()
}

func (n BaseNode) Name() string {
	return n.name
}

func (n BaseNode) UDPAddr() *net.UDPAddr {
	return n.addr
}

func (n BaseNode) UDPConnInfo() quicstream.UDPConnInfo {
	return quicstream.NewUDPConnInfo(n.addr, n.meta.tlsinsecure)
}

func (n BaseNode) TLSInsecure() bool {
	return n.meta.tlsinsecure
}

func (n BaseNode) JoinedAt() time.Time {
	return n.joinedAt
}

func (n BaseNode) Address() base.Address {
	return n.meta.address
}

func (n BaseNode) Publickey() base.Publickey {
	return n.meta.publickey
}

func (n BaseNode) Publish() NamedConnInfo {
	return n.publish
}

func (n BaseNode) MetaBytes() []byte {
	return n.metab
}

func (n BaseNode) HashBytes() []byte {
	return util.ConcatByters(n.meta.address, n.meta.publickey)
}

func (n BaseNode) MarshalZerologObject(e *zerolog.Event) {
	e.
		Str("name", n.name).
		Stringer("node", n.meta.address).
		Stringer("address", n.addr).
		Bool("tls_insecure", n.meta.tlsinsecure).
		Time("joined_at", n.joinedAt)
}

func (n BaseNode) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Name     string
		Address  string
		JoinedAt time.Time `json:"joined_at"`
		Meta     json.RawMessage
	}{
		Name:     n.name,
		Address:  n.addr.String(),
		JoinedAt: n.joinedAt,
		Meta:     n.metab,
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

	var meta nodeMeta
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

type nodeMeta struct {
	address     base.Address
	publickey   base.Publickey
	publish     string
	tlsinsecure bool
}

func newNodeMeta(address base.Address, publickey base.Publickey, publish string, tlsinsecure bool) nodeMeta {
	return nodeMeta{
		address:     address,
		publickey:   publickey,
		publish:     publish,
		tlsinsecure: tlsinsecure,
	}
}

func (n nodeMeta) IsValid([]byte) error {
	e := util.ErrInvalid.Errorf("invalid NodeMeta")

	if err := util.CheckIsValiders(nil, false,
		n.address,
		n.publickey,
		util.DummyIsValider(func([]byte) error {
			return network.IsValidAddr(n.publish)
		}),
	); err != nil {
		return e.Wrap(err)
	}

	return nil
}

type nodeMetaJSONMmarshaler struct {
	Address     base.Address   `json:"address"`
	Publickey   base.Publickey `json:"publickey"`
	Publish     string         `json:"publish"`
	TLSInsecure bool           `json:"tls_insecure"`
}

func (n nodeMeta) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(nodeMetaJSONMmarshaler{
		Address:     n.address,
		Publickey:   n.publickey,
		Publish:     n.publish,
		TLSInsecure: n.tlsinsecure,
	})
}

type nodeMetaJSONUnmarshaler struct {
	Address     string `json:"address"`
	Publickey   string `json:"publickey"`
	Publish     string `json:"publish"`
	TLSInsecure bool   `json:"tls_insecure"`
}

func (n *nodeMeta) DecodeJSON(b []byte, enc *jsonenc.Encoder) error {
	e := util.StringErrorFunc("failed to decode NodeMta")

	var u nodeMetaJSONUnmarshaler
	if err := enc.Unmarshal(b, &u); err != nil {
		return e(err, "")
	}

	switch i, err := base.DecodeAddress(u.Address, enc); {
	case err != nil:
		return e(err, "failed to decode node")
	default:
		n.address = i
	}

	switch i, err := base.DecodePublickeyFromString(u.Publickey, enc); {
	case err != nil:
		return e(err, "failed to decode publickey")
	default:
		n.publickey = i
	}

	n.publish = u.Publish
	n.tlsinsecure = u.TLSInsecure

	return nil
}
