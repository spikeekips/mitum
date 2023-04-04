package quicmemberlist

import (
	"encoding/json"
	"net"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/network"
	"github.com/spikeekips/mitum/network/quicstream"
	"github.com/spikeekips/mitum/util"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/localtime"
)

var MemberHint = hint.MustNewHint("memberlist-member-v0.0.1")

type Member interface {
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

type BaseMember struct {
	joinedAt time.Time
	addr     *net.UDPAddr
	name     string
	meta     memberMeta
	metab    []byte
	publish  NamedConnInfo
	hint.BaseHinter
}

func NewMember(
	name string,
	addr *net.UDPAddr,
	address base.Address,
	publickey base.Publickey,
	publish string,
	tlsinsecure bool,
) (BaseMember, error) {
	return newMemberWithMeta(name, addr, newMemberMeta(address, publickey, publish, tlsinsecure))
}

func newMemberFromMemberlist(node *memberlist.Node, enc *jsonenc.Encoder) (BaseMember, error) {
	e := util.StringErrorFunc("make Member from memberlist.Node")

	var meta memberMeta

	if err := meta.DecodeJSON(node.Meta, enc); err != nil {
		return BaseMember{}, e(err, "decode NodeMeta")
	}

	addr, _ := convertNetAddr(node)

	return newMemberWithMeta(node.Name, addr.(*net.UDPAddr), meta) //nolint:forcetypeassert // ...
}

func newMemberWithMeta(name string, addr *net.UDPAddr, meta memberMeta) (BaseMember, error) {
	metab, err := util.MarshalJSON(meta)
	if err != nil {
		return BaseMember{}, errors.WithMessage(err, "new Member")
	}

	return BaseMember{
		BaseHinter: hint.NewBaseHinter(MemberHint),
		name:       name,
		addr:       addr,
		joinedAt:   localtime.Now().UTC(),
		meta:       meta,
		metab:      metab,
		publish:    NewNamedConnInfo(meta.publish, meta.tlsinsecure),
	}, nil
}

func (n BaseMember) IsValid([]byte) error {
	e := util.ErrInvalid.Errorf("invalid BaseNode")

	if err := n.BaseHinter.IsValid(MemberHint.Type().Bytes()); err != nil {
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

func (n BaseMember) String() string {
	return n.publish.String()
}

func (n BaseMember) Name() string {
	return n.name
}

func (n BaseMember) UDPAddr() *net.UDPAddr {
	return n.addr
}

func (n BaseMember) UDPConnInfo() quicstream.UDPConnInfo {
	return quicstream.NewUDPConnInfo(n.addr, n.meta.tlsinsecure)
}

func (n BaseMember) TLSInsecure() bool {
	return n.meta.tlsinsecure
}

func (n BaseMember) JoinedAt() time.Time {
	return n.joinedAt
}

func (n BaseMember) Address() base.Address {
	return n.meta.address
}

func (n BaseMember) Publickey() base.Publickey {
	return n.meta.publickey
}

func (n BaseMember) Publish() NamedConnInfo {
	return n.publish
}

func (n BaseMember) MetaBytes() []byte {
	return n.metab
}

func (n BaseMember) HashBytes() []byte {
	return util.ConcatByters(n.meta.address, n.meta.publickey)
}

type baseMemberJSONMarshaler struct {
	Name     string          `json:"name"`
	Address  string          `json:"address"`
	JoinedAt time.Time       `json:"joined_at"`
	Meta     json.RawMessage `json:"meta"`
}

func (n BaseMember) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(baseMemberJSONMarshaler{
		Name:     n.name,
		Address:  n.addr.String(),
		JoinedAt: n.joinedAt,
		Meta:     n.metab,
	})
}

func (n *BaseMember) DecodeJSON(b []byte, enc *jsonenc.Encoder) error {
	e := util.StringErrorFunc("decode Member")

	var u baseMemberJSONMarshaler
	if err := json.Unmarshal(b, &u); err != nil {
		return e(err, "")
	}

	var meta memberMeta
	if err := meta.DecodeJSON(u.Meta, enc); err != nil {
		return e(err, "")
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

type memberMeta struct {
	address     base.Address
	publickey   base.Publickey
	publish     string
	tlsinsecure bool
}

func newMemberMeta(address base.Address, publickey base.Publickey, publish string, tlsinsecure bool) memberMeta {
	return memberMeta{
		address:     address,
		publickey:   publickey,
		publish:     publish,
		tlsinsecure: tlsinsecure,
	}
}

func (n memberMeta) IsValid([]byte) error {
	e := util.ErrInvalid.Errorf("invalid MemberMeta")

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

type memberMetaJSONMmarshaler struct {
	Address     base.Address   `json:"address"`
	Publickey   base.Publickey `json:"publickey"`
	Publish     string         `json:"publish"`
	TLSInsecure bool           `json:"tls_insecure"`
}

func (n memberMeta) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(memberMetaJSONMmarshaler{
		Address:     n.address,
		Publickey:   n.publickey,
		Publish:     n.publish,
		TLSInsecure: n.tlsinsecure,
	})
}

type memberMetaJSONUnmarshaler struct {
	Address     string `json:"address"`
	Publickey   string `json:"publickey"`
	Publish     string `json:"publish"`
	TLSInsecure bool   `json:"tls_insecure"`
}

func (n *memberMeta) DecodeJSON(b []byte, enc *jsonenc.Encoder) error {
	e := util.StringErrorFunc("decode MemberMeta")

	var u memberMetaJSONUnmarshaler
	if err := enc.Unmarshal(b, &u); err != nil {
		return e(err, "")
	}

	switch i, err := base.DecodeAddress(u.Address, enc); {
	case err != nil:
		return e(err, "decode node address")
	default:
		n.address = i
	}

	switch i, err := base.DecodePublickeyFromString(u.Publickey, enc); {
	case err != nil:
		return e(err, "decode publickey")
	default:
		n.publickey = i
	}

	n.publish = u.Publish
	n.tlsinsecure = u.TLSInsecure

	return nil
}
