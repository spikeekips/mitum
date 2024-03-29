package quicmemberlist

import (
	"sync"

	"github.com/hashicorp/memberlist"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/network/quicstream"
	quicstreamheader "github.com/spikeekips/mitum/network/quicstream/header"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/hint"
)

type Broadcast struct {
	notifych     chan struct{} // revive:disable-line:nested-structs
	id           string
	b            []byte
	finishedonce sync.Once
}

func NewBroadcast(b []byte, id string, notifych chan struct{}) *Broadcast {
	// NOTE size of b should be checked; it should be smaller than
	// UDPBufferSize.
	return &Broadcast{b: b, id: id, notifych: notifych}
}

func (b *Broadcast) Invalidates(o memberlist.Broadcast) bool {
	i, ok := o.(*Broadcast)

	return ok && i.id == b.id
}

func (b *Broadcast) Message() []byte {
	return b.b
}

func (b *Broadcast) Finished() {
	b.finishedonce.Do(func() {
		if b.notifych == nil {
			return
		}

		close(b.notifych)
	})
}

func (*Broadcast) UniqueBroadcast() {}

func (b *Broadcast) MarshalZerologObject(e *zerolog.Event) {
	e.Str("id", b.id)
}

var ConnInfoBroadcastMessageHint = hint.MustNewHint("conninfo-broadcast-message-v0.0.1")

type ConnInfoBroadcastMessage struct {
	id string
	ci quicstream.ConnInfo
	hint.BaseHinter
}

func NewConnInfoBroadcastMessage(id string, ci quicstream.ConnInfo) ConnInfoBroadcastMessage {
	return ConnInfoBroadcastMessage{
		BaseHinter: hint.NewBaseHinter(ConnInfoBroadcastMessageHint),
		id:         id,
		ci:         ci,
	}
}

func (m ConnInfoBroadcastMessage) IsValid([]byte) error {
	e := util.ErrInvalid.Errorf("invalid ConnInfoBroadcastMessage")

	if err := m.BaseHinter.IsValid(ConnInfoBroadcastMessageHint.Type().Bytes()); err != nil {
		return e.Wrap(err)
	}

	if len(m.id) < 1 {
		return e.Errorf("empty id")
	}

	if err := m.ci.IsValid(nil); err != nil {
		return e.Wrap(err)
	}

	return nil
}

func (m ConnInfoBroadcastMessage) ID() string {
	return m.id
}

func (m ConnInfoBroadcastMessage) ConnInfo() quicstream.ConnInfo {
	return m.ci
}

type connInfoBroadcastMessageJSONMarshaler struct {
	ID string              `json:"id"`
	CI quicstream.ConnInfo `json:"conn_info"` //nolint:tagliatelle //...
	hint.BaseHinter
}

func (m ConnInfoBroadcastMessage) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(connInfoBroadcastMessageJSONMarshaler{
		BaseHinter: m.BaseHinter,
		ID:         m.id,
		CI:         m.ci,
	})
}

func (m *ConnInfoBroadcastMessage) UnmarshalJSON(b []byte) error {
	var u connInfoBroadcastMessageJSONMarshaler

	if err := util.UnmarshalJSON(b, &u); err != nil {
		return errors.WithMessage(err, "unmarshal ConnInfoBroadcastMessage")
	}

	m.id = u.ID
	m.ci = u.CI

	return nil
}

var CallbackBroadcastMessageHeaderHint = hint.MustNewHint("callback-broadcast-message-header-v0.0.1")

type CallbackBroadcastMessageHeader struct {
	id string
	quicstreamheader.BaseRequestHeader
}

func NewCallbackBroadcastMessageHeader(id string, prefix quicstream.HandlerPrefix) CallbackBroadcastMessageHeader {
	return CallbackBroadcastMessageHeader{
		BaseRequestHeader: quicstreamheader.NewBaseRequestHeader(CallbackBroadcastMessageHeaderHint, prefix),
		id:                id,
	}
}

func (h CallbackBroadcastMessageHeader) IsValid([]byte) error {
	e := util.ErrInvalid.Errorf("invalid CallbackBroadcastMessageHeader")

	if err := h.BaseHinter.IsValid(CallbackBroadcastMessageHeaderHint.Type().Bytes()); err != nil {
		return e.Wrap(err)
	}

	if len(h.id) < 1 {
		return e.Errorf("empty id")
	}

	return nil
}

func (h CallbackBroadcastMessageHeader) ID() string {
	return h.id
}

type callbackBroadcastMessageHeaderJSONMarshaler struct {
	ID string `json:"id"`
}

func (h CallbackBroadcastMessageHeader) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(struct {
		callbackBroadcastMessageHeaderJSONMarshaler
		quicstreamheader.BaseRequestHeader
	}{
		BaseRequestHeader: h.BaseRequestHeader,
		callbackBroadcastMessageHeaderJSONMarshaler: callbackBroadcastMessageHeaderJSONMarshaler{
			ID: h.id,
		},
	})
}

func (h *CallbackBroadcastMessageHeader) UnmarshalJSON(b []byte) error {
	e := util.StringError("unmarshal CallbackBroadcastMessageHeader")

	var u callbackBroadcastMessageHeaderJSONMarshaler

	if err := util.UnmarshalJSON(b, &u); err != nil {
		return e.Wrap(err)
	}

	if err := util.UnmarshalJSON(b, &h.BaseRequestHeader); err != nil {
		return e.Wrap(err)
	}

	h.id = u.ID

	return nil
}

var EnsureBroadcastMessageHeaderHint = hint.MustNewHint("ensure-broadcast-message-header-v0.0.1")

type EnsureBroadcastMessageHeader struct {
	id string
	base.BaseNodeSign
	quicstreamheader.BaseRequestHeader
}

func NewEnsureBroadcastMessageHeader(
	id string,
	prefix quicstream.HandlerPrefix,
	node base.Address,
	signer base.Privatekey,
	networkID base.NetworkID,
) (EnsureBroadcastMessageHeader, error) {
	nodeSign, err := base.NewBaseNodeSignFromBytes(node, signer, networkID, []byte(id))
	if err != nil {
		return EnsureBroadcastMessageHeader{}, err
	}

	return EnsureBroadcastMessageHeader{
		BaseRequestHeader: quicstreamheader.NewBaseRequestHeader(EnsureBroadcastMessageHeaderHint, prefix),
		BaseNodeSign:      nodeSign,
		id:                id,
	}, nil
}

func (h EnsureBroadcastMessageHeader) IsValid([]byte) error {
	e := util.ErrInvalid.Errorf("invalid EnsureBroadcastMessageHeader")

	if err := h.BaseHinter.IsValid(EnsureBroadcastMessageHeaderHint.Type().Bytes()); err != nil {
		return e.Wrap(err)
	}

	if err := h.BaseNodeSign.IsValid(nil); err != nil {
		return e.Wrap(err)
	}

	if len(h.id) < 1 {
		return e.Errorf("empty id")
	}

	return nil
}

func (h EnsureBroadcastMessageHeader) ID() string {
	return h.id
}

type ensureBroadcastMessageHeaderJSONMarshaler struct {
	ID string `json:"id"`
}

func (h EnsureBroadcastMessageHeader) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(struct {
		ensureBroadcastMessageHeaderJSONMarshaler
		base.BaseNodeSignJSONMarshaler
		quicstreamheader.BaseRequestHeader
	}{
		BaseRequestHeader:         h.BaseRequestHeader,
		BaseNodeSignJSONMarshaler: h.BaseNodeSign.JSONMarshaler(),
		ensureBroadcastMessageHeaderJSONMarshaler: ensureBroadcastMessageHeaderJSONMarshaler{
			ID: h.id,
		},
	})
}

func (h *EnsureBroadcastMessageHeader) DecodeJSON(b []byte, enc encoder.Encoder) error {
	e := util.StringError("decode EnsureBroadcastMessageHeader")

	var u ensureBroadcastMessageHeaderJSONMarshaler

	if err := enc.Unmarshal(b, &u); err != nil {
		return e.Wrap(err)
	}

	if err := enc.Unmarshal(b, &h.BaseHeader); err != nil {
		return e.Wrap(err)
	}

	if err := h.BaseNodeSign.DecodeJSON(b, enc); err != nil {
		return e.Wrap(err)
	}

	h.id = u.ID

	return nil
}
