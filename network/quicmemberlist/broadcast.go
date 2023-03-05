package quicmemberlist

import (
	"sync"

	"github.com/hashicorp/memberlist"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/network/quicstream"
	"github.com/spikeekips/mitum/util"
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

var CallbackBroadcastMessageHint = hint.MustNewHint("callback-broadcast-message-v0.0.1")

type CallbackBroadcastMessage struct {
	id string
	ci quicstream.UDPConnInfo
	hint.BaseHinter
}

func NewCallbackBroadcastMessage(id string, ci quicstream.UDPConnInfo) CallbackBroadcastMessage {
	return CallbackBroadcastMessage{
		BaseHinter: hint.NewBaseHinter(CallbackBroadcastMessageHint),
		id:         id,
		ci:         ci,
	}
}

func (m CallbackBroadcastMessage) IsValid([]byte) error {
	e := util.ErrInvalid.Errorf("invalid CallbackBroadcastMessage")

	if err := m.BaseHinter.IsValid(CallbackBroadcastMessageHint.Type().Bytes()); err != nil {
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

func (m CallbackBroadcastMessage) ID() string {
	return m.id
}

func (m CallbackBroadcastMessage) ConnInfo() quicstream.UDPConnInfo {
	return m.ci
}

type callbackBroadcastMessageJSONMarshaler struct {
	ID string                 `json:"id"`
	CI quicstream.UDPConnInfo `json:"conn_info"` //nolint:tagliatelle //...
	hint.BaseHinter
}

func (m CallbackBroadcastMessage) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(callbackBroadcastMessageJSONMarshaler{
		BaseHinter: m.BaseHinter,
		ID:         m.id,
		CI:         m.ci,
	})
}

func (m *CallbackBroadcastMessage) UnmarshalJSON(b []byte) error {
	var u callbackBroadcastMessageJSONMarshaler

	if err := util.UnmarshalJSON(b, &u); err != nil {
		return errors.WithMessage(err, "failed to decode CallbackBroadcastMessage")
	}

	m.id = u.ID
	m.ci = u.CI

	return nil
}

var CallbackBroadcastMessageHeaderHint = hint.MustNewHint("callback-message-header-v0.0.1")

type CallbackBroadcastMessageHeader struct {
	id string
	quicstream.BaseHeader
}

func NewCallbackBroadcastMessageHeader(id string, prefix string) CallbackBroadcastMessageHeader {
	return CallbackBroadcastMessageHeader{
		BaseHeader: quicstream.NewBaseHeader(CallbackBroadcastMessageHeaderHint, prefix),
		id:         id,
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
		quicstream.BaseHeaderJSONMarshaler
	}{
		BaseHeaderJSONMarshaler: h.BaseHeader.JSONMarshaler(),
		callbackBroadcastMessageHeaderJSONMarshaler: callbackBroadcastMessageHeaderJSONMarshaler{
			ID: h.id,
		},
	})
}

func (h *CallbackBroadcastMessageHeader) UnmarshalJSON(b []byte) error {
	e := util.StringErrorFunc("failed to unmarshal CallbackBroadcastMessageHeader")

	var u callbackBroadcastMessageHeaderJSONMarshaler

	if err := util.UnmarshalJSON(b, &u); err != nil {
		return e(err, "")
	}

	if err := util.UnmarshalJSON(b, &h.BaseHeader); err != nil {
		return e(err, "")
	}

	h.id = u.ID

	return nil
}
