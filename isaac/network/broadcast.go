package isaacnetwork

import (
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/network/quicmemberlist"
	"github.com/spikeekips/mitum/network/quicstream"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/hint"
)

var CallbackBroadcastMessageHint = hint.MustNewHint("callback-broadcast-message-v0.0.1")

type CallbackBroadcaster struct {
	enc     encoder.Encoder
	m       *quicmemberlist.Memberlist
	cache   *util.GCache[string, []byte]
	localci quicstream.UDPConnInfo
	expire  time.Duration
}

func NewCallbackBroadcaster(
	localci quicstream.UDPConnInfo,
	enc encoder.Encoder,
	m *quicmemberlist.Memberlist,
) *CallbackBroadcaster {
	return &CallbackBroadcaster{
		localci: localci,
		enc:     enc,
		m:       m,
		cache:   util.NewLRUGCache("", ([]byte)(nil), 1<<13), //nolint:gomnd // NOTE big enough for big suffrage size
		expire:  time.Second * 30,                            //nolint:gomnd //...
	}
}

func (c *CallbackBroadcaster) Broadcast(id string, b []byte, notifych chan struct{}) error {
	if notifych != nil {
		defer func() {
			<-notifych
		}()
	}

	if !c.m.IsJoined() {
		return nil
	}

	e := util.StringErrorFunc("failed to broadcast")

	// NOTE save b in cache first
	c.cache.Set(id, b, c.expire)

	switch i, err := c.enc.Marshal(NewCallbackBroadcastMessage(id, c.localci)); {
	case err != nil:
		return e(err, "")
	default:
		c.m.Broadcast(quicmemberlist.NewBroadcast(i, id, notifych))
	}

	return nil
}

func (c *CallbackBroadcaster) RawMessage(id string) ([]byte, bool) {
	return c.cache.Get(id)
}

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
