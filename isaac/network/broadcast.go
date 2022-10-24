package isaacnetwork

import (
	"time"

	"github.com/spikeekips/mitum/network/quicmemberlist"
	"github.com/spikeekips/mitum/network/quicstream"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
)

type CallbackBroadcaster struct {
	enc     encoder.Encoder
	m       *quicmemberlist.Memberlist
	cache   *util.GCacheObjectPool
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
		cache:   util.NewGCacheObjectPool(1 << 13), //nolint:gomnd // NOTE big enough for big suffrage size
		expire:  time.Second * 30,                  //nolint:gomnd //...
	}
}

func (c *CallbackBroadcaster) Broadcast(id string, b []byte, notifych chan struct{}) error {
	defer func() {
		if notifych != nil {
			<-notifych
		}
	}()

	if !c.m.IsJoined() {
		return nil
	}

	e := util.StringErrorFunc("failed to broadcast")

	// NOTE save b in cache first
	c.cache.Set(id, b, &c.expire)

	switch i, err := c.enc.Marshal(NewCallbackBroadcastHeader(id, c.localci)); {
	case err != nil:
		return e(err, "")
	default:
		c.m.Broadcast(quicmemberlist.NewBroadcast(i, id, notifych))
	}

	return nil
}

func (c *CallbackBroadcaster) RawMessage(id string) ([]byte, bool) {
	switch i, found := c.cache.Get(id); {
	case !found:
		return nil, false
	case i == nil:
		return nil, found
	default:
		return i.([]byte), true //nolint:forcetypeassert //...
	}
}
