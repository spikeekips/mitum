package quicmemberlist

import (
	"net"
	"time"

	"github.com/bluele/gcache"
	"github.com/hashicorp/memberlist"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/network/quicstream"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/logging"
)

type (
	DelegateNodeFunc      func(Node) error
	DelegateJoinedFunc    func(Node)
	DelegateLeftFunc      func(Node)
	DelegateStoreConnInfo func(quicstream.ConnInfo)
)

type Delegate struct {
	local         Node
	notifyMsgFunc func(b []byte)
	*logging.Logging
	qu *memberlist.TransmitLimitedQueue
}

func NewDelegate(
	local Node,
	numNodes func() int,
	notifyMsgFunc func(b []byte),
) *Delegate {
	qu := &memberlist.TransmitLimitedQueue{NumNodes: numNodes, RetransmitMult: 2} //nolint:gomnd //...

	if notifyMsgFunc == nil {
		notifyMsgFunc = func([]byte) {} //revive:disable-line:modifies-parameter
	}

	return &Delegate{
		Logging: logging.NewLogging(func(zctx zerolog.Context) zerolog.Context {
			return zctx.Str("module", "memberlist-delegate")
		}),
		local:         local,
		qu:            qu,
		notifyMsgFunc: notifyMsgFunc,
	}
}

func (d *Delegate) NodeMeta(int) []byte {
	return d.local.MetaBytes()
}

func (d *Delegate) NotifyMsg(b []byte) {
	d.Log().Trace().Int("message_length", len(b)).Str("msg", string(b)).Msg("user message received")

	d.notifyMsgFunc(b)
}

func (d *Delegate) QueueBroadcast(b memberlist.Broadcast) {
	d.qu.QueueBroadcast(b)
}

func (d *Delegate) GetBroadcasts(overhead, limit int) [][]byte {
	return d.qu.GetBroadcasts(overhead, limit)
}

func (*Delegate) LocalState(bool) []byte {
	return nil
}

func (*Delegate) MergeRemoteState([]byte, bool) {
}

type AliveDelegate struct {
	*logging.Logging
	enc             encoder.Encoder
	laddr           *net.UDPAddr
	allowf          DelegateNodeFunc // NOTE allowf controls which node can be entered or not
	storeconninfof  DelegateStoreConnInfo
	challengef      DelegateNodeFunc
	challengecache  gcache.Cache
	challengeexpire time.Duration
}

func NewAliveDelegate(
	enc encoder.Encoder,
	laddr *net.UDPAddr,
	challengef DelegateNodeFunc,
	allowf DelegateNodeFunc,
) *AliveDelegate {
	nallowf := allowf
	if nallowf == nil {
		nallowf = func(Node) error { return errors.Errorf("all nodes not allowed") }
	}

	nchallengef := challengef
	if nchallengef == nil {
		nchallengef = func(Node) error { return errors.Errorf("failed to challenge") }
	}

	return &AliveDelegate{
		Logging: logging.NewLogging(func(zctx zerolog.Context) zerolog.Context {
			return zctx.Str("module", "memberlist-alive-delegate")
		}),
		enc:             enc,
		laddr:           laddr,
		challengef:      nchallengef,
		allowf:          nallowf,
		storeconninfof:  func(quicstream.ConnInfo) {},
		challengecache:  gcache.New(1 << 9).LRU().Build(), //nolint:gomnd //...
		challengeexpire: time.Second * 30,                 //nolint:gomnd //...
	}
}

func (d *AliveDelegate) NotifyAlive(peer *memberlist.Node) error {
	if isEqualAddress(d.laddr, peer) { // NOTE filter local
		return nil
	}

	node, err := newNodeFromMemberlist(peer, d.enc)
	if err != nil {
		d.Log().Debug().Interface("peer", peer).Err(err).Msg("invalid peer")

		return errors.WithMessage(err, "not allowed to be alive")
	}

	var willchallenge bool

	switch i, err := d.challengecache.Get(node.Addr().String()); {
	case err != nil && errors.Is(err, gcache.KeyNotFoundError):
		// NOTE challenge with node publickey
		willchallenge = true
	default:
		willchallenge = time.Now().After(i.(time.Time).Add(d.challengeexpire)) //nolint:forcetypeassert //...
	}

	if willchallenge {
		if err := d.challengef(node); err != nil {
			return errors.WithMessage(err, "failed to challenge")
		}

		_ = d.challengecache.SetWithExpire(node.Addr().String(), time.Now(), d.challengeexpire)
	}

	l := d.Log().With().Object("node", node).Logger()

	if err := d.allowf(node); err != nil {
		l.Debug().Err(err).Msg("not allowed")

		return errors.WithMessage(err, "not allowed to be alive")
	}

	d.storeconninfof(node)

	l.Trace().Msg("notified alive")

	return nil
}

type EventsDelegate struct {
	*logging.Logging
	enc     encoder.Encoder
	joinedf DelegateJoinedFunc
	leftf   DelegateLeftFunc
}

func NewEventsDelegate(
	enc encoder.Encoder,
	joinedf DelegateJoinedFunc,
	leftf DelegateLeftFunc,
) *EventsDelegate {
	njoinedf := joinedf
	if njoinedf == nil {
		njoinedf = func(Node) {}
	}

	nleftf := leftf
	if nleftf == nil {
		nleftf = func(Node) {}
	}

	return &EventsDelegate{
		Logging: logging.NewLogging(func(zctx zerolog.Context) zerolog.Context {
			return zctx.Str("module", "memberlist-events-delegate")
		}),
		enc:     enc,
		joinedf: njoinedf,
		leftf:   nleftf,
	}
}

func (d *EventsDelegate) NotifyJoin(peer *memberlist.Node) {
	node, err := newNodeFromMemberlist(peer, d.enc)
	if err != nil {
		d.Log().Error().Err(err).Interface("peer", peer).Msg("invalid peer")

		return
	}

	d.Log().Debug().Object("peer", node).Msg("notified join")

	d.joinedf(node)
}

func (d *EventsDelegate) NotifyLeave(peer *memberlist.Node) {
	node, err := newNodeFromMemberlist(peer, d.enc)
	if err != nil {
		d.Log().Error().Err(err).Interface("peer", peer).Msg("invalid peer")

		return
	}

	d.Log().Debug().Object("peer", node).Msg("notified leave")

	d.leftf(node)
}

func (d *EventsDelegate) NotifyUpdate(peer *memberlist.Node) {
	node, err := newNodeFromMemberlist(peer, d.enc)
	if err != nil {
		d.Log().Error().Err(err).Interface("peer", peer).Msg("invalid peer")

		return
	}

	d.Log().Debug().Object("peer", node).Msg("notified update")
}

func isEqualAddress(a, b interface{}) bool {
	na, err := convertNetAddr(a)
	if err != nil {
		return false
	}

	nb, err := convertNetAddr(b)
	if err != nil {
		return false
	}

	switch {
	case na.Network() != nb.Network():
		return false
	case na.String() != nb.String():
		return false
	default:
		return true
	}
}

func convertNetAddr(a interface{}) (net.Addr, error) {
	switch t := a.(type) {
	case Node:
		return t.UDPAddr(), nil
	case quicstream.ConnInfo:
		return t.UDPAddr(), nil
	case *memberlist.Node:
		return &net.UDPAddr{IP: t.Addr, Port: int(t.Port)}, nil
	case *net.TCPAddr:
		return t, nil
	case *net.UDPAddr:
		return t, nil
	default:
		return nil, errors.Errorf("unknown net.Addr, %T", a)
	}
}
