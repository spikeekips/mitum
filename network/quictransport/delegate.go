package quictransport

import (
	"net"

	"github.com/hashicorp/memberlist"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/logging"
)

type (
	DelegateAllowFunc     func(Node) error
	DelegateJoinedFunc    func(Node)
	DelegateLeftFunc      func(Node)
	DelegateStoreConnInfo func(ConnInfo)
)

type Delegate struct {
	local Node
	*logging.Logging
	qu *memberlist.TransmitLimitedQueue
}

func NewDelegate(local Node, numNodes func() int) *Delegate {
	qu := &memberlist.TransmitLimitedQueue{NumNodes: numNodes, RetransmitMult: 2}

	return &Delegate{
		Logging: logging.NewLogging(func(zctx zerolog.Context) zerolog.Context {
			return zctx.Str("module", "memberlist-delegate")
		}),
		local: local,
		qu:    qu,
	}
}

func (d *Delegate) NodeMeta(int) []byte {
	return d.local.MetaBytes()
}

func (d *Delegate) NotifyMsg(b []byte) {
	d.Log().Trace().Int("message_length", len(b)).Str("msg", string(b)).Msg("user message received")
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
	enc            encoder.Encoder
	laddr          *net.UDPAddr
	allowf         DelegateAllowFunc // NOTE allowf controls which node can be entered or not
	storeconninfof DelegateStoreConnInfo
}

func NewAliveDelegate(enc encoder.Encoder, laddr *net.UDPAddr, allowf DelegateAllowFunc) *AliveDelegate {
	nallowf := allowf
	if nallowf == nil {
		nallowf = func(Node) error { return errors.Errorf("all nodes not allowed") }
	}

	return &AliveDelegate{
		Logging: logging.NewLogging(func(zctx zerolog.Context) zerolog.Context {
			return zctx.Str("module", "memberlist-alive-delegate")
		}),
		enc:            enc,
		laddr:          laddr,
		allowf:         nallowf,
		storeconninfof: func(ConnInfo) {},
	}
}

func (d *AliveDelegate) NotifyAlive(peer *memberlist.Node) error {
	if isEqualAddress(d.laddr, peer) { // NOTE filter local
		return nil
	}

	node, err := newNodeFromMemberlist(peer, d.enc)
	if err != nil {
		d.Log().Debug().Interface("peer", peer).Err(err).Msg("invalid peer")

		return errors.Wrap(err, "not allowed to be alive")
	}

	l := d.Log().With().Object("node", node).Logger()

	if err := d.allowf(node); err != nil {
		l.Debug().Err(err).Msg("not allowed")

		return errors.Wrap(err, "not allowed to be alive")
	}

	d.storeconninfof(node)

	l.Debug().Msg("notified alive")

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
		return t.Address(), nil
	case ConnInfo:
		return t.Address(), nil
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
