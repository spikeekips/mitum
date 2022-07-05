package quicmemberlist

import (
	"context"
	"io"
	"log"
	"math"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/network/quicstream"
	"github.com/spikeekips/mitum/util"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/logging"
)

type Memberlist struct {
	local Node
	enc   *jsonenc.Encoder
	*logging.Logging
	*util.ContextDaemon
	mconfig        *memberlist.Config
	m              *memberlist.Memberlist
	delegate       *Delegate
	members        *membersPool
	cicache        *util.GCacheObjectPool
	oneMemberLimit int
	sync.Mutex
	joinedLock sync.Mutex
}

func NewMemberlist(
	local Node,
	enc *jsonenc.Encoder,
	config *memberlist.Config,
	oneMemberLimit int,
) (*Memberlist, error) {
	srv := &Memberlist{
		Logging: logging.NewLogging(func(zctx zerolog.Context) zerolog.Context {
			return zctx.Str("module", "memberlist").Str("name", config.Name)
		}),
		local:          local,
		enc:            enc,
		oneMemberLimit: oneMemberLimit,
		members:        newMembersPool(),
		cicache:        util.NewGCacheObjectPool(1 << 9), //nolint:gomnd //...
	}

	if err := srv.patchMemberlistConfig(config); err != nil {
		return nil, errors.WithMessage(err, "wrong memberlist.Config")
	}

	srv.ContextDaemon = util.NewContextDaemon(srv.start)

	return srv, nil
}

func (srv *Memberlist) Start() error {
	m, err := memberlist.Create(srv.mconfig)
	if err != nil {
		return errors.Wrap(err, "failed to create memberlist")
	}

	srv.m = m

	return srv.ContextDaemon.Start()
}

func (srv *Memberlist) Join(cis []quicstream.UDPConnInfo) error {
	e := util.StringErrorFunc("failed to join")

	if _, found := util.CheckSliceDuplicated(cis, func(_ interface{}, i int) string {
		return cis[i].UDPAddr().String()
	}); found {
		return e(nil, "duplicated join url found")
	}

	stringurls := make([]string, len(cis))

	for i := range cis {
		ci := cis[i]

		stringurls[i] = ci.UDPAddr().String()
		srv.cicache.Set(ci.UDPAddr().String(), ci, nil)
	}

	l := srv.Log().With().Strs("urls", stringurls).Logger()
	l.Debug().Msg("trying to join")

	switch joined, err := srv.m.Join(stringurls); {
	case err != nil:
		l.Error().Err(err).Msg("failed to join")
		return e(err, "")
	case joined < 1:
		l.Debug().Msg("did not join to any nodes")

		return e(nil, "nothing joined")
	}

	l.Debug().Msg("joined")

	return nil
}

func (srv *Memberlist) Leave(timeout time.Duration) error {
	if err := srv.m.Leave(timeout); err != nil {
		return errors.Wrap(err, "failed to leave")
	}

	return nil
}

func (srv *Memberlist) MembersLen() int {
	return srv.members.Len()
}

func (srv *Memberlist) Members(f func(node Node) bool) {
	srv.members.Traverse(f)
}

func (srv *Memberlist) IsJoined() bool {
	return srv.members.Len() > 1
}

func (srv *Memberlist) Broadcast(b memberlist.Broadcast) {
	if srv.delegate == nil {
		return
	}

	srv.Log().Trace().Interface("broadcast", b).Msg("enqueue broadcast")

	srv.delegate.QueueBroadcast(b)
}

func (srv *Memberlist) start(ctx context.Context) error {
	<-ctx.Done()

	// NOTE leave before shutdown
	_ = srv.m.Leave(time.Second * 3) //nolint:gomnd //...

	if err := srv.m.Shutdown(); err != nil {
		return errors.Wrap(err, "failed to shutdown memberlist")
	}

	return ctx.Err()
}

func (srv *Memberlist) patchMemberlistConfig(config *memberlist.Config) error { // revive:disable-line:function-length
	if config.Transport == nil {
		return errors.Errorf("empty Transport")
	}

	if config.Delegate == nil {
		return errors.Errorf("delegate missing")
	}

	if config.Alive == nil {
		return errors.Errorf("alive delegate missing")
	}

	switch i, ok := config.Transport.(*Transport); {
	case !ok:
		return errors.Errorf("transport should be *quicmemberlist.Transport, not %T", config.Transport)
	default:
		i.getconninfof = func(addr *net.UDPAddr) quicstream.UDPConnInfo {
			j, found := srv.cicache.Get(addr.String())
			if !found {
				return quicstream.NewUDPConnInfo(addr, true)
			}

			return j.(quicstream.UDPConnInfo) //nolint:forcetypeassert // ...
		}
	}

	config.SecretKey = nil

	if config.Delegate != nil {
		if i, ok := config.Delegate.(*Delegate); ok {
			srv.delegate = i
			srv.delegate.qu.NumNodes = srv.MembersLen
		}
	}

	if i, ok := config.Alive.(*AliveDelegate); ok {
		i.storeconninfof = func(ci quicstream.UDPConnInfo) {
			srv.cicache.Set(ci.UDPAddr().String(), ci, nil)
		}

		origallowf := i.allowf
		i.allowf = func(node Node) error {
			if err := srv.allowNode(node); err != nil {
				return err
			}

			return origallowf(node)
		}
	}

	switch {
	case config.Events == nil:
		config.Events = NewEventsDelegate(
			srv.enc,
			srv.whenJoined,
			srv.whenLeft,
		)
	default:
		if i, ok := config.Events.(*EventsDelegate); ok {
			joinedforig := i.joinedf
			i.joinedf = func(node Node) {
				srv.whenJoined(node)

				joinedforig(node)
			}

			leftforig := i.leftf
			i.leftf = func(node Node) {
				srv.whenLeft(node)

				leftforig(node)
			}
		}
	}

	srv.mconfig = config

	return nil
}

func (srv *Memberlist) whenJoined(node Node) {
	srv.joinedLock.Lock()
	defer srv.joinedLock.Unlock()

	srv.members.Set(node)

	srv.Log().Debug().Interface("node", node).Msg("node joined")
}

func (srv *Memberlist) whenLeft(node Node) {
	srv.joinedLock.Lock()
	defer srv.joinedLock.Unlock()

	_ = srv.members.Remove(node.UDPAddr())

	srv.Log().Debug().Interface("node", node).Msg("node left")
}

func (srv *Memberlist) allowNode(node Node) error {
	if srv.members.NodesLen(node.Address()) == srv.oneMemberLimit {
		return errors.Errorf("over member limit; %q", node.Name())
	}

	return nil
}

func (srv *Memberlist) SetLogging(l *logging.Logging) *logging.Logging {
	ds := []interface{}{
		srv.mconfig.Delegate,
		srv.mconfig.Events,
		srv.mconfig.Alive,
		srv.mconfig.Transport,
	}

	for i := range ds {
		if j, ok := ds[i].(logging.SetLogging); ok {
			_ = j.SetLogging(l)
		}
	}

	return srv.Logging.SetLogging(l)
}

func BasicMemberlistConfig(name string, bind, advertise *net.UDPAddr) *memberlist.Config {
	config := memberlist.DefaultWANConfig()
	config.Name = name
	config.BindAddr = bind.IP.String()
	config.BindPort = bind.Port
	config.AdvertiseAddr = advertise.IP.String()
	config.AdvertisePort = advertise.Port
	config.TCPTimeout = time.Second * 2 //nolint:gomnd //...
	config.IndirectChecks = math.MaxInt8
	config.RetransmitMult = 3
	config.ProbeTimeout = 500 * time.Millisecond //nolint:gomnd //...
	config.ProbeInterval = 1 * time.Second
	config.SuspicionMult = 1 // NOTE fast detection for failed nodes
	config.SuspicionMaxTimeoutMult = 1
	config.DisableTcpPings = true
	// config.SecretKey NO encryption
	config.Delegate = nil
	config.Events = nil
	config.Conflict = nil
	config.Merge = nil
	config.Ping = nil
	config.Alive = nil
	config.UDPBufferSize = 260000 // NOTE 260kb
	config.Logger = log.New(io.Discard, "", 0)

	return config
}

type membersPool struct {
	addrs *util.LockedMap // noline:misspell
	nodes *util.LockedMap // NOTE by node address
}

func newMembersPool() *membersPool {
	return &membersPool{
		addrs: util.NewLockedMap(),
		nodes: util.NewLockedMap(),
	}
}

func (m *membersPool) Exists(k *net.UDPAddr) bool {
	return m.addrs.Exists(nodeid(k))
}

func (m *membersPool) Get(k *net.UDPAddr) (Node, bool) {
	switch i, found := m.addrs.Value(nodeid(k)); {
	case !found:
		return nil, false
	case util.IsNilLockedValue(i):
		return nil, false
	case i == nil:
		return nil, true
	default:
		return i.(Node), false //nolint:forcetypeassert // ...
	}
}

func (m *membersPool) NodesLen(node base.Address) int {
	switch i, found := m.nodes.Value(node.String()); {
	case !found:
		return 0
	case util.IsNilLockedValue(i):
		return 0
	case i == nil:
		return 0
	default:
		return len(i.([]Node)) //nolint:forcetypeassert // ...
	}
}

func (m *membersPool) Set(node Node) bool {
	var found bool
	_, _ = m.addrs.Set(nodeid(node.UDPAddr()), func(i interface{}) (interface{}, error) {
		switch {
		case i == nil:
		case util.IsNilLockedValue(i):
		default:
			found = true
		}

		var nodes []Node

		switch i, f := m.nodes.Value(node.Address().String()); {
		case !f:
		case util.IsNilLockedValue(i):
		case i == nil:
		default:
			nodes = i.([]Node) //nolint:forcetypeassert // ...
		}

		nodes = append(nodes, node)
		m.nodes.SetValue(node.Address().String(), nodes)

		return node, nil
	})

	return found
}

func (m *membersPool) Remove(k *net.UDPAddr) error {
	_ = m.addrs.Remove(nodeid(k), func(i interface{}) error {
		switch {
		case i == nil:
		case util.IsNilLockedValue(i):
		default:
			_ = m.nodes.Remove(i.(Node).Address().String(), nil) //nolint:forcetypeassert // ...
		}

		return nil
	})

	return nil
}

func (m *membersPool) Len() int {
	return m.addrs.Len()
}

func (m *membersPool) Traverse(f func(Node) bool) {
	m.addrs.Traverse(func(k, v interface{}) bool {
		return f(v.(Node)) //nolint:forcetypeassert // ...
	})
}

func nodeid(addr *net.UDPAddr) string {
	var ip string
	if len(addr.IP) > 0 {
		ip = addr.IP.String()
	}

	return net.JoinHostPort(ip, strconv.FormatInt(int64(addr.Port), 10))
}
