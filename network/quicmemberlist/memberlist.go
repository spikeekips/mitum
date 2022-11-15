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

	"github.com/bluele/gcache"
	"github.com/hashicorp/memberlist"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/network/quicstream"
	"github.com/spikeekips/mitum/util"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/logging"
)

var errIgnoreAllowNode = util.NewError("ignore to allow node")

type Memberlist struct {
	local Node
	enc   *jsonenc.Encoder
	*logging.Logging
	*util.ContextDaemon
	whenLeftf       func(Node)
	mconfig         *memberlist.Config
	m               *memberlist.Memberlist
	delegate        *Delegate
	members         *membersPool
	cicache         *util.GCacheObjectPool
	sameMemberLimit uint64 // NOTE 0 means no additional same member
	l               sync.RWMutex
	joinedLock      sync.RWMutex
	isJoined        bool
}

func NewMemberlist(
	local Node,
	enc *jsonenc.Encoder,
	config *memberlist.Config,
	sameMemberLimit uint64,
) (*Memberlist, error) {
	srv := &Memberlist{
		Logging: logging.NewLogging(func(zctx zerolog.Context) zerolog.Context {
			return zctx.Str("module", "memberlist").Str("name", config.Name)
		}),
		local:           local,
		enc:             enc,
		sameMemberLimit: sameMemberLimit,
		members:         newMembersPool(),
		cicache:         util.NewGCacheObjectPool(1 << 9), //nolint:gomnd //...
		whenLeftf:       func(Node) {},
	}

	if err := srv.patch(config); err != nil {
		return nil, errors.WithMessage(err, "wrong memberlist.Config")
	}

	srv.ContextDaemon = util.NewContextDaemon(srv.start)

	return srv, nil
}

func (srv *Memberlist) Start() error {
	m, err := srv.createMemberlist()
	if err != nil {
		return errors.Wrap(err, "failed to create memberlist")
	}

	srv.m = m

	return srv.ContextDaemon.Start()
}

func (srv *Memberlist) Join(cis []quicstream.UDPConnInfo) error {
	e := util.StringErrorFunc("failed to join")

	if _, found := util.IsDuplicatedSlice(cis, func(i quicstream.UDPConnInfo) (bool, string) {
		return true, i.UDPAddr().String()
	}); found {
		return e(nil, "duplicated conninfo found")
	}

	filtered := util.FilterSlice(cis, func(i quicstream.UDPConnInfo) bool {
		return i.UDPAddr().String() != srv.local.UDPAddr().String()
	})

	fcis := make([]string, len(filtered))

	for i := range filtered {
		ci := filtered[i]

		fcis[i] = ci.UDPAddr().String()
		srv.cicache.Set(ci.UDPAddr().String(), ci, nil)
	}

	created, err := func() (bool, error) {
		srv.l.Lock()
		defer srv.l.Unlock()

		if srv.m != nil {
			return false, nil
		}

		m, err := srv.createMemberlist()
		if err != nil {
			return false, errors.Wrap(err, "failed to create memberlist")
		}

		srv.m = m

		return true, nil
	}()
	if err != nil {
		return err
	}

	if created && len(fcis) < 1 {
		return nil
	}

	l := srv.Log().With().Strs("cis", fcis).Logger()
	l.Debug().Msg("trying to join")

	srv.l.Lock()
	defer srv.l.Unlock()

	if _, err := srv.m.Join(fcis); err != nil {
		l.Error().Err(err).Msg("failed to join")

		return e(err, "")
	}

	return nil
}

func (srv *Memberlist) Leave(timeout time.Duration) error {
	err := func() error {
		srv.l.Lock()
		defer srv.l.Unlock()

		if srv.m == nil {
			return nil
		}

		srv.members.Empty()

		if err := srv.m.Leave(timeout); err != nil {
			srv.Log().Error().Err(err).Msg("failed to leave previous memberlist; ignored")
		}

		if err := srv.m.Shutdown(); err != nil {
			srv.Log().Error().Err(err).Msg("failed to shutdown previous memberlist; ignored")
		}

		srv.m = nil

		return nil
	}()

	srv.whenLeft(srv.local)

	return err
}

func (srv *Memberlist) MembersLen() int {
	return srv.members.Len()
}

func (srv *Memberlist) Members(f func(node Node) bool) {
	srv.members.Traverse(f)
}

func (srv *Memberlist) Remotes(f func(node Node) bool) {
	srv.members.Traverse(func(node Node) bool {
		if srv.local.Name() == node.Name() {
			return true
		}

		return f(node)
	})
}

// IsJoined indicates whether local is joined in remote network. If no other
// remote nodes, IsJoined will be false.
func (srv *Memberlist) IsJoined() bool {
	srv.joinedLock.RLock()
	defer srv.joinedLock.RUnlock()

	return srv.isJoined
}

func (srv *Memberlist) Broadcast(b memberlist.Broadcast) {
	switch {
	case srv.delegate != nil:
	case srv.IsJoined():
	default:
		b.Finished()

		return
	}

	srv.Log().Trace().Interface("broadcast", b).Msg("enqueue broadcast")

	srv.delegate.QueueBroadcast(b)
}

func (srv *Memberlist) start(ctx context.Context) error {
	<-ctx.Done()

	if err := func() error {
		if !func() bool {
			srv.l.RLock()
			defer srv.l.RUnlock()

			return srv.m != nil
		}() {
			return nil
		}

		// NOTE leave before shutdown
		if err := srv.m.Leave(time.Second * 3); err != nil { //nolint:gomnd //...
			srv.Log().Error().Err(err).Msg("failed to leave; ignored")
		}

		if err := srv.m.Shutdown(); err != nil {
			srv.Log().Error().Err(err).Msg("failed to shutdown memberlist; ignored")
		}

		if err := srv.mconfig.Transport.Shutdown(); err != nil {
			srv.Log().Error().Err(err).Msg("failed to shutdown memberlist transport; ignored")
		}

		return nil
	}(); err != nil {
		return errors.WithMessage(err, "failed to shutdown memberlist")
	}

	return ctx.Err()
}

func (srv *Memberlist) patch(config *memberlist.Config) error { // revive:disable-line:function-length
	switch {
	case config.Transport == nil:
		return errors.Errorf("empty Transport")
	case config.Delegate == nil:
		return errors.Errorf("delegate missing")
	case config.Alive == nil:
		return errors.Errorf("alive delegate missing")
	}

	notallowedcache := gcache.New(1 << 9).LRU().Build() //nolint:gomnd //...
	setnotallowedcache := func(addr string) {
		_ = notallowedcache.SetWithExpire(addr, nil, time.Second*6) //nolint:gomnd //... // FIXME set by config
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

		i.notallowf = func(addr string) bool {
			return notallowedcache.Has(addr)
		}
	}

	config.SecretKey = nil

	if i, ok := config.Delegate.(*Delegate); ok {
		srv.delegate = i
		srv.delegate.qu.NumNodes = srv.MembersLen
	}

	if i, ok := config.Alive.(*AliveDelegate); ok {
		i.storeconninfof = func(ci quicstream.UDPConnInfo) {
			srv.cicache.Set(ci.UDPAddr().String(), ci, nil)
		}

		origallowf := i.allowf
		allowf := func(node Node) error {
			if err := srv.allowNode(node); err != nil {
				return err
			}

			return origallowf(node)
		}

		i.allowf = func(node Node) error {
			err := allowf(node)
			if err != nil && !errors.Is(err, errIgnoreAllowNode) {
				srv.Log().Trace().Err(err).Interface("node", node).Msg("set not allowed")

				setnotallowedcache(node.UDPAddr().String())
			}

			return err
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

	config.LogOutput = writerFunc(func(b []byte) (int, error) {
		srv.Log().Trace().Msg(string(b))

		return len(b), nil
	})
	config.Logger = nil

	srv.mconfig = config

	return nil
}

func (srv *Memberlist) whenJoined(node Node) {
	srv.joinedLock.Lock()
	defer srv.joinedLock.Unlock()

	if !srv.isJoined && srv.local.Name() != node.Name() {
		srv.isJoined = true
	}

	if !srv.isJoined {
		srv.delegate.resetBroadcastQueue()
	}

	srv.members.Set(node)

	srv.Log().Debug().Bool("is_joined", srv.isJoined).Interface("node", node).Msg("node joined")
}

func (srv *Memberlist) whenLeft(node Node) {
	if func() bool {
		srv.joinedLock.Lock()
		defer srv.joinedLock.Unlock()

		removed, _ := srv.members.Remove(node.UDPAddr())

		switch {
		case !srv.isJoined:
		case srv.members.Len() < 1:
			srv.isJoined = false
		case srv.members.Len() < 2 && srv.members.Exists(srv.local.UDPAddr()):
			srv.isJoined = false
		}

		if !srv.isJoined {
			srv.delegate.resetBroadcastQueue()
		}

		srv.Log().Debug().Bool("is_joined", srv.isJoined).Interface("node", node).Msg("node left")

		return removed
	}() {
		srv.whenLeftf(node)
	}
}

func (srv *Memberlist) SetWhenLeftFunc(f func(Node)) {
	srv.whenLeftf = f
}

func (srv *Memberlist) SetNotifyMsg(f func([]byte)) {
	i, ok := srv.mconfig.Delegate.(*Delegate)
	if !ok {
		return
	}

	i.notifyMsgFunc = f
}

func (srv *Memberlist) allowNode(node Node) error {
	switch n := srv.members.NodesLen(node.Address()); {
	case n < 1:
	case uint64(n-1) == srv.sameMemberLimit:
		return errIgnoreAllowNode.Errorf("over member limit; %q", node.Name())
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

func (srv *Memberlist) createMemberlist() (*memberlist.Memberlist, error) {
	m, err := memberlist.Create(srv.mconfig)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create memberlist")
	}

	if i, ok := srv.mconfig.Transport.(*Transport); ok {
		_ = i.Start()
	}

	return m, nil
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
	config.UDPBufferSize = 260_000 // NOTE 260kb
	config.Logger = log.New(io.Discard, "", 0)

	return config
}

type membersPool struct {
	addrs *util.ShardedMap
	nodes *util.ShardedMap // NOTE by node address
}

func newMembersPool() *membersPool {
	return &membersPool{
		addrs: util.NewShardedMap(1 << 9), //nolint:gomnd //...
		nodes: util.NewShardedMap(1 << 9), //nolint:gomnd //...
	}
}

func (m *membersPool) Empty() {
	m.addrs.Empty()
	m.nodes.Empty()
}

func (m *membersPool) Exists(k *net.UDPAddr) bool {
	return m.addrs.Exists(nodeid(k))
}

func (m *membersPool) Get(k *net.UDPAddr) (Node, bool) {
	switch i, found := m.addrs.Value(nodeid(k)); {
	case !found:
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
	case i == nil:
		return 0
	default:
		return len(i.([]Node)) //nolint:forcetypeassert // ...
	}
}

func (m *membersPool) Set(node Node) bool {
	var found bool
	_, _ = m.addrs.Set(nodeid(node.UDPAddr()), func(addrfound bool, _ interface{}) (interface{}, error) {
		var nodes []Node

		found = addrfound

		switch i, f := m.nodes.Value(node.Address().String()); {
		case !f:
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

func (m *membersPool) Remove(k *net.UDPAddr) (bool, error) {
	return m.addrs.Remove(nodeid(k), func(i interface{}) error {
		_, _ = m.nodes.Remove(i.(Node).Address().String(), nil) //nolint:forcetypeassert // ...

		return nil
	})
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

type writerFunc func([]byte) (int, error)

func (f writerFunc) Write(b []byte) (int, error) {
	return f(b)
}
