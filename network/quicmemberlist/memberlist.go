package quicmemberlist

import (
	"bytes"
	"context"
	"fmt"
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
	"github.com/spikeekips/mitum/util/encoder"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/logging"
	"golang.org/x/sync/singleflight"
)

var callbackBroadcastMessageHeaderPrefix = []byte("memberlist-callback-message")

type MemberlistArgs struct {
	Encoder                           *jsonenc.Encoder
	Config                            *memberlist.Config
	PatchedConfig                     *memberlist.Config
	FetchCallbackBroadcastMessageFunc func(context.Context, CallbackBroadcastMessage) ([]byte, encoder.Encoder, error)
	WhenLeftFunc                      func(Member)
	// Member, which has same node address will be allowed to join up to
	// ExtraSameMemberLimit - 1. If ExtraSameMemberLimit is 0, only 1 member is
	// allowed to join.
	ExtraSameMemberLimit uint64
	// Not-allowed member will be cached for NotAllowedMemberExpire, after
	// NotAllowedMemberExpire, not-allowed member will be checked by allowf of
	// AliveDelegate.
	NotAllowedMemberExpire               time.Duration
	ChallengeExpire                      time.Duration
	CallbackBroadcastMessageExpire       time.Duration
	FetchCallbackBroadcastMessageTimeout time.Duration
}

func NewMemberlistArgs(enc *jsonenc.Encoder, config *memberlist.Config) *MemberlistArgs {
	return &MemberlistArgs{
		Encoder:                              enc,
		Config:                               config,
		ExtraSameMemberLimit:                 2, //nolint:gomnd //...
		WhenLeftFunc:                         func(Member) {},
		NotAllowedMemberExpire:               time.Second * 6, //nolint:gomnd //...
		ChallengeExpire:                      defaultNodeChallengeExpire,
		CallbackBroadcastMessageExpire:       time.Second * 30, //nolint:gomnd //...
		FetchCallbackBroadcastMessageTimeout: time.Second * 6,  //nolint:gomnd //...
		FetchCallbackBroadcastMessageFunc: func(context.Context, CallbackBroadcastMessage) (
			[]byte, encoder.Encoder, error,
		) {
			return nil, nil, util.ErrNotImplemented.Errorf("FetchCallbackBroadcastMessageFunc")
		},
	}
}

type Memberlist struct {
	local Member
	args  *MemberlistArgs
	*logging.Logging
	*util.ContextDaemon
	m          *memberlist.Memberlist
	delegate   *Delegate
	members    *membersPool
	cicache    *util.GCache[string, quicstream.UDPConnInfo]
	cbcache    *util.GCache[string, []byte]
	l          sync.RWMutex
	joinedLock sync.RWMutex
	isJoined   bool
}

func NewMemberlist(local Member, args *MemberlistArgs) (*Memberlist, error) {
	srv := &Memberlist{
		Logging: logging.NewLogging(func(zctx zerolog.Context) zerolog.Context {
			return zctx.Str("module", "memberlist").Str("name", args.Config.Name)
		}),
		local:   local,
		args:    args,
		members: newMembersPool(),
		cicache: util.NewLRUGCache("", quicstream.UDPConnInfo{}, 1<<9), //nolint:gomnd //...
		cbcache: util.NewLRUGCache("", ([]byte)(nil), 1<<13),           //nolint:gomnd //...
	}

	if err := srv.patch(args.Config); err != nil {
		return nil, errors.WithMessage(err, "wrong memberlist.Config")
	}

	srv.ContextDaemon = util.NewContextDaemon(srv.start)

	return srv, nil
}

func (srv *Memberlist) Start(ctx context.Context) error {
	m, err := srv.createMemberlist()
	if err != nil {
		return errors.Wrap(err, "failed to create memberlist")
	}

	srv.m = m

	return srv.ContextDaemon.Start(ctx)
}

func (srv *Memberlist) Join(cis []quicstream.UDPConnInfo) error {
	e := util.StringErrorFunc("failed to join")

	if _, found := util.IsDuplicatedSlice(cis, func(i quicstream.UDPConnInfo) (bool, string) {
		if i.Addr() == nil {
			return true, ""
		}

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
		srv.cicache.Set(ci.UDPAddr().String(), ci, 0)
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

func (srv *Memberlist) Members(f func(Member) bool) {
	srv.members.Traverse(f)
}

func (srv *Memberlist) Remotes(f func(Member) bool) {
	srv.members.Traverse(func(member Member) bool {
		if srv.local.Name() == member.Name() {
			return true
		}

		return f(member)
	})
}

// IsJoined indicates whether local is joined in remote network. If no other
// remote members, IsJoined will be false.
func (srv *Memberlist) IsJoined() bool {
	srv.joinedLock.RLock()
	defer srv.joinedLock.RUnlock()

	return srv.isJoined
}

func (srv *Memberlist) Broadcast(b memberlist.Broadcast) {
	if !srv.canBroadcast() {
		b.Finished()

		return
	}

	srv.Log().Trace().Interface("broadcast", b).Msg("enqueue broadcast")

	srv.delegate.QueueBroadcast(b)
}

func (srv *Memberlist) CallbackBroadcast(b []byte, id string, notifych chan struct{}) error {
	if !srv.canBroadcast() {
		if notifych != nil {
			close(notifych)
		}

		return nil
	}

	e := util.StringErrorFunc("failed callback broadcast")

	// NOTE save b in cache first
	srv.cbcache.Set(id, b, srv.args.CallbackBroadcastMessageExpire)

	buf := bytes.NewBuffer(callbackBroadcastMessageHeaderPrefix)
	defer buf.Reset()

	switch err := srv.args.Encoder.StreamEncoder(buf).Encode(
		NewCallbackBroadcastMessage(id, srv.local.UDPConnInfo())); {
	case err != nil:
		return e(err, "")
	default:
		srv.Broadcast(NewBroadcast(buf.Bytes(), id, notifych))
	}

	return nil
}

func (srv *Memberlist) CallbackBroadcastHandler() quicstream.HeaderHandler {
	var sg singleflight.Group

	return func(_ net.Addr, r io.Reader, w io.Writer,
		h quicstream.Header, _ *encoder.Encoders, enc encoder.Encoder,
	) error {
		e := util.StringErrorFunc("failed to handle callback message")

		header, ok := h.(CallbackBroadcastMessageHeader)
		if !ok {
			return e(nil, "expected CallbackBroadcastMessageHeader, but %T", h)
		}

		i, err, _ := sg.Do(header.ID(), func() (interface{}, error) {
			b, found := srv.cbcache.Get(header.ID())

			return [2]interface{}{b, found}, nil
		})

		if err != nil {
			return e(err, "")
		}

		var b []byte
		var found bool

		if i != nil {
			j := i.([2]interface{}) //nolint:forcetypeassert //...

			found = j[1].(bool) //nolint:forcetypeassert //...

			if found {
				b = j[0].([]byte) //nolint:forcetypeassert //...
			}
		}

		if err := quicstream.WriteResponseBytes(w,
			quicstream.NewDefaultResponseHeader(found, nil, quicstream.RawContentType), enc, b); err != nil {
			return e(err, "")
		}

		return nil
	}
}

func (srv *Memberlist) start(ctx context.Context) error {
	defer srv.cicache.Close()

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

		if err := srv.args.PatchedConfig.Transport.Shutdown(); err != nil {
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
		_ = notallowedcache.SetWithExpire(addr, nil, srv.args.NotAllowedMemberExpire)
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

			return j
		}

		i.args.NotAllowFunc = func(addr string) bool {
			return notallowedcache.Has(addr)
		}
	}

	config.SecretKey = nil

	if i, ok := config.Delegate.(*Delegate); ok {
		srv.delegate = i
		srv.delegate.qu.NumNodes = srv.MembersLen
		srv.delegate.notifyMsgFunc = srv.notifyMsgFunc(func([]byte, encoder.Encoder) {})
	}

	if i, ok := config.Alive.(*AliveDelegate); ok {
		i.storeconninfof = func(ci quicstream.UDPConnInfo) {
			srv.cicache.Set(ci.UDPAddr().String(), ci, 0)
		}

		origallowf := i.allowf
		allowf := func(member Member) error {
			if err := srv.allowMember(member); err != nil {
				return err
			}

			return origallowf(member)
		}

		i.allowf = func(member Member) error {
			err := allowf(member)
			if err != nil {
				srv.Log().Trace().Err(err).Interface("member", member).Msg("set not allowed")

				setnotallowedcache(member.UDPAddr().String())
			}

			return err
		}

		i.challengeexpire = srv.args.ChallengeExpire
	}

	switch {
	case config.Events == nil:
		config.Events = NewEventsDelegate(
			srv.args.Encoder,
			srv.whenJoined,
			srv.whenLeft,
		)
	default:
		if i, ok := config.Events.(*EventsDelegate); ok {
			joinedforig := i.joinedf
			i.joinedf = func(member Member) {
				srv.whenJoined(member)

				joinedforig(member)
			}

			leftforig := i.leftf
			i.leftf = func(member Member) {
				srv.whenLeft(member)

				leftforig(member)
			}
		}
	}

	config.LogOutput = writerFunc(func(b []byte) (int, error) {
		srv.Log().Trace().Msg(string(b))

		return len(b), nil
	})
	config.Logger = nil

	srv.args.PatchedConfig = config

	return nil
}

func (srv *Memberlist) whenJoined(member Member) {
	srv.joinedLock.Lock()
	defer srv.joinedLock.Unlock()

	if !srv.isJoined && srv.local.Name() != member.Name() {
		srv.isJoined = true
	}

	if !srv.isJoined {
		srv.delegate.resetBroadcastQueue()
	}

	srv.members.Set(member)

	srv.Log().Debug().Bool("is_joined", srv.isJoined).Interface("member", member).Msg("member joined")
}

func (srv *Memberlist) whenLeft(member Member) {
	if func() bool {
		srv.joinedLock.Lock()
		defer srv.joinedLock.Unlock()

		removed, _ := srv.members.Remove(member.UDPAddr())

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

		srv.Log().Debug().Bool("is_joined", srv.isJoined).Interface("member", member).Msg("member left")

		return removed
	}() {
		srv.args.WhenLeftFunc(member)
	}
}

func (srv *Memberlist) SetWhenLeftFunc(f func(Member)) *Memberlist {
	srv.args.WhenLeftFunc = f

	return srv
}

func (srv *Memberlist) SetNotifyMsg(f func([]byte, encoder.Encoder)) *Memberlist {
	if srv.delegate == nil {
		return nil
	}

	srv.delegate.notifyMsgFunc = srv.notifyMsgFunc(f)

	return srv
}

func (srv *Memberlist) notifyMsgFunc(f func([]byte, encoder.Encoder)) func([]byte) {
	return func(b []byte) {
		body := b
		var enc encoder.Encoder = srv.args.Encoder

		if bytes.HasPrefix(body, callbackBroadcastMessageHeaderPrefix) {
			switch i, e, err := srv.notifyMsgCallbackBroadcastMessage(
				body[len(callbackBroadcastMessageHeaderPrefix):]); {
			case err != nil:
				return
			default:
				enc = e
				body = i
			}
		}

		f(body, enc)
	}
}

func (srv *Memberlist) notifyMsgCallbackBroadcastMessage(b []byte) ([]byte, encoder.Encoder, error) {
	var m CallbackBroadcastMessage

	switch i, err := srv.args.Encoder.Decode(b); {
	case err != nil:
		srv.Log().Error().Err(err).Str("message", string(b)).Msg("failed to decode incoming message")

		return nil, nil, err
	default:
		srv.Log().Trace().Err(err).Interface("message", i).Msg("new message notified")

		j, ok := i.(CallbackBroadcastMessage)
		if !ok {
			srv.Log().Trace().Err(err).
				Str("message_type", fmt.Sprintf("%T", i)).
				Msg("unknown message found for callback message")

			return nil, nil, errors.Errorf("unknown message found for callback message")
		}

		if err := j.IsValid(nil); err != nil {
			srv.Log().Trace().Err(err).Msg("invalid CallbackBroadcastMessage")

			return nil, nil, err
		}

		m = j
	}

	// NOTE fetch callback message
	ctx, cancel := context.WithTimeout(context.Background(), srv.args.FetchCallbackBroadcastMessageTimeout)
	defer cancel()

	switch body, enc, err := srv.args.FetchCallbackBroadcastMessageFunc(ctx, m); {
	case err != nil:
		srv.Log().Trace().Err(err).Msg("failed to fetch callback broadcast message")

		return nil, nil, err
	case enc == nil:
		srv.Log().Trace().Msg("failed to fetch callback broadcast message; empty message")

		return nil, nil, errors.Errorf("empty message")
	default:
		return body, enc, nil
	}
}

func (srv *Memberlist) allowMember(member Member) error {
	switch n, others, found := srv.members.MembersLenOthers(member.Address(), member.UDPAddr()); {
	case n < 1:
	default:
		if !found {
			others++
		}

		if uint64(others) > srv.args.ExtraSameMemberLimit {
			return errors.Errorf("member(%s, %s) over limit, %d",
				member.Address(), member.Publish(), srv.args.ExtraSameMemberLimit)
		}
	}

	return nil
}

func (srv *Memberlist) SetLogging(l *logging.Logging) *logging.Logging {
	ds := []interface{}{
		srv.args.PatchedConfig.Delegate,
		srv.args.PatchedConfig.Events,
		srv.args.PatchedConfig.Alive,
		srv.args.PatchedConfig.Transport,
	}

	for i := range ds {
		if j, ok := ds[i].(logging.SetLogging); ok {
			_ = j.SetLogging(l)
		}
	}

	return srv.Logging.SetLogging(l)
}

func (srv *Memberlist) createMemberlist() (*memberlist.Memberlist, error) {
	m, err := memberlist.Create(srv.args.PatchedConfig)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create memberlist")
	}

	if i, ok := srv.args.PatchedConfig.Transport.(*Transport); ok {
		_ = i.Start(context.Background())
	}

	return m, nil
}

func (srv *Memberlist) canBroadcast() bool {
	switch {
	case srv.IsJoined():
	case srv.delegate != nil:
	default:
		return false
	}

	return true
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
	config.SuspicionMult = 1 // NOTE fast detection for failed members
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
	addrs   *util.ShardedMap[string, Member]
	members *util.ShardedMap[string, []Member] // NOTE by node address
}

func newMembersPool() *membersPool {
	addrs, _ := util.NewShardedMap("", (Member)(nil), 1<<9) //nolint:gomnd //...
	members, _ := util.NewShardedMap("", []Member{}, 1<<9)  //nolint:gomnd //...

	return &membersPool{
		addrs:   addrs,
		members: members,
	}
}

func (m *membersPool) Empty() {
	m.addrs.Empty()
	m.members.Empty()
}

func (m *membersPool) Exists(k *net.UDPAddr) bool {
	return m.addrs.Exists(memberid(k))
}

func (m *membersPool) Get(k *net.UDPAddr) (Member, bool) {
	switch i, found := m.addrs.Value(memberid(k)); {
	case !found, i == nil:
		return nil, false
	default:
		return i, false
	}
}

func (m *membersPool) MembersLenOthers(node base.Address, addr *net.UDPAddr) (memberslen int, others int, found bool) {
	_ = m.members.Get(node.String(), func(members []Member, memberfound bool) error {
		if !memberfound {
			return nil
		}

		id := memberid(addr)

		memberslen = len(members)

		others = util.CountFilteredSlice(members, func(n Member) bool {
			nid := memberid(n.UDPAddr())

			switch {
			case id != nid:
				return true
			case !found:
				found = true

				fallthrough
			default:
				return false
			}
		})

		return nil
	})

	return memberslen, others, found
}

func (m *membersPool) MembersLen(node base.Address) int {
	switch i, found := m.members.Value(node.String()); {
	case !found, i == nil:
		return 0
	default:
		return len(i)
	}
}

func (m *membersPool) Set(member Member) bool {
	var found bool
	_, _ = m.addrs.Set(memberid(member.UDPAddr()), func(_ Member, addrfound bool) (Member, error) {
		var members []Member

		found = addrfound

		switch i, f := m.members.Value(member.Address().String()); {
		case !f, i == nil:
		default:
			members = i
		}

		members = append(members, member)
		m.members.SetValue(member.Address().String(), members)

		return member, nil
	})

	return found
}

func (m *membersPool) Remove(k *net.UDPAddr) (bool, error) {
	return m.addrs.Remove(memberid(k), func(i Member, found bool) error {
		if found {
			_ = m.members.RemoveValue(i.Address().String())
		}

		return nil
	})
}

func (m *membersPool) Len() int {
	return m.addrs.Len()
}

func (m *membersPool) Traverse(f func(Member) bool) {
	m.addrs.Traverse(func(_ string, v Member) bool {
		return f(v)
	})
}

func memberid(addr *net.UDPAddr) string {
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

func AliveMembers(
	m *Memberlist,
	exclude func(Member) bool,
) []Member {
	l := m.MembersLen()
	if l < 1 {
		return nil
	}

	members := make([]Member, l*2)

	var i int
	m.Members(func(member Member) bool {
		if !exclude(member) {
			members[i] = member
			i++
		}

		return true
	})

	return members[:i]
}

func RandomAliveMembers(
	m *Memberlist,
	size int64,
	exclude func(Member) bool,
) ([]Member, error) {
	return util.RandomChoiceSlice(AliveMembers(m, exclude), size)
}

func FetchCallbackBroadcastMessageFunc(
	handlerPrefix string,
	requestf quicstream.HeaderClientRequestFunc,
) func(context.Context, CallbackBroadcastMessage) (
	[]byte, encoder.Encoder, error,
) {
	return func(ctx context.Context, m CallbackBroadcastMessage) (
		[]byte, encoder.Encoder, error,
	) {
		h := NewCallbackBroadcastMessageHeader(m.ID(), handlerPrefix)

		res, r, cancel, enc, rerr := requestf(ctx, m.ConnInfo(), h, nil)
		if rerr != nil {
			return nil, enc, rerr
		}

		defer func() {
			_ = cancel()
		}()

		switch {
		case !res.OK():
			return nil, enc, nil
		case res.Err() != nil:
			return nil, enc, res.Err()
		default:
			b, rerr := io.ReadAll(r)
			if rerr != nil {
				return nil, enc, errors.WithMessage(rerr, "failed to read fetched callback message")
			}

			return b, enc, nil
		}
	}
}
