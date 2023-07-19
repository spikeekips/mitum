package quicmemberlist

import (
	"bytes"
	"container/list"
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
	"github.com/spikeekips/mitum/network"
	"github.com/spikeekips/mitum/network/quicstream"
	quicstreamheader "github.com/spikeekips/mitum/network/quicstream/header"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/localtime"
	"github.com/spikeekips/mitum/util/logging"
	"golang.org/x/exp/slices"
	"golang.org/x/sync/semaphore"
	"golang.org/x/sync/singleflight"
)

var (
	callbackBroadcastMessageHeaderPrefix = []byte("memberlist-callback-message")
	ensureBroadcastMessageHeaderPrefix   = []byte("memberlist-ensure-message")
)

type MemberlistArgs struct {
	Encoder                           *jsonenc.Encoder
	Config                            *memberlist.Config
	PatchedConfig                     *memberlist.Config
	FetchCallbackBroadcastMessageFunc func(context.Context, ConnInfoBroadcastMessage) ([]byte, encoder.Encoder, error)
	PongEnsureBroadcastMessageFunc    func(context.Context, ConnInfoBroadcastMessage) error
	WhenLeftFunc                      func(Member)
	// Members, which has same node address will be allowed to join up to
	// ExtraSameMemberLimit + 1. If ExtraSameMemberLimit is 0, only 1 member is
	// allowed to join.
	ExtraSameMemberLimit func() uint64
	// Not-allowed member will be cached for NotAllowedMemberExpire, after
	// NotAllowedMemberExpire, not-allowed member will be checked by allowf of
	// AliveDelegate.
	NotAllowedMemberExpire               time.Duration
	ChallengeExpire                      time.Duration
	CallbackBroadcastMessageExpire       time.Duration
	FetchCallbackBroadcastMessageTimeout time.Duration
	PongEnsureBroadcastMessageTimeout    time.Duration
	PongEnsureBroadcastMessageExpire     time.Duration
}

func NewMemberlistArgs(enc *jsonenc.Encoder, config *memberlist.Config) *MemberlistArgs {
	return &MemberlistArgs{
		Encoder:                              enc,
		Config:                               config,
		ExtraSameMemberLimit:                 func() uint64 { return 1 },
		WhenLeftFunc:                         func(Member) {},
		NotAllowedMemberExpire:               time.Second * 6, //nolint:gomnd //...
		ChallengeExpire:                      defaultNodeChallengeExpire,
		CallbackBroadcastMessageExpire:       time.Second * 30, //nolint:gomnd //...
		FetchCallbackBroadcastMessageTimeout: time.Second * 6,  //nolint:gomnd //...
		FetchCallbackBroadcastMessageFunc: func(context.Context, ConnInfoBroadcastMessage) (
			[]byte, encoder.Encoder, error,
		) {
			return nil, nil, util.ErrNotImplemented.Errorf("FetchCallbackBroadcastMessageFunc")
		},
		PongEnsureBroadcastMessageTimeout: time.Second * 3, //nolint:gomnd //...
		PongEnsureBroadcastMessageExpire:  time.Second * 9, //nolint:gomnd //...
		PongEnsureBroadcastMessageFunc: func(context.Context, ConnInfoBroadcastMessage) error {
			return util.ErrNotImplemented.Errorf("PongEnsureBroadcastMessageFunc")
		},
	}
}

type Memberlist struct {
	local Member
	args  *MemberlistArgs
	*logging.Logging
	*util.ContextDaemon
	m                  *memberlist.Memberlist
	delegate           *Delegate
	members            *membersPool
	cicache            *util.GCache[string, quicstream.ConnInfo]
	cbcache            *util.GCache[string, []byte]
	ebtimers           *util.SimpleTimers
	usermsgs           *list.List
	handleUserMsgsFunc func([]byte, encoder.Encoder)
	ebrecords          util.LockedMap[string, []base.Address]
	l                  sync.RWMutex
	joinedLock         sync.RWMutex
	usermsgsLock       sync.Mutex
	isJoined           bool
}

func NewMemberlist(local Member, args *MemberlistArgs) (*Memberlist, error) {
	ebtimers, err := util.NewSimpleTimers(1<<13, time.Millisecond*33) //nolint:gomnd //...
	if err != nil {
		return nil, err
	}

	ebrecords, err := util.NewLockedMap[string, []base.Address](1 << 13) //nolint:gomnd //...
	if err != nil {
		return nil, err
	}

	srv := &Memberlist{
		Logging: logging.NewLogging(func(zctx zerolog.Context) zerolog.Context {
			return zctx.Str("module", "memberlist").Str("name", args.Config.Name)
		}),
		local:     local,
		args:      args,
		members:   newMembersPool(),
		cicache:   util.NewLRUGCache[string, quicstream.ConnInfo](1 << 9), //nolint:gomnd //...
		cbcache:   util.NewLRUGCache[string, []byte](1 << 13),             //nolint:gomnd //...
		ebrecords: ebrecords,
		ebtimers:  ebtimers,
		usermsgs:  list.New(),
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
		return err
	}

	srv.m = m

	return srv.ContextDaemon.Start(ctx)
}

func (srv *Memberlist) Join(cis []quicstream.ConnInfo) error {
	e := util.StringError("join")

	if len(cis) < 1 {
		return e.Errorf("empty conninfos")
	}

	if _, found := util.IsDuplicatedSlice(cis, func(i quicstream.ConnInfo) (bool, string) {
		return true, i.UDPAddr().String()
	}); found {
		return e.Errorf("duplicated conninfo found")
	}

	filtered := util.FilterSlice(cis, func(i quicstream.ConnInfo) bool {
		return !network.EqualConnInfo(i, srv.local.ConnInfo())
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
			return false, err
		}

		srv.m = m

		return true, nil
	}()
	if err != nil {
		return e.Wrap(err)
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

		return e.Wrap(err)
	}

	return nil
}

func (srv *Memberlist) Leave(timeout time.Duration) error {
	var notleft bool

	err := func() error {
		srv.l.Lock()
		defer srv.l.Unlock()

		if srv.m == nil {
			return nil
		}

		notleft = true

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

	if notleft {
		srv.whenLeft(srv.local)

		srv.Log().Debug().
			Bool("is_joined", srv.IsJoined()).
			Interface("member", srv.local).
			Bool("is_local", true).
			Msg("member left")
	}

	return err
}

func (srv *Memberlist) Exists(addr *net.UDPAddr) bool {
	return srv.members.Exists(addr)
}

// MembersLen() returns number of all the joined members.
func (srv *Memberlist) MembersLen() int {
	return srv.members.Len()
}

// RemotesLen() returns number of all the joined members except local.
func (srv *Memberlist) RemotesLen() int {
	var count int

	srv.members.Traverse(func(member Member) bool {
		if srv.local.Name() != member.Name() {
			count++
		}

		return true
	})

	return count
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
	if !srv.CanBroadcast() {
		b.Finished()

		return
	}

	srv.Log().Trace().Interface("broadcast", b).Msg("enqueue broadcast")

	srv.delegate.QueueBroadcast(b)
}

func (srv *Memberlist) CallbackBroadcast(b []byte, id string, notifych chan struct{}) error {
	if !srv.CanBroadcast() {
		if notifych != nil {
			close(notifych)
		}

		return nil
	}

	// NOTE save b in cache first
	srv.cbcache.Set(id, b, srv.args.CallbackBroadcastMessageExpire)

	buf := bytes.NewBuffer(callbackBroadcastMessageHeaderPrefix)
	defer buf.Reset()

	switch err := srv.args.Encoder.StreamEncoder(buf).Encode(
		NewConnInfoBroadcastMessage(id, srv.local.ConnInfo())); {
	case err != nil:
		return err
	default:
		srv.Broadcast(NewBroadcast(buf.Bytes(), id, notifych))
	}

	return nil
}

func (srv *Memberlist) CallbackBroadcastHandler() quicstreamheader.Handler[CallbackBroadcastMessageHeader] {
	var sg singleflight.Group

	return func(ctx context.Context, _ net.Addr,
		broker *quicstreamheader.HandlerBroker, req CallbackBroadcastMessageHeader,
	) error {
		e := util.StringError("handle callback message")

		i, _, _ := util.SingleflightDo[[2]interface{}](&sg, req.ID(), func() ([2]interface{}, error) {
			b, found := srv.cbcache.Get(req.ID())

			return [2]interface{}{b, found}, nil
		})

		var body io.Reader
		var found bool
		bodyType := quicstreamheader.EmptyBodyType

		if i[0] != nil {
			found = i[1].(bool) //nolint:forcetypeassert //...

			if found {
				buf := bytes.NewBuffer(i[0].([]byte)) //nolint:forcetypeassert //...
				defer buf.Reset()

				body = buf
				bodyType = quicstreamheader.StreamBodyType
			}
		}

		if err := broker.WriteResponseHeadOK(ctx, found, nil); err != nil {
			return e.WithMessage(err, "write response header")
		}

		if err := broker.WriteBody(ctx, bodyType, 0, body); err != nil {
			return e.WithMessage(err, "write body")
		}

		return nil
	}
}

func (srv *Memberlist) EnsureBroadcast(
	b []byte,
	id string,
	notifych chan<- error,
	intervalf func(uint64) time.Duration,
	threshold float64,
	maxRetry uint64,
	excludemember func(Member) bool,
) error {
	if !srv.CanBroadcast() {
		if notifych != nil {
			close(notifych)
		}

		return nil
	}

	th := base.Threshold(threshold)
	if err := th.IsValid(nil); err != nil {
		return err
	}

	buf := bytes.NewBuffer(ensureBroadcastMessageHeaderPrefix)

	switch i, err := srv.args.Encoder.Marshal(NewConnInfoBroadcastMessage(id, srv.local.ConnInfo())); {
	case err != nil:
		return err
	default:
		if err := util.WriteLengthed(buf, i); err != nil {
			return err
		}

		if _, err := buf.Write(b); err != nil {
			return errors.WithStack(err)
		}
	}

	var notifyonce sync.Once

	notify := func(err error) {
		if notifych == nil {
			return
		}

		notifyonce.Do(func() {
			notifych <- err
		})
	}

	checkretry := func(i uint64) bool {
		return i >= maxRetry
	}

	if maxRetry == 0 {
		checkretry = func(i uint64) bool {
			return false
		}
	}

	timer := util.NewSimpleTimer(
		util.TimerID(id),
		func(i uint64) time.Duration {
			switch {
			case srv.broadcastEnsured(id, th, excludemember):
				notify(nil)

				return 0
			case checkretry(i):
				notify(errors.Errorf("over max retry reached"))

				return 0
			}

			return intervalf(i)
		},
		func(context.Context, uint64) (bool, error) {
			if srv.broadcastEnsured(id, th, excludemember) {
				notify(nil)

				return false, nil
			}

			srv.Broadcast(NewBroadcast(buf.Bytes(), id, nil))

			return true, nil
		},
		func() {
			_ = srv.ebrecords.RemoveValue(id)

			buf.Reset()
		},
	)

	switch added, err := srv.ebtimers.NewTimer(timer); {
	case err != nil:
		return err
	case added:
		_ = srv.ebrecords.SetValue(id, nil)
	}

	return nil
}

func (srv *Memberlist) EnsureBroadcastHandler(
	networkID base.NetworkID,
	memberf func(base.Address) (base.Publickey, bool, error),
) quicstreamheader.Handler[EnsureBroadcastMessageHeader] {
	return func(ctx context.Context, _ net.Addr,
		broker *quicstreamheader.HandlerBroker, req EnsureBroadcastMessageHeader,
	) error {
		e := util.StringError("handle ensure message")

		switch pub, found, err := memberf(req.Node()); {
		case err != nil:
			return e.Wrap(err)
		case !found:
			return e.Errorf("unknown node")
		case localtime.Now().Sub(req.SignedAt()) > srv.args.PongEnsureBroadcastMessageExpire:
			return e.Errorf("signed too late")
		case !req.Signer().Equal(pub):
			return e.Errorf("publickey mismatch")
		default:
			if err := req.Verify(networkID, []byte(req.ID())); err != nil {
				return e.Wrap(err)
			}
		}

		var isset bool

		_, _ = srv.ebrecords.Set(req.ID(), func(nodes []base.Address, found bool) ([]base.Address, error) {
			if !found {
				return nil, util.ErrLockedSetIgnore.WithStack()
			}

			if slices.IndexFunc[base.Address](nodes, func(i base.Address) bool {
				return i.Equal(req.Node())
			}) >= 0 {
				return nil, util.ErrLockedSetIgnore.WithStack()
			}

			nodes = append(nodes, req.Node())
			isset = true

			return nodes, nil
		})

		if err := broker.WriteResponseHeadOK(ctx, isset, nil); err != nil {
			return e.WithMessage(err, "write response header")
		}

		return nil
	}
}

func (srv *Memberlist) start(ctx context.Context) error {
	defer srv.cicache.Close()

	if err := srv.ebtimers.Start(ctx); err != nil {
		return err
	}

	go srv.loopUserMsgs(ctx)

	<-ctx.Done()

	_ = srv.ebtimers.Stop()

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
		return errors.WithMessage(err, "shutdown memberlist")
	}

	return errors.WithStack(ctx.Err())
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
		i.getconninfof = func(addr *net.UDPAddr) (quicstream.ConnInfo, error) {
			j, found := srv.cicache.Get(addr.String())
			if !found {
				return quicstream.NewConnInfo(addr, true)
			}

			return j, nil
		}

		i.args.NotAllowFunc = func(addr string) bool {
			return notallowedcache.Has(addr)
		}
	}

	config.SecretKey = nil

	if i, ok := config.Delegate.(*Delegate); ok {
		srv.delegate = i
		srv.delegate.qu.NumNodes = srv.MembersLen
		srv.handleUserMsgsFunc = func([]byte, encoder.Encoder) {}
		srv.delegate.notifyMsgFunc = srv.notifyMsgFunc
	}

	if i, ok := config.Alive.(*AliveDelegate); ok {
		i.storeconninfof = func(ci quicstream.ConnInfo) {
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

				setnotallowedcache(member.Addr().String())
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

	if srv.members.Set(member) {
		srv.Log().Debug().Bool("is_joined", srv.isJoined).Interface("member", member).Msg("member joined")
	}
}

func (srv *Memberlist) whenLeft(member Member) {
	if func() bool {
		srv.joinedLock.Lock()
		defer srv.joinedLock.Unlock()

		removed, _ := srv.members.Remove(member.Addr())

		switch {
		case !srv.isJoined:
		case srv.members.Len() < 1:
			srv.isJoined = false
		case srv.members.Len() < 2 && srv.members.Exists(srv.local.Addr()):
			srv.isJoined = false
		}

		if !srv.isJoined {
			srv.delegate.resetBroadcastQueue()
		}

		if removed {
			srv.Log().Debug().Bool("is_joined", srv.isJoined).Interface("member", member).Msg("member left")
		}

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

	srv.handleUserMsgsFunc = f

	return srv
}

func (srv *Memberlist) notifyMsgFunc(b []byte) {
	srv.usermsgsLock.Lock()
	defer srv.usermsgsLock.Unlock()

	srv.usermsgs.PushBack(b)
}

func (srv *Memberlist) notifyMsgCallbackBroadcastMessage(b []byte) ([]byte, encoder.Encoder, error) {
	var m ConnInfoBroadcastMessage

	switch i, err := srv.args.Encoder.Decode(b); {
	case err != nil:
		srv.Log().Error().Err(err).Str("broadcast_message", string(b)).Msg("failed to decode incoming message")

		return nil, nil, err
	default:
		srv.Log().Trace().Err(err).Interface("broadcast_message", i).Msg("new message notified")

		j, ok := i.(ConnInfoBroadcastMessage)
		if !ok {
			srv.Log().Trace().Err(err).
				Str("message_type", fmt.Sprintf("%T", i)).
				Msg("unknown message found for callback message")

			return nil, nil, errors.Errorf("unknown message found for callback message")
		}

		if err := j.IsValid(nil); err != nil {
			srv.Log().Trace().Err(err).Msg("invalid ConnInfoBroadcastMessage")

			return nil, nil, err
		}

		m = j
	}

	// NOTE fetch callback message
	ctx, cancel := context.WithTimeout(context.Background(), srv.args.FetchCallbackBroadcastMessageTimeout)
	defer cancel()

	l := srv.Log().With().Str("message_id", m.ID()).Logger()

	switch body, enc, err := srv.args.FetchCallbackBroadcastMessageFunc(ctx, m); {
	case err != nil:
		l.Trace().Err(err).Msg("failed to fetch callback broadcast message")

		return nil, nil, err
	case enc == nil:
		l.Trace().Msg("failed to fetch callback broadcast message; empty message")

		return nil, nil, errors.Errorf("empty message")
	default:
		return body, enc, nil
	}
}

func (srv *Memberlist) notifyMsgEnsureBroadcastMessage(b []byte) ([]byte, error) {
	var m ConnInfoBroadcastMessage
	var left []byte

	switch i, j, err := util.ReadLengthedBytes(b); {
	case err != nil:
		return nil, err
	default:
		if err := encoder.Decode(srv.args.Encoder, i, &m); err != nil {
			return nil, err
		}

		if err := m.IsValid(nil); err != nil {
			srv.Log().Trace().Err(err).Msg("invalid ConnInfoBroadcastMessage")

			return nil, err
		}

		left = j
	}

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), srv.args.PongEnsureBroadcastMessageTimeout)
		defer cancel()

		if err := srv.args.PongEnsureBroadcastMessageFunc(ctx, m); err != nil {
			srv.Log().Error().Err(err).Msg("failed to pong ensure broadcast message")
		}
	}()

	return left, nil
}

func (srv *Memberlist) allowMember(member Member) error {
	max := srv.args.ExtraSameMemberLimit()
	id := memberid(member.Addr())

	var extras uint64

	srv.members.Traverse(func(n Member) bool {
		switch {
		case memberid(n.Addr()) == id:
		case n.Address().Equal(member.Address()):
			extras++
		}

		return true
	})

	if extras > max {
		return errors.Errorf("member(%s, %s) over limit, %d",
			member.Address(), member.Publish(), max)
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
		return nil, errors.Wrap(err, "create memberlist")
	}

	if i, ok := srv.args.PatchedConfig.Transport.(*Transport); ok {
		_ = i.Start(context.Background())
	}

	return m, nil
}

func (srv *Memberlist) CanBroadcast() bool {
	switch {
	case srv.delegate != nil:
	case srv.IsJoined():
	case srv.RemotesLen() > 0:
	default:
		return false
	}

	return true
}

func (srv *Memberlist) broadcastEnsured(id string, threshold base.Threshold, excludemember func(Member) bool) bool {
	return srv.ebrecords.Get(id, func(nodes []base.Address, found bool) error {
		switch {
		case !found:
			return util.ErrNotFound.WithStack()
		case len(nodes) < 1:
			return errors.Errorf("not yet ensured")
		}

		var total, count uint

		srv.Remotes(func(member Member) bool {
			if excludemember != nil && excludemember(member) {
				return true
			}

			total++

			if slices.IndexFunc[base.Address](nodes, func(i base.Address) bool {
				return i.Equal(member.Address())
			}) >= 0 {
				count++
			}

			return true
		})

		if count < threshold.Threshold(total) {
			return errors.Errorf("not yet ensured")
		}

		return nil
	}) == nil
}

func (srv *Memberlist) loopUserMsgs(ctx context.Context) {
	sem := semaphore.NewWeighted(maxHandleUserMsg)
	go func() {
		_ = sem.Acquire(ctx, maxHandleUserMsg+1)
	}()

	ticker := time.NewTicker(time.Millisecond * 33)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := srv.handleUserMsgs(ctx, sem); err != nil {
				srv.Log().Error().Err(err).Msg("handle user msgs")

				return
			}
		}
	}
}

var maxHandleUserMsg int64 = 333

func (srv *Memberlist) handleUserMsgs(ctx context.Context, sem *semaphore.Weighted) error {
	bs := func() [][]byte {
		srv.usermsgsLock.Lock()
		defer srv.usermsgsLock.Unlock()

		bs := make([][]byte, maxHandleUserMsg)

		var i int64
		var elem *list.Element

	end:
		for {
			switch {
			case elem == nil:
				elem = srv.usermsgs.Front()
			default:
				elem = elem.Next()
			}

			if elem == nil {
				break end
			}

			srv.usermsgs.Remove(elem)

			switch b, ok := elem.Value.([]byte); {
			case !ok, len(b) < 1:
				continue end
			default:
				bs[i] = b
			}

			i++

			if i == maxHandleUserMsg {
				break end
			}
		}

		return bs[:i]
	}()

	if len(bs) < 1 {
		return nil
	}

	for i := range bs {
		if err := sem.Acquire(ctx, 1); err != nil {
			return errors.Wrap(err, "handle user msgs")
		}

		b := bs[i]

		go func() {
			defer sem.Release(1)

			srv.handleUserMsg(b)
		}()
	}

	return nil
}

func (srv *Memberlist) handleUserMsg(b []byte) {
	body := b
	var enc encoder.Encoder = srv.args.Encoder

	switch {
	case bytes.HasPrefix(body, callbackBroadcastMessageHeaderPrefix):
		switch i, e, err := srv.notifyMsgCallbackBroadcastMessage(
			body[len(callbackBroadcastMessageHeaderPrefix):]); {
		case err != nil:
			return
		default:
			enc = e
			body = i
		}
	case bytes.HasPrefix(body, ensureBroadcastMessageHeaderPrefix):
		switch i, err := srv.notifyMsgEnsureBroadcastMessage(
			body[len(ensureBroadcastMessageHeaderPrefix):]); {
		case err != nil:
			return
		default:
			body = i
		}
	}

	srv.handleUserMsgsFunc(body, enc)
}

func DefaultMemberlistConfig(name string, bind, advertise *net.UDPAddr) *memberlist.Config {
	config := BasicMemberlistConfig()

	config.Name = name
	config.BindAddr = bind.IP.String()
	config.BindPort = bind.Port
	config.AdvertiseAddr = advertise.IP.String()
	config.AdvertisePort = advertise.Port

	return config
}

func BasicMemberlistConfig() *memberlist.Config {
	config := memberlist.DefaultWANConfig()
	config.BindAddr = "127.0.0.1"
	config.BindPort = 4321
	config.AdvertiseAddr = config.BindAddr
	config.AdvertisePort = config.BindPort
	config.TCPTimeout = time.Second * 2
	config.IndirectChecks = math.MaxInt8
	config.RetransmitMult = 3
	config.ProbeTimeout = 500 * time.Millisecond //nolint:gomnd //...
	config.ProbeInterval = 1 * time.Second
	config.SuspicionMult = 3 // NOTE fast detection for failed members
	config.SuspicionMaxTimeoutMult = 3
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
	addrs, _ := util.NewShardedMap[string, Member](1 << 9)     //nolint:gomnd //...
	members, _ := util.NewShardedMap[string, []Member](1 << 9) //nolint:gomnd //...

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
			nid := memberid(n.Addr())

			switch {
			case id != nid:
				return true
			case !found:
				found = true
			}

			return false
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

func (m *membersPool) Set(member Member) (added bool) {
	_, _ = m.addrs.Set(memberid(member.Addr()), func(_ Member, addrfound bool) (Member, error) {
		var members []Member

		added = !addrfound

		switch i, f := m.members.Value(member.Address().String()); {
		case !f, i == nil:
		default:
			members = i
		}

		members = append(members, member)
		m.members.SetValue(member.Address().String(), members)

		return member, nil
	})

	return added
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
	handlerPrefix [32]byte,
	dialf quicstreamheader.DialFunc,
) func(context.Context, ConnInfoBroadcastMessage) (
	[]byte, encoder.Encoder, error,
) {
	return func(ctx context.Context, m ConnInfoBroadcastMessage) (
		b []byte, enc encoder.Encoder, _ error,
	) {
		stream, _, err := dialf(ctx, m.ConnInfo())
		if err != nil {
			return nil, nil, err
		}

		if err := stream(ctx, func(ctx context.Context, broker *quicstreamheader.ClientBroker) error {
			if err := broker.WriteRequestHead(
				ctx, NewCallbackBroadcastMessageHeader(m.ID(), handlerPrefix)); err != nil {
				return err
			}

			switch i, rh, err := broker.ReadResponseHead(ctx); {
			case err != nil:
				return err
			case rh == nil:
				return errors.Errorf("empty response header")
			case !rh.OK():
				return nil
			case rh.Err() != nil:
				return rh.Err()
			default:
				enc = i
			}

			switch _, _, rbody, _, res, err := broker.ReadBody(ctx); {
			case err != nil:
				return nil
			case res != nil:
				if res.Err() == nil {
					return errors.Errorf("empty body; response received")
				}

				return res.Err()
			default:
				i, rerr := io.ReadAll(rbody)
				if rerr != nil {
					return errors.WithMessage(rerr, "read fetched callback message")
				}

				b = i

				return nil
			}
		}); err != nil {
			return nil, nil, err
		}

		return b, enc, nil
	}
}

func PongEnsureBroadcastMessageFunc(
	handlerPrefix [32]byte,
	node base.Address,
	signer base.Privatekey,
	networkID base.NetworkID,
	dialf quicstreamheader.DialFunc,
) func(context.Context, ConnInfoBroadcastMessage) error {
	return func(ctx context.Context, m ConnInfoBroadcastMessage) error {
		h, err := NewEnsureBroadcastMessageHeader(m.ID(), handlerPrefix, node, signer, networkID)
		if err != nil {
			return err
		}

		stream, _, err := dialf(ctx, m.ConnInfo())
		if err != nil {
			return err
		}

		return stream(ctx, func(ctx context.Context, broker *quicstreamheader.ClientBroker) error {
			if err := broker.WriteRequestHead(ctx, h); err != nil {
				return err
			}

			switch _, rh, err := broker.ReadResponseHead(ctx); {
			case err != nil:
				return err
			case rh == nil:
				return errors.Errorf("empty response header")
			case !rh.OK():
				return nil
			case rh.Err() != nil:
				return rh.Err()
			default:
				return nil
			}
		})
	}
}
