package launch

import (
	"context"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	isaacdatabase "github.com/spikeekips/mitum/isaac/database"
	isaacnetwork "github.com/spikeekips/mitum/isaac/network"
	isaacstates "github.com/spikeekips/mitum/isaac/states"
	"github.com/spikeekips/mitum/network"
	"github.com/spikeekips/mitum/network/quicmemberlist"
	"github.com/spikeekips/mitum/network/quicstream"
	quicstreamheader "github.com/spikeekips/mitum/network/quicstream/header"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/spikeekips/mitum/util/ps"
)

var (
	PNameMemberlist                         = ps.Name("memberlist")
	PNameStartMemberlist                    = ps.Name("start-memberlist")
	PNameLongRunningMemberlistJoin          = ps.Name("long-running-memberlist-join")
	PNameSuffrageVoting                     = ps.Name("suffrage-voting")
	PNamePatchMemberlist                    = ps.Name("patch-memberlist")
	MemberlistContextKey                    = util.ContextKey("memberlist")
	LongRunningMemberlistJoinContextKey     = util.ContextKey("long-running-memberlist-join")
	EventWhenMemberLeftContextKey           = util.ContextKey("event-when-member-left")
	SuffrageVotingContextKey                = util.ContextKey("suffrage-voting")
	SuffrageVotingVoteFuncContextKey        = util.ContextKey("suffrage-voting-vote-func")
	FilterMemberlistNotifyMsgFuncContextKey = util.ContextKey("filter-memberlist-notify-msg-func")
)

var HandlerNameMemberlist quicstream.HandlerName = "memberlist"

func PMemberlist(pctx context.Context) (context.Context, error) {
	e := util.StringError("prepare memberlist")

	var log *logging.Logging
	var encs *encoder.Encoders
	var local base.LocalNode
	var params *LocalParams
	var client *isaacnetwork.BaseClient
	var connectionPool *quicstream.ConnectionPool

	if err := util.LoadFromContextOK(pctx,
		LoggingContextKey, &log,
		EncodersContextKey, &encs,
		LocalContextKey, &local,
		LocalParamsContextKey, &params,
		QuicstreamClientContextKey, &client,
		ConnectionPoolContextKey, &connectionPool,
	); err != nil {
		return pctx, e.Wrap(err)
	}

	headerdial := quicstreamheader.NewDialFunc(
		connectionPool.Dial,
		encs,
		encs.Default(),
	)

	localnode, err := memberlistLocalNode(pctx)
	if err != nil {
		return pctx, e.Wrap(err)
	}

	config, err := memberlistConfig(pctx, localnode, connectionPool)
	if err != nil {
		return pctx, e.Wrap(err)
	}

	args := quicmemberlist.NewMemberlistArgs(encs.JSON(), config)
	args.ExtraSameMemberLimit = params.Memberlist.ExtraSameMemberLimit
	args.FetchCallbackBroadcastMessageFunc = quicmemberlist.FetchCallbackBroadcastMessageFunc(
		handlerPrefixMemberlistCallbackBroadcastMessage,
		headerdial,
	)

	args.PongEnsureBroadcastMessageFunc = quicmemberlist.PongEnsureBroadcastMessageFunc(
		handlerPrefixMemberlistEnsureBroadcastMessage,
		local.Address(),
		local.Privatekey(),
		params.ISAAC.NetworkID(),
		headerdial,
	)

	m, err := quicmemberlist.NewMemberlist(localnode, args)
	if err != nil {
		return pctx, e.Wrap(err)
	}

	_ = m.SetLogging(log)

	pps := ps.NewPS("event-member-left")

	m.SetWhenLeftFunc(func(quicmemberlist.Member) {
		if _, err := pps.Run(context.Background()); err != nil {
			log.Log().Error().Err(err).Msg("when member left")
		}
	})

	return util.ContextWithValues(pctx, map[util.ContextKey]interface{}{
		MemberlistContextKey:          m,
		EventWhenMemberLeftContextKey: pps,
		FilterMemberlistNotifyMsgFuncContextKey: quicmemberlist.FilterNotifyMsgFunc(
			func(interface{}) (bool, error) { return true, nil },
		),
	}), nil
}

func PStartMemberlist(pctx context.Context) (context.Context, error) {
	var m *quicmemberlist.Memberlist
	if err := util.LoadFromContextOK(pctx, MemberlistContextKey, &m); err != nil {
		return pctx, err
	}

	return pctx, m.Start(context.Background())
}

func PCloseMemberlist(pctx context.Context) (context.Context, error) {
	var m *quicmemberlist.Memberlist
	if err := util.LoadFromContext(pctx, MemberlistContextKey, &m); err != nil {
		return pctx, err
	}

	if m != nil {
		if err := m.Stop(); err != nil && !errors.Is(err, util.ErrDaemonAlreadyStopped) {
			return pctx, err
		}
	}

	return pctx, nil
}

func PLongRunningMemberlistJoin(pctx context.Context) (context.Context, error) {
	var local base.LocalNode
	var discoveries *util.Locked[[]quicstream.ConnInfo]
	var m *quicmemberlist.Memberlist
	var watcher *isaac.LastConsensusNodesWatcher

	if err := util.LoadFromContextOK(pctx,
		LocalContextKey, &local,
		DiscoveryContextKey, &discoveries,
		MemberlistContextKey, &m,
		LastConsensusNodesWatcherContextKey, &watcher,
	); err != nil {
		return nil, err
	}

	l := NewLongRunningMemberlistJoin(
		ensureJoinMemberlist(local, watcher, discoveries, m),
		m.IsJoined,
	)

	return context.WithValue(pctx, LongRunningMemberlistJoinContextKey, l), nil
}

func PPatchMemberlist(pctx context.Context) (ctx context.Context, err error) {
	ctx = pctx

	if ctx, err = patchMemberlistNotifyMsg(ctx); err != nil {
		return ctx, err
	}

	return patchMemberlistWhenMemberLeft(ctx)
}

func patchMemberlistNotifyMsg(pctx context.Context) (context.Context, error) {
	var log *logging.Logging
	var isaacparams *isaac.Params
	var ballotbox *isaacstates.Ballotbox
	var m *quicmemberlist.Memberlist
	var client *isaacnetwork.BaseClient
	var svvotef isaac.SuffrageVoteFunc
	var filternotifymsg quicmemberlist.FilterNotifyMsgFunc
	var oppool *isaacdatabase.TempPool

	if err := util.LoadFromContextOK(pctx,
		LoggingContextKey, &log,
		ISAACParamsContextKey, &isaacparams,
		BallotboxContextKey, &ballotbox,
		QuicstreamClientContextKey, &client,
		MemberlistContextKey, &m,
		SuffrageVotingVoteFuncContextKey, &svvotef,
		FilterMemberlistNotifyMsgFuncContextKey, &filternotifymsg,
		PoolDatabaseContextKey, &oppool,
	); err != nil {
		return pctx, err
	}

	l := log.Log().With().Str("module", "filter-notify-msg-memberlist").Logger()

	m.SetNotifyMsg(func(b []byte, enc encoder.Encoder) {
		m, err := enc.Decode(b) //nolint:govet //...
		if err != nil {
			l.Error().Err(err).Str("notify_message", string(b)).Msg("failed to decode incoming message")

			return
		}

		l.Trace().Interface("notify_message", m).Msg("new message notified")

		switch passed, err := filternotifymsg(m); {
		case err != nil:
			l.Trace().Err(err).Interface("notify_message", m).Msg("filter error")
		case !passed:
			l.Trace().Interface("notify_message", m).Msg("filtered")

			return
		}

		switch t := m.(type) {
		case base.Ballot:
			l.Trace().
				Interface("point", t.Point()).
				Stringer("node", t.SignFact().Node()).
				Msg("ballot notified")

			if err := t.IsValid(isaacparams.NetworkID()); err != nil {
				l.Trace().Err(err).Interface("ballot", t).Msg("new ballot; failed to vote")

				return
			}

			if _, err := ballotbox.Vote(t); err != nil {
				l.Error().Err(err).Interface("ballot", t).Msg("new ballot; failed to vote")

				return
			}
		case base.SuffrageExpelOperation:
			voted, err := svvotef(t)
			if err != nil {
				l.Error().Err(err).Interface("expel operation", t).
					Msg("new expel operation; failed to vote")

				return
			}

			l.Debug().Interface("expel", t).Bool("voted", voted).
				Msg("new expel operation; voted")
		case isaacstates.MissingBallotsRequestMessage:
			l.Trace().
				Interface("point", t.Point()).
				Interface("nodes", t.Nodes()).
				Msg("missing ballots request message notified")

			if err := t.IsValid(nil); err != nil {
				l.Trace().Err(err).Msg("invalid missing ballots request message")

				return
			}

			switch ballots := ballotbox.Voted(t.Point(), t.Nodes()); {
			case len(ballots) < 1:
				return
			default:
				if err := client.SendBallots(context.Background(), t.ConnInfo(), ballots); err != nil {
					l.Error().Err(err).Msg("failed to send ballots")
				}
			}
		case base.Operation:
			switch isset, err := oppool.SetOperation(context.Background(), t); {
			case err != nil:
				l.Error().Err(err).Msg("failed to set operation")
			default:
				l.Debug().Bool("set", isset).Msg("set operation")
			}
		default:
			l.Debug().Interface("notify_message", m).Msgf("new incoming message; ignored; but unknown, %T", t)
		}
	})

	return pctx, nil
}

func patchMemberlistWhenMemberLeft(pctx context.Context) (context.Context, error) {
	var log *logging.Logging
	var local base.LocalNode
	var pps *ps.PS
	var m *quicmemberlist.Memberlist
	var long *LongRunningMemberlistJoin
	var sp *SuffragePool
	var states *isaacstates.States

	if err := util.LoadFromContextOK(pctx,
		LoggingContextKey, &log,
		LocalContextKey, &local,
		EventWhenMemberLeftContextKey, &pps,
		MemberlistContextKey, &m,
		LongRunningMemberlistJoinContextKey, &long,
		SuffragePoolContextKey, &sp,
		StatesContextKey, &states,
	); err != nil {
		return pctx, err
	}

	_ = pps.Add("empty-members", func(ctx context.Context) (context.Context, error) {
		// NOTE if local is only member, trying to join; sometimes accidentally
		// local cast away from memberlist.
		switch {
		case m.IsJoined(),
			!states.AllowedConsensus(),
			len(quicmemberlist.AliveMembers(m, func(member quicmemberlist.Member) bool {
				return member.Address().Equal(local.Address())
			})) > 0:
			return ctx, nil
		}

		switch suf, found, err := sp.Last(); {
		case err != nil:
			return ctx, errors.WithMessage(err, "last suffrage for checking empty members")
		case !found:
			return ctx, errors.WithMessage(err, "last suffrage not found for checking empty members")
		case !suf.Exists(local.Address()):
			return ctx, nil
		default:
			log.Log().Debug().Msg("empty members; trying to join")

			_ = long.Join()
		}

		return ctx, nil
	}, nil)

	return pctx, nil
}

func memberlistLocalNode(pctx context.Context) (quicmemberlist.Member, error) {
	var design NodeDesign
	var local base.LocalNode
	var fsnodeinfo NodeInfo

	if err := util.LoadFromContextOK(pctx,
		DesignContextKey, &design,
		LocalContextKey, &local,
		FSNodeInfoContextKey, &fsnodeinfo,
	); err != nil {
		return nil, err
	}

	return quicmemberlist.NewMember(
		fsnodeinfo.ID(),
		design.Network.Publish(),
		local.Address(),
		local.Publickey(),
		design.Network.PublishString,
		design.Network.TLSInsecure,
	)
}

func memberlistConfig(
	pctx context.Context,
	localnode quicmemberlist.Member,
	connectionPool *quicstream.ConnectionPool,
) (*memberlist.Config, error) {
	var log *logging.Logging
	var encs *encoder.Encoders
	var design NodeDesign
	var fsnodeinfo NodeInfo
	var local base.LocalNode
	var syncSourcePool *isaac.SyncSourcePool

	if err := util.LoadFromContextOK(pctx,
		LoggingContextKey, &log,
		EncodersContextKey, &encs,
		DesignContextKey, &design,
		LocalContextKey, &local,
		FSNodeInfoContextKey, &fsnodeinfo,
		LocalContextKey, &local,
		SyncSourcePoolContextKey, &syncSourcePool,
	); err != nil {
		return nil, err
	}

	transport, err := memberlistTransport(pctx, connectionPool.Dial)
	if err != nil {
		return nil, err
	}

	delegate := quicmemberlist.NewDelegate(localnode, nil, func([]byte) {
		panic("set notifyMsgFunc")
	})

	alive, err := memberlistAlive(pctx)
	if err != nil {
		return nil, err
	}

	config := quicmemberlist.DefaultMemberlistConfig(
		localnode.Name(),
		design.Network.Bind,
		design.Network.Publish(),
	)

	params := design.LocalParams.Memberlist

	config.TCPTimeout = params.TCPTimeout()
	config.RetransmitMult = params.RetransmitMult()
	config.ProbeTimeout = params.ProbeTimeout()
	config.ProbeInterval = params.ProbeInterval()
	config.SuspicionMult = params.SuspicionMult()
	config.SuspicionMaxTimeoutMult = params.SuspicionMaxTimeoutMult()
	config.UDPBufferSize = params.UDPBufferSize()

	config.Transport = transport
	config.Delegate = delegate
	config.Alive = alive

	config.Events = quicmemberlist.NewEventsDelegate(
		encs.JSON(),
		func(member quicmemberlist.Member) {
			l := log.Log().With().Interface("member", member).Logger()

			l.Debug().Msg("new member found")

			cctx, cancel := context.WithTimeout(
				context.Background(), design.LocalParams.Network.TimeoutRequest())
			defer cancel()

			if _, err := connectionPool.Dial(cctx, member.ConnInfo()); err != nil {
				l.Error().Err(err).Msg("new member joined, but failed to dial")

				return
			}

			if !member.Address().Equal(local.Address()) {
				nci := isaacnetwork.NewNodeConnInfoFromMemberlistNode(member)
				added := syncSourcePool.AddNonFixed(nci)

				l.Debug().
					Bool("added", added).
					Interface("member_conninfo", nci).
					Msg("new member added to SyncSourcePool")
			}
		},
		func(member quicmemberlist.Member) {
			l := log.Log().With().Interface("member", member).Logger()

			if connectionPool.Close(member.ConnInfo()) {
				l.Debug().Msg("member removed from client pool")
			}

			nci := isaacnetwork.NewNodeConnInfoFromMemberlistNode(member)
			if syncSourcePool.RemoveNonFixed(nci) {
				l.Debug().Msg("member removed from sync source pool")
			}
		},
	)

	return config, nil
}

func memberlistTransport(
	pctx context.Context,
	dialf quicstream.ConnInfoDialFunc,
) (*quicmemberlist.Transport, error) {
	var log *logging.Logging
	var design NodeDesign
	var params *LocalParams
	var handlers *quicstream.PrefixHandler
	var rateLimitHandler *RateLimitHandler

	if err := util.LoadFromContextOK(pctx,
		LoggingContextKey, &log,
		DesignContextKey, &design,
		LocalParamsContextKey, &params,
		QuicstreamHandlersContextKey, &handlers,
		RateLimiterContextKey, &rateLimitHandler,
	); err != nil {
		return nil, err
	}

	transport := quicmemberlist.NewTransportWithQuicstream(
		design.Network.Publish(),
		HandlerNameMemberlist,
		dialf,
		nil,
		params.Network.TimeoutRequest,
	)
	_ = transport.SetLogging(log)

	var timeoutf func() time.Duration

	switch f, err := params.Network.HandlerTimeoutFunc(HandlerNameMemberlist); {
	case err != nil:
		return nil, err
	default:
		timeoutf = f
	}

	handler := rateLimitHandlerFunc(
		rateLimitHandler,
		func(prefix quicstream.HandlerPrefix) (string, bool) {
			s, found := NetworkHandlerPrefixMapRev[prefix]

			return s.String(), found
		},
	)(quicstream.TimeoutHandler(transport.QuicstreamHandler, timeoutf))

	_ = handlers.Add(HandlerNameMemberlist, handler)

	return transport, nil
}

func memberlistAlive(pctx context.Context) (*quicmemberlist.AliveDelegate, error) {
	var design NodeDesign
	var encs *encoder.Encoders

	if err := util.LoadFromContextOK(pctx,
		DesignContextKey, &design,
		EncodersContextKey, &encs,
	); err != nil {
		return nil, err
	}

	nc, err := nodeChallengeFunc(pctx)
	if err != nil {
		return nil, err
	}

	al, err := memberlistAllowFunc(pctx)
	if err != nil {
		return nil, err
	}

	return quicmemberlist.NewAliveDelegate(
		encs.JSON(),
		design.Network.Publish(),
		nc,
		al,
	), nil
}

func nodeChallengeFunc(pctx context.Context) (
	func(quicmemberlist.Member) error,
	error,
) {
	var local base.LocalNode
	var params *LocalParams
	var client *isaacnetwork.BaseClient

	if err := util.LoadFromContextOK(pctx,
		LocalContextKey, &local,
		LocalParamsContextKey, &params,
		QuicstreamClientContextKey, &client,
	); err != nil {
		return nil, err
	}

	return func(node quicmemberlist.Member) error {
		e := util.StringError("challenge memberlist node")

		ci := node.Publish().ConnInfo()

		if err := util.CheckIsValiders(nil, false, node.Publickey()); err != nil {
			return errors.WithMessage(err, "invalid memberlist node publickey")
		}

		input := util.UUID().Bytes()

		sig, err := func() (base.Signature, error) {
			ctx, cancel := context.WithTimeout(context.Background(), params.Network.TimeoutRequest())
			defer cancel()

			return client.NodeChallenge(
				ctx, node.ConnInfo(), params.ISAAC.NetworkID(),
				node.Address(), node.Publickey(), input, local,
			)
		}()
		if err != nil {
			return err
		}

		// NOTE challenge with publish address
		if !network.EqualConnInfo(node.ConnInfo(), ci) {
			ctx, cancel := context.WithTimeout(context.Background(), params.Network.TimeoutRequest())
			defer cancel()

			psig, err := client.NodeChallenge(
				ctx, ci, params.ISAAC.NetworkID(),
				node.Address(), node.Publickey(), input, local,
			)
			if err != nil {
				return err
			}

			if !sig.Equal(psig) {
				return e.Errorf("publish address returns different signature")
			}
		}

		return nil
	}, nil
}

func memberlistAllowFunc(pctx context.Context) (
	func(quicmemberlist.Member) error,
	error,
) {
	var log *logging.Logging
	var watcher *isaac.LastConsensusNodesWatcher

	if err := util.LoadFromContextOK(pctx,
		LoggingContextKey, &log,
		LastConsensusNodesWatcherContextKey, &watcher,
	); err != nil {
		return nil, err
	}

	return func(node quicmemberlist.Member) error {
		l := log.Log().With().Interface("remote", node).Logger()

		switch suf, found, err := watcher.Exists(node); {
		case err != nil:
			l.Error().Err(err).Msg("failed to check node in consensus nodes; node will not be allowed")

			return err
		case !found:
			l.Error().Err(err).Interface("suffrage", suf).Msg("node not in consensus nodes; node will not be allowed")

			return util.ErrNotFound.Errorf("node not in consensus nodes")
		default:
			return nil
		}
	}, nil
}

type LongRunningMemberlistJoin struct {
	ensureJoin    func([]quicstream.ConnInfo) (bool, error)
	isJoined      func() bool
	cancelrunning *util.Locked[context.CancelFunc]
	donech        *util.Locked[chan struct{}] // revive:disable-line:nested-structs
	interval      time.Duration
}

func NewLongRunningMemberlistJoin(
	ensureJoin func([]quicstream.ConnInfo) (bool, error),
	isJoined func() bool,
) *LongRunningMemberlistJoin {
	return &LongRunningMemberlistJoin{
		ensureJoin:    ensureJoin,
		isJoined:      isJoined,
		cancelrunning: util.EmptyLocked[context.CancelFunc](),
		donech:        util.EmptyLocked[chan struct{}](),
		interval:      time.Second * 3, //nolint:mnd //...
	}
}

func (l *LongRunningMemberlistJoin) Join(cis ...quicstream.ConnInfo) <-chan struct{} {
	if l.isJoined() {
		return nil
	}

	var donech chan struct{}

	_, _ = l.cancelrunning.Set(func(i context.CancelFunc, _ bool) (context.CancelFunc, error) {
		if i != nil {
			switch c, _ := l.donech.Value(); {
			case c == nil:
				i()
			default:
				donech = c

				return nil, util.ErrLockedSetIgnore
			}
		}

		ldonech := make(chan struct{})
		donech = ldonech
		_ = l.donech.SetValue(ldonech)

		ctx, cancel := context.WithCancel(context.Background())

		go func() {
			defer func() {
				cancel()

				_ = l.cancelrunning.EmptyValue()
			}()

			_ = util.Retry(ctx,
				func() (bool, error) {
					isjoined, err := l.ensureJoin(cis)

					return !isjoined, err
				},
				-1,
				l.interval,
			)

			close(ldonech)

			_ = l.donech.SetValue(nil)
		}()

		return cancel, nil
	})

	return donech
}

func (l *LongRunningMemberlistJoin) Cancel() error {
	_, _ = l.cancelrunning.Set(func(i context.CancelFunc, _ bool) (context.CancelFunc, error) {
		if i == nil {
			return nil, nil
		}

		i()

		return nil, nil
	})

	return nil
}

func ensureJoinMemberlist(
	local base.LocalNode,
	watcher *isaac.LastConsensusNodesWatcher,
	discoveries *util.Locked[[]quicstream.ConnInfo],
	m *quicmemberlist.Memberlist,
) func([]quicstream.ConnInfo) (bool, error) {
	return func(cis []quicstream.ConnInfo) (bool, error) {
		dis := GetDiscoveriesFromLocked(discoveries)
		dis = append(dis, cis...)

		dis, _ = util.RemoveDuplicatedSlice(
			dis,
			func(i quicstream.ConnInfo) (string, error) {
				return i.UDPAddr().String(), nil
			},
		)

		if len(dis) < 1 {
			return false, errors.Errorf("empty discovery")
		}

		if m.IsJoined() {
			return true, nil
		}

		switch _, found, err := watcher.Exists(local); {
		case err != nil:
			return false, err
		case !found:
			return true, errors.Errorf("local, not in consensus nodes")
		}

		err := m.Join(dis)

		return m.IsJoined(), err
	}
}
