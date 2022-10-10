package launch

import (
	"context"
	"io"
	"net"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	isaacnetwork "github.com/spikeekips/mitum/isaac/network"
	isaacstates "github.com/spikeekips/mitum/isaac/states"
	"github.com/spikeekips/mitum/network"
	"github.com/spikeekips/mitum/network/quicmemberlist"
	"github.com/spikeekips/mitum/network/quicstream"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/spikeekips/mitum/util/ps"
)

var (
	PNameMemberlist                     = ps.Name("memberlist")
	PNameStartMemberlist                = ps.Name("start-memberlist")
	PNameLongRunningMemberlistJoin      = ps.Name("long-running-memberlist-join")
	MemberlistContextKey                = util.ContextKey("memberlist")
	LongRunningMemberlistJoinContextKey = util.ContextKey("long-running-memberlist-join")
	EventOnEmptyMembers                 = util.ContextKey("event-on-empty-members")
)

func PMemberlist(ctx context.Context) (context.Context, error) {
	e := util.StringErrorFunc("failed to prepare memberlist")

	var log *logging.Logging
	var enc *jsonenc.Encoder
	var params *isaac.LocalParams

	if err := util.LoadFromContextOK(ctx,
		LoggingContextKey, &log,
		EncoderContextKey, &enc,
		LocalParamsContextKey, &params,
	); err != nil {
		return ctx, e(err, "")
	}

	poolclient := quicstream.NewPoolClient()

	localnode, err := memberlistLocalNode(ctx)
	if err != nil {
		return ctx, e(err, "")
	}

	config, err := memberlistConfig(ctx, localnode, poolclient)
	if err != nil {
		return ctx, e(err, "")
	}

	m, err := quicmemberlist.NewMemberlist(
		localnode,
		enc,
		config,
		params.SameMemberLimit(),
	)
	if err != nil {
		return ctx, e(err, "")
	}

	_ = m.SetLogging(log)

	pps := ps.NewPS("event-on-empty-members")

	m.SetWhenLeftFunc(func(quicmemberlist.Node) {
		if m.IsJoined() {
			return
		}

		if _, err := pps.Run(context.Background()); err != nil {
			log.Log().Error().Err(err).Msg("failed to run onEmptyMembers")
		}
	})

	//revive:disable:modifies-parameter
	ctx = context.WithValue(ctx, MemberlistContextKey, m)
	ctx = context.WithValue(ctx, EventOnEmptyMembers, pps)
	//revive:enable:modifies-parameter

	return ctx, nil
}

func PStartMemberlist(ctx context.Context) (context.Context, error) {
	var m *quicmemberlist.Memberlist
	if err := util.LoadFromContextOK(ctx, MemberlistContextKey, &m); err != nil {
		return ctx, err
	}

	return ctx, m.Start()
}

func PCloseMemberlist(ctx context.Context) (context.Context, error) {
	var m *quicmemberlist.Memberlist
	if err := util.LoadFromContext(ctx, MemberlistContextKey, &m); err != nil {
		return ctx, err
	}

	if m != nil {
		if err := m.Stop(); err != nil && !errors.Is(err, util.ErrDaemonAlreadyStopped) {
			return ctx, err
		}
	}

	return ctx, nil
}

func PLongRunningMemberlistJoin(ctx context.Context) (context.Context, error) {
	var discoveries *util.Locked
	var m *quicmemberlist.Memberlist

	if err := util.LoadFromContextOK(ctx,
		DiscoveryContextKey, &discoveries,
		MemberlistContextKey, &m,
	); err != nil {
		return nil, err
	}

	l := NewLongRunningMemberlistJoin(
		ensureJoinMemberlist(discoveries, m),
		m.IsJoined,
	)

	ctx = context.WithValue(ctx, LongRunningMemberlistJoinContextKey, l) //revive:disable-line:modifies-parameter

	return ctx, nil
}

func memberlistLocalNode(ctx context.Context) (quicmemberlist.Node, error) {
	var design NodeDesign
	var local base.LocalNode
	var fsnodeinfo NodeInfo

	if err := util.LoadFromContextOK(ctx,
		DesignContextKey, &design,
		LocalContextKey, &local,
		FSNodeInfoContextKey, &fsnodeinfo,
	); err != nil {
		return nil, err
	}

	return quicmemberlist.NewNode(
		fsnodeinfo.ID(),
		design.Network.Publish(),
		local.Address(),
		local.Publickey(),
		design.Network.PublishString,
		design.Network.TLSInsecure,
	)
}

func memberlistConfig(
	ctx context.Context,
	localnode quicmemberlist.Node,
	poolclient *quicstream.PoolClient,
) (*memberlist.Config, error) {
	var log *logging.Logging
	var enc *jsonenc.Encoder
	var design NodeDesign
	var fsnodeinfo NodeInfo
	var client *isaacnetwork.QuicstreamClient
	var local base.LocalNode
	var syncSourcePool *isaac.SyncSourcePool

	if err := util.LoadFromContextOK(ctx,
		LoggingContextKey, &log,
		EncoderContextKey, &enc,
		DesignContextKey, &design,
		LocalContextKey, &local,
		FSNodeInfoContextKey, &fsnodeinfo,
		QuicstreamClientContextKey, &client,
		EncoderContextKey, &enc,
		LocalContextKey, &local,
		SyncSourcePoolContextKey, &syncSourcePool,
	); err != nil {
		return nil, err
	}

	transport, err := memberlistTransport(ctx, poolclient)
	if err != nil {
		return nil, err
	}

	delegate, err := memberlistDelegate(ctx, localnode)
	if err != nil {
		return nil, err
	}

	alive, err := memberlistAlive(ctx)
	if err != nil {
		return nil, err
	}

	config := quicmemberlist.BasicMemberlistConfig(
		localnode.Name(),
		design.Network.Bind,
		design.Network.Publish(),
	)

	config.Transport = transport
	config.Delegate = delegate
	config.Alive = alive

	config.Events = quicmemberlist.NewEventsDelegate(
		enc,
		func(node quicmemberlist.Node) {
			l := log.Log().With().Interface("node", node).Logger()

			l.Debug().Msg("new node found")

			cctx, cancel := context.WithTimeout(
				context.Background(), time.Second*5) //nolint:gomnd //....
			defer cancel()

			c := client.NewQuicstreamClient(node.UDPConnInfo())(node.UDPAddr())
			if _, err := c.Dial(cctx); err != nil {
				l.Error().Err(err).Msg("new node joined, but failed to dial")

				return
			}

			poolclient.Add(node.UDPAddr(), c)

			if !node.Address().Equal(local.Address()) {
				nci := isaacnetwork.NewNodeConnInfoFromMemberlistNode(node)
				added := syncSourcePool.Add(nci)

				l.Debug().
					Bool("added", added).
					Interface("node_conninfo", nci).
					Msg("new node added to SyncSourcePool")
			}
		},
		func(node quicmemberlist.Node) {
			log.Log().Debug().Interface("node", node).Msg("node left")

			poolclient.Remove(node.UDPAddr())
		},
	)

	return config, nil
}

func memberlistTransport(
	ctx context.Context,
	poolclient *quicstream.PoolClient,
) (*quicmemberlist.Transport, error) {
	var log *logging.Logging
	var enc encoder.Encoder
	var design NodeDesign
	var fsnodeinfo NodeInfo
	var client *isaacnetwork.QuicstreamClient
	var local base.LocalNode
	var syncSourcePool *isaac.SyncSourcePool
	var handlers *quicstream.PrefixHandler

	if err := util.LoadFromContextOK(ctx,
		LoggingContextKey, &log,
		EncoderContextKey, &enc,
		DesignContextKey, &design,
		LocalContextKey, &local,
		FSNodeInfoContextKey, &fsnodeinfo,
		QuicstreamClientContextKey, &client,
		EncoderContextKey, &enc,
		LocalContextKey, &local,
		SyncSourcePoolContextKey, &syncSourcePool,
		QuicstreamHandlersContextKey, &handlers,
	); err != nil {
		return nil, err
	}

	transport := quicmemberlist.NewTransportWithQuicstream(
		design.Network.Publish(),
		isaacnetwork.HandlerPrefixMemberlist,
		poolclient,
		client.NewQuicstreamClient,
	)

	_ = handlers.Add(isaacnetwork.HandlerPrefixMemberlist, func(addr net.Addr, r io.Reader, w io.Writer) error {
		b, err := io.ReadAll(r)
		if err != nil {
			log.Log().Error().Err(err).Stringer("remote_address", addr).Msg("failed to read")

			return errors.WithStack(err)
		}

		if err := transport.ReceiveRaw(b, addr); err != nil {
			log.Log().Error().Err(err).Stringer("remote_address", addr).Msg("invalid message received")

			return err
		}

		return nil
	})

	return transport, nil
}

func memberlistDelegate(ctx context.Context, localnode quicmemberlist.Node) (*quicmemberlist.Delegate, error) {
	var log *logging.Logging
	var enc encoder.Encoder
	var params *isaac.LocalParams
	var db isaac.Database
	var ballotbox *isaacstates.Ballotbox

	if err := util.LoadFromContextOK(ctx,
		LoggingContextKey, &log,
		EncoderContextKey, &enc,
		LocalParamsContextKey, &params,
		CenterDatabaseContextKey, &db,
		BallotboxContextKey, &ballotbox,
	); err != nil {
		return nil, err
	}

	return quicmemberlist.NewDelegate(localnode, nil, func(b []byte) {
		i, err := enc.Decode(b) //nolint:govet //...
		if err != nil {
			log.Log().Error().Err(err).Str("message", string(b)).Msg("failed to decode incoming message")

			return
		}

		switch t := i.(type) {
		case base.Ballot:
			// FIXME fetch original ballot from location instead of real body
			log.Log().Trace().Interface("ballot", i).Msg("new incoming message")

			if err := t.IsValid(params.NetworkID()); err != nil {
				log.Log().Error().Err(err).Interface("ballot", t).Msg("failed to vote")

				return
			}

			_, err := ballotbox.Vote(t, params.Threshold())
			if err != nil {
				log.Log().Error().Err(err).Interface("ballot", t).Msg("failed to vote")
			}
		default:
			log.Log().Trace().Interface("message", i).Msgf("new incoming message; ignored; but unknown, %T", t)
		}
	}), nil
}

func memberlistAlive(ctx context.Context) (*quicmemberlist.AliveDelegate, error) {
	var design NodeDesign
	var enc *jsonenc.Encoder

	if err := util.LoadFromContextOK(ctx,
		DesignContextKey, &design,
		EncoderContextKey, &enc,
	); err != nil {
		return nil, err
	}

	nc, err := nodeChallengeFunc(ctx)
	if err != nil {
		return nil, err
	}

	al, err := memberlistAllowFunc(ctx)
	if err != nil {
		return nil, err
	}

	return quicmemberlist.NewAliveDelegate(
		enc,
		design.Network.Publish(),
		nc,
		al,
	), nil
}

func nodeChallengeFunc(pctx context.Context) (
	func(quicmemberlist.Node) error,
	error,
) {
	var params base.LocalParams
	var client *isaacnetwork.QuicstreamClient

	if err := util.LoadFromContextOK(pctx,
		LocalParamsContextKey, &params,
		QuicstreamClientContextKey, &client,
	); err != nil {
		return nil, err
	}

	return func(node quicmemberlist.Node) error {
		e := util.StringErrorFunc("failed to challenge memberlist node")

		ci, err := node.Publish().UDPConnInfo()
		if err != nil {
			return errors.WithMessage(err, "invalid publish conninfo")
		}

		if err = util.CheckIsValiders(nil, false, node.Publickey()); err != nil {
			return errors.WithMessage(err, "invalid memberlist node publickey")
		}

		input := util.UUID().Bytes()

		sig, err := func() (base.Signature, error) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*2) //nolint:gomnd //...
			defer cancel()

			return client.NodeChallenge(
				ctx, node.UDPConnInfo(), params.NetworkID(), node.Address(), node.Publickey(), input)
		}()
		if err != nil {
			return err
		}

		// NOTE challenge with publish address
		if !network.EqualConnInfo(node.UDPConnInfo(), ci) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*2) //nolint:gomnd //...
			defer cancel()

			psig, err := client.NodeChallenge(ctx, ci,
				params.NetworkID(), node.Address(), node.Publickey(), input)
			if err != nil {
				return err
			}

			if !sig.Equal(psig) {
				return e(nil, "publish address returns different signature")
			}
		}

		return nil
	}, nil
}

func memberlistAllowFunc(ctx context.Context) (
	func(quicmemberlist.Node) error,
	error,
) {
	var log *logging.Logging
	var watcher *isaac.LastConsensusNodesWatcher

	if err := util.LoadFromContextOK(ctx,
		LoggingContextKey, &log,
		LastSuffrageProofWatcherContextKey, &watcher,
	); err != nil {
		return nil, err
	}

	return func(node quicmemberlist.Node) error {
		proof, st, err := watcher.Last()
		if err != nil {
			log.Log().Error().Err(err).Msg("failed to check last consensus nodes; node will not be allowed")

			return err
		}

		switch _, found, err := isaac.IsNodeInLastConsensusNodes(node, proof, st); {
		case err != nil:
			log.Log().Error().Err(err).Msg("failed to check node in consensus nodes; node will not be allowed")

			return err
		case !found:
			log.Log().Error().Err(err).Msg("node not in consensus nodes; node will not be allowed")

			return util.ErrNotFound.Errorf("node not in consensus nodes")
		default:
			return nil
		}
	}, nil
}

type LongRunningMemberlistJoin struct {
	ensureJoin    func() (bool, error)
	isJoined      func() bool
	cancelrunning *util.Locked
	donech        *util.Locked
	interval      time.Duration
}

func NewLongRunningMemberlistJoin(
	ensureJoin func() (bool, error),
	isJoined func() bool,
) *LongRunningMemberlistJoin {
	return &LongRunningMemberlistJoin{
		ensureJoin:    ensureJoin,
		isJoined:      isJoined,
		cancelrunning: util.EmptyLocked(),
		donech:        util.EmptyLocked(),
		interval:      time.Second * 3, //nolint:gomnd //...
	}
}

func (l *LongRunningMemberlistJoin) Join() <-chan struct{} {
	if l.isJoined() {
		return nil
	}

	var donech chan struct{}

	_, _ = l.cancelrunning.Set(func(_ bool, i interface{}) (interface{}, error) {
		if i != nil {
			switch c, _ := l.donech.Value(); {
			case c == nil:
				i.(context.CancelFunc)() //nolint:forcetypeassert //...
			default:
				donech = c.(chan struct{}) //nolint:forcetypeassert //...

				return nil, util.ErrLockedSetIgnore.Call()
			}
		}

		ldonech := make(chan struct{})
		donech = ldonech
		_ = l.donech.SetValue(ldonech)

		ctx, cancel := context.WithCancel(context.Background())

		go func() {
			defer func() {
				cancel()

				_ = l.cancelrunning.Empty()
			}()

			_ = util.Retry(ctx,
				func() (bool, error) {
					isjoined, err := l.ensureJoin()

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

func (l *LongRunningMemberlistJoin) Cancel() bool {
	_, _ = l.cancelrunning.Set(func(_ bool, i interface{}) (interface{}, error) {
		if i == nil {
			return nil, nil
		}

		i.(context.CancelFunc)() //nolint:forcetypeassert //...

		return nil, nil
	})

	return true
}

func ensureJoinMemberlist(discoveries *util.Locked, m *quicmemberlist.Memberlist) func() (bool, error) {
	return func() (bool, error) {
		dis := GetDiscoveriesFromLocked(discoveries)

		if len(dis) < 1 {
			return false, errors.Errorf("empty discovery")
		}

		if m.IsJoined() {
			return true, nil
		}

		err := m.Join(dis)

		return m.IsJoined(), err
	}
}
