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
	PNameMemberlist      = ps.PName("memberlist")
	PNameStartMemberlist = ps.PName("start-memberlist")
	MemberlistContextKey = ps.ContextKey("memberlist")
)

func PMemberlist(ctx context.Context) (context.Context, error) {
	e := util.StringErrorFunc("failed to prepare memberlist")

	var log *logging.Logging
	var enc *jsonenc.Encoder

	if err := ps.LoadsFromContextOK(ctx,
		LoggingContextKey, &log,
		EncoderContextKey, &enc,
	); err != nil {
		return ctx, e(err, "")
	}

	localnode, err := memberlistLocalNode(ctx)
	if err != nil {
		return ctx, e(err, "")
	}

	config, err := memberlistConfig(ctx, localnode)
	if err != nil {
		return ctx, e(err, "")
	}

	m, err := quicmemberlist.NewMemberlist(
		localnode,
		enc,
		config,
		3, //nolint:gomnd //... // FIXME config
	)
	if err != nil {
		return ctx, e(err, "")
	}

	_ = m.SetLogging(log)

	ctx = context.WithValue(ctx, MemberlistContextKey, m) //revive:disable-line:modifies-parameter

	return ctx, nil
}

func PStartMemberlist(ctx context.Context) (context.Context, error) {
	var m *quicmemberlist.Memberlist
	if err := ps.LoadFromContextOK(ctx, MemberlistContextKey, &m); err != nil {
		return ctx, err
	}

	return ctx, m.Start()
}

func PCloseMemberlist(ctx context.Context) (context.Context, error) {
	var m *quicmemberlist.Memberlist
	if err := ps.LoadFromContext(ctx, MemberlistContextKey, &m); err != nil {
		return ctx, err
	}

	if m != nil {
		if err := m.Stop(); err != nil && !errors.Is(err, util.ErrDaemonAlreadyStopped) {
			return ctx, err
		}
	}

	return ctx, nil
}

func memberlistLocalNode(ctx context.Context) (quicmemberlist.Node, error) {
	var design NodeDesign
	var local base.LocalNode
	var fsnodeinfo NodeInfo

	if err := ps.LoadsFromContextOK(ctx,
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

func memberlistConfig(ctx context.Context, localnode quicmemberlist.Node) (*memberlist.Config, error) {
	var log *logging.Logging
	var enc *jsonenc.Encoder
	var design NodeDesign
	var fsnodeinfo NodeInfo
	var client *isaacnetwork.QuicstreamClient
	var local base.LocalNode
	var syncSourcePool *isaac.SyncSourcePool

	if err := ps.LoadsFromContextOK(ctx,
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

	transport, err := memberlistTransport(ctx)
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
		fsnodeinfo.ID(),
		design.Network.Bind,
		design.Network.Publish(),
	)

	config.Transport = transport
	config.Delegate = delegate
	config.Alive = alive

	config.Events = quicmemberlist.NewEventsDelegate(
		enc,
		func(node quicmemberlist.Node) {
			log.Log().Debug().Interface("node", node).Msg("new node found")

			if !node.Address().Equal(local.Address()) {
				nci := isaacnetwork.NewNodeConnInfoFromMemberlistNode(node)
				added := syncSourcePool.Add(nci)

				log.Log().Debug().
					Bool("added", added).
					Interface("node_conninfo", nci).
					Msg("new node added to SyncSourcePool")
			}
		},
		func(node quicmemberlist.Node) {
			log.Log().Debug().Interface("node", node).Msg("node left")

			if !node.Address().Equal(local.Address()) {
				removed := syncSourcePool.Remove(node.Address(), node.Publish().String())

				log.Log().Debug().
					Bool("removed", removed).
					Interface("node", node.Address()).
					Str("publish", node.Publish().String()).
					Msg("node removed from SyncSourcePool")
			}
		},
	)

	return config, nil
}

func memberlistTransport(ctx context.Context) (*quicmemberlist.Transport, error) {
	var log *logging.Logging
	var enc encoder.Encoder
	var design NodeDesign
	var fsnodeinfo NodeInfo
	var client *isaacnetwork.QuicstreamClient
	var local base.LocalNode
	var syncSourcePool *isaac.SyncSourcePool
	var handlers *quicstream.PrefixHandler

	if err := ps.LoadsFromContextOK(ctx,
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
		client.PoolClient(),
		client.NewClient,
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
	var ballotbox *isaacstates.Ballotbox

	if err := ps.LoadsFromContextOK(ctx,
		LoggingContextKey, &log,
		EncoderContextKey, &enc,
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
			log.Log().Trace().Interface("ballot", i).Msg("new incoming message")

			_, err := ballotbox.Vote(t)
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

	if err := ps.LoadsFromContextOK(ctx,
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
	var policy base.NodePolicy
	var client *isaacnetwork.QuicstreamClient

	if err := ps.LoadsFromContextOK(pctx,
		NodePolicyContextKey, &policy,
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

		if err = util.CheckIsValid(nil, false, node.Publickey()); err != nil {
			return errors.WithMessage(err, "invalid memberlist node publickey")
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*3) //nolint:gomnd //...
		defer cancel()

		input := util.UUID().Bytes()

		sig, err := client.NodeChallenge(
			ctx, node.UDPConnInfo(), policy.NetworkID(), node.Address(), node.Publickey(), input)
		if err != nil {
			return err
		}

		// NOTE challenge with publish address
		if !network.EqualConnInfo(node.UDPConnInfo(), ci) {
			psig, err := client.NodeChallenge(ctx, ci,
				policy.NetworkID(), node.Address(), node.Publickey(), input)
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

	if err := ps.LoadsFromContextOK(ctx,
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
