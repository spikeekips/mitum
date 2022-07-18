package main

import (
	"context"
	"io"
	"net"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	isaacnetwork "github.com/spikeekips/mitum/isaac/network"
	"github.com/spikeekips/mitum/network"
	"github.com/spikeekips/mitum/network/quicmemberlist"
	"github.com/spikeekips/mitum/util"
)

func (cmd *runCommand) prepareMemberlist() error {
	memberlisttransport := quicmemberlist.NewTransportWithQuicstream(
		cmd.design.Network.Publish(),
		isaacnetwork.HandlerPrefixMemberlist,
		cmd.client.PoolClient(),
		cmd.client.NewClient,
	)

	memberlistnode, err := quicmemberlist.NewNode(
		cmd.local.Address().String(),
		cmd.design.Network.Publish(),
		cmd.local.Address(),
		cmd.local.Publickey(),
		cmd.design.Network.PublishString,
		cmd.design.Network.TLSInsecure,
	)
	if err != nil {
		return err
	}

	memberlistconfig := quicmemberlist.BasicMemberlistConfig(
		cmd.nodeInfo.ID(),
		cmd.design.Network.Bind,
		cmd.design.Network.Publish(),
	)
	memberlistconfig.Transport = memberlisttransport

	memberlistdelegate := quicmemberlist.NewDelegate(memberlistnode, nil, func(b []byte) {
		log.Trace().Str("message", string(b)).Msg("new incoming message")

		i, err := cmd.enc.Decode(b) //nolint:govet //...
		if err != nil {
			log.Error().Err(err).Msg("failed to decode incoming message")

			return
		}

		switch t := i.(type) {
		case base.Ballot:
			_, err := cmd.ballotbox.Vote(t)
			if err != nil {
				log.Error().Err(err).Interface("ballot", t).Msg("failed to vote")
			}
		default:
			// NOTE ignore
		}
	})
	memberlistconfig.Delegate = memberlistdelegate

	memberlistalive := quicmemberlist.NewAliveDelegate(
		cmd.enc,
		cmd.design.Network.Publish(),
		cmd.nodeChallengeFunc(),
		cmd.memberlistAllowFunc(),
	)
	memberlistconfig.Alive = memberlistalive

	memberlistevents := quicmemberlist.NewEventsDelegate(
		cmd.enc,
		func(node quicmemberlist.Node) {
			log.Debug().Interface("node", node).Msg("new node found")

			if !node.Address().Equal(cmd.local.Address()) {
				nci := isaacnetwork.NewNodeConnInfoFromMemberlistNode(node)
				added := cmd.syncSourcePool.Add(nci)

				log.Debug().
					Bool("added", added).
					Interface("node_conninfo", nci).
					Msg("new node added to SyncSourcePool")
			}
		},
		func(node quicmemberlist.Node) {
			log.Debug().Interface("node", node).Msg("node left")

			if !node.Address().Equal(cmd.local.Address()) {
				removed := cmd.syncSourcePool.Remove(node.Address(), node.Publish().String())

				log.Debug().
					Bool("removed", removed).
					Interface("node", node.Address()).
					Str("publish", node.Publish().String()).
					Msg("node removed from SyncSourcePool")
			}
		},
	)
	memberlistconfig.Events = memberlistevents

	cmd.handlers.Add(isaacnetwork.HandlerPrefixMemberlist, func(addr net.Addr, r io.Reader, w io.Writer) error {
		b, err := io.ReadAll(r) //nolint:govet //...
		if err != nil {
			log.Error().Err(err).Stringer("remote_address", addr).Msg("failed to read")

			return errors.WithStack(err)
		}

		if err := memberlisttransport.ReceiveRaw(b, addr); err != nil {
			log.Error().Err(err).Stringer("remote_address", addr).Msg("invalid message received")

			return err
		}

		return nil
	})

	memberlist, err := quicmemberlist.NewMemberlist(
		memberlistnode,
		cmd.enc,
		memberlistconfig,
		3, //nolint:gomnd //...
	)
	if err != nil {
		return err
	}

	_ = memberlist.SetLogging(logging)

	cmd.memberlist = memberlist

	return nil
}

func (cmd *runCommand) nodeChallengeFunc() func(quicmemberlist.Node) error {
	return func(node quicmemberlist.Node) error {
		e := util.StringErrorFunc("failed to challenge memberlist node")

		ci, err := node.Publish().UDPConnInfo()
		if err != nil {
			return e(err, "invalid publish conninfo")
		}

		if err = util.CheckIsValid(nil, false, node.Publickey()); err != nil {
			return e(err, "invalid memberlist node publickey")
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*3) //nolint:gomnd //...
		defer cancel()

		input := util.UUID().Bytes()

		sig, err := cmd.client.NodeChallenge(
			ctx, node.UDPConnInfo(), cmd.nodePolicy.NetworkID(), node.Address(), node.Publickey(), input)
		if err != nil {
			return e(err, "")
		}

		// NOTE challenge with publish address
		if !network.EqualConnInfo(node.UDPConnInfo(), ci) {
			psig, err := cmd.client.NodeChallenge(ctx, ci,
				cmd.nodePolicy.NetworkID(), node.Address(), node.Publickey(), input)
			if err != nil {
				return e(err, "")
			}

			if !sig.Equal(psig) {
				return e(nil, "publish address returns different signature")
			}
		}

		return nil
	}
}

func (cmd *runCommand) memberlistAllowFunc() func(quicmemberlist.Node) error {
	return func(node quicmemberlist.Node) error {
		proof, st, err := cmd.lastSuffrageProofWatcher.Last()
		if err != nil {
			log.Error().Err(err).Msg("failed to get last suffrage proof; node will not be allowed")

			return err
		}

		suf, err := proof.Suffrage()
		if err != nil {
			log.Error().Err(err).Msg("wrong last suffrage proof; node will not be allowed")

			return err
		}

		if suf.ExistsPublickey(node.Address(), node.Publickey()) {
			return nil
		}

		if st != nil {
			candidates, err := base.LoadNodesFromSuffrageCandidateState(st)
			if err != nil {
				return err
			}

			for i := range candidates {
				n := candidates[i]

				if n.Address().Equal(node.Address()) && n.Publickey().Equal(node.Publickey()) {
					return nil
				}
			}
		}

		return errors.Errorf("not in suffrage or candidates; not allowed")
	}
}
