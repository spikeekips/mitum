package main

import (
	"context"
	"io"
	"net"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	isaacnetwork "github.com/spikeekips/mitum/isaac/network"
	"github.com/spikeekips/mitum/network/quictransport"
	"github.com/spikeekips/mitum/util"
)

func (cmd *runCommand) prepareMemberlist() error {
	memberlisttransport := quictransport.NewTransportWithQuicstream(
		cmd.design.Network.Publish,
		isaacnetwork.HandlerPrefixMemberlist,
		cmd.client.PoolClient(),
		cmd.client.NewClient,
	)

	memberlistnode, err := quictransport.NewNode(
		cmd.local.Address().String(),
		cmd.design.Network.Publish,
		quictransport.NewNodeMeta(cmd.local.Address(), cmd.local.Publickey(), cmd.design.Network.TLSInsecure),
	)
	if err != nil {
		return err
	}

	memberlistconfig := quictransport.BasicMemberlistConfig(
		cmd.nodeInfo.ID(),
		cmd.design.Network.Bind,
		cmd.design.Network.Publish,
	)
	memberlistconfig.Transport = memberlisttransport

	memberlistdelegate := quictransport.NewDelegate(memberlistnode, nil, func(b []byte) {
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

	memberlistalive := quictransport.NewAliveDelegate(
		cmd.enc,
		cmd.design.Network.Publish,
		cmd.memberlistNodeChallengeFunc(),
		cmd.memberlistAllowFunc(),
	)
	memberlistconfig.Alive = memberlistalive

	memberlistevents := quictransport.NewEventsDelegate(
		cmd.enc,
		func(node quictransport.Node) {
			log.Debug().Interface("node", node).Msg("new node found")
		},
		func(node quictransport.Node) {
			log.Debug().Interface("node", node).Msg("node left")
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

	memberlist, err := quictransport.NewMemberlist(
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

func (cmd *runCommand) startMmemberlist(ctx context.Context) error {
	if err := cmd.memberlist.Start(); err != nil {
		return err
	}

	if len(cmd.discoveries) < 1 {
		return nil
	}

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	log.Debug().Interface("discoveries", cmd.discoveries).Msg("trying to join")

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			switch err := cmd.memberlist.Join(cmd.discoveries); {
			case err != nil:
				log.Error().Err(err).Msg("failed to join")
			default:
				log.Debug().Msg("joined")

				return nil
			}
		}
	}
}

func (cmd *runCommand) memberlistNodeChallengeFunc() func(quictransport.Node) error {
	return func(node quictransport.Node) error {
		e := util.StringErrorFunc("failed to challenge memberlist node")

		pub := node.Meta().Publickey()

		if err := util.CheckIsValid(nil, false, pub); err != nil {
			return e(err, "invalid memberlist node publickey")
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*3) //nolint:gomnd //...
		defer cancel()

		input := util.UUID().Bytes()

		sig, err := cmd.client.MemberlistNodeChallenge(ctx, node, input)
		if err != nil {
			return e(err, "")
		}

		if err := pub.Verify(util.ConcatBytesSlice(cmd.nodePolicy.NetworkID(), input), sig); err != nil {
			return e(err, "")
		}

		return nil
	}
}

func (*runCommand) memberlistAllowFunc() func(quictransport.Node) error {
	// FIXME last suffrage from suffrageStateBuilder

	return func(node quictransport.Node) error {
		return nil // FIXME disallow by last suffrage nodes
	}
}
