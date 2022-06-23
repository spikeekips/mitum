package main

import (
	"context"
	"io"
	"net"
	"time"

	"github.com/pkg/errors"
	isaacnetwork "github.com/spikeekips/mitum/isaac/network"
	"github.com/spikeekips/mitum/network/quictransport"
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
		quictransport.NewNodeMeta(cmd.local.Address(), cmd.design.Network.TLSInsecure),
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

	memberlistdelegate := quictransport.NewDelegate(memberlistnode, nil)
	memberlistconfig.Delegate = memberlistdelegate

	memberlistalive := quictransport.NewAliveDelegate(
		cmd.enc,
		cmd.design.Network.Publish,
		func(quictransport.Node) error {
			return nil // FIXME disallow by last suffrage nodes
		},
	)
	memberlistconfig.Alive = memberlistalive

	memberlistevents := quictransport.NewEventsDelegate(
		cmd.enc,
		func(node quictransport.Node) {
			log.Debug().Interface("node", node).Msg("new node joined")
		},
		func(node quictransport.Node) {
			log.Debug().Interface("node", node).Msg("node left")
		},
	)
	memberlistconfig.Events = memberlistevents

	cmd.handlers.Add(isaacnetwork.HandlerPrefixMemberlist, func(addr net.Addr, r io.Reader, w io.Writer) error {
		var b []byte
		if b, err = io.ReadAll(r); err != nil {
			log.Error().Err(err).Stringer("remote_address", addr).Msg("failed to read")

			return errors.Wrap(err, "")
		}

		if err = memberlisttransport.ReceiveRaw(b, addr); err != nil {
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
