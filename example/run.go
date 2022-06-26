package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	isaacdatabase "github.com/spikeekips/mitum/isaac/database"
	isaacnetwork "github.com/spikeekips/mitum/isaac/network"
	isaacstates "github.com/spikeekips/mitum/isaac/states"
	"github.com/spikeekips/mitum/launch"
	"github.com/spikeekips/mitum/network/quicstream"
	"github.com/spikeekips/mitum/network/quictransport"
	"github.com/spikeekips/mitum/util"
)

type runCommand struct { //nolint:govet //...
	baseNodeCommand
	Discovery            []launch.ConnInfoFlag `help:"discoveries" placeholder:"ConnInfo"`
	SyncNode             []launch.ConnInfoFlag `help:"node for syncing" placeholder:"ConnInfo"`
	Hold                 bool                  `help:"hold consensus states"`
	nodeInfo             launch.NodeInfo
	db                   isaac.Database
	perm                 isaac.PermanentDatabase
	getProposal          func(_ context.Context, facthash util.Hash) (base.ProposalSignedFact, error)
	suffrageStateBuilder *isaacstates.SuffrageStateBuilder
	proposalSelector     *isaac.BaseProposalSelector
	pool                 *isaacdatabase.TempPool
	getSuffrage          func(blockheight base.Height) (base.Suffrage, bool, error)
	newProposalProcessor newProposalProcessorFunc
	getLastManifest      func() (base.Manifest, bool, error)
	getSuffrageBooting   func(blockheight base.Height) (base.Suffrage, bool, error)
	states               *isaacstates.States
	memberlist           *quictransport.Memberlist
	getManifest          func(height base.Height) (base.Manifest, error)
	client               *isaacnetwork.QuicstreamClient
	handlers             *quicstream.PrefixHandler
	ballotbox            *isaacstates.Ballotbox
	quicstreamserver     *quicstream.Server
	discoveries          []quictransport.ConnInfo
}

func (cmd *runCommand) Run() error {
	switch stop, err := cmd.prepare(); {
	case err != nil:
		return err
	default:
		defer func() {
			_ = stop()
		}()
	}

	log.Debug().Msg("node started")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := cmd.quicstreamserver.Start(); err != nil {
		return err
	}

	if err := cmd.startMmemberlist(ctx); err != nil {
		return err
	}

	defer func() {
		_ = cmd.memberlist.Stop()
	}()

	var statesch <-chan error = make(chan error)

	if !cmd.Hold {
		statesch = cmd.states.Wait(ctx)
	}

	if cmd.states != nil {
		defer func() {
			if err := cmd.states.Stop(); err != nil {
				log.Error().Err(err).Msg("failed to stop states")
			}
		}()
	}

	select {
	case <-ctx.Done(): // NOTE graceful stop
		return nil
	case err := <-statesch:
		return err
	}
}
