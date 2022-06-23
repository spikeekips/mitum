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

type runCommand struct {
	nodeInfo             launch.NodeInfo
	db                   isaac.Database
	perm                 isaac.PermanentDatabase
	memberlist           *quictransport.Memberlist
	suffrageStateBuilder *isaacstates.SuffrageStateBuilder
	proposalSelector     *isaac.BaseProposalSelector
	pool                 *isaacdatabase.TempPool
	getSuffrage          func(blockheight base.Height) (base.Suffrage, bool, error)
	newProposalProcessor newProposalProcessorFunc
	getLastManifest      func() (base.Manifest, bool, error)
	getSuffrageBooting   func(blockheight base.Height) (base.Suffrage, bool, error)
	quicstreamserver     *quicstream.Server
	getProposal          func(_ context.Context, facthash util.Hash) (base.ProposalSignedFact, error)
	getManifest          func(height base.Height) (base.Manifest, error)
	client               *isaacnetwork.QuicstreamClient
	handlers             *quicstream.PrefixHandler
	baseNodeCommand
	SyncNode    []launch.ConnInfoFlag `help:"node for syncing" placeholder:"ConnInfo"`
	Discovery   []launch.ConnInfoFlag `help:"discoveries" placeholder:"ConnInfo"`
	discoveries []quictransport.ConnInfo
	nodePolicy  isaac.NodePolicy
	Hold        bool `help:"hold consensus states"`
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

	var states *isaacstates.States
	var statesch <-chan error = make(chan error)

	if !cmd.Hold {
		var err error

		states, err = cmd.states()
		if err != nil {
			return err
		}

		statesch = states.Wait(ctx)
	}

	select {
	case <-ctx.Done(): // NOTE graceful stop
		if states != nil {
			if err := states.Stop(); err != nil {
				log.Error().Err(err).Msg("failed to stop states")
			}
		}

		return nil
	case err := <-statesch:
		return err
	}
}
