package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	isaacdatabase "github.com/spikeekips/mitum/isaac/database"
	isaacnetwork "github.com/spikeekips/mitum/isaac/network"
	isaacstates "github.com/spikeekips/mitum/isaac/states"
	"github.com/spikeekips/mitum/launch"
	"github.com/spikeekips/mitum/network/quicmemberlist"
	"github.com/spikeekips/mitum/network/quicstream"
	"github.com/spikeekips/mitum/util"
)

type runCommand struct { //nolint:govet //...
	baseNodeCommand
	Discovery                []launch.ConnInfoFlag `help:"member discovery" placeholder:"ConnInfo"`
	Hold                     bool                  `help:"hold consensus states"`
	nodeInfo                 launch.NodeInfo
	db                       isaac.Database
	perm                     isaac.PermanentDatabase
	getProposal              func(_ context.Context, facthash util.Hash) (base.ProposalSignedFact, error)
	lastSuffrageProofWatcher *isaacstates.LastSuffrageProofWatcher
	proposalSelector         *isaac.BaseProposalSelector
	pool                     *isaacdatabase.TempPool
	getSuffrage              func(blockheight base.Height) (base.Suffrage, bool, error)
	newProposalProcessor     newProposalProcessorFunc
	getLastManifest          func() (base.Manifest, bool, error)
	states                   *isaacstates.States
	memberlist               *quicmemberlist.Memberlist
	getManifest              func(height base.Height) (base.Manifest, error)
	client                   *isaacnetwork.QuicstreamClient
	handlers                 *quicstream.PrefixHandler
	ballotbox                *isaacstates.Ballotbox
	quicstreamserver         *quicstream.Server
	discoveries              []quicstream.UDPConnInfo
	syncSourceChecker        *isaacnetwork.SyncSourceChecker
	syncSourcePool           *isaac.SyncSourcePool
	syncSourcesRetryInterval time.Duration
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

	if err := cmd.syncSourceChecker.Start(); err != nil {
		return err
	}

	if err := cmd.lastSuffrageProofWatcher.Start(); err != nil {
		return err
	}

	defer func() {
		_ = cmd.syncSourceChecker.Stop()
		_ = cmd.lastSuffrageProofWatcher.Stop()
		_ = cmd.memberlist.Stop()
	}()

	exitch := make(chan error)

	if !cmd.Hold {
		go func() {
			exitch <- <-cmd.states.Wait(ctx)
		}()
	}

	if cmd.states != nil {
		defer func() {
			if err := cmd.states.Stop(); err != nil && !errors.Is(err, util.ErrDaemonAlreadyStopped) {
				log.Error().Err(err).Msg("failed to stop states")
			}
		}()
	}

	select {
	case <-ctx.Done(): // NOTE graceful stop
		return nil
	case err := <-exitch:
		return err
	}
}
