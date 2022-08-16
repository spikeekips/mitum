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
	"github.com/spikeekips/mitum/launch2"
	"github.com/spikeekips/mitum/network/quicmemberlist"
	"github.com/spikeekips/mitum/network/quicstream"
	leveldbstorage "github.com/spikeekips/mitum/storage/leveldb"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/spikeekips/mitum/util/ps"
)

type RunCommand struct { //nolint:govet //...
	baseNodeCommand
	Discovery                   []launch.ConnInfoFlag `help:"member discovery" placeholder:"ConnInfo"`
	Hold                        launch.HeightFlag     `help:"hold consensus states"`
	nodeInfo                    launch2.NodeInfo
	db                          isaac.Database
	perm                        isaac.PermanentDatabase
	getProposal                 func(_ context.Context, facthash util.Hash) (base.ProposalSignedFact, error)
	lastSuffrageProofWatcher    *isaac.LastConsensusNodesWatcher
	proposalSelector            *isaac.BaseProposalSelector
	pool                        *isaacdatabase.TempPool
	getSuffrage                 func(blockheight base.Height) (base.Suffrage, bool, error)
	newProposalProcessor        newProposalProcessorFunc
	getLastManifest             func() (base.Manifest, bool, error)
	states                      *isaacstates.States
	memberlist                  *quicmemberlist.Memberlist
	getManifest                 func(height base.Height) (base.Manifest, error)
	client                      *isaacnetwork.QuicstreamClient
	handlers                    *quicstream.PrefixHandler
	ballotbox                   *isaacstates.Ballotbox
	quicstreamserver            *quicstream.Server
	discoveries                 []quicstream.UDPConnInfo
	syncSourceChecker           *isaacnetwork.SyncSourceChecker
	syncSourcePool              *isaac.SyncSourcePool
	syncSourcesRetryInterval    time.Duration
	suffrageCandidateLimiterSet *hint.CompatibleSet
	nodeInConsensusNodes        isaac.NodeInConsensusNodesFunc
	exitf                       func(error)
	st                          *leveldbstorage.Storage
	proposalMaker               *isaac.ProposalMaker
	nodeinfo                    *isaacnetwork.NodeInfoUpdater
}

func (cmd *RunCommand) Run(pctx context.Context) error {
	var log *logging.Logging
	if err := ps.LoadFromContextOK(pctx, launch2.LoggingContextKey, &log); err != nil {
		return err
	}

	//revive:disable:modifies-parameter
	pctx = context.WithValue(pctx,
		launch2.DesignFileContextKey, cmd.Design)
	pctx = context.WithValue(pctx,
		launch2.DiscoveryFlagContextKey, cmd.Discovery)
	//revive:enable:modifies-parameter

	pps := launch2.DefaultRunPS()
	_ = pps.SetLogging(log)

	log.Log().Debug().Interface("process", pps.Verbose()).Msg("process ready")

	pctx, err := pps.Run(pctx) //revive:disable-line:modifies-parameter
	defer func() {
		log.Log().Debug().Interface("process", pps.Verbose()).Msg("process will be closed")

		if _, err = pps.Close(pctx); err != nil {
			log.Log().Error().Err(err).Msg("failed to close")
		}
	}()

	if err != nil {
		return err
	}

	switch stop, err := cmd.prepare(pctx); {
	case err != nil:
		return err
	default:
		defer func() {
			_ = stop()
		}()
	}

	log.Log().Debug().
		Interface("discovery", cmd.Discovery).
		Interface("hold", cmd.Hold.Height()).
		Msg("node started")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := cmd.quicstreamserver.Start(); err != nil {
		return err
	}

	if err := cmd.pool.Start(); err != nil {
		return err
	}

	if err := cmd.memberlist.Start(); err != nil {
		return err
	}

	if len(cmd.discoveries) < 1 {
		log.Log().Warn().Msg("empty discoveries; will wait to be joined by remote nodes")
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

	cmd.exitf = func(err error) {
		exitch <- err
	}

	deferf, err := cmd.startStates(ctx)
	if err != nil {
		return err
	}

	select {
	case <-ctx.Done(): // NOTE graceful stop
		defer deferf()

		return ctx.Err()
	case err := <-exitch:
		if errors.Is(err, errHoldStop) {
			deferf()

			<-ctx.Done()

			return ctx.Err()
		}

		defer deferf()

		return err
	}
}

var errHoldStop = util.NewError("hold stop")

func (cmd *RunCommand) startStates(ctx context.Context) (func(), error) {
	var holded bool

	switch {
	case !cmd.Hold.IsSet():
	case cmd.Hold.Height() < base.GenesisHeight:
		holded = true
	default:
		switch m, found, err := cmd.db.LastBlockMap(); {
		case err != nil:
			return func() {}, err
		case !found:
		case cmd.Hold.Height() <= m.Manifest().Height():
			holded = true
		}
	}

	if holded {
		return func() {}, nil
	}

	go func() {
		cmd.exitf(<-cmd.states.Wait(ctx))
	}()

	return func() {
		if err := cmd.states.Stop(); err != nil && !errors.Is(err, util.ErrDaemonAlreadyStopped) {
			log.Log().Error().Err(err).Msg("failed to stop states")

			return
		}

		log.Log().Debug().Msg("states stopped")
	}, nil
}
