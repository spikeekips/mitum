package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

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
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/spikeekips/mitum/util/ps"
)

type RunCommand struct { //nolint:govet //...
	baseNodeCommand
	Discovery []launch.ConnInfoFlag `help:"member discovery" placeholder:"ConnInfo"`
	Hold      launch.HeightFlag     `help:"hold consensus states"`
	exitf     func(error)
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

	log.Log().Debug().
		Interface("discovery", cmd.Discovery).
		Interface("hold", cmd.Hold.Height()).
		Msg("node started")

	var db isaac.Database
	var discoveries []quicstream.UDPConnInfo
	var quicstreamserver *quicstream.Server
	var pool *isaacdatabase.TempPool
	var memberlist *quicmemberlist.Memberlist
	var syncSourceChecker *isaacnetwork.SyncSourceChecker
	var lastSuffrageProofWatcher *isaac.LastConsensusNodesWatcher
	var states *isaacstates.States
	if err := ps.LoadsFromContextOK(pctx,
		launch2.CenterDatabaseContextKey, &db,
		launch2.DiscoveryContextKey, &discoveries,
		launch2.QuicstreamServerContextKey, &quicstreamserver,
		launch2.PoolDatabaseContextKey, &pool,
		launch2.StatesContextKey, &states,
		launch2.MemberlistContextKey, &memberlist,
		launch2.SyncSourceCheckerContextKey, &syncSourceChecker,
		launch2.LastSuffrageProofWatcherContextKey, &lastSuffrageProofWatcher,
	); err != nil {
		return err
	}

	if len(discoveries) < 1 {
		log.Log().Warn().Msg("empty discoveries; will wait to be joined by remote nodes")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := quicstreamserver.Start(); err != nil {
		return err
	}

	if err := pool.Start(); err != nil {
		return err
	}

	if err := memberlist.Start(); err != nil {
		return err
	}

	if err := syncSourceChecker.Start(); err != nil {
		return err
	}

	if err := lastSuffrageProofWatcher.Start(); err != nil {
		return err
	}

	defer func() {
		_ = syncSourceChecker.Stop()
		_ = lastSuffrageProofWatcher.Stop()
		_ = memberlist.Stop()
	}()

	exitch := make(chan error)

	cmd.exitf = func(err error) {
		exitch <- err
	}

	deferf, err := cmd.startStates(ctx, states, db)
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

func (cmd *RunCommand) startStates(
	ctx context.Context,
	states *isaacstates.States,
	db isaac.Database,
) (func(), error) {
	var holded bool

	switch {
	case !cmd.Hold.IsSet():
	case cmd.Hold.Height() < base.GenesisHeight:
		holded = true
	default:
		switch m, found, err := db.LastBlockMap(); {
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
		cmd.exitf(<-states.Wait(ctx))
	}()

	return func() {
		if err := states.Stop(); err != nil && !errors.Is(err, util.ErrDaemonAlreadyStopped) {
			log.Log().Error().Err(err).Msg("failed to stop states")

			return
		}

		log.Log().Debug().Msg("states stopped")
	}, nil
}
