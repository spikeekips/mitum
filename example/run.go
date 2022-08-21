package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	isaacnetwork "github.com/spikeekips/mitum/isaac/network"
	isaacstates "github.com/spikeekips/mitum/isaac/states"
	"github.com/spikeekips/mitum/launch"
	"github.com/spikeekips/mitum/network/quicstream"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/spikeekips/mitum/util/ps"
)

type RunCommand struct { //nolint:govet //...
	Design    string                `arg:"" name:"node design" help:"node design" type:"filepath"`
	Vault     string                `name:"vault" help:"privatekey path of vault"`
	Discovery []launch.ConnInfoFlag `help:"member discovery" placeholder:"ConnInfo"`
	Hold      launch.HeightFlag     `help:"hold consensus states"`
	exitf     func(error)
	log       *zerolog.Logger
}

func (cmd *RunCommand) Run(pctx context.Context) error {
	var log *logging.Logging
	if err := ps.LoadFromContextOK(pctx, launch.LoggingContextKey, &log); err != nil {
		return err
	}

	cmd.log = log.Log()

	//revive:disable:modifies-parameter
	pctx = context.WithValue(pctx, launch.DesignFileContextKey, cmd.Design)
	pctx = context.WithValue(pctx, launch.DiscoveryFlagContextKey, cmd.Discovery)
	pctx = context.WithValue(pctx, launch.VaultContextKey, cmd.Vault)
	//revive:enable:modifies-parameter

	pps := launch.DefaultRunPS()

	_ = pps.POK(launch.PNameStates).PreAddOK(
		pNameWhenNewBlockSavedInConsensusStateFunc, cmd.pWhenNewBlockSavedInConsensusStateFunc)

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

	return cmd.run(pctx)
}

var errHoldStop = util.NewError("hold stop")

func (cmd *RunCommand) run(pctx context.Context) error {
	var db isaac.Database
	var discoveries []quicstream.UDPConnInfo
	var states *isaacstates.States

	if err := ps.LoadsFromContextOK(pctx,
		launch.CenterDatabaseContextKey, &db,
		launch.DiscoveryContextKey, &discoveries,
		launch.StatesContextKey, &states,
	); err != nil {
		return err
	}

	if len(discoveries) < 1 {
		cmd.log.Warn().Msg("empty discoveries; will wait to be joined by remote nodes")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	exitch := make(chan error)

	cmd.exitf = func(err error) {
		exitch <- err
	}

	var holded bool

	switch {
	case !cmd.Hold.IsSet():
	case cmd.Hold.Height() < base.GenesisHeight:
		holded = true
	default:
		switch m, found, err := db.LastBlockMap(); {
		case err != nil:
			return err
		case !found:
		case cmd.Hold.Height() <= m.Manifest().Height():
			holded = true
		}
	}

	go func() {
		cmd.exitf(<-states.Wait(ctx))
	}()

	if !holded {
		defer func() {
			if err := states.Stop(); err != nil && !errors.Is(err, util.ErrDaemonAlreadyStopped) {
				cmd.log.Error().Err(err).Msg("failed to stop states")

				return
			}

			cmd.log.Debug().Msg("states stopped")
		}()
	}

	select {
	case <-ctx.Done(): // NOTE graceful stop
		return ctx.Err()
	case err := <-exitch:
		if errors.Is(err, errHoldStop) {
			<-ctx.Done()

			return ctx.Err()
		}

		return err
	}
}

var pNameWhenNewBlockSavedInConsensusStateFunc = ps.Name("when-new-block-saved-in-consensus-state-func")

func (cmd *RunCommand) pWhenNewBlockSavedInConsensusStateFunc(pctx context.Context) (context.Context, error) {
	var log *logging.Logging
	var db isaac.Database
	var ballotbox *isaacstates.Ballotbox
	var nodeinfo *isaacnetwork.NodeInfoUpdater

	if err := ps.LoadsFromContextOK(pctx,
		launch.LoggingContextKey, &log,
		launch.CenterDatabaseContextKey, &db,
		launch.BallotboxContextKey, &ballotbox,
		launch.NodeInfoContextKey, &nodeinfo,
	); err != nil {
		return pctx, err
	}

	f := func(height base.Height) {
		launch.WhenNewBlockSavedInConsensusStateFunc(ballotbox, db, nodeinfo)(height)

		l := log.Log().With().Interface("height", height).Logger()
		l.Debug().Msg("new block saved")

		if cmd.Hold.IsSet() && height == cmd.Hold.Height() {
			l.Debug().Msg("will be stopped by hold")

			cmd.exitf(errHoldStop.Call())

			return
		}
	}

	//revive:disable-next-line:modifies-parameter
	pctx = context.WithValue(pctx, launch.WhenNewBlockSavedInConsensusStateFuncContextKey, f)

	return pctx, nil
}
