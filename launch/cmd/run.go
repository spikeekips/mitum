package launchcmd

import (
	"context"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/arl/statsviz"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	isaacstates "github.com/spikeekips/mitum/isaac/states"
	"github.com/spikeekips/mitum/launch"
	"github.com/spikeekips/mitum/network/quicstream"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/spikeekips/mitum/util/ps"
)

type RunCommand struct { //nolint:govet //...
	//revive:disable:line-length-limit
	launch.DesignFlag
	launch.DevFlags `embed:"" prefix:"dev."`
	Vault           string                `name:"vault" help:"privatekey path of vault"`
	Discovery       []launch.ConnInfoFlag `help:"member discovery" placeholder:"connection info"`
	Hold            launch.HeightFlag     `help:"hold consensus states" placeholder:"height"`
	HTTPState       string                `name:"http-state" help:"runtime statistics thru https" placeholder:"bind address"`
	exitf           func(error)
	log             *zerolog.Logger
	holded          bool
	//revive:enable:line-length-limit
}

func (cmd *RunCommand) Run(pctx context.Context) error {
	var log *logging.Logging
	if err := util.LoadFromContextOK(pctx, launch.LoggingContextKey, &log); err != nil {
		return err
	}

	log.Log().Debug().
		Interface("design", cmd.DesignFlag).
		Interface("vault", cmd.Vault).
		Interface("discovery", cmd.Discovery).
		Interface("hold", cmd.Hold).
		Interface("http_state", cmd.HTTPState).
		Interface("dev", cmd.DevFlags).
		Msg("flags")

	cmd.log = log.Log()

	if len(cmd.HTTPState) > 0 {
		if err := cmd.runHTTPState(cmd.HTTPState); err != nil {
			return errors.Wrap(err, "run http state")
		}
	}

	//revive:disable:modifies-parameter
	pctx = context.WithValue(pctx, launch.DesignFlagContextKey, cmd.DesignFlag)
	pctx = context.WithValue(pctx, launch.DevFlagsContextKey, cmd.DevFlags)
	pctx = context.WithValue(pctx, launch.DiscoveryFlagContextKey, cmd.Discovery)
	pctx = context.WithValue(pctx, launch.VaultContextKey, cmd.Vault)
	//revive:enable:modifies-parameter

	pps := launch.DefaultRunPS()

	_ = pps.POK(launch.PNameStorage).PostAddOK(ps.Name("check-hold"), cmd.pCheckHold)
	_ = pps.POK(launch.PNameStates).
		PreAddOK(ps.Name("when-new-block-saved-in-consensus-state-func"), cmd.pWhenNewBlockSavedInConsensusStateFunc).
		PreAddOK(ps.Name("when-new-block-confirmed-func"), cmd.pWhenNewBlockConfirmed)

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

var errHoldStop = util.NewMError("hold stop")

func (cmd *RunCommand) run(pctx context.Context) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	exitch := make(chan error)

	cmd.exitf = func(err error) {
		exitch <- err
	}

	stopstates := func() {}

	if !cmd.holded {
		deferred, err := cmd.runStates(ctx, pctx)
		if err != nil {
			return err
		}

		stopstates = deferred
	}

	select {
	case <-ctx.Done(): // NOTE graceful stop
		return ctx.Err()
	case err := <-exitch:
		if errors.Is(err, errHoldStop) {
			stopstates()

			<-ctx.Done()

			return ctx.Err()
		}

		return err
	}
}

func (cmd *RunCommand) runStates(ctx, pctx context.Context) (func(), error) {
	var discoveries *util.Locked[[]quicstream.UDPConnInfo]
	var states *isaacstates.States

	if err := util.LoadFromContextOK(pctx,
		launch.DiscoveryContextKey, &discoveries,
		launch.StatesContextKey, &states,
	); err != nil {
		return nil, err
	}

	if dis := launch.GetDiscoveriesFromLocked(discoveries); len(dis) < 1 {
		cmd.log.Warn().Msg("empty discoveries; will wait to be joined by remote nodes")
	}

	go func() {
		cmd.exitf(<-states.Wait(ctx))
	}()

	return func() {
		if err := states.Hold(); err != nil && !errors.Is(err, util.ErrDaemonAlreadyStopped) {
			cmd.log.Error().Err(err).Msg("failed to stop states")

			return
		}

		cmd.log.Debug().Msg("states stopped")
	}, nil
}

func (cmd *RunCommand) pWhenNewBlockSavedInConsensusStateFunc(pctx context.Context) (context.Context, error) {
	var log *logging.Logging

	if err := util.LoadFromContextOK(pctx,
		launch.LoggingContextKey, &log,
	); err != nil {
		return pctx, err
	}

	f := func(height base.Height) {
		l := log.Log().With().Interface("height", height).Logger()

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

func (*RunCommand) pWhenNewBlockConfirmed(pctx context.Context) (context.Context, error) {
	var log *logging.Logging

	if err := util.LoadFromContextOK(pctx,
		launch.LoggingContextKey, &log,
	); err != nil {
		return pctx, err
	}

	return context.WithValue(pctx,
		launch.WhenNewBlockConfirmedFuncContextKey, func(base.Height) {
			// NOTE nothing happened
		},
	), nil
}

func (cmd *RunCommand) pCheckHold(pctx context.Context) (context.Context, error) {
	var db isaac.Database
	if err := util.LoadFromContextOK(pctx, launch.CenterDatabaseContextKey, &db); err != nil {
		return pctx, err
	}

	switch {
	case !cmd.Hold.IsSet():
	case cmd.Hold.Height() < base.GenesisHeight:
		cmd.holded = true
	default:
		switch m, found, err := db.LastBlockMap(); {
		case err != nil:
			return pctx, err
		case !found:
		case cmd.Hold.Height() <= m.Manifest().Height():
			cmd.holded = true
		}
	}

	return pctx, nil
}

func (cmd *RunCommand) runHTTPState(bind string) error {
	addr, err := net.ResolveTCPAddr("tcp", bind)
	if err != nil {
		return errors.Wrap(err, "parse --http-state")
	}

	mux := http.NewServeMux()
	if err := statsviz.Register(mux); err != nil {
		return errors.Wrap(err, "register statsviz for http-state")
	}

	cmd.log.Debug().Stringer("bind", addr).Msg("statsviz started")

	go func() {
		_ = http.ListenAndServe(addr.String(), mux)
	}()

	return nil
}
