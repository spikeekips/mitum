package launchcmd

import (
	"context"

	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/launch"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/spikeekips/mitum/util/ps"
)

type CleanCommand struct { //nolint:govet //...
	launch.DesignFlag
	Vault           string `name:"vault" help:"privatekey path of vault"`
	log             *zerolog.Logger
	launch.DevFlags `embed:"" prefix:"dev."`
}

func (cmd *CleanCommand) Run(pctx context.Context) error {
	var log *logging.Logging
	if err := util.LoadFromContextOK(pctx, launch.LoggingContextKey, &log); err != nil {
		return err
	}

	log.Log().Debug().
		Interface("design", cmd.DesignFlag).
		Interface("vault", cmd.Vault).
		Interface("dev", cmd.DevFlags).
		Msg("flags")

	cmd.log = log.Log()

	pps := ps.NewPS("cmd-clean")
	_ = pps.SetLogging(log)

	_ = pps.
		AddOK(launch.PNameEncoder, launch.PEncoder, nil).
		AddOK(launch.PNameDesign, launch.PLoadDesign, nil, launch.PNameEncoder).
		AddOK(launch.PNameLocal, launch.PLocal, nil, launch.PNameDesign).
		AddOK(launch.PNameStorage, launch.PStorage, launch.PCloseStorage, launch.PNameLocal)

	_ = pps.POK(launch.PNameEncoder).
		PostAddOK(launch.PNameAddHinters, launch.PAddHinters)

	_ = pps.POK(launch.PNameDesign).
		PostAddOK(launch.PNameCheckDesign, launch.PCheckDesign)

	_ = pps.POK(launch.PNameStorage).
		PreAddOK(launch.PNameCleanStorage, launch.PCleanStorage)

	//revive:disable:modifies-parameter
	pctx = context.WithValue(pctx, launch.DesignFlagContextKey, cmd.DesignFlag)
	pctx = context.WithValue(pctx, launch.DevFlagsContextKey, cmd.DevFlags)
	pctx = context.WithValue(pctx, launch.VaultContextKey, cmd.Vault)
	//revive:enable:modifies-parameter

	cmd.log.Debug().Interface("process", pps.Verbose()).Msg("process ready")

	pctx, err := pps.Run(pctx) //revive:disable-line:modifies-parameter
	defer func() {
		cmd.log.Debug().Interface("process", pps.Verbose()).Msg("process will be closed")

		if _, err = pps.Close(pctx); err != nil {
			cmd.log.Error().Err(err).Msg("failed to close")
		}
	}()

	return err
}
