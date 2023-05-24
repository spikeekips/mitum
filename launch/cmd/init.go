package launchcmd

import (
	"context"

	"github.com/spikeekips/mitum/launch"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
)

type INITCommand struct {
	GenesisDesign string `arg:"" name:"genesis design" help:"genesis design" type:"filepath"`
	Vault         string `name:"vault" help:"privatekey path of vault"`
	launch.DesignFlag
	launch.DevFlags `embed:"" prefix:"dev."`
}

func (cmd *INITCommand) Run(pctx context.Context) error {
	var log *logging.Logging
	if err := util.LoadFromContextOK(pctx, launch.LoggingContextKey, &log); err != nil {
		return err
	}

	nctx := context.WithValue(pctx, launch.DesignFlagContextKey, cmd.DesignFlag)
	nctx = context.WithValue(nctx, launch.DevFlagsContextKey, cmd.DevFlags)
	nctx = context.WithValue(nctx, launch.GenesisDesignFileContextKey, cmd.GenesisDesign)
	nctx = context.WithValue(nctx, launch.VaultContextKey, cmd.Vault)

	pps := launch.DefaultINITPS()
	_ = pps.SetLogging(log)

	log.Log().Debug().Interface("process", pps.Verbose()).Msg("process ready")

	nctx, err := pps.Run(nctx)
	defer func() {
		log.Log().Debug().Interface("process", pps.Verbose()).Msg("process will be closed")

		if _, err = pps.Close(nctx); err != nil {
			log.Log().Error().Err(err).Msg("failed to close")
		}
	}()

	return err
}
