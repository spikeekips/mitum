package main

import (
	"context"

	"github.com/spikeekips/mitum/launch2"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/spikeekips/mitum/util/ps"
)

type INITCommand struct {
	Design        string `arg:"" name:"node design" help:"node design" type:"filepath"`
	GenesisDesign string `arg:"" name:"genesis design" help:"genesis design" type:"filepath"`
}

func (cmd *INITCommand) Run(pctx context.Context) error {
	var log *logging.Logging
	if err := ps.LoadFromContextOK(pctx, launch2.LoggingContextKey, &log); err != nil {
		return err
	}

	pctx = context.WithValue(pctx, launch2.DesignFileContextKey, cmd.Design)
	pctx = context.WithValue(pctx, launch2.GenesisDesignFileContextKey, cmd.GenesisDesign)

	pps := launch2.DefaultINITPS()
	_ = pps.SetLogging(log)

	log.Log().Debug().Interface("process", pps.Verbose()).Msg("process ready")

	pctx, err := pps.Run(pctx)
	defer func() {
		log.Log().Debug().Interface("process", pps.Verbose()).Msg("process will be closed")

		if _, err = pps.Close(pctx); err != nil {
			log.Log().Error().Err(err).Msg("failed to close")
		}
	}()

	return err
}
