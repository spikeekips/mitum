package main

import (
	"context"

	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/launch"
	"github.com/spikeekips/mitum/util/encoder"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/spikeekips/mitum/util/ps"
)

type baseCommand struct {
	enc  *jsonenc.Encoder
	encs *encoder.Encoders
	log  *zerolog.Logger
}

func (cmd *baseCommand) prepare(pctx context.Context) (context.Context, error) {
	pps := ps.NewPS()

	_ = pps.
		AddOK(launch.PNameEncoder, launch.PEncoder, nil)

	_ = pps.POK(launch.PNameEncoder).
		PostAddOK(launch.PNameAddHinters, launch.PAddHinters)

	var log *logging.Logging
	if err := ps.LoadFromContextOK(pctx, launch.LoggingContextKey, &log); err != nil {
		return pctx, err
	}

	cmd.log = log.Log()

	pctx, err := pps.Run(pctx) //revive:disable-line:modifies-parameter
	if err != nil {
		return pctx, err
	}

	return pctx, ps.LoadsFromContextOK(pctx,
		launch.EncodersContextKey, &cmd.encs,
		launch.EncoderContextKey, &cmd.enc,
	)
}
