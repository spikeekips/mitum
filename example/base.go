package main

import (
	"context"

	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/launch2"
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
		AddOK(launch2.PNameEncoder, launch2.PEncoder, nil)

	_ = pps.POK(launch2.PNameEncoder).
		PostAddOK(launch2.PNameAddHinters, launch2.PAddHinters)

	var log *logging.Logging
	if err := ps.LoadFromContextOK(pctx, launch2.LoggingContextKey, &log); err != nil {
		return pctx, err
	}

	cmd.log = log.Log()

	pctx, err := pps.Run(pctx) //revive:disable-line:modifies-parameter
	if err != nil {
		return pctx, err
	}

	return pctx, ps.LoadsFromContextOK(pctx,
		launch2.EncodersContextKey, &cmd.encs,
		launch2.EncoderContextKey, &cmd.enc,
	)
}
