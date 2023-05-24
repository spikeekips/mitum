package launchcmd

import (
	"context"

	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/launch"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/spikeekips/mitum/util/ps"
)

type BaseCommand struct {
	Encoders *encoder.Encoders `kong:"-"`
	Encoder  *jsonenc.Encoder  `kong:"-"`
	Log      *zerolog.Logger   `kong:"-"`
}

func (cmd *BaseCommand) prepare(pctx context.Context) (context.Context, error) {
	pps := ps.NewPS("cmd")

	_ = pps.
		AddOK(launch.PNameEncoder, launch.PEncoder, nil)

	_ = pps.POK(launch.PNameEncoder).
		PostAddOK(launch.PNameAddHinters, launch.PAddHinters)

	var log *logging.Logging
	if err := util.LoadFromContextOK(pctx, launch.LoggingContextKey, &log); err != nil {
		return pctx, err
	}

	cmd.Log = log.Log()

	nctx, err := pps.Run(pctx)
	if err != nil {
		return nctx, err
	}

	return nctx, util.LoadFromContextOK(nctx,
		launch.EncodersContextKey, &cmd.Encoders,
		launch.EncoderContextKey, &cmd.Encoder,
	)
}
