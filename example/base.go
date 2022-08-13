package main

import (
	"context"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/launch"
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

type baseNodeCommand struct { // FIXME remove
	baseCommand
	nodePolicy *isaac.NodePolicy
	Design     string `arg:"" name:"node design" help:"node design" type:"filepath"`
	local      base.LocalNode
	design     launch.NodeDesign
}

func (cmd *baseNodeCommand) prepareDesigns() error {
	switch d, b, err := launch.NodeDesignFromFile(cmd.Design, cmd.enc); {
	case err != nil:
		return err
	default:
		log.Log().Debug().Interface("design", d).Str("design_file", string(b)).Msg("design loaded")

		cmd.design = d
	}

	return nil
}

func (cmd *baseNodeCommand) prepareLocal() error {
	local, err := launch.LocalFromDesign(cmd.design)
	if err != nil {
		return errors.Wrap(err, "failed to prepare local")
	}

	cmd.local = local

	nodePolicy, err := launch.NodePolicyFromDesign(cmd.design)
	if err != nil {
		return errors.Wrap(err, "failed to prepare node policy")
	}

	cmd.nodePolicy = nodePolicy

	log.Log().Info().
		Interface("node_policy", cmd.nodePolicy).
		Msg("node policy loaded")

	return nil
}
