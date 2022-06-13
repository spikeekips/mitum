package main

import (
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/launch"
	"github.com/spikeekips/mitum/util/encoder"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
)

type baseCommand struct {
	enc    encoder.Encoder
	local  base.LocalNode
	encs   *encoder.Encoders
	Design string `arg:"" name:"node design" help:"node design" type:"filepath"`
	design launch.NodeDesign
}

func (cmd *baseCommand) prepareEncoder() error {
	switch encs, enc, err := launch.PrepareEncoders(); { // FIXME return jsonenc too
	case err != nil:
		return errors.Wrap(err, "")
	default:
		cmd.encs = encs
		cmd.enc = enc
	}

	return nil
}

func (cmd *baseCommand) prepareDesigns() error {
	switch d, b, err := launch.NodeDesignFromFile( //nolint:forcetypeassert //...
		cmd.Design, cmd.enc.(*jsonenc.Encoder)); {
	case err != nil:
		return errors.Wrap(err, "")
	default:
		log.Debug().Interface("design", d).Str("design_file", string(b)).Msg("design loaded")

		cmd.design = d
	}

	return nil
}

func (cmd *baseCommand) prepareLocal() error {
	local, err := launch.LocalFromDesign(cmd.design)
	if err != nil {
		return errors.Wrap(err, "failed to prepare local")
	}

	cmd.local = local

	return nil
}
