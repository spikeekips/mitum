package main

import (
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/launch"
	"github.com/spikeekips/mitum/util/encoder"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
)

type baseCommand struct {
	enc  encoder.Encoder
	encs *encoder.Encoders
}

func (cmd *baseCommand) prepareEncoder() error {
	switch encs, enc, err := launch.PrepareEncoders(); {
	case err != nil:
		return errors.Wrap(err, "")
	default:
		cmd.encs = encs
		cmd.enc = enc
	}

	_ = cmd.enc.Add(encoder.DecodeDetail{Hint: stateRequestHeaderHint, Instance: stateRequestHeader{}})
	_ = cmd.enc.Add(encoder.DecodeDetail{
		Hint: existsInStateOperationRequestHeaderHint, Instance: existsInStateOperationRequestHeader{},
	})

	return nil
}

type baseNodeCommand struct {
	baseCommand
	local  base.LocalNode
	Design string `arg:"" name:"node design" help:"node design" type:"filepath"`
	design launch.NodeDesign
}

func (cmd *baseNodeCommand) prepareDesigns() error {
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

func (cmd *baseNodeCommand) prepareLocal() error {
	local, err := launch.LocalFromDesign(cmd.design)
	if err != nil {
		return errors.Wrap(err, "failed to prepare local")
	}

	cmd.local = local

	return nil
}
