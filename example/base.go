package main

import (
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/launch"
	"github.com/spikeekips/mitum/util/encoder"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
)

type baseCommand struct {
	enc  *jsonenc.Encoder
	encs *encoder.Encoders
}

func (cmd *baseCommand) prepareEncoder() error {
	switch encs, enc, err := launch.PrepareEncoders(); {
	case err != nil:
		return err
	default:
		cmd.encs = encs
		cmd.enc = enc
	}

	return nil
}

type baseNodeCommand struct {
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
