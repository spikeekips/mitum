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

	_ = cmd.enc.Add(encoder.DecodeDetail{Hint: StateRequestHeaderHint, Instance: StateRequestHeader{}})
	_ = cmd.enc.Add(encoder.DecodeDetail{
		Hint: ExistsInStateOperationRequestHeaderHint, Instance: ExistsInStateOperationRequestHeader{},
	})

	return nil
}

type baseNodeCommand struct {
	baseCommand
	Design     string `arg:"" name:"node design" help:"node design" type:"filepath"`
	local      base.LocalNode
	design     launch.NodeDesign
	nodePolicy isaac.NodePolicy
}

func (cmd *baseNodeCommand) prepareDesigns() error {
	switch d, b, err := launch.NodeDesignFromFile(cmd.Design, cmd.enc); {
	case err != nil:
		return err
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

	cmd.nodePolicy = isaac.DefaultNodePolicy(networkID)
	log.Info().
		Interface("node_policy", cmd.nodePolicy).
		Msg("node policy loaded")

	return nil
}
