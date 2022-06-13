package main

import (
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/launch"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
)

type initCommand struct {
	baseCommand
	GenesisDesign string `arg:"" name:"genesis design" help:"genesis design" type:"filepath"`
	genesisDesign launch.GenesisDesign
}

func (cmd *initCommand) Run() error {
	if err := cmd.prepareEncoder(); err != nil {
		return errors.Wrap(err, "")
	}

	if err := cmd.prepareDesigns(); err != nil {
		return errors.Wrap(err, "")
	}

	if err := cmd.prepareLocal(); err != nil {
		return errors.Wrap(err, "")
	}

	if err := launch.CleanStorage(
		cmd.design.Storage.Database.String(),
		cmd.design.Storage.Base,
		cmd.encs, cmd.enc,
	); err != nil {
		return errors.Wrap(err, "")
	}

	nodeinfo, err := launch.CreateLocalFS(
		launch.CreateDefaultNodeInfo(networkID, version), cmd.design.Storage.Base, cmd.enc)
	if err != nil {
		return errors.Wrap(err, "")
	}

	db, _, pool, err := launch.LoadDatabase(
		nodeinfo, cmd.design.Storage.Database.String(), cmd.design.Storage.Base, cmd.encs, cmd.enc)
	if err != nil {
		return errors.Wrap(err, "")
	}

	_ = db.SetLogging(logging)

	g := launch.NewGenesisBlockGenerator(
		cmd.local,
		networkID,
		cmd.enc,
		db,
		pool,
		launch.LocalFSDataDirectory(cmd.design.Storage.Base),
		cmd.genesisDesign.Facts,
	)
	_ = g.SetLogging(logging)

	if _, err := g.Generate(); err != nil {
		return errors.Wrap(err, "")
	}

	return nil
}

func (cmd *initCommand) prepareDesigns() error {
	if err := cmd.baseCommand.prepareDesigns(); err != nil {
		return errors.Wrap(err, "")
	}

	switch d, b, err := launch.GenesisDesignFromFile( //nolint:forcetypeassert //...
		cmd.GenesisDesign, cmd.enc.(*jsonenc.Encoder)); {
	case err != nil:
		return errors.Wrap(err, "")
	default:
		log.Debug().Interface("design", d).Str("design_file", string(b)).Msg("genesis design loaded")

		cmd.genesisDesign = d
	}

	return nil
}
