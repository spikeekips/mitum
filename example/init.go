package main

import (
	"github.com/spikeekips/mitum/launch"
)

type initCommand struct { //nolint:govet //...
	baseNodeCommand
	GenesisDesign string `arg:"" name:"genesis design" help:"genesis design" type:"filepath"`
	genesisDesign launch.GenesisDesign
}

func (cmd *initCommand) Run() error {
	if err := cmd.prepareEncoder(); err != nil {
		return err
	}

	if err := cmd.prepareDesigns(); err != nil {
		return err
	}

	if err := cmd.prepareLocal(); err != nil {
		return err
	}

	if err := launch.CleanStorage(
		cmd.design.Storage.Database.String(),
		cmd.design.Storage.Base,
		cmd.encs, cmd.enc,
	); err != nil {
		return err
	}

	nodeinfo, err := launch.CreateLocalFS(
		launch.CreateDefaultNodeInfo(cmd.nodePolicy.NetworkID(), version), cmd.design.Storage.Base, cmd.enc)
	if err != nil {
		return err
	}

	db, _, pool, err := launch.LoadDatabase(
		nodeinfo, cmd.design.Storage.Database.String(), cmd.design.Storage.Base, cmd.encs, cmd.enc)
	if err != nil {
		return err
	}

	_ = db.SetLogging(logging)

	g := launch.NewGenesisBlockGenerator(
		cmd.local,
		cmd.nodePolicy.NetworkID(),
		cmd.enc,
		db,
		pool,
		launch.LocalFSDataDirectory(cmd.design.Storage.Base),
		cmd.genesisDesign.Facts,
	)
	_ = g.SetLogging(logging)

	if _, err := g.Generate(); err != nil {
		return err
	}

	return nil
}

func (cmd *initCommand) prepareDesigns() error {
	if err := cmd.baseNodeCommand.prepareDesigns(); err != nil {
		return err
	}

	switch d, b, err := launch.GenesisDesignFromFile(cmd.GenesisDesign, cmd.enc); {
	case err != nil:
		return err
	default:
		log.Debug().Interface("design", d).Str("design_file", string(b)).Msg("genesis design loaded")

		cmd.genesisDesign = d
	}

	return nil
}
