package main

import (
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/launch"
)

type initCommand struct {
	Address launch.AddressFlag `arg:"" name:"local address" help:"node address"`
	local   base.LocalNode
}

func (cmd *initCommand) Run() error {
	encs, enc, err := launch.PrepareEncoders()
	if err != nil {
		return errors.Wrap(err, "")
	}

	local, err := prepareLocal(cmd.Address.Address())
	if err != nil {
		return errors.Wrap(err, "failed to prepare local")
	}

	cmd.local = local

	dbroot := defaultDBRoot(cmd.local.Address())

	if err = launch.CleanDatabase(dbroot); err != nil {
		return errors.Wrap(err, "")
	}

	if err = launch.InitializeDatabase(dbroot); err != nil {
		return errors.Wrap(err, "")
	}

	perm, err := loadPermanentDatabase(launch.DBRootPermDirectory(dbroot), encs, enc)
	if err != nil {
		return errors.Wrap(err, "")
	}

	if err = perm.Clean(); err != nil {
		return errors.Wrap(err, "")
	}

	db, pool, err := launch.PrepareDatabase(perm, dbroot, encs, enc)
	if err != nil {
		return errors.Wrap(err, "")
	}

	g := launch.NewGenesisBlockGenerator(cmd.local, networkID, enc, db, pool, launch.DBRootDataDirectory(dbroot))
	_ = g.SetLogging(logging)

	if _, err := g.Generate(); err != nil {
		return errors.Wrap(err, "")
	}

	return nil
}
