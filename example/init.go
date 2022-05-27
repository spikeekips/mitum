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

	localfsroot := defaultLocalFSRoot(cmd.local.Address())
	permuri := defaultPermanentDatabaseURI()

	if err = launch.CleanStorage(
		permuri,
		localfsroot,
		encs, enc,
	); err != nil {
		return errors.Wrap(err, "")
	}

	nodeinfo, err := launch.CreateLocalFS(localfsroot, enc)
	if err != nil {
		return errors.Wrap(err, "")
	}

	db, _, pool, err := launch.LoadDatabase(nodeinfo, permuri, localfsroot, encs, enc)
	if err != nil {
		return errors.Wrap(err, "")
	}

	g := launch.NewGenesisBlockGenerator(cmd.local, networkID, enc, db, pool, launch.LocalFSDataDirectory(localfsroot))
	_ = g.SetLogging(logging)

	if _, err := g.Generate(); err != nil {
		return errors.Wrap(err, "")
	}

	return nil
}
