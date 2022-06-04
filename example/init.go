package main

import (
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/launch"
	"github.com/spikeekips/mitum/util/encoder"
)

type initCommand struct {
	Address launch.AddressFlag `arg:"" name:"local address" help:"node address"`
	enc     encoder.Encoder
	encs    *encoder.Encoders
	local   base.LocalNode
}

func (cmd *initCommand) Run() error {
	switch encs, enc, err := launch.PrepareEncoders(); {
	case err != nil:
		return errors.Wrap(err, "")
	default:
		cmd.encs = encs
		cmd.enc = enc
	}

	switch local, err := prepareLocal(cmd.Address.Address()); {
	case err != nil:
		return errors.Wrap(err, "failed to prepare local")
	default:
		cmd.local = local
	}

	var localfsroot string

	switch i, err := defaultLocalFSRoot(cmd.local.Address()); {
	case err != nil:
		return errors.Wrap(err, "")
	default:
		localfsroot = i

		log.Debug().Str("localfs_root", localfsroot).Msg("localfs root")
	}

	permuri := defaultPermanentDatabaseURI()

	if err := launch.CleanStorage(
		permuri,
		localfsroot,
		cmd.encs, cmd.enc,
	); err != nil {
		return errors.Wrap(err, "")
	}

	nodeinfo, err := launch.CreateLocalFS(launch.CreateDefaultNodeInfo(networkID, version), localfsroot, cmd.enc)
	if err != nil {
		return errors.Wrap(err, "")
	}

	db, _, pool, err := launch.LoadDatabase(nodeinfo, permuri, localfsroot, cmd.encs, cmd.enc)
	if err != nil {
		return errors.Wrap(err, "")
	}

	g := launch.NewGenesisBlockGenerator(
		cmd.local, networkID, cmd.enc, db, pool, launch.LocalFSDataDirectory(localfsroot),
	)
	_ = g.SetLogging(logging)

	if _, err := g.Generate(); err != nil {
		return errors.Wrap(err, "")
	}

	return nil
}
