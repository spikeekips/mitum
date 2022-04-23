package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/alecthomas/kong"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/launch"
	mitumlogging "github.com/spikeekips/mitum/util/logging"
)

var networkID = base.NetworkID([]byte("mitum-example-node"))

var (
	logging *mitumlogging.Logging
	log     *zerolog.Logger
)

func main() {
	logging = mitumlogging.Setup(os.Stderr, zerolog.DebugLevel, "json", false)
	log = mitumlogging.NewLogging(func(lctx zerolog.Context) zerolog.Context {
		return lctx.Str("module", "main")
	}).SetLogging(logging).Log()

	var cli struct {
		Init initCommand `cmd:"" help:"init node"`
		Run  runCommand  `cmd:"" help:"run node"`
	}

	kctx := kong.Parse(&cli)

	log.Info().Str("command", kctx.Command()).Msg("start command")

	if err := func() error {
		defer log.Info().Msg("stopped")

		return kctx.Run()
	}(); err != nil {
		log.Error().Err(err).Msg("stopped by error")

		kctx.FatalIfErrorf(err)
	}
}

type initCommand struct {
	Address launch.AddressFlag `arg:"" name:"address" help:"node address"`
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

	root := filepath.Join(os.TempDir(), "mitum-example-"+local.Address().String())
	db, err := launch.PrepareDatabase(root, encs, enc)
	if err != nil {
		return errors.Wrap(err, "")
	}

	g := launch.NewGenesisBlockGenerator(local, networkID, enc, db, launch.FSRootDataDirectory(root))
	_ = g.SetLogging(logging)
	if _, err := g.Generate(); err != nil {
		return errors.Wrap(err, "")
	}

	return nil
}

type runCommand struct {
	Address launch.AddressFlag `arg:"" name:"address" help:"node address"`
}

func (cmd *runCommand) Run() error {
	encs, enc, err := launch.PrepareEncoders()
	if err != nil {
		return errors.Wrap(err, "")
	}

	local, err := prepareLocal(cmd.Address.Address())
	if err != nil {
		return errors.Wrap(err, "failed to prepare local")
	}

	root := filepath.Join(os.TempDir(), "mitum-example-"+local.Address().String())
	db, err := launch.PrepareDatabase(root, encs, enc)
	if err != nil {
		return errors.Wrap(err, "")
	}

	fmt.Println(">", db)

	return nil
}

func prepareLocal(address base.Address) (base.LocalNode, error) {
	// NOTE make privatekey, based on node address
	b := make([]byte, base.PrivatekeyMinSeedSize)
	copy(b, address.Bytes())

	priv, err := base.NewMPrivatekeyFromSeed(string(b))
	if err != nil {
		return nil, errors.Wrap(err, "failed to create privatekey from node address")
	}

	log.Info().
		Stringer("address", address).
		Stringer("privatekey", priv).
		Stringer("publickey", priv.Publickey()).
		Msg("keypair generated")

	return isaac.NewLocalNode(priv, address), nil
}
