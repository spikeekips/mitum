package main

import (
	"github.com/alecthomas/kong"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/launch"
	"github.com/spikeekips/mitum/util"
	mitumlogging "github.com/spikeekips/mitum/util/logging"
)

var (
	version   = util.MustNewVersion("v0.0.1")
	networkID = base.NetworkID([]byte("mitum-example-node"))
)

var (
	logging *mitumlogging.Logging
	log     *zerolog.Logger
)

func main() {
	var cli struct { //nolint:govet //...
		launch.Logging `embed:"" prefix:"log."`
		Init           initCommand   `cmd:"" help:"init node"`
		Import         importCommand `cmd:"" help:"import from block data"`
		Run            runCommand    `cmd:"" help:"run node"`
		Network        struct {      // revive:disable-line:nested-structs
			Client networkClientCommand `cmd:"" help:"network client"`
		} `cmd:"" help:"network"`
	}

	kctx := kong.Parse(&cli)

	l, err := launch.SetupLoggingFromFlags(cli.Logging)
	if err != nil {
		kctx.FatalIfErrorf(err)
	}

	logging = l

	log = mitumlogging.NewLogging(func(lctx zerolog.Context) zerolog.Context {
		return lctx.Str("module", "main")
	}).SetLogging(logging).Log()

	log.Info().Str("command", kctx.Command()).Msg("start command")

	if err := func() error {
		defer log.Info().Msg("stopped")

		return errors.Wrap(kctx.Run(), "")
	}(); err != nil {
		log.Error().Err(err).Msg("stopped by error")

		kctx.FatalIfErrorf(err)
	}
}
