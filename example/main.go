package main

import (
	"github.com/alecthomas/kong"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/launch"
	"github.com/spikeekips/mitum/util"
	mitumlogging "github.com/spikeekips/mitum/util/logging"
)

var version = util.MustNewVersion("v0.0.1")

var (
	logging *mitumlogging.Logging
	log     *zerolog.Logger
)

func main() {
	//revive:disable:nested-structs
	var cli struct { //nolint:govet //...
		launch.Logging `embed:"" prefix:"log."`
		Import         importCommand `cmd:"" help:"import from block data"`
		Init           initCommand   `cmd:"" help:"init node"`
		Run            runCommand    `cmd:"" help:"run node"`
		Network        struct {
			Client networkClientCommand `cmd:"" help:"network client"`
		} `cmd:"" help:"network"`
		Key struct {
			New  keyNewCommand  `cmd:"" help:"generate new key"`
			Load keyLoadCommand `cmd:"" help:"load key"`
			Sign keySignCommand `cmd:"" help:"sign"`
		} `cmd:"" help:"key"`
	}
	//revive:enable:nested-structs

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

		return errors.WithStack(kctx.Run())
	}(); err != nil {
		log.Error().Err(err).Msg("stopped by error")

		kctx.FatalIfErrorf(err)
	}
}
