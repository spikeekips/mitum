package main

import (
	"context"

	"github.com/alecthomas/kong"
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/launch"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/spikeekips/mitum/util/ps"
)

var (
	Version = "v0.0.1"
	version util.Version
)

func init() {
	if len(Version) > 0 {
		v, err := util.ParseVersion(Version)
		if err == nil {
			version = v
		}
	}
}

//revive:disable:nested-structs
var CLI struct { //nolint:govet //...
	launch.BaseFlags
	Import  ImportCommand `cmd:"" help:"import from block data"`
	Init    INITCommand   `cmd:"" help:"init node"`
	Run     RunCommand    `cmd:"" help:"run node"`
	Network struct {
		Client NetworkClientCommand `cmd:"" help:"network client"`
	} `cmd:"" help:"network"`
	Key struct {
		New  KeyNewCommand  `cmd:"" help:"generate new key"`
		Load KeyLoadCommand `cmd:"" help:"load key"`
		Sign KeySignCommand `cmd:"" help:"sign"`
	} `cmd:"" help:"key"`
}

//revive:enable:nested-structs

func main() {
	kctx := kong.Parse(&CLI)

	pctx := context.Background()
	pctx = context.WithValue(pctx, launch.VersionContextKey, version)
	pctx = context.WithValue(pctx, launch.FlagsContextKey, CLI.BaseFlags)
	pctx = context.WithValue(pctx, launch.KongContextContextKey, kctx)

	pss := launch.DefaultMainPS()

	switch i, err := pss.Run(pctx); {
	case err != nil:
		kctx.FatalIfErrorf(err)
	default:
		pctx = i

		kctx = kong.Parse(&CLI, kong.BindTo(pctx, (*context.Context)(nil)))
	}

	var log *logging.Logging
	if err := ps.LoadFromContextOK(pctx, launch.LoggingContextKey, &log); err != nil {
		kctx.FatalIfErrorf(err)
	}

	log.Log().Debug().Interface("main_process", pss.Verbose()).Msg("processed")

	if err := func() error {
		defer log.Log().Debug().Msg("stopped")

		return errors.WithStack(kctx.Run(pctx))
	}(); err != nil {
		log.Log().Error().Err(err).Msg("stopped by error")

		kctx.FatalIfErrorf(err)
	}
}
