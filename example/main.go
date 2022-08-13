package main

import (
	"context"

	"github.com/alecthomas/kong"
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/launch2"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/spikeekips/mitum/util/ps"
)

var (
	Version = "v0.0.1"
	version util.Version
)

var log *logging.Logging // FIXME remove

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
	launch2.BaseFlags
	Import ImportCommand `cmd:"" help:"import from block data"`
	Init   INITCommand   `cmd:"" help:"init node"`
	// Run     RunCommand    `cmd:"" help:"run node"`
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
	pctx = context.WithValue(pctx, launch2.VersionContextKey, version)
	pctx = context.WithValue(pctx, launch2.FlagsContextKey, CLI.BaseFlags)
	pctx = context.WithValue(pctx, launch2.KongContextContextKey, kctx)

	pss := launch2.DefaultMainPS()

	switch i, err := pss.Run(pctx); {
	case err != nil:
		kctx.FatalIfErrorf(err)
	default:
		pctx = i

		kctx = kong.Parse(&CLI, kong.BindTo(pctx, (*context.Context)(nil)))
	}

	if err := ps.LoadFromContextOK(pctx, launch2.LoggingContextKey, &log); err != nil {
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
