package main

import (
	"context"
	"fmt"
	"os"
	"strconv"

	"github.com/alecthomas/kong"
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/launch"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
)

var (
	Version   = "v0.0.1"
	BuildTime = "-"
	GitBranch = "-"
	GitCommit = "-"
)

var flagDefaults = kong.Vars{
	"log_out":         "stderr",
	"log_format":      "terminal",
	"log_level":       "debug",
	"log_force_color": "false",
	"max_concurrency": strconv.FormatInt(maxConcurrency, 10),
}

var CLI struct { //nolint:govet //...
	//revive:disable:nested-structs
	launch.BaseFlags
	Manager PManager `cmd:"" help:"run manager"`
	Worker  PWorker  `cmd:"" help:"run worker"`
	Version struct{} `cmd:"" help:"version"`
	//revive:enable:nested-structs
}

func main() {
	kctx := kong.Parse(&CLI, flagDefaults)

	bi, err := util.ParseBuildInfo(Version, GitBranch, GitCommit, BuildTime)
	if err != nil {
		kctx.FatalIfErrorf(err)
	}

	if kctx.Command() == "version" {
		_, _ = fmt.Fprintln(os.Stdout, bi.String())

		return
	}

	pctx := util.ContextWithValues(context.Background(), map[util.ContextKey]interface{}{
		launch.VersionContextKey:     util.MustNewVersion("v0.0.1"),
		launch.FlagsContextKey:       CLI.BaseFlags,
		launch.KongContextContextKey: kctx,
	})

	pss := launch.DefaultMainPS()

	switch i, err := pss.Run(pctx); {
	case err != nil:
		kctx.FatalIfErrorf(err)
	default:
		pctx = i

		kctx = kong.Parse(
			&CLI,
			kong.BindTo(pctx, (*context.Context)(nil)),
			flagDefaults,
		)
	}

	var log *logging.Logging
	if err := util.LoadFromContextOK(pctx, launch.LoggingContextKey, &log); err != nil {
		kctx.FatalIfErrorf(err)
	}

	log.Log().Debug().Interface("flags", os.Args).Msg("flags")
	log.Log().Debug().Interface("main", pss.Verbose()).Msg("processed")

	if err := func() error {
		defer log.Log().Debug().Msg("stopped")

		return errors.WithStack(kctx.Run(kctx, pctx))
	}(); err != nil {
		log.Log().Error().Err(err).Msg("stopped by error")

		kctx.FatalIfErrorf(err)
	}
}
