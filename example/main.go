package main

import (
	"context"
	"fmt"
	"os"

	"github.com/alecthomas/kong"
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/launch"
	launchcmd "github.com/spikeekips/mitum/launch/cmd"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
)

var (
	Version   = "v0.0.1"
	BuildTime = "-"
	GitBranch = "-"
	GitCommit = "-"
)

var CLI struct { //nolint:govet //...
	//revive:disable:nested-structs
	launch.BaseFlags
	Init    launchcmd.INITCommand    `cmd:"" help:"init node"`
	Run     launchcmd.RunCommand     `cmd:"" help:"run node"`
	Storage launchcmd.StorageCommand `cmd:""`
	Network struct {
		Client launchcmd.NetworkClientCommand `cmd:"" help:"network client"`
	} `cmd:"" help:"network"`
	Key struct {
		New  launchcmd.KeyNewCommand  `cmd:"" help:"generate new key"`
		Load launchcmd.KeyLoadCommand `cmd:"" help:"load key"`
		Sign launchcmd.KeySignCommand `cmd:"" help:"sign"`
	} `cmd:"" help:"key"`
	Handover launchcmd.HandoverCommands `cmd:""`
	Version  struct{}                   `cmd:"" help:"version"`
	//revive:enable:nested-structs
}

var flagDefaults = kong.Vars{
	"log_out":         "stderr",
	"log_format":      "terminal",
	"log_level":       "debug",
	"log_force_color": "false",
	"design_uri":      launch.DefaultDesignURI,
	"safe_threshold":  base.SafeThreshold.String(),
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
		launch.VersionContextKey:     bi.Version,
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
	log.Log().Debug().Interface("main_process", pss.Verbose()).Msg("processed")

	if err := func() error {
		defer log.Log().Debug().Msg("stopped")

		return errors.WithStack(kctx.Run(kctx, pctx))
	}(); err != nil {
		log.Log().Error().Err(err).Msg("stopped by error")

		kctx.FatalIfErrorf(err)
	}
}
