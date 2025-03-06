package main

import (
	"fmt"
	"io"
	"maps"
	"os"
	"runtime/debug"
	"slices"
	"strings"

	"github.com/alecthomas/kong"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/launch"
)

var CLI struct { //nolint:govet //...
	launch.BaseFlags
	Manager managerCommand `cmd:"" help:"manager"`
	Worker  workerCommand  `cmd:"" help:"worker"`
	Version struct{}       `cmd:"" help:"version"`
}

var flagDefaults = kong.Vars{
	"log_out":         "stderr",
	"log_format":      "terminal",
	"log_level":       "debug",
	"log_force_color": "false",
	"bind":            "0.0.0.0:5555",
}

func main() {
	kctx := kong.Parse(&CLI, flagDefaults)

	var log *zerolog.Logger
	var logout io.Writer

	switch l, o, err := launch.SetupLoggingFromFlags(CLI.LoggingFlags); {
	case err != nil:
		kctx.FatalIfErrorf(err)
	default:
		log, logout = l.Log(), o
	}

	kctx = kong.Parse(
		&CLI,
		kong.BindTo(log, (*zerolog.Logger)(nil)),
		flagDefaults,
	)

	if kctx.Command() == "version" {
		for k, v := range maps.All(loadVersion()) {
			fmt.Fprintln(logout, fmt.Sprintf("%9s: %s", k, v))
		}

		return
	}

	log.Debug().
		Func(func(e *zerolog.Event) {
			for k, v := range maps.All(loadVersion()) {
				e.Str(k, v)
			}
		}).
		Str("command", kctx.Command()).
		Interface("flags", os.Args).
		Msg("started")

	switch err := func() error {
		return errors.WithStack(kctx.Run(log))
	}(); {
	case err != nil:
		log.Error().Err(err).Msg("stopped by error")

		kctx.FatalIfErrorf(err)
	default:
		log.Debug().Msg("stopped")
	}
}

func loadVersion() map[string]string {
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		return nil
	}

	m := map[string]string{
		"go": bi.GoVersion,
	}

	for i := range slices.Values(bi.Settings) {
		if !strings.HasPrefix(i.Key, "vcs.") {
			continue
		}

		m[i.Key[4:]] = i.Value
	}

	return m
}
