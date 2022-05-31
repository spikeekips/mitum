package main

import (
	"os"
	"time"

	"github.com/alecthomas/kong"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	mitumlogging "github.com/spikeekips/mitum/util/logging"
)

var (
	version       = util.MustNewVersion("v0.0.1")
	networkID     = base.NetworkID([]byte("mitum-example-node"))
	envKeyFSRootf = "MITUM_FS_ROOT"
)

var (
	logging *mitumlogging.Logging
	log     *zerolog.Logger
)

func init() { //nolint:gochecknoinits //...
	zerolog.TimeFieldFormat = time.RFC3339Nano
	zerolog.ErrorStackMarshaler = util.ZerologMarshalStack
}

func main() {
	logging = mitumlogging.Setup(os.Stderr, zerolog.DebugLevel, "json", false)
	log = mitumlogging.NewLogging(func(lctx zerolog.Context) zerolog.Context {
		return lctx.Str("module", "main")
	}).SetLogging(logging).Log()

	var cli struct {
		Import importCommand `cmd:"" help:"import from block data"`
		Init   initCommand   `cmd:"" help:"init node"`
		Run    runCommand    `cmd:"" help:"run node"`
	}

	kctx := kong.Parse(&cli)

	log.Info().Str("command", kctx.Command()).Msg("start command")

	if err := func() error {
		defer log.Info().Msg("stopped")

		return errors.Wrap(kctx.Run(), "")
	}(); err != nil {
		log.Error().Err(err).Msg("stopped by error")

		kctx.FatalIfErrorf(err)
	}
}
