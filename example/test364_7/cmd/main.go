package main

import (
	"context"
	"fmt"
	"io"
	"iter"
	"net/url"
	"os"
	"runtime/debug"
	"slices"
	"strings"

	"github.com/alecthomas/kong"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	dop5 "github.com/spikeekips/mitum/example/test364_5"
	jsonenc "github.com/spikeekips/mitum/example/test364_5/encoder/json"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/launch"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/valkey-io/valkey-go"
)

var cliMain struct {
	launch.LoggingFlags `embed:"" prefix:"log."`
	Control             cliControl `cmd:""`
	Manager             cliManager `cmd:""`
	Worker              cliWorker  `cmd:""`
	Version             struct{}   `cmd:""`
}

var flagDefaults = kong.Vars{
	"log_out":         "stderr",
	"log_format":      "json",
	"log_level":       "debug",
	"log_force_color": "false",
}

func main() {
	log, logout := loadLogger()

	kctx := kong.Parse(
		&cliMain,
		kong.BindTo(log, (*zerolog.Logger)(nil)),
		flagDefaults,
	)

	if kctx.Command() == "version" {
		printBuildInfo(logout)

		return
	}

	log.Debug().
		Func(func(e *zerolog.Event) {
			for k, v := range readBuildInfo() {
				e.Str(k, v)
			}
		}).
		Str("command", kctx.Command()).
		Interface("flags", os.Args).
		Msg("cli started")

	switch err := func() error {
		return errors.WithStack(kctx.Run(log))
	}(); {
	case err != nil:
		log.Error().Err(err).Msg("cli stopped by error")

		kctx.FatalIfErrorf(err)
	default:
		log.Debug().Msg("cli stopped")
	}
}

type baseCommand struct {
	Redis     string          `default:"valkey://127.0.0.1:6379"`
	ctx       context.Context //nolint:containedctx
	ctxCancel dutil.InErrorFunc[error]
	log       *zerolog.Logger
	enc       *jsonenc.Encoder
	dbClient  valkey.Client
}

func (cmd *baseCommand) prepare(
	log *zerolog.Logger,
) error {
	cmd.log = log

	cmd.ctx, cmd.ctxCancel = dutil.ContextWithCancelCause(context.Background())

	cmd.enc = jsonenc.NewEncoder(cmd.ctx, 1<<9) //nolint:mnd
	if err := cmd.addHinters(); err != nil {
		return err
	}

	if err := cmd.prepareValkey(); err != nil {
		return err
	}

	return nil
}

func (cmd *baseCommand) prepareValkey() error {
	var dbtype, addr string

	switch u, err := url.Parse(cmd.Redis); {
	case err != nil:
		return errors.WithStack(err)
	default:
		dbtype = u.Scheme
		addr = u.Host
	}

	switch dbtype {
	case "valkey":
		client, err := valkey.NewClient(valkey.ClientOption{
			InitAddress:  []string{addr},
			DisableRetry: true,
		})
		if err != nil {
			return errors.WithStack(err)
		}

		cmd.dbClient = client
	default:
		return errors.Errorf("unknown db type, %q", dbtype)
	}

	return nil
}

func addDecode[T any](enc *jsonenc.Encoder, ht hint.Hint, sample T, decode any) dutil.ErrorFunc {
	return func() error {
		return jsonenc.AddDecode(enc, ht, sample, decode)
	}
}

func (cmd *baseCommand) addHinters() error {
	return dutil.FuncChain(
		addDecode(cmd.enc, base.MPublickeyHint, &base.MPublickey{}, dop5.DecodeJSONMPublickey),
		addDecode(cmd.enc, dop5.AccountOpFactHint, &dop5.AccountOpFact{}, dop5.AccountOpFactDecodeJSON),
		addDecode(cmd.enc, dop5.AccountSMVHint, &dop5.AccountSMV{}, dop5.AccountSMVDecodeJSON),
		addDecode(cmd.enc, dop5.AccountSVHint, &dop5.AccountSV{}, dop5.AccountSVDecodeJSON),
		addDecode(cmd.enc, dop5.BaseStateHint, &dop5.BaseState{}, dop5.BaseStateDecodeJSON),
	)
}

func loadLogger() (*zerolog.Logger, io.Writer) {
	kctx := kong.Parse(&cliMain, flagDefaults)

	switch l, o, err := launch.SetupLoggingFromFlags(cliMain.LoggingFlags); {
	case err != nil:
		kctx.FatalIfErrorf(err)

		return nil, nil
	default:
		return l.Log(), o
	}
}

func printBuildInfo(out io.Writer) {
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		_, _ = fmt.Fprintln(out, "-")

		return
	}

	// build version
	m := map[string]string{}
	for key, value := range readBuildInfo() {
		if !strings.HasPrefix(key, "vcs.") {
			continue
		}

		m[key[4:]] = value
	}

	_, _ = fmt.Fprintln(out, "- version")
	for k := range slices.Values([]string{"revision", "modified"}) {
		_, _ = fmt.Fprintf(out, "%15s: %s\n", k, m[k])
	}

	clear(m)
	m["go"] = bi.GoVersion

	_, _ = fmt.Fprintln(out, "- build")
	for i := range slices.Values(bi.Settings) {
		var k string

		switch {
		case i.Key == "-race":
			k = "race"
		case strings.HasPrefix(i.Key, "vcs."):
			continue
		default:
			k = i.Key
		}

		_, _ = fmt.Fprintf(out, "%15s: %s\n", k, i.Value)
	}
}

func readBuildInfo() iter.Seq2[string, string] {
	return func(yield func(string, string) bool) {
		bi, ok := debug.ReadBuildInfo()
		if !ok {
			return
		}

		for i := range slices.Values(bi.Settings) {
			if !yield(i.Key, i.Value) {
				return
			}
		}
	}
}
