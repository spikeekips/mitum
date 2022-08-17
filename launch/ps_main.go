package launch

import (
	"context"
	"strings"

	"github.com/alecthomas/kong"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/spikeekips/mitum/util/ps"
)

var (
	PNameLogging        = ps.PName("logging")
	PNameLoggingWithCli = ps.PName("logging-cli")
)

var (
	VersionContextKey     = ps.ContextKey("version")
	FlagsContextKey       = ps.ContextKey("flags")
	KongContextContextKey = ps.ContextKey("kong-context")
	LoggingContextKey     = ps.ContextKey("logging")
)

func DefaultMainPS() *ps.PS {
	ips := ps.NewPS()

	_ = ips.
		AddOK(ps.PNameINIT, PINIT, nil)

	_ = ips.POK(ps.PNameINIT).
		PostAddOK(PNameLogging, PLogging).
		PostAddOK(PNameLoggingWithCli, PLoggingWithCli)

	return ips
}

func PINIT(ctx context.Context) (context.Context, error) {
	e := util.StringErrorFunc("failed init")

	var version util.Version

	switch err := ps.LoadFromContextOK(ctx, VersionContextKey, &version); {
	case err != nil:
		return ctx, e(err, "")
	default:
		if err := version.IsValid(nil); err != nil {
			return ctx, e(err, "")
		}
	}

	return ctx, nil
}

func PLogging(ctx context.Context) (context.Context, error) {
	e := util.StringErrorFunc("failed logging")

	var flags BaseFlags
	if err := ps.LoadFromContextOK(ctx, FlagsContextKey, &flags); err != nil {
		return ctx, e(err, "")
	}

	log, err := SetupLoggingFromFlags(flags.LoggingFlags)
	if err != nil {
		return ctx, e(err, "")
	}

	ctx = context.WithValue(ctx, LoggingContextKey, log) //revive:disable-line:modifies-parameter

	return ctx, nil
}

func PLoggingWithCli(ctx context.Context) (context.Context, error) {
	var log *logging.Logging
	if err := ps.LoadFromContextOK(ctx, LoggingContextKey, &log); err != nil {
		return ctx, err
	}

	var kctx *kong.Context
	if err := ps.LoadFromContextOK(ctx, KongContextContextKey, &kctx); err != nil {
		return ctx, err
	}

	log = logging.NewLogging(func(lctx zerolog.Context) zerolog.Context {
		var name string

		if len(kctx.Path) > 0 {
			for i := range kctx.Path {
				j := len(kctx.Path) - i - 1
				n := kctx.Path[j].Node()

				switch {
				case n == nil:
					continue
				case len(n.Path()) < 1:
					continue
				}

				name = "-" + strings.Replace(n.Path(), " ", "-", -1)

				break
			}
		}

		return lctx.Str("module", "main"+name)
	}).SetLogging(log)

	ctx = context.WithValue(ctx, LoggingContextKey, log) //revive:disable-line:modifies-parameter

	return ctx, nil
}
