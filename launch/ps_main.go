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
	PNameLogging        = ps.Name("logging")
	PNameLoggingWithCLI = ps.Name("logging-cli")
)

var (
	VersionContextKey     = util.ContextKey("version")
	FlagsContextKey       = util.ContextKey("flags")
	KongContextContextKey = util.ContextKey("kong-context")
	LoggingContextKey     = util.ContextKey("logging")
)

func DefaultMainPS() *ps.PS {
	ips := ps.NewPS("cmd-main")

	_ = ips.
		AddOK(ps.NameINIT, PINIT, nil)

	_ = ips.POK(ps.NameINIT).
		PostAddOK(PNameLogging, PLogging).
		PostAddOK(PNameLoggingWithCLI, PLoggingWithCLI)

	return ips
}

func PINIT(pctx context.Context) (context.Context, error) {
	e := util.StringError("init")

	var version util.Version

	switch err := util.LoadFromContextOK(pctx, VersionContextKey, &version); {
	case err != nil:
		return pctx, e.Wrap(err)
	default:
		if err := version.IsValid(nil); err != nil {
			return pctx, e.Wrap(err)
		}
	}

	return pctx, nil
}

func PLogging(pctx context.Context) (context.Context, error) {
	e := util.StringError("logging")

	var flags BaseFlags
	if err := util.LoadFromContextOK(pctx, FlagsContextKey, &flags); err != nil {
		return pctx, e.Wrap(err)
	}

	log, err := SetupLoggingFromFlags(flags.LoggingFlags)
	if err != nil {
		return pctx, e.Wrap(err)
	}

	return context.WithValue(pctx, LoggingContextKey, log), nil
}

func PLoggingWithCLI(pctx context.Context) (context.Context, error) {
	var log *logging.Logging
	if err := util.LoadFromContextOK(pctx, LoggingContextKey, &log); err != nil {
		return pctx, err
	}

	var kctx *kong.Context
	if err := util.LoadFromContextOK(pctx, KongContextContextKey, &kctx); err != nil {
		return pctx, err
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

	return context.WithValue(pctx, LoggingContextKey, log), nil
}
