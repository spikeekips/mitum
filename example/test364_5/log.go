package main

import (
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/util/logging"
)

func newModuleLogging(module string, f func(zctx zerolog.Context) zerolog.Context) *logging.Logging {
	if f == nil {
		f = func(zctx zerolog.Context) zerolog.Context { return zctx }
	}

	return logging.NewLogging(func(zctx zerolog.Context) zerolog.Context {
		zctx = zctx.Str("module", module)

		if f == nil {
			return f(zctx)
		}

		return zctx
	})
}
