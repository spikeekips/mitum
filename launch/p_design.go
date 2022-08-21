package launch

import (
	"context"

	"github.com/spikeekips/mitum/util"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/spikeekips/mitum/util/ps"
)

var (
	PNameDesign                 = ps.Name("design")
	PNameGenesisDesign          = ps.Name("genesis-design")
	DesignFileContextKey        = ps.ContextKey("design-file")
	GenesisDesignFileContextKey = ps.ContextKey("genesis-design-file")
	DesignContextKey            = ps.ContextKey("design")
	GenesisDesignContextKey     = ps.ContextKey("genesis-design")
)

func PDesign(ctx context.Context) (context.Context, error) {
	e := util.StringErrorFunc("failed to load design")

	var log *logging.Logging
	var designfile string
	var enc *jsonenc.Encoder

	if err := ps.LoadsFromContextOK(ctx,
		LoggingContextKey, &log,
		DesignFileContextKey, &designfile,
		EncoderContextKey, &enc,
	); err != nil {
		return ctx, e(err, "")
	}

	switch d, _, err := NodeDesignFromFile(designfile, enc); {
	case err != nil:
		return ctx, e(err, "")
	default:
		log.Log().Debug().Object("design", d).Msg("design loaded")

		ctx = context.WithValue(ctx, DesignContextKey, d) //revive:disable-line:modifies-parameter
	}

	return ctx, nil
}

func PGenesisDesign(ctx context.Context) (context.Context, error) {
	e := util.StringErrorFunc("failed to load genesis design")

	var log *logging.Logging
	if err := ps.LoadFromContextOK(ctx, LoggingContextKey, &log); err != nil {
		return ctx, e(err, "")
	}

	var designfile string
	if err := ps.LoadFromContextOK(ctx, GenesisDesignFileContextKey, &designfile); err != nil {
		return ctx, e(err, "")
	}

	var enc *jsonenc.Encoder
	if err := ps.LoadFromContextOK(ctx, EncoderContextKey, &enc); err != nil {
		return ctx, e(err, "")
	}

	switch d, b, err := GenesisDesignFromFile(designfile, enc); {
	case err != nil:
		return ctx, e(err, "")
	default:
		log.Log().Debug().Interface("design", d).Str("design_file", string(b)).Msg("genesis design loaded")

		ctx = context.WithValue(ctx, GenesisDesignContextKey, d) //revive:disable-line:modifies-parameter
	}

	return ctx, nil
}
