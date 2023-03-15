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
	PNameCheckDesign            = ps.Name("check-design")
	PNameGenesisDesign          = ps.Name("genesis-design")
	DesignFlagContextKey        = util.ContextKey("design-flag")
	DevFlagsContextKey          = util.ContextKey("dev-flags")
	GenesisDesignFileContextKey = util.ContextKey("genesis-design-file")
	DesignContextKey            = util.ContextKey("design")
	DesignStringContextKey      = util.ContextKey("design-string")
	GenesisDesignContextKey     = util.ContextKey("genesis-design")
	VaultContextKey             = util.ContextKey("vault")
)

func PLoadDesign(pctx context.Context) (context.Context, error) {
	e := util.StringErrorFunc("load design")

	var log *logging.Logging
	var flag DesignFlag
	var enc *jsonenc.Encoder
	var privfromvault string

	if err := util.LoadFromContextOK(pctx,
		LoggingContextKey, &log,
		DesignFlagContextKey, &flag,
		EncoderContextKey, &enc,
		VaultContextKey, &privfromvault,
	); err != nil {
		return pctx, e(err, "")
	}

	var design NodeDesign
	var designString string

	switch flag.Scheme() {
	case "file":
		switch d, b, err := NodeDesignFromFile(flag.URL().Path, enc); {
		case err != nil:
			return pctx, e(err, "")
		default:
			design = d
			designString = string(b)
		}
	case "http", "https":
		switch d, b, err := NodeDesignFromHTTP(flag.URL().String(), flag.Properties().HTTPSTLSInsecure, enc); {
		case err != nil:
			return pctx, e(err, "")
		default:
			design = d
			designString = string(b)
		}
	case "consul":
		switch d, b, err := NodeDesignFromConsul(flag.URL().Host, flag.URL().Path, enc); {
		case err != nil:
			return pctx, e(err, "")
		default:
			design = d
			designString = string(b)
		}
	default:
		return pctx, e(nil, "unknown design uri, %q", flag.URL())
	}

	log.Log().Debug().Object("design", design).Msg("design loaded")

	if len(privfromvault) > 0 {
		priv, err := loadPrivatekeyFromVault(privfromvault, enc)
		if err != nil {
			return pctx, e(err, "")
		}

		log.Log().Debug().Interface("privatekey", priv.Publickey()).Msg("privatekey loaded from vault")

		design.Privatekey = priv
	}

	pctx = context.WithValue(pctx, DesignContextKey, design)             //revive:disable-line:modifies-parameter
	pctx = context.WithValue(pctx, DesignStringContextKey, designString) //revive:disable-line:modifies-parameter

	return pctx, nil
}

func PGenesisDesign(pctx context.Context) (context.Context, error) {
	e := util.StringErrorFunc("load genesis design")

	var log *logging.Logging
	if err := util.LoadFromContextOK(pctx, LoggingContextKey, &log); err != nil {
		return pctx, e(err, "")
	}

	var designfile string
	if err := util.LoadFromContextOK(pctx, GenesisDesignFileContextKey, &designfile); err != nil {
		return pctx, e(err, "")
	}

	var enc *jsonenc.Encoder
	if err := util.LoadFromContextOK(pctx, EncoderContextKey, &enc); err != nil {
		return pctx, e(err, "")
	}

	switch d, b, err := GenesisDesignFromFile(designfile, enc); {
	case err != nil:
		return pctx, e(err, "")
	default:
		log.Log().Debug().Interface("design", d).Str("design_file", string(b)).Msg("genesis design loaded")

		pctx = context.WithValue(pctx, GenesisDesignContextKey, d) //revive:disable-line:modifies-parameter
	}

	return pctx, nil
}

func PCheckDesign(pctx context.Context) (context.Context, error) {
	e := util.StringErrorFunc("check design")

	var log *logging.Logging
	var flag DesignFlag
	var devflags DevFlags
	var design NodeDesign

	if err := util.LoadFromContextOK(pctx,
		LoggingContextKey, &log,
		DesignFlagContextKey, &flag,
		DevFlagsContextKey, &devflags,
		DesignContextKey, &design,
	); err != nil {
		return pctx, e(err, "")
	}

	if err := design.IsValid(nil); err != nil {
		return pctx, e(err, "")
	}

	if err := design.Check(devflags); err != nil {
		return pctx, e(err, "")
	}

	log.Log().Debug().Object("design", design).Msg("design checked")

	//revive:disable:modifies-parameter
	pctx = context.WithValue(pctx, DesignContextKey, design)
	pctx = context.WithValue(pctx, LocalParamsContextKey, design.LocalParams)
	//revive:enable:modifies-parameter

	if err := updateFromConsulAfterCheckDesign(pctx, flag); err != nil {
		return pctx, e(err, "")
	}

	return pctx, nil
}
