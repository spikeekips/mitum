package launch

import (
	"context"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/spikeekips/mitum/util/ps"
)

var (
	PNameDesign                 = ps.Name("design")
	PNameCheckDesign            = ps.Name("check-design")
	PNameINITObjectCache        = ps.Name("init-object-cache")
	PNameGenesisDesign          = ps.Name("genesis-design")
	DesignFlagContextKey        = util.ContextKey("design-flag")
	DevFlagsContextKey          = util.ContextKey("dev-flags")
	GenesisDesignFileContextKey = util.ContextKey("genesis-design-file")
	DesignContextKey            = util.ContextKey("design")
	DesignStringContextKey      = util.ContextKey("design-string")
	GenesisDesignContextKey     = util.ContextKey("genesis-design")
	PrivatekeyFlagsContextKey   = util.ContextKey("privatekey-from")
	ACLFlagsContextKey          = util.ContextKey("acl-from")
)

func PLoadDesign(pctx context.Context) (context.Context, error) {
	e := util.StringError("load design")

	var log *logging.Logging
	var flag DesignFlag
	var encs *encoder.Encoders
	var privfrom PrivatekeyFlags

	if err := util.LoadFromContextOK(pctx,
		LoggingContextKey, &log,
		DesignFlagContextKey, &flag,
		EncodersContextKey, &encs,
		PrivatekeyFlagsContextKey, &privfrom,
	); err != nil {
		return pctx, e.Wrap(err)
	}

	jsonencoder := encs.JSON()

	var design NodeDesign
	var designString string

	switch flag.Scheme() {
	case "file":
		f := flag.URL().Path

		switch d, b, err := NodeDesignFromFile(f, jsonencoder); {
		case err != nil:
			return pctx, e.Wrap(err)
		default:
			design = d
			designString = string(b)
		}
	case "http", "https":
		switch d, b, err := NodeDesignFromHTTP(flag.URL().String(), flag.Properties().HTTPSTLSInsecure, jsonencoder); {
		case err != nil:
			return pctx, e.Wrap(err)
		default:
			design = d
			designString = string(b)
		}
	case "consul":
		switch d, b, err := NodeDesignFromConsul(flag.URL().Host, flag.URL().Path, jsonencoder); {
		case err != nil:
			return pctx, e.Wrap(err)
		default:
			design = d
			designString = string(b)
		}
	default:
		return pctx, e.Errorf("unknown design uri, %q", flag.URL())
	}

	log.Log().Debug().Interface("design", design).Msg("design loaded")

	if b := privfrom.Flag.Body(); len(b) > 0 {
		priv, err := base.DecodePrivatekeyFromString(string(b), jsonencoder)
		if err != nil {
			return pctx, e.Wrap(err)
		}

		log.Log().Debug().Interface("privatekey", priv.Publickey()).Msg("privatekey loaded from somewhere")

		design.Privatekey = priv
	}

	return util.ContextWithValues(pctx, map[util.ContextKey]interface{}{
		DesignContextKey:       design,
		DesignStringContextKey: designString,
	}), nil
}

func PGenesisDesign(pctx context.Context) (context.Context, error) {
	e := util.StringError("load genesis design")

	var log *logging.Logging
	if err := util.LoadFromContextOK(pctx, LoggingContextKey, &log); err != nil {
		return pctx, e.Wrap(err)
	}

	var designfile string
	if err := util.LoadFromContextOK(pctx, GenesisDesignFileContextKey, &designfile); err != nil {
		return pctx, e.Wrap(err)
	}

	var encs *encoder.Encoders
	if err := util.LoadFromContextOK(pctx, EncodersContextKey, &encs); err != nil {
		return pctx, e.Wrap(err)
	}

	switch d, b, err := GenesisDesignFromFile(designfile, encs.JSON()); {
	case err != nil:
		return pctx, e.Wrap(err)
	default:
		log.Log().Debug().Interface("design", d).Str("design_file", string(b)).Msg("genesis design loaded")

		return context.WithValue(pctx, GenesisDesignContextKey, d), nil
	}
}

func PCheckDesign(pctx context.Context) (context.Context, error) {
	e := util.StringError("check design")

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
		return pctx, e.Wrap(err)
	}

	if err := design.IsValid(nil); err != nil {
		return pctx, e.Wrap(err)
	}

	if err := design.Check(devflags); err != nil {
		return pctx, e.Wrap(err)
	}

	log.Log().Debug().Interface("design", design).Msg("design checked")

	return util.ContextWithValues(pctx, map[util.ContextKey]interface{}{
		DesignContextKey:      design,
		LocalParamsContextKey: design.LocalParams,
		ISAACParamsContextKey: design.LocalParams.ISAAC,
	}), nil
}

func PINITObjectCache(pctx context.Context) (context.Context, error) {
	var log *logging.Logging
	var design NodeDesign

	if err := util.LoadFromContextOK(pctx,
		LoggingContextKey, &log,
		DesignContextKey, &design,
	); err != nil {
		return pctx, err
	}

	cachesize := design.LocalParams.MISC.ObjectCacheSize()

	base.SetObjCache(util.NewGCacheObjectPool(int(cachesize)))

	log.Log().Debug().Uint64("cache_size", cachesize).Msg("set object cache size")

	return pctx, nil
}
