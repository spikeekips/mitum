package launch

import (
	"context"

	consulapi "github.com/hashicorp/consul/api"
	consulwatch "github.com/hashicorp/consul/api/watch"
	"github.com/pkg/errors"
	isaacnetwork "github.com/spikeekips/mitum/isaac/network"
	"github.com/spikeekips/mitum/network/quicstream"
	"github.com/spikeekips/mitum/util"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/spikeekips/mitum/util/ps"
)

var PNameWatchDesign = ps.Name("watch-design")

func PWatchDesign(pctx context.Context) (context.Context, error) {
	e := util.StringError("watch design")

	var log *logging.Logging
	var flag DesignFlag

	if err := util.LoadFromContextOK(pctx,
		LoggingContextKey, &log,
		DesignFlagContextKey, &flag,
	); err != nil {
		return pctx, e.Wrap(err)
	}

	f, err := watchDesignFuncs(pctx)
	if err != nil {
		return pctx, e.Wrap(err)
	}

	switch flag.Scheme() {
	case "consul":
		runf, err := watchDesignFromConsul(pctx, f)
		if err != nil {
			return pctx, e.WithMessage(err, "watch thru consul")
		}

		go func() {
			if err := runf(); err != nil {
				log.Log().Error().Err(err).Msg("watch stopped")
			}
		}()
	default:
		log.Log().Debug().Msg("design uri does not support watch")

		return pctx, nil
	}

	return pctx, nil
}

func checkDesignFromConsul(
	pctx context.Context,
	flag DesignFlag,
	log *logging.Logging,
) error {
	switch flag.Scheme() {
	case "file", "http", "https":
		// TODO maybe supported later
	case "consul":
		client, err := consulClient(flag.URL().Host)
		if err != nil {
			return err
		}

		fs, err := checkDesignFuncs(pctx)
		if err != nil {
			return err
		}

		prefix := flag.URL().Path

		for i := range fs {
			update := writeDesignValueFromConsul(prefix, fs[i], log)

			var kvs consulapi.KVPairs

			switch l, _, err := client.KV().List(prefix+"/"+i, nil); {
			case err != nil:
				return errors.WithMessagef(err, "get key from consul, %q", i)
			case len(l) < 1:
			default:
				kvs = l
			}

			for j := range kvs {
				p := kvs[j]

				switch _, err := update(p.Key, string(p.Value), p.CreateIndex); {
				case errors.Is(err, util.ErrNotFound):
					log.Log().Debug().Str("key", p.Key).Msg("unknown key found from consul")
				case err != nil:
					return errors.WithMessagef(err, "update from consul, %q", p.Key)
				}
			}
		}
	}

	return nil
}

func watchDesignFromConsul(
	pctx context.Context,
	f writeDesignValueFunc,
) (func() error, error) {
	var log *logging.Logging
	var flag DesignFlag

	if err := util.LoadFromContextOK(pctx,
		LoggingContextKey, &log,
		DesignFlagContextKey, &flag,
	); err != nil {
		return nil, err
	}

	prefix := flag.URL().Path
	consulparams := make(map[string]interface{})
	consulparams["type"] = "keyprefix"
	consulparams["prefix"] = prefix + "/"

	wp, err := consulwatch.Parse(consulparams)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	wf := writeDesignValueFromConsul(prefix, f, log)

	wp.Handler = func(idx uint64, data interface{}) {
		var pairs consulapi.KVPairs
		switch t := data.(type) {
		case consulapi.KVPairs:
			pairs = t
		default:
			log.Log().Error().Err(err).Msgf("unknown consul data, %T", data)

			return
		}

		for i := range pairs {
			v := pairs[i]

			switch updated, err := func() (bool, error) {
				if v == nil {
					return false, nil
				}

				return wf(v.Key, string(v.Value), v.CreateIndex)
			}(); {
			case err != nil:
				log.Log().Error().Err(err).Interface("updated", v).Msg("failed to write consul data received")
			case updated:
				log.Log().Debug().Interface("updated", v).Msg("consul data wrote")
			}
		}
	}

	return func() error {
		defer wp.Stop()

		if err := wp.Run(flag.URL().Host); err != nil {
			return errors.WithStack(err)
		}

		return nil
	}, nil
}

func checkDesignFuncs(pctx context.Context) (map[string]writeDesignValueFunc, error) {
	var log *logging.Logging
	var enc *jsonenc.Encoder
	var design NodeDesign
	var params *LocalParams
	var discoveries *util.Locked[[]quicstream.ConnInfo]

	if err := util.LoadFromContextOK(pctx,
		LoggingContextKey, &log,
		EncoderContextKey, &enc,
		DesignContextKey, &design,
		LocalParamsContextKey, &params,
		DiscoveryContextKey, &discoveries,
	); err != nil {
		return nil, err
	}

	return checkWriteDesign(enc, design, params, discoveries), nil
}

func watchDesignFuncs(pctx context.Context) (writeDesignValueFunc, error) {
	var log *logging.Logging
	var enc *jsonenc.Encoder
	var design NodeDesign
	var params *LocalParams
	var discoveries *util.Locked[[]quicstream.ConnInfo]
	var syncSourceChecker *isaacnetwork.SyncSourceChecker

	if err := util.LoadFromContextOK(pctx,
		LoggingContextKey, &log,
		EncoderContextKey, &enc,
		DesignContextKey, &design,
		LocalParamsContextKey, &params,
		DiscoveryContextKey, &discoveries,
		SyncSourceCheckerContextKey, &syncSourceChecker,
	); err != nil {
		return nil, err
	}

	return writeDesign(enc, design, params, discoveries, syncSourceChecker), nil
}

func writeDesignValueFromConsul(
	prefix string,
	f writeDesignValueFunc,
	log *logging.Logging,
) func(key, value string, createIndex uint64) (bool, error) {
	updated := util.NewSingleLockedMap[string, uint64]()

	return func(key, value string, createIndex uint64) (bool, error) {
		var ignored bool

		_, _, _ = updated.Set(key, func(i uint64, found bool) (uint64, error) {
			switch {
			case !found, createIndex > i:
				return createIndex, nil
			default:
				ignored = true

				return 0, util.ErrLockedSetIgnore.WithStack()
			}
		})

		switch {
		case ignored:
			return false, nil
		case len(key) < len(prefix):
			return false, errors.Errorf("wrong key")
		}

		fullkey := key[len(prefix):]

		switch prev, next, err := f(fullkey, value); {
		case err != nil:
			return false, err
		default:
			log.Log().Debug().
				Str("key", fullkey).
				Interface("prev", prev).
				Interface("next", next).
				Msg("wrote")

			return true, nil
		}
	}
}
