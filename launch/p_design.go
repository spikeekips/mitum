package launch

import (
	"context"
	"strconv"

	consulapi "github.com/hashicorp/consul/api"
	consulwatch "github.com/hashicorp/consul/api/watch"
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/spikeekips/mitum/util/ps"
)

var (
	PNameDesign                 = ps.Name("design")
	PNameCheckDesign            = ps.Name("check-design")
	PNameGenesisDesign          = ps.Name("genesis-design")
	PNameWatchDesign            = ps.Name("watch-design")
	DesignFlagContextKey        = ps.ContextKey("design-flag")
	GenesisDesignFileContextKey = ps.ContextKey("genesis-design-file")
	DesignContextKey            = ps.ContextKey("design")
	GenesisDesignContextKey     = ps.ContextKey("genesis-design")
	VaultContextKey             = ps.ContextKey("vault")
)

func PLoadDesign(ctx context.Context) (context.Context, error) {
	e := util.StringErrorFunc("failed to load design")

	var log *logging.Logging
	var flag DesignFlag
	var enc *jsonenc.Encoder
	var privfromvault string

	if err := ps.LoadFromContextOK(ctx,
		LoggingContextKey, &log,
		DesignFlagContextKey, &flag,
		EncoderContextKey, &enc,
		VaultContextKey, &privfromvault,
	); err != nil {
		return ctx, e(err, "")
	}

	var design NodeDesign

	switch flag.Scheme() {
	case "file":
		switch d, _, err := NodeDesignFromFile(flag.URL().Path, enc); {
		case err != nil:
			return ctx, e(err, "")
		default:
			design = d
		}
	case "http", "https":
		switch d, err := NodeDesignFromHTTP(flag.URL().String(), flag.Properties().HTTPSTLSInsecure, enc); {
		case err != nil:
			return ctx, e(err, "")
		default:
			design = d
		}
	case "consul":
		switch d, err := NodeDesignFromConsul(flag.URL().Host, flag.URL().Path, enc); {
		case err != nil:
			return ctx, e(err, "")
		default:
			design = d
		}
	default:
		return ctx, e(nil, "unknown design uri, %q", flag.URL())
	}

	log.Log().Debug().Object("design", design).Msg("design loaded")

	if len(privfromvault) > 0 {
		priv, err := loadPrivatekeyFromVault(privfromvault, enc)
		if err != nil {
			return ctx, e(err, "")
		}

		log.Log().Debug().Interface("privatekey", priv.Publickey()).Msg("privatekey loaded from vault")

		design.Privatekey = priv
	}

	ctx = context.WithValue(ctx, DesignContextKey, design) //revive:disable-line:modifies-parameter

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

func PCheckDesign(ctx context.Context) (context.Context, error) {
	e := util.StringErrorFunc("failed to check design")

	var log *logging.Logging
	var flag DesignFlag
	var design NodeDesign

	if err := ps.LoadFromContextOK(ctx,
		LoggingContextKey, &log,
		DesignFlagContextKey, &flag,
		DesignContextKey, &design,
	); err != nil {
		return ctx, e(err, "")
	}

	if err := design.IsValid(nil); err != nil {
		return ctx, e(err, "")
	}

	log.Log().Debug().Object("design", design).Msg("design checked")

	//revive:disable:modifies-parameter
	ctx = context.WithValue(ctx, DesignContextKey, design)
	ctx = context.WithValue(ctx, LocalParamsContextKey, design.LocalParams)
	//revive:enable:modifies-parameter

	if err := updateFromConsulAfterCheckDesign(ctx, flag); err != nil {
		return ctx, e(err, "")
	}

	return ctx, nil
}

func PWatchDesign(ctx context.Context) (context.Context, error) {
	e := util.StringErrorFunc("failed to watch design")

	var log *logging.Logging
	var flag DesignFlag

	if err := ps.LoadFromContextOK(ctx,
		LoggingContextKey, &log,
		DesignFlagContextKey, &flag,
	); err != nil {
		return ctx, e(err, "")
	}

	watchUpdatefs, err := watchUpdateFuncs(ctx)
	if err != nil {
		return ctx, e(err, "")
	}

	switch flag.Scheme() {
	case "consul":
		runf, err := consulWatch(ctx, watchUpdatefs)
		if err != nil {
			return ctx, e(err, "failed to watch thru consul")
		}

		go func() {
			if err := runf(); err != nil {
				log.Log().Error().Err(err).Msg("watch stopped")
			}
		}()
	default:
		log.Log().Debug().Msg("design uri does not support watch")

		return ctx, nil
	}

	return ctx, nil
}

func consulWatch(
	ctx context.Context,
	updatef map[string]func(string) error,
) (func() error, error) {
	var log *logging.Logging
	var flag DesignFlag

	if err := ps.LoadFromContextOK(ctx,
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
		return nil, errors.Wrap(err, "")
	}

	watchUpdateFromConsulf := watchUpdateFromConsulFunc(prefix, updatef)

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

			switch updated, err := watchUpdateFromConsulf(v); {
			case err != nil:
				log.Log().Error().Err(err).Interface("updated", v).Msg("failed to update consul data received")
			case updated:
				log.Log().Debug().Interface("updated", v).Msg("updated consul data received")
			}
		}
	}

	return func() error {
		defer wp.Stop()

		if err := wp.Run(flag.URL().Host); err != nil {
			return errors.Wrap(err, "")
		}

		return nil
	}, nil
}

func watchUpdateFuncs(ctx context.Context) (map[string]func(string) error, error) {
	var log *logging.Logging
	var params *isaac.LocalParams

	if err := ps.LoadFromContextOK(ctx,
		LoggingContextKey, &log,
		LocalParamsContextKey, &params,
	); err != nil {
		return nil, err
	}

	updaters := map[string]func(string) error{
		"parameters/threshold":                    updateLocalParamThreshold(params, log),
		"parameters/interval_broadcast_ballot":    updateLocalParamIntervalBroadcastBallot(params, log),
		"parameters/wait_preparing_init_ballot":   updateLocalParamWaitPreparingINITBallot(params, log),
		"parameters/timeout_request_proposal":     updateLocalParamTimeoutRequestProposal(params, log),
		"parameters/sync_source_checker_interval": updateLocalParamSyncSourceCheckerInterval(params, log),
		"parameters/valid_proposal_operation_expire": updateLocalParamValidProposalOperationExpire(
			params, log),
		"parameters/valid_proposal_suffrage_operations_expire": updateLocalParamValidProposalSuffrageOperationsExpire(
			params, log),
		"parameters/max_operation_size": updateLocalParamMaxOperationSize(params, log),
		"parameters/same_member_limit":  updateLocalParamSameMemberLimit(params, log),
	}

	return updaters, nil
}

func watchUpdateFromConsulFunc(
	prefix string,
	fs map[string]func(string) error,
) func(*consulapi.KVPair) (bool, error) {
	updated := util.NewLockedMap()

	return func(v *consulapi.KVPair) (bool, error) {
		if v == nil {
			return false, nil
		}

		switch i, found := updated.Value(v.Key); {
		case !found, i == nil:
		case v.CreateIndex > i.(uint64): //nolint:forcetypeassert //...
		default:
			return false, nil
		}

		_ = updated.SetValue(v.Key, v.CreateIndex)

		f, found := fs[v.Key[len(prefix):]]
		if !found {
			return false, nil
		}

		return true, f(string(v.Value))
	}
}

func updateFromConsulAfterCheckDesign(
	ctx context.Context,
	flag DesignFlag,
) error {
	switch flag.Scheme() {
	case "file", "http", "https":
		// NOTE maybe supported later
	case "consul":
		client, err := consulClient(flag.URL().Host)
		if err != nil {
			return err
		}

		fs, err := watchUpdateFuncs(ctx)
		if err != nil {
			return err
		}

		prefix := flag.URL().Path
		update := watchUpdateFromConsulFunc(prefix, fs)

		for i := range fs {
			switch v, _, err := client.KV().Get(prefix+"/"+i, nil); {
			case err != nil:
				return errors.WithMessagef(err, "failed to get key from consul, %q", i)
			case v == nil:
			default:
				if _, err := update(v); err != nil {
					return errors.WithMessagef(err, "failed to update key from consul, %q", i)
				}
			}
		}
	}

	return nil
}

func updateLocalParamThreshold(
	params *isaac.LocalParams,
	log *logging.Logging,
) func(string) error {
	return func(s string) error {
		var t base.Threshold
		if err := t.UnmarshalText([]byte(s)); err != nil {
			return errors.WithMessagef(err, "invalid threshold, %q", s)
		}

		prev := params.Threshold()
		_ = params.SetThreshold(t)

		log.Log().Debug().
			Str("key", "threshold").
			Interface("prev", prev).
			Interface("updated", params.Threshold()).
			Msg("local parameter updated")

		return nil
	}
}

func updateLocalParamIntervalBroadcastBallot(
	params *isaac.LocalParams,
	log *logging.Logging,
) func(string) error {
	return func(s string) error {
		d, err := util.ParseDuration(s)
		if err != nil {
			return errors.Wrap(err, "failed to parse duration")
		}

		prev := params.IntervalBroadcastBallot()
		_ = params.SetIntervalBroadcastBallot(d)

		log.Log().Debug().
			Str("key", "interval_broadcast_ballot").
			Interface("prev", prev).
			Interface("updated", params.IntervalBroadcastBallot()).
			Msg("local parameter updated")

		return nil
	}
}

func updateLocalParamWaitPreparingINITBallot(
	params *isaac.LocalParams,
	log *logging.Logging,
) func(string) error {
	return func(s string) error {
		d, err := util.ParseDuration(s)
		if err != nil {
			return errors.Wrap(err, "failed to parse duration")
		}

		prev := params.WaitPreparingINITBallot()
		_ = params.SetWaitPreparingINITBallot(d)

		log.Log().Debug().
			Str("key", "wait_preparing_init_ballot").
			Interface("prev", prev).
			Interface("updated", params.WaitPreparingINITBallot()).
			Msg("local parameter updated")

		return nil
	}
}

func updateLocalParamTimeoutRequestProposal(
	params *isaac.LocalParams,
	log *logging.Logging,
) func(string) error {
	return func(s string) error {
		d, err := util.ParseDuration(s)
		if err != nil {
			return errors.Wrap(err, "failed to parse duration")
		}

		prev := params.TimeoutRequestProposal()
		_ = params.SetTimeoutRequestProposal(d)

		log.Log().Debug().
			Str("key", "timeout_request_proposal").
			Interface("prev", prev).
			Interface("updated", params.TimeoutRequestProposal()).
			Msg("local parameter updated")

		return nil
	}
}

func updateLocalParamSyncSourceCheckerInterval(
	params *isaac.LocalParams,
	log *logging.Logging,
) func(string) error {
	return func(s string) error {
		d, err := util.ParseDuration(s)
		if err != nil {
			return errors.Wrap(err, "failed to parse duration")
		}

		prev := params.SyncSourceCheckerInterval()
		_ = params.SetSyncSourceCheckerInterval(d)

		log.Log().Debug().
			Str("key", "sync_source_checker_interval").
			Interface("prev", prev).
			Interface("updated", params.SyncSourceCheckerInterval()).
			Msg("local parameter updated")

		return nil
	}
}

func updateLocalParamValidProposalOperationExpire(
	params *isaac.LocalParams,
	log *logging.Logging,
) func(string) error {
	return func(s string) error {
		d, err := util.ParseDuration(s)
		if err != nil {
			return errors.Wrap(err, "failed to parse duration")
		}

		prev := params.ValidProposalOperationExpire()
		_ = params.SetValidProposalOperationExpire(d)

		log.Log().Debug().
			Str("key", "valid_proposal_operation_expire").
			Interface("prev", prev).
			Interface("updated", params.ValidProposalOperationExpire()).
			Msg("local parameter updated")

		return nil
	}
}

func updateLocalParamValidProposalSuffrageOperationsExpire(
	params *isaac.LocalParams,
	log *logging.Logging,
) func(string) error {
	return func(s string) error {
		d, err := util.ParseDuration(s)
		if err != nil {
			return errors.Wrap(err, "failed to parse duration")
		}

		prev := params.ValidProposalSuffrageOperationsExpire()
		_ = params.SetValidProposalSuffrageOperationsExpire(d)

		log.Log().Debug().
			Str("key", "valid_proposal_suffrage_operations_expire").
			Interface("prev", prev).
			Interface("updated", params.ValidProposalSuffrageOperationsExpire()).
			Msg("local parameter updated")

		return nil
	}
}

func updateLocalParamMaxOperationSize(
	params *isaac.LocalParams,
	log *logging.Logging,
) func(string) error {
	return func(s string) error {
		i, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			return errors.Wrap(err, "failed to parse uint64")
		}

		prev := params.MaxOperationSize()
		_ = params.SetMaxOperationSize(i)

		log.Log().Debug().
			Str("key", "max_operation_size").
			Interface("prev", prev).
			Interface("updated", params.MaxOperationSize()).
			Msg("local parameter updated")

		return nil
	}
}

func updateLocalParamSameMemberLimit(
	params *isaac.LocalParams,
	log *logging.Logging,
) func(string) error {
	return func(s string) error {
		i, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			return errors.Wrap(err, "failed to parse uint64")
		}

		prev := params.SameMemberLimit()
		_ = params.SetSameMemberLimit(i)

		log.Log().Debug().
			Str("key", "same_member_limit").
			Interface("prev", prev).
			Interface("updated", params.SameMemberLimit()).
			Msg("local parameter updated")

		return nil
	}
}
