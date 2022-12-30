package launch

import (
	"context"
	"strconv"

	consulapi "github.com/hashicorp/consul/api"
	consulwatch "github.com/hashicorp/consul/api/watch"
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	isaacnetwork "github.com/spikeekips/mitum/isaac/network"
	"github.com/spikeekips/mitum/network"
	"github.com/spikeekips/mitum/network/quicstream"
	"github.com/spikeekips/mitum/util"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/spikeekips/mitum/util/ps"
	"gopkg.in/yaml.v3"
)

var PNameWatchDesign = ps.Name("watch-design")

func PWatchDesign(ctx context.Context) (context.Context, error) {
	e := util.StringErrorFunc("failed to watch design")

	var log *logging.Logging
	var flag DesignFlag

	if err := util.LoadFromContextOK(ctx,
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

	if err := util.LoadFromContextOK(ctx,
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
			return errors.WithStack(err)
		}

		return nil
	}, nil
}

func watchUpdateFuncs(ctx context.Context) (map[string]func(string) error, error) {
	var log *logging.Logging
	var enc *jsonenc.Encoder
	var design NodeDesign
	var params *isaac.LocalParams
	var discoveries *util.Locked[[]quicstream.UDPConnInfo]
	var syncSourceChecker *isaacnetwork.SyncSourceChecker

	if err := util.LoadFromContextOK(ctx,
		LoggingContextKey, &log,
		EncoderContextKey, &enc,
		DesignContextKey, &design,
		LocalParamsContextKey, &params,
		DiscoveryContextKey, &discoveries,
		SyncSourceCheckerContextKey, &syncSourceChecker,
	); err != nil {
		return nil, err
	}

	//revive:disable:line-length-limit
	updaters := map[string]func(string) error{
		"parameters/threshold":                                 updateLocalParamThreshold(params, log),
		"parameters/interval_broadcast_ballot":                 updateLocalParamIntervalBroadcastBallot(params, log),
		"parameters/wait_preparing_init_ballot":                updateLocalParamWaitPreparingINITBallot(params, log),
		"parameters/timeout_request_proposal":                  updateLocalParamTimeoutRequestProposal(params, log),
		"parameters/sync_source_checker_interval":              updateLocalParamSyncSourceCheckerInterval(params, log),
		"parameters/valid_proposal_operation_expire":           updateLocalParamValidProposalOperationExpire(params, log),
		"parameters/valid_proposal_suffrage_operations_expire": updateLocalParamValidProposalSuffrageOperationsExpire(params, log),
		"parameters/max_operation_size":                        updateLocalParamMaxMessageSize(params, log),
		"parameters/same_member_limit":                         updateLocalParamSameMemberLimit(params, log),
		"discoveries":                                          updateDiscoveries(discoveries, log),
		"sync_sources":                                         updateSyncSources(enc, design, syncSourceChecker, log),
	}
	//revive:enable:line-length-limit

	return updaters, nil
}

func watchUpdateFromConsulFunc(
	prefix string,
	fs map[string]func(string) error,
) func(*consulapi.KVPair) (bool, error) {
	updated := util.NewSingleLockedMap("", uint64(0))

	return func(v *consulapi.KVPair) (bool, error) {
		if v == nil {
			return false, nil
		}

		switch i, found := updated.Value(v.Key); {
		case !found, v.CreateIndex > i:
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

func updateLocalParamMaxMessageSize(
	params *isaac.LocalParams,
	log *logging.Logging,
) func(string) error {
	return func(s string) error {
		i, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			return errors.Wrap(err, "failed to parse uint64")
		}

		prev := params.MaxMessageSize()
		_ = params.SetMaxMessageSize(i)

		log.Log().Debug().
			Str("key", "max_message_size").
			Interface("prev", prev).
			Interface("updated", params.MaxMessageSize()).
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

func updateSyncSources(
	enc *jsonenc.Encoder,
	design NodeDesign,
	syncSourceChecker *isaacnetwork.SyncSourceChecker,
	log *logging.Logging,
) func(string) error {
	return func(s string) error {
		e := util.StringErrorFunc("failed to update sync source")

		var sources SyncSourcesDesign
		if err := sources.DecodeYAML([]byte(s), enc); err != nil {
			return e(err, "")
		}

		if err := IsValidSyncSourcesDesign(
			sources,
			design.Address,
			design.Network.PublishString,
			design.Network.publish.String(),
		); err != nil {
			return e(err, "")
		}

		prev := syncSourceChecker.Sources()
		syncSourceChecker.UpdateSources(sources)

		log.Log().Debug().
			Str("key", "sync_sources").
			Interface("prev", prev).
			Interface("updated", sources).
			Msg("sync sources updated")

		return nil
	}
}

func updateDiscoveries(
	discoveries *util.Locked[[]quicstream.UDPConnInfo],
	log *logging.Logging,
) func(string) error {
	return func(s string) error {
		e := util.StringErrorFunc("failed to update discoveries")

		var sl []string
		if err := yaml.Unmarshal([]byte(s), &sl); err != nil {
			return e(err, "")
		}

		cis := make([]quicstream.UDPConnInfo, len(sl))

		for i := range sl {
			if err := network.IsValidAddr(sl[i]); err != nil {
				return e(err, "")
			}

			addr, tlsinsecure := network.ParseTLSInsecure(sl[i])

			ci, err := quicstream.NewUDPConnInfoFromStringAddress(addr, tlsinsecure)
			if err != nil {
				return e(err, "")
			}

			cis[i] = ci
		}

		prev := GetDiscoveriesFromLocked(discoveries)
		_ = discoveries.SetValue(cis)

		log.Log().Debug().
			Str("key", "sync_sources").
			Interface("prev", prev).
			Interface("updated", cis).
			Msg("discoveries updated")

		return nil
	}
}
