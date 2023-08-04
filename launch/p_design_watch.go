package launch

import (
	"context"
	"strconv"
	"strings"

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

type (
	updateDesignValueFunc func(key, value string) (prev, next interface{}, _ error)
	updateDesignSetFunc   func(key, prefix, nextkey, value string) (prev, next interface{}, _ error)
)

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

	watchUpdatefs, err := watchDesignFuncs(pctx)
	if err != nil {
		return pctx, e.Wrap(err)
	}

	switch flag.Scheme() {
	case "consul":
		runf, err := watchConsul(pctx, watchUpdatefs)
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
		update := watchUpdateFromConsulFunc(prefix, fs, log)

		for i := range fs {
			switch v, _, err := client.KV().Get(prefix+"/"+i, nil); {
			case err != nil:
				return errors.WithMessagef(err, "get key from consul, %q", i)
			case v == nil:
			default:
				if _, err := func() (bool, error) {
					if v == nil {
						return false, nil
					}

					return update(v.Key, string(v.Value), v.CreateIndex)
				}(); err != nil {
					return errors.WithMessagef(err, "update from consul, %q", i)
				}
			}
		}
	}

	return nil
}

func watchConsul(
	pctx context.Context,
	updatef map[string]updateDesignValueFunc,
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

	f := watchUpdateFromConsulFunc(prefix, updatef, log)

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

				return f(v.Key, string(v.Value), v.CreateIndex)
			}(); {
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

func checkDesignFuncs(pctx context.Context) (map[string]updateDesignValueFunc, error) {
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

	updaters := map[string]updateDesignValueFunc{
		//revive:disable:line-length-limit
		"parameters":   updateLocalParam(params),
		"discoveries":  updateDiscoveries(discoveries),
		"sync_sources": updateDesignSyncSources(enc, design),
		//revive:enable:line-length-limit
	}

	return updaters, nil
}

func watchDesignFuncs(pctx context.Context) (map[string]updateDesignValueFunc, error) {
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

	updaters := map[string]updateDesignValueFunc{
		//revive:disable:line-length-limit
		"parameters":   updateLocalParam(params),
		"discoveries":  updateDiscoveries(discoveries),
		"sync_sources": updateSyncSources(enc, design, syncSourceChecker),
		//revive:enable:line-length-limit
	}

	return updaters, nil
}

func watchUpdateFromConsulFunc(
	prefix string,
	fs map[string]updateDesignValueFunc,
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
		nextkey := fullkey

		var f updateDesignValueFunc

		switch i, found := fs[fullkey]; {
		case found:
			f = i
		default:
			for j := range fs {
				if strings.HasPrefix(fullkey, j) {
					f = fs[j]
					nextkey = fullkey[len(j):]

					break
				}
			}

			if f == nil {
				return false, errors.Errorf("unknown key, %q", fullkey)
			}
		}

		switch prev, next, err := f(nextkey, value); {
		case err != nil:
			return false, err
		default:
			log.Log().Debug().
				Str("key", fullkey).
				Interface("prev", prev).
				Interface("next", next).
				Msg("updated")

			return true, nil
		}
	}
}

func updateDesignSet(f updateDesignSetFunc) updateDesignValueFunc {
	return func(key, value string) (prev interface{}, next interface{}, err error) {
		i := strings.SplitN(strings.TrimPrefix(key, "/"), "/", 2)

		var nextkey string
		if len(i) > 1 {
			nextkey = i[1]
		}

		return f(key, i[0], nextkey, value)
	}
}

func updateLocalParam(
	params *LocalParams,
) updateDesignValueFunc {
	return updateDesignSet(func(key, prefix, nextkey, value string) (prev interface{}, next interface{}, err error) {
		switch prefix {
		case "isaac":
			return updateLocalParamISAAC(params.ISAAC, nextkey, value)
		case "misc":
			return updateLocalParamMISC(params.MISC, nextkey, value)
		case "memberlist":
			return updateLocalParamMemberlist(params.Memberlist, nextkey, value)
		case "network":
			return updateLocalParamNetwork(params.Network, nextkey, value)
		default:
			return prev, next, errors.Errorf("unknown key, %q for params", key)
		}
	})
}

func updateLocalParamISAAC(
	params *isaac.Params,
	key, value string,
) (prev interface{}, next interface{}, err error) {
	return updateDesignSet(func(key, prefix, nextkey, value string) (prev, next interface{}, _ error) {
		switch prefix {
		case "threshold":
			return updateLocalParamISAACThreshold(params, nextkey, value)
		case "interval_broadcast_ballot":
			return updateLocalParamISAACIntervalBroadcastBallot(params, nextkey, value)
		case "wait_preparing_init_ballot":
			return updateLocalParamISAACWaitPreparingINITBallot(params, nextkey, value)
		case "max_try_handover_y_broker_sync_data":
			return updateLocalParamISAACMaxTryHandoverYBrokerSyncData(params, nextkey, value)
		default:
			return prev, next, errors.Errorf("unknown key, %q for isaac", key)
		}
	})(key, value)
}

func updateLocalParamISAACThreshold(
	params *isaac.Params,
	_, value string,
) (interface{}, interface{}, error) {
	var t base.Threshold
	if err := t.UnmarshalText([]byte(value)); err != nil {
		return nil, nil, errors.WithMessagef(err, "invalid threshold, %q", value)
	}

	prev := params.Threshold()

	if err := params.SetThreshold(t); err != nil {
		return nil, nil, err
	}

	return prev, params.Threshold(), nil
}

func updateLocalParamISAACIntervalBroadcastBallot(
	params *isaac.Params,
	_, value string,
) (interface{}, interface{}, error) {
	d, err := util.ParseDuration(value)
	if err != nil {
		return nil, nil, err
	}

	prev := params.IntervalBroadcastBallot()

	if err := params.SetIntervalBroadcastBallot(d); err != nil {
		return nil, nil, err
	}

	return prev, params.IntervalBroadcastBallot(), nil
}

func updateLocalParamISAACWaitPreparingINITBallot(
	params *isaac.Params,
	_, value string,
) (interface{}, interface{}, error) {
	d, err := util.ParseDuration(value)
	if err != nil {
		return nil, nil, err
	}

	prev := params.WaitPreparingINITBallot()

	if err := params.SetWaitPreparingINITBallot(d); err != nil {
		return nil, nil, err
	}

	return prev, params.WaitPreparingINITBallot(), nil
}

func updateLocalParamISAACMaxTryHandoverYBrokerSyncData(
	params *isaac.Params,
	_, value string,
) (interface{}, interface{}, error) {
	d, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}

	prev := params.MaxTryHandoverYBrokerSyncData()

	if err := params.SetMaxTryHandoverYBrokerSyncData(d); err != nil {
		return nil, nil, err
	}

	return prev, params.MaxTryHandoverYBrokerSyncData(), nil
}

func updateLocalParamMISC(
	params *MISCParams,
	key, value string,
) (prev interface{}, next interface{}, err error) {
	return updateDesignSet(func(key, prefix, nextkey, value string) (prev, next interface{}, _ error) {
		switch prefix {
		case "sync_source_checker_interval":
			return updateLocalParamMISCSyncSourceCheckerInterval(params, nextkey, value)
		case "valid_proposal_operation_expire":
			return updateLocalParamMISCValidProposalOperationExpire(params, nextkey, value)
		case "valid_proposal_suffrage_operations_expire":
			return updateLocalParamMISCValidProposalSuffrageOperationsExpire(params, nextkey, value)
		case "max_message_size":
			return updateLocalParamMISCMaxMessageSize(params, nextkey, value)
		default:
			return prev, next, errors.Errorf("unknown key, %q for misc", key)
		}
	})(key, value)
}

func updateLocalParamMISCSyncSourceCheckerInterval(
	params *MISCParams,
	_, value string,
) (interface{}, interface{}, error) {
	d, err := util.ParseDuration(value)
	if err != nil {
		return nil, nil, err
	}

	prev := params.SyncSourceCheckerInterval()

	if err := params.SetSyncSourceCheckerInterval(d); err != nil {
		return nil, nil, err
	}

	return prev, params.SyncSourceCheckerInterval(), nil
}

func updateLocalParamMISCValidProposalOperationExpire(
	params *MISCParams,
	_, value string,
) (interface{}, interface{}, error) {
	d, err := util.ParseDuration(value)
	if err != nil {
		return nil, nil, err
	}

	prev := params.ValidProposalOperationExpire()

	if err := params.SetValidProposalOperationExpire(d); err != nil {
		return nil, nil, err
	}

	return prev, params.ValidProposalOperationExpire(), nil
}

func updateLocalParamMISCValidProposalSuffrageOperationsExpire(
	params *MISCParams,
	_, value string,
) (interface{}, interface{}, error) {
	d, err := util.ParseDuration(value)
	if err != nil {
		return nil, nil, err
	}

	prev := params.ValidProposalSuffrageOperationsExpire()

	if err := params.SetValidProposalSuffrageOperationsExpire(d); err != nil {
		return nil, nil, err
	}

	return prev, params.ValidProposalSuffrageOperationsExpire(), nil
}

func updateLocalParamMISCMaxMessageSize(
	params *MISCParams,
	_, value string,
) (interface{}, interface{}, error) {
	i, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}

	prev := params.MaxMessageSize()

	if err := params.SetMaxMessageSize(i); err != nil {
		return nil, nil, err
	}

	return prev, params.MaxMessageSize(), nil
}

func updateLocalParamMemberlist(
	params *MemberlistParams,
	key, value string,
) (prev interface{}, next interface{}, err error) {
	return updateDesignSet(func(key, prefix, nextkey, value string) (prev, next interface{}, _ error) {
		switch prefix {
		case "extra_same_member_limit":
			return updateLocalParamExtraSameMemberLimit(params, nextkey, value)
		default:
			return prev, next, errors.Errorf("unknown key, %q for memberlist", key)
		}
	})(key, value)
}

func updateLocalParamExtraSameMemberLimit(
	params *MemberlistParams,
	_, value string,
) (interface{}, interface{}, error) {
	i, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}

	prev := params.ExtraSameMemberLimit()

	if err := params.SetExtraSameMemberLimit(i); err != nil {
		return nil, nil, err
	}

	return prev, params.ExtraSameMemberLimit(), nil
}

func updateLocalParamNetwork(
	params *NetworkParams,
	key, value string,
) (prev interface{}, next interface{}, err error) {
	return updateDesignSet(func(key, prefix, nextkey, value string) (prev, next interface{}, _ error) {
		switch prefix {
		case "timeout_request":
			return updateLocalParamNetworkTimeoutRequest(params, nextkey, value)
		default:
			return prev, next, errors.Errorf("unknown key, %q for network", key)
		}
	})(key, value)
}

func updateLocalParamNetworkTimeoutRequest(
	params *NetworkParams,
	_, value string,
) (interface{}, interface{}, error) {
	d, err := util.ParseDuration(value)
	if err != nil {
		return nil, nil, err
	}

	prev := params.TimeoutRequest()

	if err := params.SetTimeoutRequest(d); err != nil {
		return nil, nil, err
	}

	return prev, params.TimeoutRequest(), nil
}

func updateDesignSyncSources(
	enc *jsonenc.Encoder,
	design NodeDesign,
) updateDesignValueFunc {
	return func(_, value string) (interface{}, interface{}, error) {
		e := util.StringError("update sync source")

		var sources *SyncSourcesDesign
		if err := sources.DecodeYAML([]byte(value), enc); err != nil {
			return nil, nil, e.Wrap(err)
		}

		if err := IsValidSyncSourcesDesign(
			sources,
			design.Network.PublishString,
			design.Network.publish.String(),
		); err != nil {
			return nil, nil, e.Wrap(err)
		}

		prev := design.SyncSources

		design.SyncSources.Update(sources.Sources())

		return prev, sources, nil
	}
}

func updateSyncSources(
	enc *jsonenc.Encoder,
	design NodeDesign,
	syncSourceChecker *isaacnetwork.SyncSourceChecker,
) updateDesignValueFunc {
	return func(_, value string) (interface{}, interface{}, error) {
		e := util.StringError("update sync source")

		var sources *SyncSourcesDesign
		if err := sources.DecodeYAML([]byte(value), enc); err != nil {
			return nil, nil, e.Wrap(err)
		}

		if err := IsValidSyncSourcesDesign(
			sources,
			design.Network.PublishString,
			design.Network.publish.String(),
		); err != nil {
			return nil, nil, e.Wrap(err)
		}

		prev := syncSourceChecker.Sources()

		if err := syncSourceChecker.UpdateSources(context.Background(), sources.Sources()); err != nil {
			return nil, nil, err
		}

		return prev, sources, nil
	}
}

func updateDiscoveries(
	discoveries *util.Locked[[]quicstream.ConnInfo],
) updateDesignValueFunc {
	return func(_, value string) (interface{}, interface{}, error) {
		e := util.StringError("update discoveries")

		var sl []string
		if err := yaml.Unmarshal([]byte(value), &sl); err != nil {
			return nil, nil, e.Wrap(err)
		}

		cis := make([]quicstream.ConnInfo, len(sl))

		for i := range sl {
			if err := network.IsValidAddr(sl[i]); err != nil {
				return nil, nil, e.Wrap(err)
			}

			addr, tlsinsecure := network.ParseTLSInsecure(sl[i])

			ci, err := quicstream.NewConnInfoFromStringAddr(addr, tlsinsecure)
			if err != nil {
				return nil, nil, e.Wrap(err)
			}

			cis[i] = ci
		}

		prev := GetDiscoveriesFromLocked(discoveries)

		_ = discoveries.SetValue(cis)

		return prev, cis, nil
	}
}
