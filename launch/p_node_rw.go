package launch

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	consulapi "github.com/hashicorp/consul/api"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	isaacnetwork "github.com/spikeekips/mitum/isaac/network"
	isaacstates "github.com/spikeekips/mitum/isaac/states"
	"github.com/spikeekips/mitum/network"
	"github.com/spikeekips/mitum/network/quicstream"
	quicstreamheader "github.com/spikeekips/mitum/network/quicstream/header"
	"github.com/spikeekips/mitum/util"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/spikeekips/mitum/util/ps"
	"gopkg.in/yaml.v3"
)

var PNameNetworkHandlersReadWriteNode = ps.Name("node-network-handler")

var (
	ReadNodeHeaderHint           = hint.MustNewHint("read-node-header-v0.0.1")
	WriteNodeHeaderHint          = hint.MustNewHint("write-node-header-v0.0.1")
	HandlerPrefixNodeReadString  = "node_read"
	HandlerPrefixNodeWriteString = "node_write"
	HandlerPrefixNodeRead        = quicstream.HashPrefix(HandlerPrefixNodeReadString)
	HandlerPrefixNodeWrite       = quicstream.HashPrefix(HandlerPrefixNodeWriteString)
)

var AllNodeReadKeys = []string{
	"states.allow_consensus",
	"design._source",    // NOTE user defined design
	"design._generated", // NOTE generated design from source
	"discovery",
	"acl",
}

var AllNodeWriteKeys = []string{
	"states.allow_consensus",
	"design.parameters.isaac.threshold",
	"design.parameters.isaac.interval_broadcast_ballot",
	"design.parameters.isaac.wait_preparing_init_ballot",
	"design.parameters.isaac.max_try_handover_y_broker_sync_data",
	"design.parameters.misc.sync_source_checker_interval",
	"design.parameters.misc.valid_proposal_operation_expire",
	"design.parameters.misc.valid_proposal_suffrage_operations_expire",
	"design.parameters.misc.max_message_size",
	"design.parameters.memberlist.extra_same_member_limit",
	"design.parameters.network.timeout_request",
	"design.parameters.network.ratelimit",
	"design.sync_sources",
	"discovery",
	"acl",
}

type (
	//revive:disable:line-length-limit
	writeNodeValueFunc     func(_ context.Context, key, value, acluser string) (prev, next interface{}, updated bool, _ error)
	writeNodeNextValueFunc func(_ context.Context, key, nextkey, value, acluser string) (prev, next interface{}, updated bool, _ error)
	readNodeValueFunc      func(_ context.Context, key, acluser string) (interface{}, error)
	readNodeNextValueFunc  func(_ context.Context, key, nextkey, acluser string) (interface{}, error)
	//revive:enable:line-length-limit
)

var NodeReadWriteEventLogger EventLoggerName = "node_readwrite"

var (
	DesignACLScope               = ACLScope("design")
	StatesAllowConsensusACLScope = ACLScope("states.allow_consensus")
	DiscoveryACLScope            = ACLScope("discovery")
	ACLACLScope                  = ACLScope("acl")
)

var (
	NodeReadACLPerm  = NewAllowACLPerm(0)
	NodeWriteACLPerm = NewAllowACLPerm(1)
)

func PNetworkHandlersReadWriteNode(pctx context.Context) (context.Context, error) {
	var design NodeDesign
	var params *LocalParams
	var local base.LocalNode
	var eventLogging *EventLogging

	if err := util.LoadFromContextOK(pctx,
		DesignContextKey, &design,
		LocalParamsContextKey, &params,
		LocalContextKey, &local,
		EventLoggingContextKey, &eventLogging,
	); err != nil {
		return pctx, err
	}

	var rl, wl zerolog.Logger

	switch el, found := eventLogging.Logger(NodeReadWriteEventLogger); {
	case !found:
		return pctx, errors.Errorf("node read/write event logger not found")
	default:
		rl = el.With().Str("module", "read").Logger()
		wl = el.With().Str("module", "write").Logger()
	}

	lock := &sync.RWMutex{}

	rf, err := readNode(pctx, lock)
	if err != nil {
		return pctx, err
	}

	wf, err := writeNode(pctx, lock)
	if err != nil {
		return pctx, err
	}

	var gerror error

	EnsureHandlerAdd(pctx, &gerror,
		HandlerPrefixNodeReadString,
		networkHandlerNodeRead(params.ISAAC.NetworkID(), rf, rl), nil)

	EnsureHandlerAdd(pctx, &gerror,
		HandlerPrefixNodeWriteString,
		networkHandlerNodeWrite(params.ISAAC.NetworkID(), wf, wl), nil)

	return pctx, gerror
}

func writeNodeKey(f writeNodeNextValueFunc) writeNodeValueFunc {
	return func(ctx context.Context, key, value, acluser string) (interface{}, interface{}, bool, error) {
		i := strings.SplitN(strings.TrimPrefix(key, "."), ".", 2)

		var nextkey string
		if len(i) > 1 {
			nextkey = i[1]
		}

		return f(ctx, i[0], nextkey, value, acluser)
	}
}

func writeNode(pctx context.Context, lock *sync.RWMutex) (writeNodeValueFunc, error) {
	fStates, err := writeStates(pctx)
	if err != nil {
		return nil, err
	}

	fDesign, err := writeDesign(pctx)
	if err != nil {
		return nil, err
	}

	fDiscovery, err := writeDiscovery(pctx)
	if err != nil {
		return nil, err
	}

	fACL, err := writeACL(pctx)
	if err != nil {
		return nil, err
	}

	return writeNodeKey(func(
		ctx context.Context, key, nextkey, value, acluser string,
	) (interface{}, interface{}, bool, error) {
		lock.Lock()
		defer lock.Unlock()

		switch key {
		case "states":
			return fStates(ctx, nextkey, value, acluser)
		case "design":
			return fDesign(ctx, nextkey, value, acluser)
		case "discovery":
			return fDiscovery(ctx, nextkey, value, acluser)
		case "acl":
			return fACL(ctx, nextkey, value, acluser)
		default:
			return nil, nil, false, util.ErrNotFound.Errorf("unknown key, %q for params", key)
		}
	}), nil
}

func writeStates(pctx context.Context) (writeNodeValueFunc, error) {
	fAllowConsensus, err := writeAllowConsensus(pctx)
	if err != nil {
		return nil, err
	}

	return writeNodeKey(func(
		ctx context.Context, key, nextkey, value, acluser string,
	) (interface{}, interface{}, bool, error) {
		switch key {
		case "allow_consensus":
			return fAllowConsensus(ctx, nextkey, value, acluser)
		default:
			return nil, nil, false, util.ErrNotFound.Errorf("unknown key, %q for node", key)
		}
	}), nil
}

func writeDesign(pctx context.Context) (writeNodeValueFunc, error) {
	var log *logging.Logging
	var flag DesignFlag

	if err := util.LoadFromContextOK(pctx,
		LoggingContextKey, &log,
		DesignFlagContextKey, &flag,
	); err != nil {
		return nil, err
	}

	defaultReadDesignFileF := func() ([]byte, error) { return nil, errors.Errorf("design file; can not read") }
	readDesignFileF := defaultReadDesignFileF
	writeDesignFileF := func([]byte) error { return errors.Errorf("design file; can not write") }

	switch i, err := readDesignFileFunc(flag); {
	case err != nil:
		return nil, err
	case i == nil:
		log.Log().Warn().Stringer("design", flag.URL()).Msg("design file not writable")
	default:
		readDesignFileF = i
	}

	switch i, err := writeDesignFileFunc(flag); {
	case err != nil:
		return nil, err
	case i == nil:
		readDesignFileF = defaultReadDesignFileF

		log.Log().Warn().Stringer("design", flag.URL()).Msg("design file not writable")
	default:
		writeDesignFileF = i
	}

	var m map[string]writeNodeValueFunc

	switch i, err := writeDesignMap(pctx); {
	case err != nil:
		return nil, err
	default:
		m = i
	}

	var aclallow ACLAllowFunc

	switch i, err := pACLAllowFunc(pctx); {
	case err != nil:
		return nil, err
	default:
		aclallow = i
	}

	return func(ctx context.Context, key, value, acluser string) (interface{}, interface{}, bool, error) {
		extra := zerolog.Dict().
			Str("key", key)

		if !aclallow(ctx, acluser, DesignACLScope, NodeWriteACLPerm, extra) {
			return nil, nil, false, ErrACLAccessDenied.WithStack()
		}

		switch f, found := m[key]; {
		case !found:
			return nil, nil, false, util.ErrNotFound.Errorf("unknown key, %q for design", key)
		default:
			switch prev, next, updated, err := f(ctx, key, value, acluser); {
			case err != nil:
				return nil, nil, false, err
			case !updated:
				return prev, next, false, nil
			default:
				return prev, next, updated, errors.WithMessage(
					writeDesignFile(key, value, readDesignFileF, writeDesignFileF),
					"updated, but failed to update design file",
				)
			}
		}
	}, nil
}

func writeDesignMap(pctx context.Context) (map[string]writeNodeValueFunc, error) {
	var enc *jsonenc.Encoder
	var design NodeDesign
	var params *LocalParams
	var syncSourceChecker *isaacnetwork.SyncSourceChecker

	if err := util.LoadFromContextOK(pctx,
		EncoderContextKey, &enc,
		DesignContextKey, &design,
		LocalParamsContextKey, &params,
		SyncSourceCheckerContextKey, &syncSourceChecker,
	); err != nil {
		return nil, err
	}

	m := map[string]writeNodeValueFunc{
		//revive:disable:line-length-limit
		"parameters.isaac.threshold":                           writeLocalParamISAACThreshold(params.ISAAC),
		"parameters.isaac.interval_broadcast_ballot":           writeLocalParamISAACIntervalBroadcastBallot(params.ISAAC),
		"parameters.isaac.wait_preparing_init_ballot":          writeLocalParamISAACWaitPreparingINITBallot(params.ISAAC),
		"parameters.isaac.max_try_handover_y_broker_sync_data": writeLocalParamISAACMaxTryHandoverYBrokerSyncData(params.ISAAC),

		"parameters.misc.sync_source_checker_interval":              writeLocalParamMISCSyncSourceCheckerInterval(params.MISC),
		"parameters.misc.valid_proposal_operation_expire":           writeLocalParamMISCValidProposalOperationExpire(params.MISC),
		"parameters.misc.valid_proposal_suffrage_operations_expire": writeLocalParamMISCValidProposalSuffrageOperationsExpire(params.MISC),
		"parameters.misc.max_message_size":                          writeLocalParamMISCMaxMessageSize(params.MISC),

		"parameters.memberlist.extra_same_member_limit": writeLocalParamExtraSameMemberLimit(params.Memberlist),

		"parameters.network.timeout_request":    writeLocalParamNetworkTimeoutRequest(params.Network),
		"parameters.network.ratelimit.node":     writeLocalParamNetworkRateLimit(params.Network.RateLimit(), "node"),
		"parameters.network.ratelimit.net":      writeLocalParamNetworkRateLimit(params.Network.RateLimit(), "net"),
		"parameters.network.ratelimit.suffrage": writeLocalParamNetworkRateLimit(params.Network.RateLimit(), "suffrage"),
		"parameters.network.ratelimit.default":  writeLocalParamNetworkRateLimit(params.Network.RateLimit(), "default"),

		"parameters.sync_sources": writeSyncSources(enc, design, syncSourceChecker),
		//revive:enable:line-length-limit
	}

	return m, nil
}

func writeLocalParamISAACThreshold(
	params *isaac.Params,
) writeNodeValueFunc {
	return writeNodeKey(func(
		_ context.Context, _, _, value, acluser string,
	) (prev, next interface{}, updated bool, _ error) {
		var s string
		if err := yaml.Unmarshal([]byte(value), &s); err != nil {
			return nil, nil, false, errors.WithStack(err)
		}

		var t base.Threshold
		if err := t.UnmarshalText([]byte(s)); err != nil {
			return nil, nil, false, errors.WithMessagef(err, "invalid threshold, %q", value)
		}

		prevt := params.Threshold()
		if prevt.Equal(t) {
			return prevt, nil, false, nil
		}

		if err := params.SetThreshold(t); err != nil {
			return nil, nil, false, err
		}

		return prevt, params.Threshold(), true, nil
	})
}

func writeLocalParamISAACIntervalBroadcastBallot(
	params *isaac.Params,
) writeNodeValueFunc {
	return writeNodeKey(func(
		_ context.Context, _, _, value, acluser string,
	) (prev, next interface{}, updated bool, _ error) {
		d, err := parseNodeValueDuration(value)
		if err != nil {
			return nil, nil, false, err
		}

		prev = params.IntervalBroadcastBallot()
		if prev == d {
			return prev, nil, false, nil
		}

		if err := params.SetIntervalBroadcastBallot(d); err != nil {
			return nil, nil, false, err
		}

		return prev, params.IntervalBroadcastBallot(), true, nil
	})
}

func writeLocalParamISAACWaitPreparingINITBallot(
	params *isaac.Params,
) writeNodeValueFunc {
	return writeNodeKey(func(
		_ context.Context, _, _, value, acluser string,
	) (prev, next interface{}, updated bool, _ error) {
		d, err := parseNodeValueDuration(value)
		if err != nil {
			return nil, nil, false, err
		}

		prev = params.WaitPreparingINITBallot()
		if prev == d {
			return prev, nil, false, nil
		}

		if err := params.SetWaitPreparingINITBallot(d); err != nil {
			return nil, nil, false, err
		}

		return prev, params.WaitPreparingINITBallot(), true, nil
	})
}

func writeLocalParamISAACMaxTryHandoverYBrokerSyncData(
	params *isaac.Params,
) writeNodeValueFunc {
	return writeNodeKey(func(
		_ context.Context, _, _, value, acluser string,
	) (prev, next interface{}, updated bool, _ error) {
		d, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			return nil, nil, false, errors.WithStack(err)
		}

		prev = params.MaxTryHandoverYBrokerSyncData()
		if prev == d {
			return prev, nil, false, nil
		}

		if err := params.SetMaxTryHandoverYBrokerSyncData(d); err != nil {
			return nil, nil, false, err
		}

		return prev, params.MaxTryHandoverYBrokerSyncData(), true, nil
	})
}

func writeLocalParamMISCSyncSourceCheckerInterval(
	params *MISCParams,
) writeNodeValueFunc {
	return writeNodeKey(func(
		_ context.Context, _, _, value, acluser string,
	) (prev, next interface{}, updated bool, _ error) {
		d, err := parseNodeValueDuration(value)
		if err != nil {
			return nil, nil, false, err
		}

		prev = params.SyncSourceCheckerInterval()
		if prev == d {
			return prev, nil, false, nil
		}

		if err := params.SetSyncSourceCheckerInterval(d); err != nil {
			return nil, nil, false, err
		}

		return prev, params.SyncSourceCheckerInterval(), true, nil
	})
}

func writeLocalParamMISCValidProposalOperationExpire(
	params *MISCParams,
) writeNodeValueFunc {
	return writeNodeKey(func(
		_ context.Context, _, _, value, acluser string,
	) (prev, next interface{}, updated bool, _ error) {
		d, err := parseNodeValueDuration(value)
		if err != nil {
			return nil, nil, false, err
		}

		prev = params.ValidProposalOperationExpire()
		if prev == d {
			return prev, nil, false, nil
		}

		if err := params.SetValidProposalOperationExpire(d); err != nil {
			return nil, nil, false, err
		}

		return prev, params.ValidProposalOperationExpire(), true, nil
	})
}

func writeLocalParamMISCValidProposalSuffrageOperationsExpire(
	params *MISCParams,
) writeNodeValueFunc {
	return writeNodeKey(func(
		_ context.Context, _, _, value, acluser string,
	) (prev, next interface{}, updated bool, _ error) {
		d, err := parseNodeValueDuration(value)
		if err != nil {
			return nil, nil, false, err
		}

		prev = params.ValidProposalSuffrageOperationsExpire()
		if prev == d {
			return prev, nil, false, nil
		}

		if err := params.SetValidProposalSuffrageOperationsExpire(d); err != nil {
			return nil, nil, false, err
		}

		return prev, params.ValidProposalSuffrageOperationsExpire(), true, nil
	})
}

func writeLocalParamMISCMaxMessageSize(
	params *MISCParams,
) writeNodeValueFunc {
	return writeNodeKey(func(
		_ context.Context, _, _, value, acluser string,
	) (prev, next interface{}, updated bool, _ error) {
		i, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			return nil, nil, false, errors.WithStack(err)
		}

		prev = params.MaxMessageSize()
		if prev == i {
			return prev, nil, false, nil
		}

		if err := params.SetMaxMessageSize(i); err != nil {
			return nil, nil, false, err
		}

		return prev, params.MaxMessageSize(), true, nil
	})
}

func writeLocalParamExtraSameMemberLimit(
	params *MemberlistParams,
) writeNodeValueFunc {
	return writeNodeKey(
		func(_ context.Context, _, _, value, acluser string) (prev, next interface{}, updated bool, _ error) {
			i, err := strconv.ParseUint(value, 10, 64)
			if err != nil {
				return nil, nil, false, errors.WithStack(err)
			}

			prev = params.ExtraSameMemberLimit()
			if prev == i {
				return prev, nil, false, nil
			}

			if err := params.SetExtraSameMemberLimit(i); err != nil {
				return nil, nil, false, err
			}

			return prev, params.ExtraSameMemberLimit(), true, nil
		})
}

func writeLocalParamNetworkTimeoutRequest(
	params *NetworkParams,
) writeNodeValueFunc {
	return writeNodeKey(func(
		_ context.Context, _, _, value, acluser string,
	) (prev, next interface{}, updated bool, _ error) {
		d, err := parseNodeValueDuration(value)
		if err != nil {
			return nil, nil, false, err
		}

		prev = params.TimeoutRequest()
		if prev == d {
			return prev, nil, false, nil
		}

		if err := params.SetTimeoutRequest(d); err != nil {
			return nil, nil, false, err
		}

		return prev, params.TimeoutRequest(), true, nil
	})
}

func writeSyncSources(
	enc *jsonenc.Encoder,
	design NodeDesign,
	syncSourceChecker *isaacnetwork.SyncSourceChecker,
) writeNodeValueFunc {
	return func(_ context.Context, _, value, acluser string) (prev, next interface{}, updated bool, _ error) {
		e := util.StringError("update sync source")

		var sources *SyncSourcesDesign
		if err := sources.DecodeYAML([]byte(value), enc); err != nil {
			return nil, nil, false, e.Wrap(err)
		}

		if err := IsValidSyncSourcesDesign(
			sources,
			design.Network.PublishString,
			design.Network.publish.String(),
		); err != nil {
			return nil, nil, false, e.Wrap(err)
		}

		prev = syncSourceChecker.Sources()

		if err := syncSourceChecker.UpdateSources(context.Background(), sources.Sources()); err != nil {
			return nil, nil, false, err
		}

		return prev, sources, true, nil
	}
}

func writeDiscovery(pctx context.Context) (writeNodeValueFunc, error) {
	var discoveries *util.Locked[[]quicstream.ConnInfo]

	if err := util.LoadFromContextOK(pctx,
		DiscoveryContextKey, &discoveries,
	); err != nil {
		return nil, err
	}

	var aclallow ACLAllowFunc

	switch i, err := pACLAllowFunc(pctx); {
	case err != nil:
		return nil, err
	default:
		aclallow = i
	}

	return func(ctx context.Context, _, value, acluser string) (prev, next interface{}, updated bool, _ error) {
		if !aclallow(ctx, acluser, DiscoveryACLScope, NodeWriteACLPerm, nil) {
			return nil, nil, false, ErrACLAccessDenied.WithStack()
		}

		e := util.StringError("update discoveries")

		var sl []string
		if err := yaml.Unmarshal([]byte(value), &sl); err != nil {
			return nil, nil, false, e.Wrap(err)
		}

		cis := make([]quicstream.ConnInfo, len(sl))

		for i := range sl {
			if err := network.IsValidAddr(sl[i]); err != nil {
				return nil, nil, false, e.Wrap(err)
			}

			addr, tlsinsecure := network.ParseTLSInsecure(sl[i])

			ci, err := quicstream.NewConnInfoFromStringAddr(addr, tlsinsecure)
			if err != nil {
				return nil, nil, false, e.Wrap(err)
			}

			cis[i] = ci
		}

		prevd := GetDiscoveriesFromLocked(discoveries)

		switch {
		case len(prevd) != len(cis):
		case len(util.Filter2Slices(prevd, cis, func(a, b quicstream.ConnInfo) bool {
			return a.String() == b.String()
		})) < 1:
			return prevd, nil, false, nil
		}

		_ = discoveries.SetValue(cis)

		return prevd, cis, true, nil
	}, nil
}

func writeLocalParamNetworkRateLimit(
	params *NetworkRateLimitParams,
	param string,
) writeNodeValueFunc {
	switch param {
	case "node",
		"net",
		"suffrage",
		"default":
	default:
		panic(fmt.Sprintf("unknown key, %q for network ratelimit", param))
	}

	return func(_ context.Context, _, value, acluser string) (prev, next interface{}, updated bool, err error) {
		switch param {
		case "node":
			prev = params.NodeRuleSet()
		case "net":
			prev = params.NetRuleSet()
		case "suffrage":
			prev = params.SuffrageRuleSet()
		case "default":
			prev = params.DefaultRuleMap()
		default:
			return nil, nil, false, util.ErrNotFound.Errorf("unknown key, %q for network ratelimit", param)
		}

		switch i, err := unmarshalRateLimitRule(param, value); {
		case err != nil:
			return nil, nil, false, err
		default:
			next = i
		}

		return prev, next, true, func() error {
			switch param {
			case "node":
				return params.SetNodeRuleSet(next.(RateLimiterRuleSet)) //nolint:forcetypeassert //...
			case "net":
				return params.SetNetRuleSet(next.(RateLimiterRuleSet)) //nolint:forcetypeassert //...
			case "suffrage":
				return params.SetSuffrageRuleSet(next.(RateLimiterRuleSet)) //nolint:forcetypeassert //...
			case "default":
				return params.SetDefaultRuleMap(next.(RateLimiterRuleMap)) //nolint:forcetypeassert //...
			default:
				return util.ErrNotFound.Errorf("unknown key, %q for network", param)
			}
		}()
	}
}

func writeAllowConsensus(pctx context.Context) (writeNodeValueFunc, error) {
	var states *isaacstates.States

	if err := util.LoadFromContext(pctx,
		StatesContextKey, &states,
	); err != nil {
		return nil, err
	}

	var aclallow ACLAllowFunc

	switch i, err := pACLAllowFunc(pctx); {
	case err != nil:
		return nil, err
	default:
		aclallow = i
	}

	return func(ctx context.Context, _, value, acluser string) (prev, next interface{}, updated bool, _ error) {
		extra := zerolog.Dict().
			Str("value", value)

		if !aclallow(ctx, acluser, StatesAllowConsensusACLScope, NodeWriteACLPerm, extra) {
			return nil, nil, false, ErrACLAccessDenied.WithStack()
		}

		var allow bool
		if err := yaml.Unmarshal([]byte(value), &allow); err != nil {
			return nil, nil, false, errors.WithStack(err)
		}

		preva := states.AllowedConsensus()
		if preva == allow {
			return preva, nil, false, nil
		}

		if states.SetAllowConsensus(allow) {
			next = allow
		}

		return preva, next, true, nil
	}, nil
}

func writeACL(pctx context.Context) (writeNodeValueFunc, error) {
	var enc *jsonenc.Encoder
	var acl *YAMLACL

	if err := util.LoadFromContextOK(pctx,
		EncoderContextKey, &enc,
		ACLContextKey, &acl,
	); err != nil {
		return nil, err
	}

	var aclallow ACLAllowFunc

	switch i, err := pACLAllowFunc(pctx); {
	case err != nil:
		return nil, err
	default:
		aclallow = i
	}

	return writeNodeKey(func(
		ctx context.Context, key, nextkey, value, acluser string,
	) (interface{}, interface{}, bool, error) {
		fullkey := strings.Join([]string{"acl", key, nextkey}, "")

		if len(key) > 0 {
			return nil, nil, false, errors.Errorf("unknown key, %q", fullkey)
		}

		extra := zerolog.Dict().Str("key", fullkey)

		if !aclallow(ctx, acluser, ACLACLScope, NodeWriteACLPerm, extra) {
			return nil, nil, false, ErrACLAccessDenied.WithStack()
		}

		prev := acl.Export()

		switch updated, err := acl.Import([]byte(value), enc); {
		case err != nil:
			return nil, nil, false, err
		default:
			return prev, acl.Export(), updated, nil
		}
	}), nil
}

func unmarshalRateLimitRule(rule, value string) (interface{}, error) {
	var u interface{}
	if err := yaml.Unmarshal([]byte(value), &u); err != nil {
		return nil, errors.WithStack(err)
	}

	var i interface{}

	switch rule {
	case "node":
		i = NodeRateLimiterRuleSet{}
	case "net":
		i = NetRateLimiterRuleSet{}
	case "suffrage":
		i = &SuffrageRateLimiterRuleSet{}
	case "default":
		i = RateLimiterRuleMap{}
	default:
		return nil, util.ErrNotFound.Errorf("unknown prefix, %q", rule)
	}

	switch b, err := util.MarshalJSON(u); {
	case err != nil:
		return nil, err
	default:
		if err := util.UnmarshalJSON(b, &i); err != nil {
			return nil, err
		}

		if j, ok := i.(util.IsValider); ok {
			if err := j.IsValid(nil); err != nil {
				return nil, err
			}
		}

		return i, nil
	}
}

func networkHandlerNodeWrite(
	networkID base.NetworkID,
	f writeNodeValueFunc,
	eventlogger zerolog.Logger,
) quicstreamheader.Handler[WriteNodeHeader] {
	handler := func(ctx context.Context, addr net.Addr,
		broker *quicstreamheader.HandlerBroker, header WriteNodeHeader,
		l zerolog.Logger,
	) (sentresponse bool, value string, _ error) {
		if err := isaacnetwork.QuicstreamHandlerVerifyNode(
			ctx, addr, broker,
			header.ACLUser(), networkID,
		); err != nil {
			return false, "", err
		}

		var body io.Reader

		switch bodyType, _, b, _, res, err := broker.ReadBody(ctx); {
		case err != nil:
			return false, "", err
		case res != nil:
			return false, "", res.Err()
		case bodyType == quicstreamheader.FixedLengthBodyType,
			bodyType == quicstreamheader.StreamBodyType:
			body = b
		}

		if body != nil {
			b, err := io.ReadAll(body)
			if err != nil {
				return false, "", errors.WithStack(err)
			}

			value = string(b)
		}

		switch prev, next, updated, err := f(ctx, header.Key, value, header.ACLUser().String()); {
		case err != nil:
			return false, value, err
		default:
			l.Debug().
				Str("key", header.Key).
				Str("value", value).
				Interface("prev", prev).
				Interface("next", next).
				Bool("updated", updated).
				Msg("wrote")

			return true, value, broker.WriteResponseHeadOK(ctx, updated, nil)
		}
	}

	return func(ctx context.Context, addr net.Addr,
		broker *quicstreamheader.HandlerBroker, header WriteNodeHeader,
	) (context.Context, error) {
		e := util.StringError("write node")

		l := quicstream.ConnectionLoggerFromContext(ctx, &eventlogger).With().
			Str("cid", util.UUID().String()).
			Logger()

		switch sentresponse, value, err := handler(ctx, addr, broker, header, l); {
		case err != nil:
			l.Error().Err(err).
				Str("key", header.Key).
				Str("value", value).
				Send()

			if !sentresponse {
				return ctx, e.WithMessage(broker.WriteResponseHeadOK(ctx, false, err), "write response header")
			}

			return ctx, e.Wrap(err)
		default:
			return ctx, nil
		}
	}
}

func WriteNodeFromNetworkHandler(
	ctx context.Context,
	priv base.Privatekey,
	networkID base.NetworkID,
	key string,
	value string,
	stream quicstreamheader.StreamFunc,
) (found bool, _ error) {
	header := NewWriteNodeHeader(key, priv.Publickey())
	if err := header.IsValid(nil); err != nil {
		return false, err
	}

	body := bytes.NewBuffer([]byte(value))
	bodyclosef := func() {
		body.Reset()
	}

	err := stream(ctx, func(ctx context.Context, broker *quicstreamheader.ClientBroker) error {
		if err := broker.WriteRequestHead(ctx, header); err != nil {
			defer bodyclosef()

			return err
		}

		if err := isaacnetwork.VerifyNode(ctx, broker, priv, networkID); err != nil {
			defer bodyclosef()

			return err
		}

		wch := make(chan error, 1)
		go func() {
			defer bodyclosef()

			wch <- broker.WriteBody(ctx, quicstreamheader.StreamBodyType, 0, body)
		}()

		switch _, res, err := broker.ReadResponseHead(ctx); {
		case err != nil:
			return err
		case res.Err() != nil:
			return res.Err()
		case !res.OK():
			return nil
		default:
			found = true

			return <-wch
		}
	})

	return found, err
}

func ReadNodeFromNetworkHandler(
	ctx context.Context,
	priv base.Privatekey,
	networkID base.NetworkID,
	key string,
	stream quicstreamheader.StreamFunc,
) (t interface{}, found bool, _ error) {
	header := NewReadNodeHeader(key, priv.Publickey())
	if err := header.IsValid(nil); err != nil {
		return t, false, err
	}

	err := stream(ctx, func(ctx context.Context, broker *quicstreamheader.ClientBroker) error {
		switch b, i, err := readNodeFromNetworkHandler(ctx, priv, networkID, broker, header); {
		case err != nil:
			return err
		case !i:
			return nil
		default:
			found = true

			if err := yaml.Unmarshal(b, &t); err != nil {
				return errors.WithStack(err)
			}

			return nil
		}
	})

	return t, found, err
}

func readNodeFromNetworkHandler(
	ctx context.Context,
	priv base.Privatekey,
	networkID base.NetworkID,
	broker *quicstreamheader.ClientBroker,
	header ReadNodeHeader,
) ([]byte, bool, error) {
	if err := broker.WriteRequestHead(ctx, header); err != nil {
		return nil, false, err
	}

	if err := isaacnetwork.VerifyNode(ctx, broker, priv, networkID); err != nil {
		return nil, false, err
	}

	switch _, res, err := broker.ReadResponseHead(ctx); {
	case err != nil:
		return nil, false, err
	case res.Err() != nil, !res.OK():
		return nil, res.OK(), res.Err()
	}

	var body io.Reader

	switch bodyType, bodyLength, b, _, res, err := broker.ReadBody(ctx); {
	case err != nil:
		return nil, false, err
	case res != nil:
		return nil, res.OK(), res.Err()
	case bodyType == quicstreamheader.FixedLengthBodyType:
		if bodyLength > 0 {
			body = b
		}
	case bodyType == quicstreamheader.StreamBodyType:
		body = b
	}

	if body == nil {
		return nil, false, errors.Errorf("empty value")
	}

	b, err := io.ReadAll(body)

	return b, true, errors.WithStack(err)
}

func readNodeKey(f readNodeNextValueFunc) readNodeValueFunc {
	return func(ctx context.Context, key, acluser string) (interface{}, error) {
		i := strings.SplitN(strings.TrimPrefix(key, "."), ".", 2)

		var nextkey string
		if len(i) > 1 {
			nextkey = i[1]
		}

		return f(ctx, i[0], nextkey, acluser)
	}
}

func readNode(pctx context.Context, lock *sync.RWMutex) (readNodeValueFunc, error) {
	fStates, err := readStates(pctx)
	if err != nil {
		return nil, err
	}

	fDesign, err := readDesign(pctx)
	if err != nil {
		return nil, err
	}

	fDiscovery, err := readDiscovery(pctx)
	if err != nil {
		return nil, err
	}

	fACL, err := readACL(pctx)
	if err != nil {
		return nil, err
	}

	return readNodeKey(func(ctx context.Context, key, nextkey, acluser string) (interface{}, error) {
		lock.RLock()
		defer lock.RUnlock()

		switch key {
		case "states":
			return fStates(ctx, nextkey, acluser)
		case "design":
			return fDesign(ctx, nextkey, acluser)
		case "discovery":
			return fDiscovery(ctx, nextkey, acluser)
		case "acl":
			return fACL(ctx, nextkey, acluser)
		default:
			return nil, util.ErrNotFound.Errorf("unknown key, %q for params", key)
		}
	}), nil
}

func readStates(pctx context.Context) (readNodeValueFunc, error) {
	fAllowConsensus, err := readAllowConsensus(pctx)
	if err != nil {
		return nil, err
	}

	return readNodeKey(func(ctx context.Context, key, nextkey, acluser string) (interface{}, error) {
		switch key {
		case "allow_consensus":
			return fAllowConsensus(ctx, nextkey, acluser)
		default:
			return nil, util.ErrNotFound.Errorf("unknown key, %q for node", key)
		}
	}), nil
}

func readDesign(pctx context.Context) (readNodeValueFunc, error) {
	var design NodeDesign
	var log *logging.Logging
	var flag DesignFlag

	if err := util.LoadFromContextOK(pctx,
		DesignContextKey, &design,
		LoggingContextKey, &log,
		DesignFlagContextKey, &flag,
	); err != nil {
		return nil, err
	}

	var aclallow ACLAllowFunc

	switch i, err := pACLAllowFunc(pctx); {
	case err != nil:
		return nil, err
	default:
		aclallow = i
	}

	readDesignFileF := func() ([]byte, error) { return nil, errors.Errorf("design file; can not read") }

	switch i, err := readDesignFileFunc(flag); {
	case err != nil:
		return nil, err
	case i == nil:
		log.Log().Warn().Stringer("design", flag.URL()).Msg("design file not readable")
	default:
		readDesignFileF = i
	}

	m := map[string]func() (interface{}, error){}

	m["_source"] = func() (interface{}, error) {
		return readDesignFileF()
	}
	m["_generated"] = func() (interface{}, error) {
		b, err := yaml.Marshal(design)
		if err != nil {
			return nil, errors.WithStack(err)
		}

		return b, nil
	}

	return func(ctx context.Context, key, acluser string) (interface{}, error) {
		extra := zerolog.Dict().
			Str("key", "design."+key)

		if !aclallow(ctx, acluser, DesignACLScope, NodeReadACLPerm, extra) {
			return nil, ErrACLAccessDenied.WithStack()
		}

		switch f, found := m[key]; {
		case !found:
			return nil, util.ErrNotFound.Errorf("unknown key, %q for design", key)
		default:
			return f()
		}
	}, nil
}

func readAllowConsensus(pctx context.Context) (readNodeValueFunc, error) {
	var states *isaacstates.States

	if err := util.LoadFromContext(pctx,
		StatesContextKey, &states,
	); err != nil {
		return nil, err
	}

	var aclallow ACLAllowFunc

	switch i, err := pACLAllowFunc(pctx); {
	case err != nil:
		return nil, err
	default:
		aclallow = i
	}

	return func(ctx context.Context, _, acluser string) (interface{}, error) {
		extra := zerolog.Dict().
			Str("key", "states.allow_consensus")

		if !aclallow(ctx, acluser, StatesAllowConsensusACLScope, NodeReadACLPerm, extra) {
			return nil, ErrACLAccessDenied.WithStack()
		}

		return states.AllowedConsensus(), nil
	}, nil
}

func readDiscovery(pctx context.Context) (readNodeValueFunc, error) {
	var discoveries *util.Locked[[]quicstream.ConnInfo]

	if err := util.LoadFromContextOK(pctx,
		DiscoveryContextKey, &discoveries,
	); err != nil {
		return nil, err
	}

	var aclallow ACLAllowFunc

	switch i, err := pACLAllowFunc(pctx); {
	case err != nil:
		return nil, err
	default:
		aclallow = i
	}

	return func(ctx context.Context, _, acluser string) (interface{}, error) {
		extra := zerolog.Dict().
			Str("key", "discovery")

		if !aclallow(ctx, acluser, DiscoveryACLScope, NodeReadACLPerm, extra) {
			return nil, ErrACLAccessDenied.WithStack()
		}

		return GetDiscoveriesFromLocked(discoveries), nil
	}, nil
}

func readACL(pctx context.Context) (readNodeValueFunc, error) {
	var acl *YAMLACL

	if err := util.LoadFromContextOK(pctx,
		ACLContextKey, &acl,
	); err != nil {
		return nil, err
	}

	var aclallow ACLAllowFunc

	switch i, err := pACLAllowFunc(pctx); {
	case err != nil:
		return nil, err
	default:
		aclallow = i
	}

	return readNodeKey(func(ctx context.Context, key, nextkey, acluser string) (interface{}, error) {
		fullkey := strings.Join([]string{"acl", key, nextkey}, "")

		if len(key) > 0 {
			return nil, errors.Errorf("unknown key, %q", fullkey)
		}

		extra := zerolog.Dict().Str("key", fullkey)

		if !aclallow(ctx, acluser, ACLACLScope, NodeReadACLPerm, extra) {
			return nil, ErrACLAccessDenied.WithStack()
		}

		b, err := yaml.Marshal(acl.Export())

		return b, errors.WithStack(err)
	}), nil
}

func networkHandlerNodeRead(
	networkID base.NetworkID,
	f readNodeValueFunc,
	eventlogger zerolog.Logger,
) quicstreamheader.Handler[ReadNodeHeader] {
	handler := func(ctx context.Context, addr net.Addr,
		broker *quicstreamheader.HandlerBroker, header ReadNodeHeader,
	) (sentresponse bool, _ error) {
		if err := isaacnetwork.QuicstreamHandlerVerifyNode(
			ctx, addr, broker,
			header.ACLUser(), networkID,
		); err != nil {
			return false, err
		}

		switch body, err := func() (*bytes.Buffer, error) {
			switch v, err := f(ctx, header.Key, header.ACLUser().String()); {
			case err != nil:
				return nil, err
			default:
				if b, ok := v.([]byte); ok {
					return bytes.NewBuffer(b), nil
				}

				b, err := broker.Encoder.Marshal(v)
				if err != nil {
					return nil, err
				}

				return bytes.NewBuffer(b), nil
			}
		}(); {
		case err != nil:
			return false, err
		default:
			defer body.Reset()

			if err := broker.WriteResponseHeadOK(ctx, true, nil); err != nil {
				return true, err
			}

			return true, broker.WriteBody(ctx, quicstreamheader.StreamBodyType, 0, body)
		}
	}

	return func(ctx context.Context, addr net.Addr,
		broker *quicstreamheader.HandlerBroker, header ReadNodeHeader,
	) (context.Context, error) {
		e := util.StringError("read node")

		l := quicstream.ConnectionLoggerFromContext(ctx, &eventlogger).With().
			Str("key", header.Key).
			Str("cid", util.UUID().String()).
			Logger()

		switch sentresponse, err := handler(ctx, addr, broker, header); {
		case errors.Is(err, util.ErrNotFound):
			l.Error().Err(err).Msg("key not found")

			return ctx, e.WithMessage(broker.WriteResponseHeadOK(ctx, false, nil), "write response header")
		case err != nil:
			l.Error().Err(err).Send()

			if !sentresponse {
				return ctx, e.WithMessage(broker.WriteResponseHeadOK(ctx, false, err), "write response header")
			}

			return ctx, e.Wrap(err)
		default:
			l.Debug().Msg("read")

			return ctx, nil
		}
	}
}

type rwNodeHeader struct {
	Key     string
	aclUser base.Publickey
	isaacnetwork.BaseHeader
}

func newRWNodeHeader(
	ht hint.Hint,
	prefix [32]byte,
	key string,
	acluser base.Publickey,
) rwNodeHeader {
	return rwNodeHeader{
		BaseHeader: isaacnetwork.BaseHeader{
			BaseRequestHeader: quicstreamheader.NewBaseRequestHeader(ht, prefix),
		},
		Key:     key,
		aclUser: acluser,
	}
}

func (h rwNodeHeader) IsValid(b []byte) error {
	e := util.ErrInvalid.Errorf("invalid rwNodeHeader")

	if err := h.BaseHinter.IsValid(b); err != nil {
		return e.Wrap(err)
	}

	if len(h.Key) < 1 {
		return e.Errorf("empty key")
	}

	if err := util.CheckIsValiders(nil, false, h.aclUser); err != nil {
		return e.WithMessage(err, "acl user")
	}

	return nil
}

func (h rwNodeHeader) ACLUser() base.Publickey {
	return h.aclUser
}

type rwNodeHeaderJSONMarshaler struct {
	ACLUser base.Publickey `json:"acl_user"`
	Key     string         `json:"key"`
}

func (h rwNodeHeader) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(struct {
		rwNodeHeaderJSONMarshaler
		isaacnetwork.BaseHeaderJSONMarshaler
	}{
		BaseHeaderJSONMarshaler: h.BaseHeader.JSONMarshaler(),
		rwNodeHeaderJSONMarshaler: rwNodeHeaderJSONMarshaler{
			Key:     h.Key,
			ACLUser: h.aclUser,
		},
	})
}

type rwNodeHeaderJSONUnmarshaler struct {
	Key     string `json:"key"`
	ACLUser string `json:"acl_user"`
}

func (h *rwNodeHeader) DecodeJSON(b []byte, enc *jsonenc.Encoder) error {
	e := util.StringError("unmarshal rwNodeHeader")

	if err := util.UnmarshalJSON(b, &h.BaseHeader); err != nil {
		return e.Wrap(err)
	}

	var u rwNodeHeaderJSONUnmarshaler
	if err := util.UnmarshalJSON(b, &u); err != nil {
		return e.Wrap(err)
	}

	h.Key = u.Key

	switch i, err := base.DecodePublickeyFromString(u.ACLUser, enc); {
	case err != nil:
		return e.WithMessage(err, "acl user")
	default:
		h.aclUser = i
	}

	return nil
}

type WriteNodeHeader struct {
	rwNodeHeader
}

func NewWriteNodeHeader(key string, acluser base.Publickey) WriteNodeHeader {
	return WriteNodeHeader{
		rwNodeHeader: newRWNodeHeader(WriteNodeHeaderHint, HandlerPrefixNodeWrite, key, acluser),
	}
}

func (h WriteNodeHeader) IsValid([]byte) error {
	e := util.ErrInvalid.Errorf("invalid WriteNodeHeader")

	if err := h.rwNodeHeader.IsValid(WriteNodeHeaderHint.Type().Bytes()); err != nil {
		return e.Wrap(err)
	}

	return nil
}

type ReadNodeHeader struct {
	rwNodeHeader
}

func NewReadNodeHeader(key string, acluser base.Publickey) ReadNodeHeader {
	return ReadNodeHeader{
		rwNodeHeader: newRWNodeHeader(ReadNodeHeaderHint, HandlerPrefixNodeRead, key, acluser),
	}
}

func (h ReadNodeHeader) IsValid([]byte) error {
	e := util.ErrInvalid.Errorf("invalid ReadNodeHeader")

	if err := h.rwNodeHeader.IsValid(ReadNodeHeaderHint.Type().Bytes()); err != nil {
		return e.Wrap(err)
	}

	return nil
}

func parseNodeValueDuration(value string) (time.Duration, error) {
	var s string
	if err := yaml.Unmarshal([]byte(value), &s); err != nil {
		return 0, errors.WithStack(err)
	}

	return util.ParseDuration(s)
}

func omapFromDesignMap(m *util.YAMLOrderedMap, key string) (lastkey string, _ *util.YAMLOrderedMap, _ error) {
	l := strings.Split(key, ".")

	if err := util.TraverseSlice(l, func(_ int, i string) error {
		if len(strings.TrimSpace(i)) < 1 {
			return errors.Errorf("empty key found")
		}

		return nil
	}); err != nil {
		return "", nil, err
	}

	p := m

	for i := range l[:len(l)-1] {
		k := l[i]

		j, found := p.Get(k)
		if !found {
			n := util.NewYAMLOrderedMap()

			p.Set(k, n)

			p = n

			continue
		}

		switch n, ok := j.(*util.YAMLOrderedMap); {
		case !ok:
			return "", nil, errors.Errorf("not *YAMLOrderedMap, %T", j)
		default:
			p = n
		}
	}

	return l[len(l)-1], p, nil
}

func updateDesignMap(m *util.YAMLOrderedMap, key string, value interface{}) error {
	switch k, i, err := omapFromDesignMap(m, key); {
	case err != nil:
		return err
	default:
		_ = i.Set(k, value)

		return nil
	}
}

func writeDesignFile(
	key string, value interface{},
	read func() ([]byte, error),
	write func([]byte) error,
) error {
	var m *util.YAMLOrderedMap

	switch b, err := read(); {
	case err != nil:
		return err
	default:
		if err := yaml.Unmarshal(b, &m); err != nil {
			return errors.WithStack(err)
		}
	}

	if err := updateDesignMap(m, key, value); err != nil {
		return err
	}

	switch b, err := yaml.Marshal(m); {
	case err != nil:
		return errors.WithStack(err)
	default:
		return write(b)
	}
}

func readDesignFileFunc(flag DesignFlag) (func() ([]byte, error), error) {
	switch flag.Scheme() {
	case "file":
		return func() ([]byte, error) {
			b, err := os.ReadFile(flag.URL().Path)

			return b, errors.WithStack(err)
		}, nil
	case "http", "https":
		return func() ([]byte, error) {
			return getFromHTTP(flag.URL().String(), flag.Properties().HTTPSTLSInsecure)
		}, nil
	case "consul":
		return func() ([]byte, error) {
			return getFromConsul(flag.URL().Host, flag.URL().Path)
		}, nil
	default:
		return nil, errors.Errorf("unknown design uri, %q", flag.URL())
	}
}

func writeDesignFileFunc(flag DesignFlag) (func([]byte) error, error) {
	switch flag.Scheme() {
	case "file":
		f := flag.URL().Path

		switch fi, err := os.Stat(filepath.Clean(f)); {
		case err != nil:
			return nil, errors.WithStack(err)
		case fi.Mode()&os.ModePerm == os.ModePerm:
			return nil, nil
		}

		return func(b []byte) error {
			f, err := os.OpenFile(filepath.Clean(f), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
			if err != nil {
				return errors.WithStack(err)
			}

			_, err = f.Write(b)

			return errors.WithStack(err)
		}, nil
	case "http", "https":
		return nil, nil
	case "consul":
		return func(b []byte) error {
			switch client, err := consulClient(flag.URL().Host); {
			case err != nil:
				return err
			default:
				kv := &consulapi.KVPair{Key: flag.URL().Path, Value: b}

				_, err = client.KV().Put(kv, nil)

				return errors.WithStack(err)
			}
		}, nil
	default:
		return nil, errors.Errorf("unknown design uri, %q", flag.URL())
	}
}
