package launch

import (
	"bytes"
	"context"
	"io"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
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
	"github.com/spikeekips/mitum/util/ps"
	"golang.org/x/sync/singleflight"
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

var AllNodeRWKeys = []string{
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
}

type (
	writeNodeValueFunc     func(key, value string) (prev, next interface{}, _ error)
	writeNodeNextValueFunc func(key, nextkey, value string) (prev, next interface{}, _ error)
	readNodeValueFunc      func(key string) (interface{}, error)
	readNodeNextValueFunc  func(key, nextkey string) (interface{}, error)
)

func PNetworkHandlersReadWriteNode(pctx context.Context) (context.Context, error) {
	var design NodeDesign
	var params *LocalParams
	var local base.LocalNode

	if err := util.LoadFromContextOK(pctx,
		DesignContextKey, &design,
		LocalParamsContextKey, &params,
		LocalContextKey, &local,
	); err != nil {
		return nil, err
	}

	rf, err := readNode(pctx)
	if err != nil {
		return pctx, err
	}

	wf, err := writeNode(pctx)
	if err != nil {
		return pctx, err
	}

	var gerror error

	ensureHandlerAdd(pctx, &gerror,
		HandlerPrefixNodeReadString,
		handlerNodeRead(local.Publickey(), params.ISAAC.NetworkID(), rf), nil)

	ensureHandlerAdd(pctx, &gerror,
		HandlerPrefixNodeWriteString,
		handlerNodeWrite(local.Publickey(), params.ISAAC.NetworkID(), wf), nil)

	return pctx, gerror
}

func writeNodeKey(f writeNodeNextValueFunc) writeNodeValueFunc {
	return func(key, value string) (interface{}, interface{}, error) {
		i := strings.SplitN(strings.TrimPrefix(key, "."), ".", 2)

		var nextkey string
		if len(i) > 1 {
			nextkey = i[1]
		}

		return f(i[0], nextkey, value)
	}
}

func writeNode(pctx context.Context) (writeNodeValueFunc, error) {
	var discoveries *util.Locked[[]quicstream.ConnInfo]

	if err := util.LoadFromContextOK(pctx,
		DiscoveryContextKey, &discoveries,
	); err != nil {
		return nil, err
	}

	fStates, err := writeStates(pctx)
	if err != nil {
		return nil, err
	}

	fDesign, err := writeDesign(pctx)
	if err != nil {
		return nil, err
	}

	fDiscoveries := writeDiscoveries(discoveries)

	return writeNodeKey(func(key, nextkey, value string) (interface{}, interface{}, error) {
		switch key {
		case "states":
			return fStates(nextkey, value)
		case "design":
			return fDesign(nextkey, value)
		case "discovery":
			return fDiscoveries(nextkey, value)
		default:
			return nil, nil, util.ErrNotFound.Errorf("unknown key, %q for params", key)
		}
	}), nil
}

func writeStates(pctx context.Context) (writeNodeValueFunc, error) {
	fAllowConsensus, err := writeAllowConsensus(pctx)
	if err != nil {
		return nil, err
	}

	return writeNodeKey(func(key, nextkey, value string) (interface{}, interface{}, error) {
		switch key {
		case "allow_consensus":
			return fAllowConsensus(nextkey, value)
		default:
			return nil, nil, util.ErrNotFound.Errorf("unknown key, %q for node", key)
		}
	}), nil
}

func writeDesign(pctx context.Context) (writeNodeValueFunc, error) {
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

	fLocalparams := writeLocalParam(params)
	fSyncSources := writeSyncSources(enc, design, syncSourceChecker)

	return writeNodeKey(func(key, nextkey, value string) (interface{}, interface{}, error) {
		switch key {
		case "parameters":
			return fLocalparams(nextkey, value)
		case "sync_sources":
			return fSyncSources(nextkey, value)
		default:
			return nil, nil, util.ErrNotFound.Errorf("unknown key, %q for params", key)
		}
	}), nil
}

func writeLocalParam(
	params *LocalParams,
) writeNodeValueFunc {
	fISAAC := writeLocalParamISAAC(params.ISAAC)
	fMisc := writeLocalParamMISC(params.MISC)
	fMemberlist := writeLocalParamMemberlist(params.Memberlist)
	fnetwork := writeLocalParamNetwork(params.Network)

	return writeNodeKey(func(key, nextkey, value string) (interface{}, interface{}, error) {
		switch key {
		case "isaac":
			return fISAAC(nextkey, value)
		case "misc":
			return fMisc(nextkey, value)
		case "memberlist":
			return fMemberlist(nextkey, value)
		case "network":
			return fnetwork(nextkey, value)
		default:
			return nil, nil, util.ErrNotFound.Errorf("unknown key, %q for params", key)
		}
	})
}

func writeLocalParamISAAC(
	params *isaac.Params,
) writeNodeValueFunc {
	fThreshold := writeLocalParamISAACThreshold(params)
	fIntervalBroadcastBallot := writeLocalParamISAACIntervalBroadcastBallot(params)
	fWaitPreparingINITBallot := writeLocalParamISAACWaitPreparingINITBallot(params)
	fMaxTryHandoverYBrokerSyncData := writeLocalParamISAACMaxTryHandoverYBrokerSyncData(params)

	return writeNodeKey(func(key, nextkey, value string) (prev, next interface{}, _ error) {
		switch key {
		case "threshold":
			return fThreshold(nextkey, value)
		case "interval_broadcast_ballot":
			return fIntervalBroadcastBallot(nextkey, value)
		case "wait_preparing_init_ballot":
			return fWaitPreparingINITBallot(nextkey, value)
		case "max_try_handover_y_broker_sync_data":
			return fMaxTryHandoverYBrokerSyncData(nextkey, value)
		default:
			return prev, next, util.ErrNotFound.Errorf("unknown key, %q for isaac", key)
		}
	})
}

func writeLocalParamISAACThreshold(
	params *isaac.Params,
) writeNodeValueFunc {
	return writeNodeKey(func(_, _, value string) (prev, next interface{}, _ error) {
		var s string
		if err := yaml.Unmarshal([]byte(value), &s); err != nil {
			return nil, nil, errors.WithStack(err)
		}

		var t base.Threshold
		if err := t.UnmarshalText([]byte(s)); err != nil {
			return nil, nil, errors.WithMessagef(err, "invalid threshold, %q", value)
		}

		prev = params.Threshold()

		if err := params.SetThreshold(t); err != nil {
			return nil, nil, err
		}

		return prev, params.Threshold(), nil
	})
}

func writeLocalParamISAACIntervalBroadcastBallot(
	params *isaac.Params,
) writeNodeValueFunc {
	return writeNodeKey(func(_, _, value string) (prev, next interface{}, _ error) {
		d, err := parseNodeValueDuration(value)
		if err != nil {
			return nil, nil, err
		}

		prev = params.IntervalBroadcastBallot()

		if err := params.SetIntervalBroadcastBallot(d); err != nil {
			return nil, nil, err
		}

		return prev, params.IntervalBroadcastBallot(), nil
	})
}

func writeLocalParamISAACWaitPreparingINITBallot(
	params *isaac.Params,
) writeNodeValueFunc {
	return writeNodeKey(func(_, _, value string) (prev, next interface{}, _ error) {
		d, err := parseNodeValueDuration(value)
		if err != nil {
			return nil, nil, err
		}

		prev = params.WaitPreparingINITBallot()

		if err := params.SetWaitPreparingINITBallot(d); err != nil {
			return nil, nil, err
		}

		return prev, params.WaitPreparingINITBallot(), nil
	})
}

func writeLocalParamISAACMaxTryHandoverYBrokerSyncData(
	params *isaac.Params,
) writeNodeValueFunc {
	return writeNodeKey(func(_, _, value string) (prev, next interface{}, _ error) {
		d, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			return nil, nil, errors.WithStack(err)
		}

		prev = params.MaxTryHandoverYBrokerSyncData()

		if err := params.SetMaxTryHandoverYBrokerSyncData(d); err != nil {
			return nil, nil, err
		}

		return prev, params.MaxTryHandoverYBrokerSyncData(), nil
	})
}

func writeLocalParamMISC(
	params *MISCParams,
) writeNodeValueFunc {
	fSyncSourceCheckerInterval := writeLocalParamMISCSyncSourceCheckerInterval(params)
	fValidProposalOperationExpire := writeLocalParamMISCValidProposalOperationExpire(params)
	fValidProposalSuffrageOperationsExpire := writeLocalParamMISCValidProposalSuffrageOperationsExpire(params)
	fMaxMessageSize := writeLocalParamMISCMaxMessageSize(params)

	return writeNodeKey(func(key, nextkey, value string) (prev, next interface{}, _ error) {
		switch key {
		case "sync_source_checker_interval":
			return fSyncSourceCheckerInterval(nextkey, value)
		case "valid_proposal_operation_expire":
			return fValidProposalOperationExpire(nextkey, value)
		case "valid_proposal_suffrage_operations_expire":
			return fValidProposalSuffrageOperationsExpire(nextkey, value)
		case "max_message_size":
			return fMaxMessageSize(nextkey, value)
		default:
			return prev, next, util.ErrNotFound.Errorf("unknown key, %q for misc", key)
		}
	})
}

func writeLocalParamMISCSyncSourceCheckerInterval(
	params *MISCParams,
) writeNodeValueFunc {
	return writeNodeKey(func(_, _, value string) (prev, next interface{}, _ error) {
		d, err := parseNodeValueDuration(value)
		if err != nil {
			return nil, nil, err
		}

		prev = params.SyncSourceCheckerInterval()

		if err := params.SetSyncSourceCheckerInterval(d); err != nil {
			return nil, nil, err
		}

		return prev, params.SyncSourceCheckerInterval(), nil
	})
}

func writeLocalParamMISCValidProposalOperationExpire(
	params *MISCParams,
) writeNodeValueFunc {
	return writeNodeKey(func(_, _, value string) (prev, next interface{}, _ error) {
		d, err := parseNodeValueDuration(value)
		if err != nil {
			return nil, nil, err
		}

		prev = params.ValidProposalOperationExpire()

		if err := params.SetValidProposalOperationExpire(d); err != nil {
			return nil, nil, err
		}

		return prev, params.ValidProposalOperationExpire(), nil
	})
}

func writeLocalParamMISCValidProposalSuffrageOperationsExpire(
	params *MISCParams,
) writeNodeValueFunc {
	return writeNodeKey(func(_, _, value string) (prev, next interface{}, _ error) {
		d, err := parseNodeValueDuration(value)
		if err != nil {
			return nil, nil, err
		}

		prev = params.ValidProposalSuffrageOperationsExpire()

		if err := params.SetValidProposalSuffrageOperationsExpire(d); err != nil {
			return nil, nil, err
		}

		return prev, params.ValidProposalSuffrageOperationsExpire(), nil
	})
}

func writeLocalParamMISCMaxMessageSize(
	params *MISCParams,
) writeNodeValueFunc {
	return writeNodeKey(func(_, _, value string) (prev, next interface{}, _ error) {
		i, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			return nil, nil, errors.WithStack(err)
		}

		prev = params.MaxMessageSize()

		if err := params.SetMaxMessageSize(i); err != nil {
			return nil, nil, err
		}

		return prev, params.MaxMessageSize(), nil
	})
}

func writeLocalParamMemberlist(
	params *MemberlistParams,
) writeNodeValueFunc {
	fExtraSameMemberLimit := writeLocalParamExtraSameMemberLimit(params)

	return writeNodeKey(func(key, nextkey, value string) (prev, next interface{}, _ error) {
		switch key {
		case "extra_same_member_limit":
			return fExtraSameMemberLimit(nextkey, value)
		default:
			return prev, next, util.ErrNotFound.Errorf("unknown key, %q for memberlist", key)
		}
	})
}

func writeLocalParamExtraSameMemberLimit(
	params *MemberlistParams,
) writeNodeValueFunc {
	return writeNodeKey(func(_, _, value string) (prev, next interface{}, _ error) {
		i, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			return nil, nil, errors.WithStack(err)
		}

		prev = params.ExtraSameMemberLimit()

		if err := params.SetExtraSameMemberLimit(i); err != nil {
			return nil, nil, err
		}

		return prev, params.ExtraSameMemberLimit(), nil
	})
}

func writeLocalParamNetwork(
	params *NetworkParams,
) writeNodeValueFunc {
	fTimeoutRequest := writeLocalParamNetworkTimeoutRequest(params)
	fRateLimit := writeLocalParamNetworkRateLimit(params.RateLimit())

	return writeNodeKey(func(key, nextkey, value string) (prev, next interface{}, _ error) {
		switch key {
		case "timeout_request":
			return fTimeoutRequest(nextkey, value)
		case "ratelimit":
			return fRateLimit(nextkey, value)
		default:
			return prev, next, util.ErrNotFound.Errorf("unknown key, %q for network", key)
		}
	})
}

func writeLocalParamNetworkTimeoutRequest(
	params *NetworkParams,
) writeNodeValueFunc {
	return writeNodeKey(func(_, _, value string) (prev, next interface{}, _ error) {
		d, err := parseNodeValueDuration(value)
		if err != nil {
			return nil, nil, err
		}

		prev = params.TimeoutRequest()

		if err := params.SetTimeoutRequest(d); err != nil {
			return nil, nil, err
		}

		return prev, params.TimeoutRequest(), nil
	})
}

func writeSyncSources(
	enc *jsonenc.Encoder,
	design NodeDesign,
	syncSourceChecker *isaacnetwork.SyncSourceChecker,
) writeNodeValueFunc {
	return func(_, value string) (prev, next interface{}, _ error) {
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

		prev = syncSourceChecker.Sources()

		if err := syncSourceChecker.UpdateSources(context.Background(), sources.Sources()); err != nil {
			return nil, nil, err
		}

		return prev, sources, nil
	}
}

func writeDiscoveries(
	discoveries *util.Locked[[]quicstream.ConnInfo],
) writeNodeValueFunc {
	return func(_, value string) (prev, next interface{}, _ error) {
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

		prev = GetDiscoveriesFromLocked(discoveries)

		_ = discoveries.SetValue(cis)

		return prev, cis, nil
	}
}

func writeLocalParamNetworkRateLimit(
	params *NetworkRateLimitParams,
) writeNodeValueFunc {
	return writeNodeKey(func(key, nextkey, value string) (prev, next interface{}, err error) {
		switch key {
		case "node":
			prev = params.NodeRuleSet()
		case "net":
			prev = params.NetRuleSet()
		case "suffrage":
			prev = params.SuffrageRuleSet()
		case "default":
			prev = params.DefaultRuleMap()
		default:
			return prev, next, util.ErrNotFound.Errorf("unknown key, %q for network ratelimit", key)
		}

		switch i, err := unmarshalRateLimitRule(key, value); {
		case err != nil:
			return prev, next, err
		default:
			next = i
		}

		return prev, next, func() error {
			switch key {
			case "node":
				return params.SetNodeRuleSet(next.(RateLimiterRuleSet)) //nolint:forcetypeassert //...
			case "net":
				return params.SetNetRuleSet(next.(RateLimiterRuleSet)) //nolint:forcetypeassert //...
			case "suffrage":
				return params.SetSuffrageRuleSet(next.(RateLimiterRuleSet)) //nolint:forcetypeassert //...
			case "default":
				return params.SetDefaultRuleMap(next.(RateLimiterRuleMap)) //nolint:forcetypeassert //...
			default:
				return util.ErrNotFound.Errorf("unknown key, %q for network", key)
			}
		}()
	})
}

func writeAllowConsensus(pctx context.Context) (writeNodeValueFunc, error) {
	var states *isaacstates.States

	if err := util.LoadFromContext(pctx,
		StatesContextKey, &states,
	); err != nil {
		return nil, err
	}

	return func(key, value string) (prev, next interface{}, _ error) {
		var allow bool
		if err := yaml.Unmarshal([]byte(value), &allow); err != nil {
			return nil, nil, errors.WithStack(err)
		}

		prev = states.AllowedConsensus()

		if states.SetAllowConsensus(allow) {
			next = allow
		}

		return prev, next, nil
	}, nil
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

func handlerNodeWrite(
	pub base.Publickey,
	networkID base.NetworkID,
	f writeNodeValueFunc,
) quicstreamheader.Handler[WriteNodeHeader] {
	handler := func(ctx context.Context, addr net.Addr,
		broker *quicstreamheader.HandlerBroker, header WriteNodeHeader,
	) (sentresponse bool, _ error) {
		if err := isaacnetwork.QuicstreamHandlerVerifyNode(
			ctx, addr, broker,
			pub, networkID,
		); err != nil {
			return false, err
		}

		var body io.Reader

		switch bodyType, _, b, _, res, err := broker.ReadBody(ctx); {
		case err != nil:
			return false, err
		case res != nil:
			return false, res.Err()
		case bodyType == quicstreamheader.FixedLengthBodyType,
			bodyType == quicstreamheader.StreamBodyType:
			body = b
		}

		var value string

		if body != nil {
			b, err := io.ReadAll(body)
			if err != nil {
				return false, errors.WithStack(err)
			}

			value = string(b)
		}

		switch _, _, err := f(header.Key, value); {
		case errors.Is(err, util.ErrNotFound):
			return true, broker.WriteResponseHeadOK(ctx, false, nil)
		case err != nil:
			return false, err
		default:
			return true, broker.WriteResponseHeadOK(ctx, true, nil)
		}
	}

	return func(ctx context.Context, addr net.Addr,
		broker *quicstreamheader.HandlerBroker, header WriteNodeHeader,
	) (context.Context, error) {
		e := util.StringError("write node")

		switch sentresponse, err := handler(ctx, addr, broker, header); {
		case err != nil:
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
	header := NewWriteNodeHeader(key)
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
	header := NewReadNodeHeader(key)
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

			if err := broker.Encoder.Unmarshal(b, &t); err != nil {
				return err
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
	return func(key string) (interface{}, error) {
		i := strings.SplitN(strings.TrimPrefix(key, "."), ".", 2)

		var nextkey string
		if len(i) > 1 {
			nextkey = i[1]
		}

		return f(i[0], nextkey)
	}
}

func readNode(
	pctx context.Context,
) (readNodeValueFunc, error) {
	var discoveries *util.Locked[[]quicstream.ConnInfo]

	if err := util.LoadFromContextOK(pctx,
		DiscoveryContextKey, &discoveries,
	); err != nil {
		return nil, err
	}

	fStates, err := readStates(pctx)
	if err != nil {
		return nil, err
	}

	fDesign, err := readDesign(pctx)
	if err != nil {
		return nil, err
	}

	return readNodeKey(func(key, nextkey string) (interface{}, error) {
		switch key {
		case "states":
			return fStates(nextkey)
		case "design":
			return fDesign(nextkey)
		case "discovery":
			return GetDiscoveriesFromLocked(discoveries), nil
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

	return readNodeKey(func(key, nextkey string) (interface{}, error) {
		switch key {
		case "allow_consensus":
			return fAllowConsensus(nextkey)
		default:
			return nil, util.ErrNotFound.Errorf("unknown key, %q for node", key)
		}
	}), nil
}

func readDesign(pctx context.Context) (readNodeValueFunc, error) {
	var params *LocalParams
	var syncSourceChecker *isaacnetwork.SyncSourceChecker

	if err := util.LoadFromContextOK(pctx,
		LocalParamsContextKey, &params,
		SyncSourceCheckerContextKey, &syncSourceChecker,
	); err != nil {
		return nil, err
	}

	fLocalparams := readLocalParams(params)

	return readNodeKey(func(key, nextkey string) (interface{}, error) {
		switch key {
		case "parameters":
			return fLocalparams(nextkey)
		case "sync_sources":
			return syncSourceChecker.Sources(), nil
		default:
			return nil, util.ErrNotFound.Errorf("unknown key, %q for params", key)
		}
	}), nil
}

func readLocalParams(params *LocalParams) readNodeValueFunc {
	fisaac := readLocalParamISAAC(params.ISAAC)
	fmisc := readLocalParamMISC(params.MISC)
	fmemberlist := readLocalParamMemberlist(params.Memberlist)
	fnetwork := readLocalParamNetwork(params.Network)

	return readNodeKey(func(key, nextkey string) (interface{}, error) {
		switch key {
		case "isaac":
			return fisaac(nextkey)
		case "misc":
			return fmisc(nextkey)
		case "memberlist":
			return fmemberlist(nextkey)
		case "network":
			return fnetwork(nextkey)
		default:
			return nil, util.ErrNotFound.Errorf("unknown key, %q for params", key)
		}
	})
}

func readLocalParamISAAC(params *isaac.Params) readNodeValueFunc {
	return readNodeKey(func(key, _ string) (interface{}, error) {
		switch key {
		case "threshold":
			return params.Threshold(), nil
		case "interval_broadcast_ballot":
			return util.ReadableDuration(params.IntervalBroadcastBallot()), nil
		case "wait_preparing_init_ballot":
			return util.ReadableDuration(params.WaitPreparingINITBallot()), nil
		case "max_try_handover_y_broker_sync_data":
			return params.MaxTryHandoverYBrokerSyncData(), nil
		default:
			return nil, util.ErrNotFound.Errorf("unknown key, %q for isaac", key)
		}
	})
}

func readLocalParamMISC(params *MISCParams) readNodeValueFunc {
	return readNodeKey(func(key, nextkey string) (interface{}, error) {
		switch key {
		case "sync_source_checker_interval":
			return util.ReadableDuration(params.SyncSourceCheckerInterval()), nil
		case "valid_proposal_operation_expire":
			return util.ReadableDuration(params.ValidProposalOperationExpire()), nil
		case "valid_proposal_suffrage_operations_expire":
			return util.ReadableDuration(params.ValidProposalSuffrageOperationsExpire()), nil
		case "max_message_size":
			return params.MaxMessageSize(), nil
		default:
			return nil, util.ErrNotFound.Errorf("unknown key, %q for misc", key)
		}
	})
}

func readLocalParamMemberlist(params *MemberlistParams) readNodeValueFunc {
	return readNodeKey(func(key, nextkey string) (interface{}, error) {
		switch key {
		case "extra_same_member_limit":
			return params.ExtraSameMemberLimit(), nil
		default:
			return nil, util.ErrNotFound.Errorf("unknown key, %q for memberlist", key)
		}
	})
}

func readLocalParamNetwork(params *NetworkParams) readNodeValueFunc {
	fratelimit := readLocalParamNetworkRateLimit(params.RateLimit())

	return readNodeKey(func(key, nextkey string) (interface{}, error) {
		switch key {
		case "timeout_request":
			return util.ReadableDuration(params.TimeoutRequest()), nil
		case "ratelimit":
			return fratelimit(nextkey)
		default:
			return nil, util.ErrNotFound.Errorf("unknown key, %q for network", key)
		}
	})
}

func readLocalParamNetworkRateLimit(params *NetworkRateLimitParams) readNodeValueFunc {
	return readNodeKey(func(key, nextkey string) (v interface{}, _ error) {
		switch key {
		case "node":
			v = params.NodeRuleSet()
		case "net":
			v = params.NetRuleSet()
		case "suffrage":
			v = params.SuffrageRuleSet()
		case "default":
			v = params.DefaultRuleMap()
		default:
			return nil, util.ErrNotFound.Errorf("unknown key, %q for network ratelimit", key)
		}

		return v, nil
	})
}

func readAllowConsensus(pctx context.Context) (readNodeValueFunc, error) {
	var states *isaacstates.States

	if err := util.LoadFromContext(pctx,
		StatesContextKey, &states,
	); err != nil {
		return nil, err
	}

	return func(string) (interface{}, error) {
		return states.AllowedConsensus(), nil
	}, nil
}

func handlerNodeRead(
	pub base.Publickey,
	networkID base.NetworkID,
	f readNodeValueFunc,
) quicstreamheader.Handler[ReadNodeHeader] {
	var sg singleflight.Group

	handler := func(ctx context.Context, addr net.Addr,
		broker *quicstreamheader.HandlerBroker, header ReadNodeHeader,
	) (sentresponse bool, _ error) {
		i, err, _ := util.SingleflightDo[[]byte](&sg, header.Key, func() ([]byte, error) {
			if err := isaacnetwork.QuicstreamHandlerVerifyNode(
				ctx, addr, broker,
				pub, networkID,
			); err != nil {
				return nil, err
			}

			switch v, err := f(header.Key); {
			case err != nil:
				return nil, err
			default:
				return broker.Encoder.Marshal(v)
			}
		})
		if err != nil {
			return false, err
		}

		body := bytes.NewBuffer(i) //nolint:forcetypeassert //...
		defer body.Reset()

		if err := broker.WriteResponseHeadOK(ctx, true, nil); err != nil {
			return true, err
		}

		return true, broker.WriteBody(ctx, quicstreamheader.StreamBodyType, 0, body)
	}

	return func(ctx context.Context, addr net.Addr,
		broker *quicstreamheader.HandlerBroker, header ReadNodeHeader,
	) (context.Context, error) {
		e := util.StringError("read node")

		switch sentresponse, err := handler(ctx, addr, broker, header); {
		case errors.Is(err, util.ErrNotFound):
			return ctx, e.WithMessage(broker.WriteResponseHeadOK(ctx, false, nil), "write response header")
		case err != nil:
			if !sentresponse {
				return ctx, e.WithMessage(broker.WriteResponseHeadOK(ctx, false, err), "write response header")
			}

			return ctx, e.Wrap(err)
		default:
			return ctx, nil
		}
	}
}

type rwNodeHeader struct {
	Key string
	isaacnetwork.BaseHeader
}

func (h rwNodeHeader) IsValid(b []byte) error {
	e := util.ErrInvalid.Errorf("invalid rwNodeHeader")

	if err := h.BaseHinter.IsValid(b); err != nil {
		return e.Wrap(err)
	}

	if len(h.Key) < 1 {
		return e.Errorf("empty key")
	}

	return nil
}

type rwNodeHeaderJSONMarshaler struct {
	Key string `json:"key"`
}

func (h rwNodeHeader) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(struct {
		rwNodeHeaderJSONMarshaler
		isaacnetwork.BaseHeaderJSONMarshaler
	}{
		BaseHeaderJSONMarshaler: h.BaseHeader.JSONMarshaler(),
		rwNodeHeaderJSONMarshaler: rwNodeHeaderJSONMarshaler{
			Key: h.Key,
		},
	})
}

func (h *rwNodeHeader) UnmarshalJSON(b []byte) error {
	e := util.StringError("unmarshal rwNodeHeader")

	if err := util.UnmarshalJSON(b, &h.BaseHeader); err != nil {
		return e.Wrap(err)
	}

	var u rwNodeHeaderJSONMarshaler
	if err := util.UnmarshalJSON(b, &u); err != nil {
		return e.Wrap(err)
	}

	h.Key = u.Key

	return nil
}

type WriteNodeHeader struct {
	rwNodeHeader
}

func NewWriteNodeHeader(key string) WriteNodeHeader {
	h := WriteNodeHeader{}

	h.BaseHeader = isaacnetwork.BaseHeader{
		BaseRequestHeader: quicstreamheader.NewBaseRequestHeader(WriteNodeHeaderHint, HandlerPrefixNodeWrite),
	}
	h.Key = key

	return h
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

func NewReadNodeHeader(key string) ReadNodeHeader {
	h := ReadNodeHeader{}

	h.BaseHeader = isaacnetwork.BaseHeader{
		BaseRequestHeader: quicstreamheader.NewBaseRequestHeader(ReadNodeHeaderHint, HandlerPrefixNodeRead),
	}
	h.Key = key

	return h
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
