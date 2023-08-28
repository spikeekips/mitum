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

var PNameNetworkHandlersReadWriteDesign = ps.Name("design-network-handler")

var (
	ReadDesignHeaderHint           = hint.MustNewHint("read-design-header-v0.0.1")
	WriteDesignHeaderHint          = hint.MustNewHint("write-design-header-v0.0.1")
	HandlerPrefixDesignReadString  = "design_read"
	HandlerPrefixDesignWriteString = "design_write"
	HandlerPrefixDesignRead        = quicstream.HashPrefix(HandlerPrefixDesignReadString)
	HandlerPrefixDesignWrite       = quicstream.HashPrefix(HandlerPrefixDesignWriteString)
)

type (
	writeDesignValueFunc     func(key, value string) (prev, next interface{}, _ error)
	writeDesignNextValueFunc func(key, nextkey, value string) (prev, next interface{}, _ error)
	readDesignValueFunc      func(key string) (interface{}, error)
	readDesignNextValueFunc  func(key, nextkey string) (interface{}, error)
)

func PNetworkHandlersReadWriteDesign(pctx context.Context) (context.Context, error) {
	var design NodeDesign
	var params *LocalParams
	var local base.LocalNode
	var enc *jsonenc.Encoder
	var discoveries *util.Locked[[]quicstream.ConnInfo]
	var syncSourceChecker *isaacnetwork.SyncSourceChecker

	if err := util.LoadFromContextOK(pctx,
		DesignContextKey, &design,
		LocalParamsContextKey, &params,
		LocalContextKey, &local,
		EncoderContextKey, &enc,
		DiscoveryContextKey, &discoveries,
		SyncSourceCheckerContextKey, &syncSourceChecker,
	); err != nil {
		return nil, err
	}

	rf := readDesign(params, discoveries, syncSourceChecker)
	wf := writeDesign(enc, design, params, discoveries, syncSourceChecker)

	var gerror error

	ensureHandlerAdd(pctx, &gerror,
		HandlerPrefixDesignReadString,
		handlerDesignRead(local.Publickey(), params.ISAAC.NetworkID(), rf), nil)

	ensureHandlerAdd(pctx, &gerror,
		HandlerPrefixDesignWriteString,
		handlerDesignWrite(local.Publickey(), params.ISAAC.NetworkID(), wf), nil)

	return pctx, gerror
}

func writeDesignKey(f writeDesignNextValueFunc) writeDesignValueFunc {
	return func(key, value string) (interface{}, interface{}, error) {
		i := strings.SplitN(strings.TrimPrefix(key, "/"), "/", 2)

		var nextkey string
		if len(i) > 1 {
			nextkey = i[1]
		}

		return f(i[0], nextkey, value)
	}
}

func writeDesign(
	enc *jsonenc.Encoder,
	design NodeDesign,
	params *LocalParams,
	discoveries *util.Locked[[]quicstream.ConnInfo],
	syncSourceChecker *isaacnetwork.SyncSourceChecker,
) writeDesignValueFunc {
	fLocalparams := writeLocalParam(params)
	fDiscoveries := writeDiscoveries(discoveries)
	fSyncSources := writeSyncSources(enc, design, syncSourceChecker)

	return writeDesignKey(func(key, nextkey, value string) (interface{}, interface{}, error) {
		switch key {
		case "parameters":
			return fLocalparams(nextkey, value)
		case "discoveries":
			return fDiscoveries(nextkey, value)
		case "sync_sources":
			return fSyncSources(nextkey, value)
		default:
			return nil, nil, util.ErrNotFound.Errorf("unknown key, %q for params", key)
		}
	})
}

func writeLocalParam(
	params *LocalParams,
) writeDesignValueFunc {
	fISAAC := writeLocalParamISAAC(params.ISAAC)
	fMisc := writeLocalParamMISC(params.MISC)
	fMemberlist := writeLocalParamMemberlist(params.Memberlist)
	fnetwork := writeLocalParamNetwork(params.Network)

	return writeDesignKey(func(key, nextkey, value string) (interface{}, interface{}, error) {
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
) writeDesignValueFunc {
	fThreshold := writeLocalParamISAACThreshold(params)
	fIntervalBroadcastBallot := writeLocalParamISAACIntervalBroadcastBallot(params)
	fWaitPreparingINITBallot := writeLocalParamISAACWaitPreparingINITBallot(params)
	fMaxTryHandoverYBrokerSyncData := writeLocalParamISAACMaxTryHandoverYBrokerSyncData(params)

	return writeDesignKey(func(key, nextkey, value string) (prev, next interface{}, _ error) {
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
) writeDesignValueFunc {
	return writeDesignKey(func(_, _, value string) (prev, next interface{}, _ error) {
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
) writeDesignValueFunc {
	return writeDesignKey(func(_, _, value string) (prev, next interface{}, _ error) {
		d, err := parseDesignValueDuration(value)
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
) writeDesignValueFunc {
	return writeDesignKey(func(_, _, value string) (prev, next interface{}, _ error) {
		d, err := parseDesignValueDuration(value)
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
) writeDesignValueFunc {
	return writeDesignKey(func(_, _, value string) (prev, next interface{}, _ error) {
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
) writeDesignValueFunc {
	fSyncSourceCheckerInterval := writeLocalParamMISCSyncSourceCheckerInterval(params)
	fValidProposalOperationExpire := writeLocalParamMISCValidProposalOperationExpire(params)
	fValidProposalSuffrageOperationsExpire := writeLocalParamMISCValidProposalSuffrageOperationsExpire(params)
	fMaxMessageSize := writeLocalParamMISCMaxMessageSize(params)

	return writeDesignKey(func(key, nextkey, value string) (prev, next interface{}, _ error) {
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
) writeDesignValueFunc {
	return writeDesignKey(func(_, _, value string) (prev, next interface{}, _ error) {
		d, err := parseDesignValueDuration(value)
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
) writeDesignValueFunc {
	return writeDesignKey(func(_, _, value string) (prev, next interface{}, _ error) {
		d, err := parseDesignValueDuration(value)
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
) writeDesignValueFunc {
	return writeDesignKey(func(_, _, value string) (prev, next interface{}, _ error) {
		d, err := parseDesignValueDuration(value)
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
) writeDesignValueFunc {
	return writeDesignKey(func(_, _, value string) (prev, next interface{}, _ error) {
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
) writeDesignValueFunc {
	fExtraSameMemberLimit := writeLocalParamExtraSameMemberLimit(params)

	return writeDesignKey(func(key, nextkey, value string) (prev, next interface{}, _ error) {
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
) writeDesignValueFunc {
	return writeDesignKey(func(_, _, value string) (prev, next interface{}, _ error) {
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
) writeDesignValueFunc {
	fTimeoutRequest := writeLocalParamNetworkTimeoutRequest(params)
	fRateLimit := writeLocalParamNetworkRateLimit(params.RateLimit())

	return writeDesignKey(func(key, nextkey, value string) (prev, next interface{}, _ error) {
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
) writeDesignValueFunc {
	return writeDesignKey(func(_, _, value string) (prev, next interface{}, _ error) {
		d, err := parseDesignValueDuration(value)
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
) writeDesignValueFunc {
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
) writeDesignValueFunc {
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
) writeDesignValueFunc {
	return writeDesignKey(func(key, nextkey, value string) (prev, next interface{}, err error) {
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

func handlerDesignWrite(
	pub base.Publickey,
	networkID base.NetworkID,
	f writeDesignValueFunc,
) quicstreamheader.Handler[WriteDesignHeader] {
	handler := func(ctx context.Context, addr net.Addr,
		broker *quicstreamheader.HandlerBroker, header WriteDesignHeader,
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
		broker *quicstreamheader.HandlerBroker, header WriteDesignHeader,
	) (context.Context, error) {
		e := util.StringError("write design")

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

func WriteDesignFromNetworkHandler(
	ctx context.Context,
	priv base.Privatekey,
	networkID base.NetworkID,
	key string,
	value string,
	stream quicstreamheader.StreamFunc,
) (found bool, _ error) {
	header := NewWriteDesignHeader(key)
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

func ReadDesignFromNetworkHandler(
	ctx context.Context,
	priv base.Privatekey,
	networkID base.NetworkID,
	key string,
	stream quicstreamheader.StreamFunc,
) (t interface{}, found bool, _ error) {
	header := NewReadDesignHeader(key)
	if err := header.IsValid(nil); err != nil {
		return t, false, err
	}

	err := stream(ctx, func(ctx context.Context, broker *quicstreamheader.ClientBroker) error {
		switch b, i, err := readDesignFromNetworkHandler(ctx, priv, networkID, broker, header); {
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

func readDesignFromNetworkHandler(
	ctx context.Context,
	priv base.Privatekey,
	networkID base.NetworkID,
	broker *quicstreamheader.ClientBroker,
	header ReadDesignHeader,
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

func readDesignKey(f readDesignNextValueFunc) readDesignValueFunc {
	return func(key string) (interface{}, error) {
		i := strings.SplitN(strings.TrimPrefix(key, "/"), "/", 2)

		var nextkey string
		if len(i) > 1 {
			nextkey = i[1]
		}

		return f(i[0], nextkey)
	}
}

func readDesign(
	params *LocalParams,
	discoveries *util.Locked[[]quicstream.ConnInfo],
	syncSourceChecker *isaacnetwork.SyncSourceChecker,
) readDesignValueFunc {
	fLocalparams := readLocalParams(params)

	return readDesignKey(func(key, nextkey string) (interface{}, error) {
		switch key {
		case "parameters":
			return fLocalparams(nextkey)
		case "discoveries":
			return GetDiscoveriesFromLocked(discoveries), nil
		case "sync_sources":
			return syncSourceChecker.Sources(), nil
		default:
			return nil, util.ErrNotFound.Errorf("unknown key, %q for params", key)
		}
	})
}

func readLocalParams(params *LocalParams) readDesignValueFunc {
	fisaac := readLocalParamISAAC(params.ISAAC)
	fmisc := readLocalParamMISC(params.MISC)
	fmemberlist := readLocalParamMemberlist(params.Memberlist)
	fnetwork := readLocalParamNetwork(params.Network)

	return readDesignKey(func(key, nextkey string) (interface{}, error) {
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

func readLocalParamISAAC(params *isaac.Params) readDesignValueFunc {
	return readDesignKey(func(key, _ string) (interface{}, error) {
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

func readLocalParamMISC(params *MISCParams) readDesignValueFunc {
	return readDesignKey(func(key, nextkey string) (interface{}, error) {
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

func readLocalParamMemberlist(params *MemberlistParams) readDesignValueFunc {
	return readDesignKey(func(key, nextkey string) (interface{}, error) {
		switch key {
		case "extra_same_member_limit":
			return params.ExtraSameMemberLimit(), nil
		default:
			return nil, util.ErrNotFound.Errorf("unknown key, %q for memberlist", key)
		}
	})
}

func readLocalParamNetwork(params *NetworkParams) readDesignValueFunc {
	fratelimit := readLocalParamNetworkRateLimit(params.RateLimit())

	return readDesignKey(func(key, nextkey string) (interface{}, error) {
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

func readLocalParamNetworkRateLimit(params *NetworkRateLimitParams) readDesignValueFunc {
	return readDesignKey(func(key, nextkey string) (v interface{}, _ error) {
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

func handlerDesignRead(
	pub base.Publickey,
	networkID base.NetworkID,
	f readDesignValueFunc,
) quicstreamheader.Handler[ReadDesignHeader] {
	var sg singleflight.Group

	handler := func(ctx context.Context, addr net.Addr,
		broker *quicstreamheader.HandlerBroker, header ReadDesignHeader,
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
		broker *quicstreamheader.HandlerBroker, header ReadDesignHeader,
	) (context.Context, error) {
		e := util.StringError("read design")

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

type rwDesignHeader struct {
	Key string
	isaacnetwork.BaseHeader
}

func (h rwDesignHeader) IsValid(b []byte) error {
	e := util.ErrInvalid.Errorf("invalid rwDesignHeader")

	if err := h.BaseHinter.IsValid(b); err != nil {
		return e.Wrap(err)
	}

	if len(h.Key) < 1 {
		return e.Errorf("empty key")
	}

	return nil
}

type rwDesignHeaderJSONMarshaler struct {
	Key string `json:"key"`
}

func (h rwDesignHeader) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(struct {
		rwDesignHeaderJSONMarshaler
		isaacnetwork.BaseHeaderJSONMarshaler
	}{
		BaseHeaderJSONMarshaler: h.BaseHeader.JSONMarshaler(),
		rwDesignHeaderJSONMarshaler: rwDesignHeaderJSONMarshaler{
			Key: h.Key,
		},
	})
}

func (h *rwDesignHeader) UnmarshalJSON(b []byte) error {
	e := util.StringError("unmarshal rwDesignHeader")

	if err := util.UnmarshalJSON(b, &h.BaseHeader); err != nil {
		return e.Wrap(err)
	}

	var u rwDesignHeaderJSONMarshaler
	if err := util.UnmarshalJSON(b, &u); err != nil {
		return e.Wrap(err)
	}

	h.Key = u.Key

	return nil
}

type WriteDesignHeader struct {
	rwDesignHeader
}

func NewWriteDesignHeader(key string) WriteDesignHeader {
	h := WriteDesignHeader{}

	h.BaseHeader = isaacnetwork.BaseHeader{
		BaseRequestHeader: quicstreamheader.NewBaseRequestHeader(WriteDesignHeaderHint, HandlerPrefixDesignWrite),
	}
	h.Key = key

	return h
}

func (h WriteDesignHeader) IsValid([]byte) error {
	e := util.ErrInvalid.Errorf("invalid WriteDesignHeader")

	if err := h.rwDesignHeader.IsValid(WriteDesignHeaderHint.Type().Bytes()); err != nil {
		return e.Wrap(err)
	}

	return nil
}

type ReadDesignHeader struct {
	rwDesignHeader
}

func NewReadDesignHeader(key string) ReadDesignHeader {
	h := ReadDesignHeader{}

	h.BaseHeader = isaacnetwork.BaseHeader{
		BaseRequestHeader: quicstreamheader.NewBaseRequestHeader(ReadDesignHeaderHint, HandlerPrefixDesignRead),
	}
	h.Key = key

	return h
}

func (h ReadDesignHeader) IsValid([]byte) error {
	e := util.ErrInvalid.Errorf("invalid ReadDesignHeader")

	if err := h.rwDesignHeader.IsValid(ReadDesignHeaderHint.Type().Bytes()); err != nil {
		return e.Wrap(err)
	}

	return nil
}

func parseDesignValueDuration(value string) (time.Duration, error) {
	var s string
	if err := yaml.Unmarshal([]byte(value), &s); err != nil {
		return 0, errors.WithStack(err)
	}

	return util.ParseDuration(s)
}
