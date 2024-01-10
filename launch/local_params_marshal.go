package launch

import (
	"reflect"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/network/quicstream"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/hint"
	"gopkg.in/yaml.v3"
)

func (p *LocalParams) MarshalYAML() (interface{}, error) {
	m := map[string]interface{}{}

	if p.ISAAC != nil {
		switch b, err := util.MarshalJSON(p.ISAAC); {
		case err != nil:
			return nil, err
		default:
			var i map[string]interface{}

			if err := util.UnmarshalJSON(b, &i); err != nil {
				return nil, err
			}

			delete(i, "_hint")

			m["isaac"] = i
		}
	}

	if p.Memberlist != nil {
		m["memberlist"] = p.Memberlist
	}

	if p.MISC != nil {
		m["misc"] = p.MISC
	}

	if p.Network != nil {
		m["network"] = p.Network
	}

	return m, nil
}

type LocalParamsYAMLUnmarshaler struct {
	ISAAC      map[string]interface{} `yaml:"isaac"`
	Memberlist *MemberlistParams      `yaml:"memberlist,omitempty"`
	MISC       *MISCParams            `yaml:"misc,omitempty"`
	Network    *NetworkParams         `yaml:"network,omitempty"`
}

func (p *LocalParams) DecodeYAML(b []byte, jsonencoder encoder.Encoder) error {
	if len(b) < 1 {
		return nil
	}

	e := util.StringError("decode LocalParams")

	u := LocalParamsYAMLUnmarshaler{Memberlist: p.Memberlist}

	if err := yaml.Unmarshal(b, &u); err != nil {
		return e.Wrap(err)
	}

	switch lb, err := jsonencoder.Marshal(u.ISAAC); {
	case err != nil:
		return e.Wrap(err)
	default:
		if err := jsonencoder.Unmarshal(lb, p.ISAAC); err != nil {
			return e.Wrap(err)
		}

		p.ISAAC.BaseHinter = hint.NewBaseHinter(isaac.ParamsHint)
	}

	if u.Memberlist != nil {
		p.Memberlist = u.Memberlist
	}

	if u.MISC != nil {
		p.MISC = u.MISC
	}

	if u.Network != nil {
		p.Network = u.Network
	}

	return nil
}

type memberlistParamsMarshaler struct {
	//revive:disable:line-length-limit
	TCPTimeout              util.ReadableDuration `json:"tcp_timeout,omitempty" yaml:"tcp_timeout,omitempty"`
	RetransmitMult          int                   `json:"retransmit_mult,omitempty" yaml:"retransmit_mult,omitempty"`
	ProbeTimeout            util.ReadableDuration `json:"probe_timeout,omitempty" yaml:"probe_timeout,omitempty"`
	ProbeInterval           util.ReadableDuration `json:"probe_interval,omitempty" yaml:"probe_interval,omitempty"`
	SuspicionMult           int                   `json:"suspicion_mult,omitempty" yaml:"suspicion_mult,omitempty"`
	SuspicionMaxTimeoutMult int                   `json:"suspicion_max_timeout_mult,omitempty" yaml:"suspicion_max_timeout_mult,omitempty"`
	UDPBufferSize           int                   `json:"udp_buffer_size,omitempty" yaml:"udp_buffer_size,omitempty"`
	ExtraSameMemberLimit    uint64                `json:"extra_same_member_limit,omitempty" yaml:"extra_same_member_limit,omitempty"`
	//revive:enable:line-length-limit
}

func (p *MemberlistParams) marshaler() memberlistParamsMarshaler {
	return memberlistParamsMarshaler{
		TCPTimeout:              util.ReadableDuration(p.tcpTimeout),
		RetransmitMult:          p.retransmitMult,
		ProbeTimeout:            util.ReadableDuration(p.probeTimeout),
		ProbeInterval:           util.ReadableDuration(p.probeInterval),
		SuspicionMult:           p.suspicionMult,
		SuspicionMaxTimeoutMult: p.suspicionMaxTimeoutMult,
		UDPBufferSize:           p.udpBufferSize,
		ExtraSameMemberLimit:    p.extraSameMemberLimit,
	}
}

func (p *MemberlistParams) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(p.marshaler())
}

func (p *MemberlistParams) MarshalYAML() (interface{}, error) {
	return p.marshaler(), nil
}

type memberlistParamsUnmarshaler struct {
	//revive:disable:line-length-limit
	TCPTimeout              *util.ReadableDuration `json:"tcp_timeout,omitempty" yaml:"tcp_timeout,omitempty"`
	RetransmitMult          *int                   `json:"retransmit_mult,omitempty" yaml:"retransmit_mult,omitempty"`
	ProbeTimeout            *util.ReadableDuration `json:"probe_timeout,omitempty" yaml:"probe_timeout,omitempty"`
	ProbeInterval           *util.ReadableDuration `json:"probe_interval,omitempty" yaml:"probe_interval,omitempty"`
	SuspicionMult           *int                   `json:"suspicion_mult,omitempty" yaml:"suspicion_mult,omitempty"`
	SuspicionMaxTimeoutMult *int                   `json:"suspicion_max_timeout_mult,omitempty" yaml:"suspicion_max_timeout_mult,omitempty"`
	UDPBufferSize           *int                   `json:"udp_buffer_size,omitempty" yaml:"udp_buffer_size,omitempty"`
	ExtraSameMemberLimit    *uint64                `json:"extra_same_member_limit,omitempty" yaml:"extra_same_member_limit,omitempty"`
	//revive:enable:line-length-limit
}

func (p *MemberlistParams) UnmarshalJSON(b []byte) error {
	d := defaultMemberlistParams()
	*p = *d

	e := util.StringError("unmarshal MemberlistParams")

	var u memberlistParamsUnmarshaler

	if err := util.UnmarshalJSON(b, &u); err != nil {
		return e.Wrap(err)
	}

	return e.Wrap(p.unmarshal(u))
}

func (p *MemberlistParams) UnmarshalYAML(y *yaml.Node) error {
	d := defaultMemberlistParams()
	*p = *d

	e := util.StringError("unmarshal MemberlistParams")

	var u memberlistParamsUnmarshaler

	if err := y.Decode(&u); err != nil {
		return e.Wrap(err)
	}

	return e.Wrap(p.unmarshal(u))
}

func (p *MemberlistParams) unmarshal(u memberlistParamsUnmarshaler) error {
	if u.RetransmitMult != nil {
		p.retransmitMult = *u.RetransmitMult
	}

	if u.SuspicionMult != nil {
		p.suspicionMult = *u.SuspicionMult
	}

	if u.SuspicionMaxTimeoutMult != nil {
		p.suspicionMaxTimeoutMult = *u.SuspicionMaxTimeoutMult
	}

	if u.UDPBufferSize != nil {
		p.udpBufferSize = *u.UDPBufferSize
	}

	if u.ExtraSameMemberLimit != nil {
		p.extraSameMemberLimit = *u.ExtraSameMemberLimit
	}

	durargs := [][2]interface{}{
		{u.TCPTimeout, &p.tcpTimeout},
		{u.ProbeTimeout, &p.probeTimeout},
		{u.ProbeInterval, &p.probeInterval},
	}

	for i := range durargs {
		v := durargs[i][0].(*util.ReadableDuration) //nolint:forcetypeassert //...
		t := durargs[i][1].(*time.Duration)         //nolint:forcetypeassert //...

		if reflect.ValueOf(v).IsZero() {
			continue
		}

		if err := util.SetInterfaceValue[time.Duration](time.Duration(*v), t); err != nil {
			return err
		}
	}

	return nil
}

type miscParamsYAMLMarshaler struct {
	//revive:disable:line-length-limit
	SyncSourceCheckerInterval             util.ReadableDuration `json:"sync_source_checker_interval,omitempty" yaml:"sync_source_checker_interval,omitempty"`
	ValidProposalOperationExpire          util.ReadableDuration `json:"valid_proposal_operation_expire,omitempty" yaml:"valid_proposal_operation_expire,omitempty"`
	ValidProposalSuffrageOperationsExpire util.ReadableDuration `json:"valid_proposal_suffrage_operations_expire,omitempty" yaml:"valid_proposal_suffrage_operations_expire,omitempty"`
	BlockItemReadersRemoveEmptyAfter      util.ReadableDuration `json:"block_item_readers_remove_empty_after,omitempty" yaml:"block_item_readers_remove_empty_after,omitempty"`
	BlockItemReadersRemoveEmptyInterval   util.ReadableDuration `json:"block_item_readers_remove_empty_interval,omitempty" yaml:"block_item_readers_remove_empty_interval,omitempty"`
	MaxMessageSize                        uint64                `json:"max_message_size,omitempty" yaml:"max_message_size,omitempty"`
	ObjectCacheSize                       uint64                `json:"object_cache_size,omitempty" yaml:"object_cache_size,omitempty"`
	//revive:enable:line-length-limit
}

func (p *MISCParams) marshaler() miscParamsYAMLMarshaler {
	return miscParamsYAMLMarshaler{
		SyncSourceCheckerInterval:             util.ReadableDuration(p.syncSourceCheckerInterval),
		ValidProposalOperationExpire:          util.ReadableDuration(p.validProposalOperationExpire),
		ValidProposalSuffrageOperationsExpire: util.ReadableDuration(p.validProposalSuffrageOperationsExpire),
		BlockItemReadersRemoveEmptyAfter:      util.ReadableDuration(p.blockItemReadersRemoveEmptyAfter),
		BlockItemReadersRemoveEmptyInterval:   util.ReadableDuration(p.blockItemReadersRemoveEmptyInterval),
		MaxMessageSize:                        p.maxMessageSize,
		ObjectCacheSize:                       p.objectCacheSize,
	}
}

func (p *MISCParams) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(p.marshaler())
}

func (p *MISCParams) MarshalYAML() (interface{}, error) {
	return p.marshaler(), nil
}

type miscParamsYAMLUnmarshaler struct {
	//revive:disable:line-length-limit
	SyncSourceCheckerInterval             *util.ReadableDuration `json:"sync_source_checker_interval,omitempty" yaml:"sync_source_checker_interval,omitempty"`
	ValidProposalOperationExpire          *util.ReadableDuration `json:"valid_proposal_operation_expire,omitempty" yaml:"valid_proposal_operation_expire,omitempty"`
	ValidProposalSuffrageOperationsExpire *util.ReadableDuration `json:"valid_proposal_suffrage_operations_expire,omitempty" yaml:"valid_proposal_suffrage_operations_expire,omitempty"`
	BlockItemReadersRemoveEmptyAfter      *util.ReadableDuration `json:"block_item_readers_remove_empty_after,omitempty" yaml:"block_item_readers_remove_empty_after,omitempty"`
	BlockItemReadersRemoveEmptyInterval   *util.ReadableDuration `json:"block_item_readers_remove_empty_interval,omitempty" yaml:"block_item_readers_remove_empty_interval,omitempty"`
	MaxMessageSize                        *uint64                `json:"max_message_size,omitempty" yaml:"max_message_size,omitempty"`
	ObjectCacheSize                       *uint64                `json:"object_cache_size,omitempty" yaml:"object_cache_size,omitempty"`
	//revive:enable:line-length-limit
}

func (p *MISCParams) UnmarshalJSON(b []byte) error {
	d := defaultMISCParams()
	*p = *d

	e := util.StringError("decode MISCParams")

	var u miscParamsYAMLUnmarshaler

	if err := util.UnmarshalJSON(b, &u); err != nil {
		return e.Wrap(err)
	}

	return e.Wrap(p.unmarshal(u))
}

func (p *MISCParams) UnmarshalYAML(y *yaml.Node) error {
	d := defaultMISCParams()
	*p = *d

	e := util.StringError("decode MISCParams")

	var u miscParamsYAMLUnmarshaler

	if err := y.Decode(&u); err != nil {
		return e.Wrap(err)
	}

	return e.Wrap(p.unmarshal(u))
}

func (p *MISCParams) unmarshal(u miscParamsYAMLUnmarshaler) error {
	if u.MaxMessageSize != nil {
		p.maxMessageSize = *u.MaxMessageSize
	}

	if u.ObjectCacheSize != nil {
		p.objectCacheSize = *u.ObjectCacheSize
	}

	durargs := [][2]interface{}{
		{u.SyncSourceCheckerInterval, &p.syncSourceCheckerInterval},
		{u.ValidProposalOperationExpire, &p.validProposalOperationExpire},
		{u.ValidProposalSuffrageOperationsExpire, &p.validProposalSuffrageOperationsExpire},
		{u.BlockItemReadersRemoveEmptyAfter, &p.blockItemReadersRemoveEmptyAfter},
		{u.BlockItemReadersRemoveEmptyInterval, &p.blockItemReadersRemoveEmptyInterval},
	}

	for i := range durargs {
		v := durargs[i][0].(*util.ReadableDuration) //nolint:forcetypeassert //...
		t := durargs[i][1].(*time.Duration)         //nolint:forcetypeassert //...

		if reflect.ValueOf(v).IsZero() {
			continue
		}

		if err := util.SetInterfaceValue(time.Duration(*v), t); err != nil {
			return err
		}
	}

	return nil
}

type networkParamsYAMLMarshaler struct {
	//revive:disable:line-length-limit
	RateLimit             *NetworkRateLimitParams                          `json:"ratelimit,omitempty" yaml:"ratelimit,omitempty"` //nolint:tagliatelle //...
	HandlerTimeout        map[quicstream.HandlerName]util.ReadableDuration `json:"handler_timeout,omitempty" yaml:"handler_timeout,omitempty"`
	TimeoutRequest        util.ReadableDuration                            `json:"timeout_request,omitempty" yaml:"timeout_request,omitempty"`
	HandshakeIdleTimeout  util.ReadableDuration                            `json:"handshake_idle_timeout,omitempty" yaml:"handshake_idle_timeout,omitempty"`
	MaxIdleTimeout        util.ReadableDuration                            `json:"max_idle_timeout,omitempty" yaml:"max_idle_timeout,omitempty"`
	KeepAlivePeriod       util.ReadableDuration                            `json:"keep_alive_period,omitempty" yaml:"keep_alive_period,omitempty"`
	DefaultHandlerTimeout util.ReadableDuration                            `json:"default_handler_timeout,omitempty" yaml:"default_handler_timeout,omitempty"`
	ConnectionPoolSize    uint64                                           `json:"connection_pool_size,omitempty" yaml:"connection_pool_size,omitempty"`
	MaxIncomingStreams    uint64                                           `json:"max_incoming_streams,omitempty" yaml:"max_incoming_streams,omitempty"`
	MaxStreamTimeout      util.ReadableDuration                            `json:"max_stream_timeout,omitempty" yaml:"max_stream_timeout,omitempty"`
	//revive:enable:line-length-limit
}

func (p *NetworkParams) marshaler() networkParamsYAMLMarshaler {
	handlerTimeouts := map[quicstream.HandlerName]util.ReadableDuration{}

	for i := range p.handlerTimeouts {
		v := p.handlerTimeouts[i]

		// NOTE skip default value
		if d, found := defaultHandlerTimeouts[i]; found && v == d {
			continue
		}

		if v == p.defaultHandlerTimeout {
			continue
		}

		handlerTimeouts[i] = util.ReadableDuration(v)
	}

	return networkParamsYAMLMarshaler{
		TimeoutRequest:        util.ReadableDuration(p.timeoutRequest),
		HandshakeIdleTimeout:  util.ReadableDuration(p.handshakeIdleTimeout),
		MaxIdleTimeout:        util.ReadableDuration(p.maxIdleTimeout),
		KeepAlivePeriod:       util.ReadableDuration(p.keepAlivePeriod),
		DefaultHandlerTimeout: util.ReadableDuration(p.defaultHandlerTimeout),
		HandlerTimeout:        handlerTimeouts,
		ConnectionPoolSize:    p.connectionPoolSize,
		MaxIncomingStreams:    p.maxIncomingStreams,
		MaxStreamTimeout:      util.ReadableDuration(p.maxStreamTimeout),
		RateLimit:             p.rateLimit,
	}
}

func (p *NetworkParams) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(p.marshaler())
}

func (p *NetworkParams) MarshalYAML() (interface{}, error) {
	return p.marshaler(), nil
}

type networkParamsYAMLUnmarshaler struct {
	//revive:disable:line-length-limit
	TimeoutRequest        *util.ReadableDuration                           `json:"timeout_request,omitempty" yaml:"timeout_request,omitempty"`
	HandshakeIdleTimeout  *util.ReadableDuration                           `json:"handshake_idle_timeout,omitempty" yaml:"handshake_idle_timeout,omitempty"`
	MaxIdleTimeout        *util.ReadableDuration                           `json:"max_idle_timeout,omitempty" yaml:"max_idle_timeout,omitempty"`
	KeepAlivePeriod       *util.ReadableDuration                           `json:"keep_alive_period,omitempty" yaml:"keep_alive_period,omitempty"`
	DefaultHandlerTimeout *util.ReadableDuration                           `json:"default_handler_timeout,omitempty" yaml:"default_handler_timeout,omitempty"`
	HandlerTimeout        map[quicstream.HandlerName]util.ReadableDuration `json:"handler_timeout,omitempty" yaml:"handler_timeout,omitempty"`
	ConnectionPoolSize    *uint64                                          `json:"connection_pool_size,omitempty" yaml:"connection_pool_size,omitempty"`
	MaxIncomingStreams    *uint64                                          `json:"max_incoming_streams,omitempty" yaml:"max_incoming_streams,omitempty"`
	MaxStreamTimeout      *util.ReadableDuration                           `json:"max_stream_timeout,omitempty" yaml:"max_stream_timeout,omitempty"`
	RateLimit             *NetworkRateLimitParams                          `json:"ratelimit,omitempty" yaml:"ratelimit,omitempty"` //nolint:tagliatelle //...
	//revive:enable:line-length-limit
}

func (p *NetworkParams) UnmarshalJSON(b []byte) error {
	d := defaultNetworkParams()
	*p = *d

	e := util.StringError("decode NetworkParams")

	var u networkParamsYAMLUnmarshaler

	if err := util.UnmarshalJSON(b, &u); err != nil {
		return e.Wrap(err)
	}

	return e.Wrap(p.unmarshal(u))
}

func (p *NetworkParams) UnmarshalYAML(y *yaml.Node) error {
	d := defaultNetworkParams()
	*p = *d

	e := util.StringError("decode NetworkParams")

	var u networkParamsYAMLUnmarshaler

	if err := y.Decode(&u); err != nil {
		return e.Wrap(err)
	}

	return e.Wrap(p.unmarshal(u))
}

func (p *NetworkParams) unmarshal(u networkParamsYAMLUnmarshaler) error {
	durargs := [][2]interface{}{
		{u.TimeoutRequest, &p.timeoutRequest},
		{u.HandshakeIdleTimeout, &p.handshakeIdleTimeout},
		{u.MaxIdleTimeout, &p.maxIdleTimeout},
		{u.KeepAlivePeriod, &p.keepAlivePeriod},
		{u.DefaultHandlerTimeout, &p.defaultHandlerTimeout},
		{u.MaxStreamTimeout, &p.maxStreamTimeout},
	}

	for i := range durargs {
		v := durargs[i][0].(*util.ReadableDuration) //nolint:forcetypeassert //...
		t := durargs[i][1].(*time.Duration)         //nolint:forcetypeassert //...

		if reflect.ValueOf(v).IsZero() {
			continue
		}

		if err := util.SetInterfaceValue(time.Duration(*v), t); err != nil {
			return err
		}
	}

	for i := range u.HandlerTimeout {
		p.handlerTimeouts[i] = time.Duration(u.HandlerTimeout[i])
	}

	if u.ConnectionPoolSize != nil {
		p.connectionPoolSize = *u.ConnectionPoolSize
	}

	if u.MaxIncomingStreams != nil {
		p.maxIncomingStreams = *u.MaxIncomingStreams
	}

	if u.RateLimit != nil {
		p.rateLimit = u.RateLimit
	}

	return nil
}

type networkRateLimitParamsMarshaler struct {
	Suffrage RateLimiterRuleSet `json:"suffrage,omitempty" yaml:"suffrage,omitempty"`
	Node     RateLimiterRuleSet `json:"node,omitempty" yaml:"node,omitempty"`
	Net      RateLimiterRuleSet `json:"net,omitempty" yaml:"net,omitempty"`
	Default  RateLimiterRuleMap `json:"default,omitempty" yaml:"default,omitempty"`
}

func (p *NetworkRateLimitParams) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(networkRateLimitParamsMarshaler{
		Suffrage: p.SuffrageRuleSet(),
		Node:     p.NodeRuleSet(),
		Net:      p.NetRuleSet(),
		Default:  p.DefaultRuleMap(),
	})
}

func (p *NetworkRateLimitParams) MarshalYAML() (interface{}, error) {
	switch b, err := p.MarshalJSON(); {
	case err != nil:
		return nil, err
	default:
		var u map[string]interface{}
		if err := yaml.Unmarshal(b, &u); err != nil {
			return nil, errors.WithStack(err)
		}

		return u, nil
	}
}

type NetworkRateLimitParamsUnmarshaler struct {
	Suffrage *SuffrageRateLimiterRuleSet `json:"suffrage,omitempty" yaml:"suffrage,omitempty"`
	Node     *NodeRateLimiterRuleSet     `json:"node,omitempty" yaml:"node,omitempty"`
	Net      *NetRateLimiterRuleSet      `json:"net,omitempty" yaml:"net,omitempty"`
	Default  *RateLimiterRuleMap         `json:"default,omitempty" yaml:"default,omitempty"`
}

func (p *NetworkRateLimitParams) unmarshal(u NetworkRateLimitParamsUnmarshaler) error {
	p.RateLimiterRules = &RateLimiterRules{}

	if u.Suffrage != nil {
		if err := p.SetSuffrageRuleSet(u.Suffrage); err != nil {
			return err
		}
	}

	if u.Node != nil {
		if err := p.SetNodeRuleSet(*u.Node); err != nil {
			return err
		}
	}

	if u.Net != nil {
		if err := p.SetNetRuleSet(*u.Net); err != nil {
			return err
		}
	}

	if u.Default != nil && !u.Default.IsEmpty() {
		if err := p.SetDefaultRuleMap(*u.Default); err != nil {
			return err
		}
	}

	return nil
}

func (p *NetworkRateLimitParams) UnmarshalJSON(b []byte) error {
	e := util.StringError("decode NetworkRateLimitParams")

	var u NetworkRateLimitParamsUnmarshaler
	if err := util.UnmarshalJSON(b, &u); err != nil {
		return e.Wrap(err)
	}

	return e.Wrap(p.unmarshal(u))
}

func (p *NetworkRateLimitParams) UnmarshalYAML(y *yaml.Node) error {
	e := util.StringError("decode NetworkRateLimitParams")

	var u map[string]interface{}
	if err := y.Decode(&u); err != nil {
		return e.Wrap(err)
	}

	defer clear(u)

	switch b, err := util.MarshalJSON(u); {
	case err != nil:
		return e.Wrap(err)
	default:
		return p.UnmarshalJSON(b)
	}
}
