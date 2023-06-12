package launch

import (
	"reflect"
	"time"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/network/quicmemberlist"
	"github.com/spikeekips/mitum/util"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/hint"
	"gopkg.in/yaml.v3"
)

type LocalParams struct {
	ISAAC      *isaac.Params     `yaml:"isaac,omitempty" json:"isaac,omitempty"`
	Memberlist *MemberlistParams `yaml:"memberlist,omitempty" json:"memberlist,omitempty"`
	MISC       *MISCParams       `yaml:"misc,omitempty" json:"misc,omitempty"`
}

func defaultLocalParams(networkID base.NetworkID) *LocalParams {
	return &LocalParams{
		ISAAC:      isaac.DefaultParams(networkID),
		Memberlist: defaultMemberlistParams(),
		MISC:       defaultMISCParams(),
	}
}

func (p *LocalParams) IsValid(networkID base.NetworkID) error {
	e := util.ErrInvalid.Errorf("invalid LocalParams")

	if p.ISAAC == nil {
		return e.Errorf("empty ISAAC")
	}

	if p.Memberlist == nil {
		return e.Errorf("empty Memberlist")
	}

	if p.MISC == nil {
		return e.Errorf("empty MISC")
	}

	_ = p.ISAAC.SetNetworkID(networkID)

	if err := p.ISAAC.IsValid(networkID); err != nil {
		return e.Wrap(err)
	}

	if err := p.Memberlist.IsValid(nil); err != nil {
		return e.Wrap(err)
	}

	if err := p.MISC.IsValid(nil); err != nil {
		return e.Wrap(err)
	}

	return nil
}

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

	return m, nil
}

type LocalParamsYAMLUnmarshaler struct {
	ISAAC      map[string]interface{} `yaml:"isaac"`
	Memberlist *MemberlistParams      `yaml:"memberlist,omitempty"`
	MISC       *MISCParams            `yaml:"misc,omitempty"`
}

func (p *LocalParams) DecodeYAML(b []byte, enc *jsonenc.Encoder) error {
	if len(b) < 1 {
		return nil
	}

	e := util.StringError("decode LocalParams")

	u := LocalParamsYAMLUnmarshaler{Memberlist: p.Memberlist}

	if err := yaml.Unmarshal(b, &u); err != nil {
		return e.Wrap(err)
	}

	switch lb, err := enc.Marshal(u.ISAAC); {
	case err != nil:
		return e.Wrap(err)
	default:
		if err := enc.Unmarshal(lb, p.ISAAC); err != nil {
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

	return nil
}

type MemberlistParams struct {
	*util.BaseParams
	tcpTimeout              time.Duration
	retransmitMult          int
	probeTimeout            time.Duration
	probeInterval           time.Duration
	suspicionMult           int
	suspicionMaxTimeoutMult int
	udpBufferSize           int
	extraSameMemberLimit    uint64
}

func defaultMemberlistParams() *MemberlistParams {
	config := quicmemberlist.BasicMemberlistConfig()

	return &MemberlistParams{
		BaseParams:              util.NewBaseParams(),
		tcpTimeout:              config.TCPTimeout,
		retransmitMult:          config.RetransmitMult,
		probeTimeout:            config.ProbeTimeout,
		probeInterval:           config.ProbeInterval,
		suspicionMult:           config.SuspicionMult,
		suspicionMaxTimeoutMult: config.SuspicionMaxTimeoutMult,
		udpBufferSize:           config.UDPBufferSize,
		extraSameMemberLimit:    1, //nolint:gomnd //...
	}
}

func (*MemberlistParams) IsValid([]byte) error {
	return nil
}

func (p *MemberlistParams) TCPTimeout() time.Duration {
	return p.tcpTimeout
}

func (p *MemberlistParams) SetTCPTimeout(d time.Duration) error {
	return p.SetDuration(d, func(d time.Duration) (bool, error) {
		if p.tcpTimeout == d {
			return false, nil
		}

		p.tcpTimeout = d

		return true, nil
	})
}

func (p *MemberlistParams) RetransmitMult() int {
	return p.retransmitMult
}

func (p *MemberlistParams) SetRetransmitMult(d int) error {
	return p.SetInt(d, func(d int) (bool, error) {
		if p.retransmitMult == d {
			return false, nil
		}

		p.retransmitMult = d

		return true, nil
	})
}

func (p *MemberlistParams) ProbeTimeout() time.Duration {
	return p.probeTimeout
}

func (p *MemberlistParams) SetProbeTimeout(d time.Duration) error {
	return p.SetDuration(d, func(d time.Duration) (bool, error) {
		if p.probeTimeout == d {
			return false, nil
		}

		p.probeTimeout = d

		return true, nil
	})
}

func (p *MemberlistParams) ProbeInterval() time.Duration {
	return p.probeInterval
}

func (p *MemberlistParams) SetProbeInterval(d time.Duration) error {
	return p.SetDuration(d, func(d time.Duration) (bool, error) {
		if p.probeInterval == d {
			return false, nil
		}

		p.probeInterval = d

		return true, nil
	})
}

func (p *MemberlistParams) SuspicionMult() int {
	return p.suspicionMult
}

func (p *MemberlistParams) SetSuspicionMult(d int) error {
	return p.SetInt(d, func(d int) (bool, error) {
		if p.suspicionMult == d {
			return false, nil
		}

		p.suspicionMult = d

		return true, nil
	})
}

func (p *MemberlistParams) SuspicionMaxTimeoutMult() int {
	return p.suspicionMaxTimeoutMult
}

func (p *MemberlistParams) SetSuspicionMaxTimeoutMult(d int) error {
	return p.SetInt(d, func(d int) (bool, error) {
		if p.suspicionMaxTimeoutMult == d {
			return false, nil
		}

		p.suspicionMaxTimeoutMult = d

		return true, nil
	})
}

func (p *MemberlistParams) UDPBufferSize() int {
	return p.udpBufferSize
}

func (p *MemberlistParams) SetUDPBufferSize(d int) error {
	return p.SetInt(d, func(d int) (bool, error) {
		if p.udpBufferSize == d {
			return false, nil
		}

		p.udpBufferSize = d

		return true, nil
	})
}

func (p *MemberlistParams) ExtraSameMemberLimit() uint64 {
	return p.extraSameMemberLimit
}

func (p *MemberlistParams) SetExtraSameMemberLimit(d uint64) error {
	return p.SetUint64(d, func(d uint64) (bool, error) {
		if p.extraSameMemberLimit == d {
			return false, nil
		}

		p.extraSameMemberLimit = d

		return true, nil
	})
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

func (p *MemberlistParams) UnmarshalYAML(unmarshal func(interface{}) error) error {
	d := defaultMemberlistParams()
	*p = *d

	e := util.StringError("unmarshal MemberlistParams")

	var u memberlistParamsUnmarshaler

	if err := unmarshal(&u); err != nil {
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

		if reflect.ValueOf(v).IsZero() {
			continue
		}

		if err := util.InterfaceSetValue(time.Duration(*v), durargs[i][1]); err != nil {
			return err
		}
	}

	return nil
}

type MISCParams struct {
	*util.BaseParams
	timeoutRequest                        time.Duration
	syncSourceCheckerInterval             time.Duration
	validProposalOperationExpire          time.Duration
	validProposalSuffrageOperationsExpire time.Duration
	maxMessageSize                        uint64
	objectCacheSize                       uint64
}

func defaultMISCParams() *MISCParams {
	return &MISCParams{
		BaseParams:                            util.NewBaseParams(),
		timeoutRequest:                        isaac.DefaultTimeoutRequest,
		syncSourceCheckerInterval:             time.Second * 30, //nolint:gomnd //...
		validProposalOperationExpire:          time.Hour * 24,   //nolint:gomnd //...
		validProposalSuffrageOperationsExpire: time.Hour * 2,
		maxMessageSize:                        1 << 18, //nolint:gomnd //...
		objectCacheSize:                       1 << 13, //nolint:gomnd // big enough
	}
}

func (p *MISCParams) IsValid([]byte) error {
	e := util.ErrInvalid.Errorf("invalid MISCParams")

	if err := p.BaseParams.IsValid(nil); err != nil {
		return e.Wrap(err)
	}

	if p.timeoutRequest < 0 {
		return e.Errorf("wrong duration; invalid timeoutRequest")
	}

	if p.syncSourceCheckerInterval < 0 {
		return e.Errorf("wrong duration; invalid syncSourceCheckerInterval")
	}

	if p.validProposalOperationExpire < 0 {
		return e.Errorf("wrong duration; invalid validProposalOperationExpire")
	}

	if p.validProposalSuffrageOperationsExpire < 0 {
		return e.Errorf("wrong duration; invalid validProposalSuffrageOperationsExpire")
	}

	if p.maxMessageSize < 1 {
		return e.Errorf("wrong maxMessageSize")
	}

	if p.objectCacheSize < 1 {
		return e.Errorf("wrong objectCacheSize")
	}

	return nil
}

func (p *MISCParams) TimeoutRequest() time.Duration {
	p.RLock()
	defer p.RUnlock()

	return p.timeoutRequest
}

func (p *MISCParams) SetTimeoutRequest(d time.Duration) error {
	return p.SetDuration(d, func(d time.Duration) (bool, error) {
		if p.timeoutRequest == d {
			return false, nil
		}

		p.timeoutRequest = d

		return true, nil
	})
}

func (p *MISCParams) SyncSourceCheckerInterval() time.Duration {
	p.RLock()
	defer p.RUnlock()

	return p.syncSourceCheckerInterval
}

func (p *MISCParams) SetSyncSourceCheckerInterval(d time.Duration) error {
	return p.SetDuration(d, func(d time.Duration) (bool, error) {
		if p.syncSourceCheckerInterval == d {
			return false, nil
		}

		p.syncSourceCheckerInterval = d

		return true, nil
	})
}

func (p *MISCParams) ValidProposalOperationExpire() time.Duration {
	p.RLock()
	defer p.RUnlock()

	return p.validProposalOperationExpire
}

func (p *MISCParams) SetValidProposalOperationExpire(d time.Duration) error {
	return p.SetDuration(d, func(d time.Duration) (bool, error) {
		if p.validProposalOperationExpire == d {
			return false, nil
		}

		p.validProposalOperationExpire = d

		return true, nil
	})
}

func (p *MISCParams) ValidProposalSuffrageOperationsExpire() time.Duration {
	p.RLock()
	defer p.RUnlock()

	return p.validProposalSuffrageOperationsExpire
}

func (p *MISCParams) SetValidProposalSuffrageOperationsExpire(d time.Duration) error {
	return p.SetDuration(d, func(d time.Duration) (bool, error) {
		if p.validProposalSuffrageOperationsExpire == d {
			return false, nil
		}

		p.validProposalSuffrageOperationsExpire = d

		return true, nil
	})
}

func (p *MISCParams) MaxMessageSize() uint64 {
	p.RLock()
	defer p.RUnlock()

	return p.maxMessageSize
}

func (p *MISCParams) SetMaxMessageSize(d uint64) error {
	return p.SetUint64(d, func(d uint64) (bool, error) {
		if p.maxMessageSize == d {
			return false, nil
		}

		p.maxMessageSize = d

		return true, nil
	})
}

func (p *MISCParams) ObjectCacheSize() uint64 {
	p.RLock()
	defer p.RUnlock()

	return p.objectCacheSize
}

func (p *MISCParams) SetObjectCacheSize(d uint64) error {
	return p.SetUint64(d, func(d uint64) (bool, error) {
		if p.objectCacheSize == d {
			return false, nil
		}

		p.objectCacheSize = d

		return true, nil
	})
}

type miscParamsYAMLMarshaler struct {
	//revive:disable:line-length-limit
	TimeoutRequest                        util.ReadableDuration `json:"timeout_request,omitempty" yaml:"timeout_request,omitempty"`
	SyncSourceCheckerInterval             util.ReadableDuration `json:"sync_source_checker_interval,omitempty" yaml:"sync_source_checker_interval,omitempty"`
	ValidProposalOperationExpire          util.ReadableDuration `json:"valid_proposal_operation_expire,omitempty" yaml:"valid_proposal_operation_expire,omitempty"`
	ValidProposalSuffrageOperationsExpire util.ReadableDuration `json:"valid_proposal_suffrage_operations_expire,omitempty" yaml:"valid_proposal_suffrage_operations_expire,omitempty"`
	MaxMessageSize                        uint64                `json:"max_message_size,omitempty" yaml:"max_message_size,omitempty"`
	ObjectCacheSize                       uint64                `json:"object_cache_size,omitempty" yaml:"object_cache_size,omitempty"`
	//revive:enable:line-length-limit
}

func (p *MISCParams) marshaler() miscParamsYAMLMarshaler {
	return miscParamsYAMLMarshaler{
		TimeoutRequest:                        util.ReadableDuration(p.timeoutRequest),
		SyncSourceCheckerInterval:             util.ReadableDuration(p.syncSourceCheckerInterval),
		ValidProposalOperationExpire:          util.ReadableDuration(p.validProposalOperationExpire),
		ValidProposalSuffrageOperationsExpire: util.ReadableDuration(p.validProposalSuffrageOperationsExpire),
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
	TimeoutRequest                        *util.ReadableDuration `json:"timeout_request,omitempty" yaml:"timeout_request,omitempty"`
	SyncSourceCheckerInterval             *util.ReadableDuration `json:"sync_source_checker_interval,omitempty" yaml:"sync_source_checker_interval,omitempty"`
	ValidProposalOperationExpire          *util.ReadableDuration `json:"valid_proposal_operation_expire,omitempty" yaml:"valid_proposal_operation_expire,omitempty"`
	ValidProposalSuffrageOperationsExpire *util.ReadableDuration `json:"valid_proposal_suffrage_operations_expire,omitempty" yaml:"valid_proposal_suffrage_operations_expire,omitempty"`
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

func (p *MISCParams) UnmarshalYAML(unmarshal func(interface{}) error) error {
	d := defaultMISCParams()
	*p = *d

	e := util.StringError("decode MISCParams")

	var u miscParamsYAMLUnmarshaler

	if err := unmarshal(&u); err != nil {
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
		{u.TimeoutRequest, &p.timeoutRequest},
		{u.SyncSourceCheckerInterval, &p.syncSourceCheckerInterval},
		{u.ValidProposalOperationExpire, &p.validProposalOperationExpire},
		{u.ValidProposalSuffrageOperationsExpire, &p.validProposalSuffrageOperationsExpire},
	}

	for i := range durargs {
		v := durargs[i][0].(*util.ReadableDuration) //nolint:forcetypeassert //...

		if reflect.ValueOf(v).IsZero() {
			continue
		}

		if err := util.InterfaceSetValue(time.Duration(*v), durargs[i][1]); err != nil {
			return err
		}
	}

	return nil
}
