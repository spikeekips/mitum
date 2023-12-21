package launch

import (
	"container/list"
	"context"
	"fmt"
	"io"
	"math"
	"net"
	"sync"
	"time"

	"github.com/spikeekips/mitum/base"
	isaacnetwork "github.com/spikeekips/mitum/isaac/network"
	"github.com/spikeekips/mitum/network/quicstream"
	quicstreamheader "github.com/spikeekips/mitum/network/quicstream/header"
	"github.com/spikeekips/mitum/util"
	"golang.org/x/time/rate"
)

var (
	ErrRateLimited     = util.NewIDError("over ratelimit")
	defaultRateLimiter = RateLimiterRule{
		Limit: makeLimit(time.Second*3, 33), Burst: 33, //nolint:gomnd //...
	}
	defaultSuffrageRateLimiter = RateLimiterRule{
		Limit: makeLimit(time.Second*3, 66), Burst: 66, //nolint:gomnd //...
	}
	defaultRateLimitHandlerPoolSizes = []uint64{1 << 7, 1 << 7, 1 << 7}
)

var (
	RateLimiterLimiterPrefixContextKey = util.ContextKey("ratelimit-limiter-prefix")
	RateLimiterClientIDContextKey      = util.ContextKey("ratelimit-limiter-clientid")
	RateLimiterResultContextKey        = util.ContextKey("ratelimit-limiter-result")
	RateLimiterResultAllowedContextKey = util.ContextKey("ratelimit-limiter-result-allowed")
)

type RateLimiter struct {
	*rate.Limiter
	t         string
	checksum  string
	desc      string
	updatedAt int64
	nolimit   bool
	sync.RWMutex
}

// NewRateLimiter make new *RateLimiter;
// - if limit is zero or burst is zero, all events will be rejected
// - if limit is rate.Inf, no limit
func NewRateLimiter(limit rate.Limit, burst int, checksum, t, desc string) *RateLimiter {
	r := &RateLimiter{
		updatedAt: time.Now().UnixNano(),
		checksum:  checksum,
		t:         t,
		desc:      desc,
	}

	switch {
	case limit == rate.Inf:
		r.nolimit = true
	case limit == 0, burst < 1:
	default:
		r.Limiter = rate.NewLimiter(limit, burst)
	}

	return r
}

func (r *RateLimiter) Checksum() string {
	r.RLock()
	defer r.RUnlock()

	return r.checksum
}

func (r *RateLimiter) Type() string {
	r.RLock()
	defer r.RUnlock()

	return r.t
}

func (r *RateLimiter) UpdatedAt() int64 {
	r.RLock()
	defer r.RUnlock()

	return r.updatedAt
}

func (r *RateLimiter) Update(limit rate.Limit, burst int, checksum, t, desc string) *RateLimiter {
	r.Lock()
	defer r.Unlock()

	if r.Limiter == nil || (r.Limiter.Limit() != limit || r.Limiter.Burst() != burst) {
		switch {
		case limit == rate.Inf:
			r.nolimit = true
			r.Limiter = nil
		case limit == 0, burst < 1:
			r.nolimit = false
			r.Limiter = nil
		default:
			r.nolimit = false
			r.Limiter = rate.NewLimiter(limit, burst)
		}
	}

	switch {
	case r.Limiter == nil, r.t != t:
		r.t = t
		r.checksum = checksum
	default:
		if r.checksum != checksum {
			r.checksum = checksum
		}
	}

	r.updatedAt = time.Now().UnixNano()
	r.desc = desc

	return r
}

func (r *RateLimiter) Allow() bool {
	r.RLock()
	defer r.RUnlock()

	if r.Limiter == nil {
		return r.nolimit
	}

	return r.Limiter.Allow()
}

func (r *RateLimiter) Limit() rate.Limit {
	r.RLock()
	defer r.RUnlock()

	switch {
	case r.Limiter != nil:
		return r.Limiter.Limit()
	case r.nolimit:
		return rate.Inf
	default:
		return 0
	}
}

func (r *RateLimiter) Burst() int {
	r.RLock()
	defer r.RUnlock()

	switch {
	case r.Limiter != nil:
		return r.Limiter.Burst()
	default:
		return 0
	}
}

func (r *RateLimiter) Tokens() float64 {
	r.RLock()
	defer r.RUnlock()

	switch {
	case r.Limiter == nil:
		return 1
	default:
		return r.Limiter.Tokens()
	}
}

func (r *RateLimiter) Desc() string {
	r.RLock()
	defer r.RUnlock()

	return r.desc
}

type RateLimitHandlerArgs struct {
	Rules     *RateLimiterRules
	PoolSizes []uint64
	// ExpireAddr sets the expire duration for idle addr. if addr is over
	// ExpireAddr, it will be removed.
	ExpireAddr     time.Duration
	ShrinkInterval time.Duration
	// MaxAddrs limits the number of network addresses; if new address over
	// MaxAddrs, the oldes addr will be removed.
	MaxAddrs uint64
}

func NewRateLimitHandlerArgs() *RateLimitHandlerArgs {
	return &RateLimitHandlerArgs{
		ExpireAddr:     time.Second * 33, //nolint:gomnd //...
		ShrinkInterval: time.Second * 33, //nolint:gomnd // long enough
		PoolSizes:      defaultRateLimitHandlerPoolSizes,
		Rules:          NewRateLimiterRules(),
		MaxAddrs:       math.MaxUint32,
	}
}

type RateLimitHandler struct {
	*util.ContextDaemon
	args  *RateLimitHandlerArgs
	rules *RateLimiterRules
	pool  *addrPool
}

func NewRateLimitHandler(args *RateLimitHandlerArgs) (*RateLimitHandler, error) {
	pool, err := newAddrPool(args.PoolSizes)
	if err != nil {
		return nil, err
	}

	r := &RateLimitHandler{
		args:  args,
		pool:  pool,
		rules: args.Rules,
	}

	r.ContextDaemon = util.NewContextDaemon(r.start)

	return r, nil
}

func (r *RateLimitHandler) Func(
	ctx context.Context,
	addr net.Addr,
	f func(context.Context) (context.Context, error),
) (context.Context, error) {
	var prefix string

	switch i, ok := ctx.Value(RateLimiterLimiterPrefixContextKey).(string); {
	case !ok:
		return f(ctx)
	default:
		prefix = i
	}

	var hint RateLimitRuleHint
	if i, ok := ctx.Value(RateLimiterClientIDContextKey).(string); ok {
		hint.ClientID = i
	}

	l, allowed := r.allow(addr, prefix, hint)

	ictx := util.ContextWithValues(ctx, map[util.ContextKey]interface{}{
		RateLimiterResultContextKey: func() RateLimiterResult {
			return RateLimiterResult{
				Limiter:     humanizeRateLimiter(l.Limit(), l.Burst()),
				Tokens:      l.Tokens(),
				Allowed:     allowed,
				RulesetType: l.Type(),
				RulesetDesc: l.Desc(),
				Hint:        hint,
				Prefix:      prefix,
				Addr:        addr,
			}
		},
		RateLimiterResultAllowedContextKey: allowed,
	})

	if !allowed {
		return ictx, ErrRateLimited.Errorf("prefix=%q tokens=%0.7f", prefix, l.Tokens())
	}

	jctx, err := f(ictx)

	if jctx != nil {
		if node, ok := jctx.Value(isaacnetwork.ContextKeyNodeChallengedNode).(base.Address); ok {
			_ = r.AddNode(addr, node)
		}
	}

	return jctx, err
}

func (r *RateLimitHandler) AddNode(addr net.Addr, node base.Address) bool {
	return r.pool.addNode(addr.String(), node)
}

func (r *RateLimitHandler) allow(addr net.Addr, handler string, hint RateLimitRuleHint) (*RateLimiter, bool) {
	l := r.pool.rateLimiter(
		addr.String(),
		handler,
		hint,
		r.rateLimiterFunc(addr, handler),
	)

	return l, l.Allow()
}

func (r *RateLimitHandler) shrink(ctx context.Context) (removed uint64) {
	point := time.Now().Add(r.args.ExpireAddr * -1)

	return r.pool.shrink(ctx, point, r.args.MaxAddrs)
}

func (r *RateLimitHandler) start(ctx context.Context) error {
	ticker := time.NewTicker(r.args.ShrinkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			r.shrink(ctx)
		}
	}
}

func (r *RateLimitHandler) rateLimiterFunc(
	addr net.Addr,
	handler string,
) func(_ *RateLimiter, found, created bool, hint RateLimitRuleHint) (*RateLimiter, error) {
	return func(l *RateLimiter, found, created bool, hint RateLimitRuleHint) (*RateLimiter, error) {
		switch i, isnew := r.rules.Rule(addr, handler, hint, l); {
		case isnew:
			return i, nil
		default:
			return nil, util.ErrLockedSetIgnore
		}
	}
}

type RateLimiterRule struct {
	s     string
	Limit rate.Limit
	Burst int
}

func NewRateLimiterRule(d time.Duration, burst int) RateLimiterRule {
	var limit rate.Limit

	switch {
	case d < 1, burst < 1:
		limit = 0
		burst = 0 //revive:disable-line:modifies-parameter
	default:
		limit = makeLimit(d, burst)
	}

	return RateLimiterRule{Limit: limit, Burst: burst, s: fmt.Sprintf("%d/%s", burst, d)}
}

type RateLimiterRules struct {
	IsInConsensusNodesFunc func() (
		statehash util.Hash,
		exists func(base.Address) (found bool),
		_ error,
	)
	suffrage            RateLimiterRuleSet
	nodes               RateLimiterRuleSet
	nets                RateLimiterRuleSet
	clientid            RateLimiterRuleSet
	defaultMap          RateLimiterRuleMap
	defaultMapUpdatedAt int64
	sync.RWMutex
}

func NewRateLimiterRules() *RateLimiterRules {
	r := &RateLimiterRules{
		defaultMap:          NewRateLimiterRuleMap(&defaultRateLimiter, nil),
		defaultMapUpdatedAt: time.Now().UnixNano(),
		IsInConsensusNodesFunc: func() (util.Hash, func(base.Address) bool, error) {
			return nil, func(base.Address) bool { return false }, nil
		},
	}

	_ = r.SetSuffrageRuleSet(NewSuffrageRateLimiterRuleSet(NewRateLimiterRuleMap(&defaultSuffrageRateLimiter, nil)))

	return r
}

type RateLimitRuleHint struct {
	Node     base.Address `json:"node,omitempty"`
	ClientID string       `json:"client_id,omitempty"`
}

func (r *RateLimiterRules) Rule(
	addr net.Addr,
	handler string,
	hint RateLimitRuleHint,
	l *RateLimiter,
) (_ *RateLimiter, isnew bool) {
	if l == nil {
		checksum, rule, t, desc, _ := r.rule(addr, handler, hint, "", 0)

		return NewRateLimiter(rule.Limit, rule.Burst, checksum, t, desc), true
	}

	if r.clientid != nil && l.Type() == "clientid" &&
		len(hint.ClientID) > 0 && l.UpdatedAt() >= r.clientid.UpdatedAt() {
		return l, false
	}

	if r.nets != nil && l.Type() == "net" && l.UpdatedAt() >= r.nets.UpdatedAt() {
		return l, false
	}

	if i, isnew, found := r.ruleByNode(addr, handler, hint, l); found {
		return i, isnew
	}

	checksum, rule, t, desc, refreshed := r.rule(addr, handler, hint, l.Type(), l.UpdatedAt())
	if !refreshed {
		return l, false
	}

	return l.Update(rule.Limit, rule.Burst, checksum, t, desc), false
}

func (r *RateLimiterRules) DefaultRuleMap() RateLimiterRuleMap {
	r.RLock()
	defer r.RUnlock()

	return r.defaultMap
}

func (r *RateLimiterRules) SetDefaultRuleMap(rule RateLimiterRuleMap) error {
	r.Lock()
	defer r.Unlock()

	r.defaultMap = rule
	r.defaultMapUpdatedAt = time.Now().UnixNano()

	return nil
}

func (r *RateLimiterRules) SuffrageRuleSet() RateLimiterRuleSet {
	r.RLock()
	defer r.RUnlock()

	return r.suffrage
}

func (r *RateLimiterRules) SetSuffrageRuleSet(l RateLimiterRuleSet) error {
	r.Lock()
	defer r.Unlock()

	switch i, err := util.AssertInterfaceValue[*SuffrageRateLimiterRuleSet](l); {
	case err != nil:
		return err
	default:
		i.IsInConsensusNodesFunc = r.IsInConsensusNodesFunc
	}

	r.suffrage = l

	return nil
}

func (r *RateLimiterRules) NodeRuleSet() RateLimiterRuleSet {
	r.RLock()
	defer r.RUnlock()

	return r.nodes
}

func (r *RateLimiterRules) SetNodeRuleSet(l RateLimiterRuleSet) error {
	r.Lock()
	defer r.Unlock()

	r.nodes = l

	return nil
}

func (r *RateLimiterRules) NetRuleSet() RateLimiterRuleSet {
	r.RLock()
	defer r.RUnlock()

	return r.nets
}

func (r *RateLimiterRules) SetNetRuleSet(l RateLimiterRuleSet) error {
	r.Lock()
	defer r.Unlock()

	r.nets = l

	return nil
}

func (r *RateLimiterRules) ClientIDRuleSet() RateLimiterRuleSet {
	r.RLock()
	defer r.RUnlock()

	return r.clientid
}

func (r *RateLimiterRules) SetClientIDRuleSet(l RateLimiterRuleSet) error {
	r.Lock()
	defer r.Unlock()

	r.clientid = l

	return nil
}

func (r *RateLimiterRules) DefaultMapUpdatedAt() int64 {
	r.RLock()
	defer r.RUnlock()

	return r.defaultMapUpdatedAt
}

func (r *RateLimiterRules) SetIsInConsensusNodesFunc(f func() (
	statehash util.Hash,
	exists func(base.Address) (found bool),
	_ error,
),
) {
	r.IsInConsensusNodesFunc = f

	if i, ok := r.suffrage.(*SuffrageRateLimiterRuleSet); ok {
		i.IsInConsensusNodesFunc = r.IsInConsensusNodesFunc
	}
}

func (r *RateLimiterRules) rule(
	addr net.Addr,
	handler string,
	hint RateLimitRuleHint,
	t string,
	updatedAt int64,
) (checksum string, _ RateLimiterRule, rulesetType string, desc string, updated bool) {
	r.RLock()
	defer r.RUnlock()

	var node base.Address
	if hint.Node != nil {
		node = hint.Node
	}

	if r.clientid != nil {
		if checksum, l, desc, found := r.clientid.Rule(addr, handler, hint); found {
			return checksum, l, "clientid", desc, true
		}
	}

	if r.nets != nil {
		if checksum, l, desc, found := r.nets.Rule(addr, handler, hint); found {
			return checksum, l, "net", desc, true
		}
	}

	if node != nil && r.nodes != nil {
		if checksum, l, desc, found := r.nodes.Rule(addr, handler, hint); found {
			return checksum, l, "node", desc, true
		}
	}

	if node != nil && r.suffrage != nil {
		if checksum, l, desc, found := r.suffrage.Rule(addr, handler, hint); found {
			return checksum, l, "suffrage", desc, true
		}
	}

	switch {
	case t == "defaultmap" && updatedAt >= r.defaultMapUpdatedAt:
		return "", defaultRateLimiter, "defaultmap", "", false
	default:
		if l, found := r.defaultMap.Rule(handler); found {
			return "", l, "defaultmap", "", true
		}

		return "", defaultRateLimiter, "default", "", true
	}
}

func (r *RateLimiterRules) IsValid([]byte) error {
	if r.defaultMap.IsEmpty() {
		return util.ErrInvalid.Errorf("empty default rule map")
	}

	return nil
}

func (r *RateLimiterRules) ruleByNode(
	addr net.Addr,
	handler string,
	hint RateLimitRuleHint,
	l *RateLimiter,
) (_ *RateLimiter, isnew, found bool) {
	var node base.Address
	if hint.Node != nil {
		node = hint.Node
	}

	if node != nil && r.nodes != nil && l.Type() == "node" && l.UpdatedAt() >= r.nodes.UpdatedAt() {
		return l, false, true
	}

	if node != nil && r.suffrage != nil && l.Type() == "suffrage" && l.UpdatedAt() >= r.suffrage.UpdatedAt() {
		switch st, exists, err := r.IsInConsensusNodesFunc(); {
		case err != nil:
		case !exists(node):
		case st.String() != l.Checksum():
			if checksum, rule, desc, found := r.suffrage.Rule(addr, handler, hint); found {
				return l.Update(rule.Limit, rule.Burst, checksum, "suffrage", desc), false, true
			}
		default:
			return l, false, true
		}
	}

	return l, false, false
}

type RateLimiterRuleSet interface {
	Rule(addr net.Addr, handler string, hint RateLimitRuleHint) (
		checksum string, _ RateLimiterRule, desc string, found bool)
	UpdatedAt() int64
	util.IsValider
}

type RateLimiterRuleMap struct {
	d *RateLimiterRule
	m map[ /* handler */ string]RateLimiterRule
}

func NewRateLimiterRuleMap(
	d *RateLimiterRule,
	m map[string]RateLimiterRule,
) RateLimiterRuleMap {
	return RateLimiterRuleMap{d: d, m: m}
}

func (m RateLimiterRuleMap) Len() int {
	return len(m.m)
}

func (m RateLimiterRuleMap) IsEmpty() bool {
	return m.d == nil && len(m.m) < 1
}

func (m RateLimiterRuleMap) Rule(handler string) (rule RateLimiterRule, found bool) {
	switch i, found := m.rule(handler); {
	case found:
		return i, true
	case m.d != nil:
		return *m.d, true
	default:
		return rule, false
	}
}

func (m RateLimiterRuleMap) rule(handler string) (rule RateLimiterRule, found bool) {
	if m.m == nil {
		return rule, false
	}

	i, found := m.m[handler]

	return i, found
}

type NetRateLimiterRuleSet struct {
	rules     map[ /* ipnet */ string]RateLimiterRuleMap
	ipnets    []*net.IPNet
	updatedAt int64
}

func NewNetRateLimiterRuleSet() NetRateLimiterRuleSet {
	return NetRateLimiterRuleSet{
		rules:     map[string]RateLimiterRuleMap{},
		updatedAt: time.Now().UnixNano(),
	}
}

func (rs NetRateLimiterRuleSet) IsValid([]byte) error {
	e := util.ErrInvalid.Errorf("NetRateLimiterRuleSet")

	switch {
	case len(rs.rules) != len(rs.ipnets):
		return e.Errorf("rules length != ipnet length")
	case len(rs.ipnets) < 1:
		return nil
	}

	for i := range rs.ipnets {
		k := rs.ipnets[i].String()

		if _, found := rs.rules[k]; !found {
			return e.Errorf("ipnet, %q has no rule", k)
		}
	}

	return nil
}

func (rs NetRateLimiterRuleSet) UpdatedAt() int64 {
	return rs.updatedAt
}

func (rs *NetRateLimiterRuleSet) Add(ipnet *net.IPNet, rule RateLimiterRuleMap) *NetRateLimiterRuleSet {
	rs.ipnets = append(rs.ipnets, ipnet)
	rs.rules[ipnet.String()] = rule

	return rs
}

func (rs NetRateLimiterRuleSet) Rule(
	addr net.Addr, handler string, _ RateLimitRuleHint,
) (_ string, _ RateLimiterRule, _ string, _ bool) {
	l, desc, found := rs.rule(addr, handler)

	return "", l, desc, found
}

func (rs NetRateLimiterRuleSet) rule(addr net.Addr, handler string) (rule RateLimiterRule, _ string, _ bool) {
	var ip net.IP

	switch t := addr.(type) {
	case *net.UDPAddr:
		ip = t.IP
	default:
		return rule, "", false
	}

	for i := range rs.ipnets {
		in := rs.ipnets[i]
		if !in.Contains(ip) {
			continue
		}

		m, found := rs.rules[in.String()]
		if !found {
			return rule, "", false
		}

		l, found := m.Rule(handler)
		if !found {
			return rule, "", false
		}

		return l, fmt.Sprintf(`{"net":%q}`, in), true
	}

	return rule, "", false
}

type StringKeyRateLimiterRuleSet struct {
	rules     map[ /* node address */ string]RateLimiterRuleMap
	updatedAt int64
}

func newStringKeyRateLimiterRuleSet(
	rules map[string]RateLimiterRuleMap,
) StringKeyRateLimiterRuleSet {
	return StringKeyRateLimiterRuleSet{rules: rules, updatedAt: time.Now().UnixNano()}
}

func (StringKeyRateLimiterRuleSet) IsValid([]byte) error {
	return nil
}

func (rs StringKeyRateLimiterRuleSet) UpdatedAt() int64 {
	return rs.updatedAt
}

func (rs StringKeyRateLimiterRuleSet) rule(i string, handler string) (rule RateLimiterRule, _ bool) {
	m, found := rs.rules[i]
	if !found {
		return rule, false
	}

	l, found := m.Rule(handler)

	return l, found
}

type NodeRateLimiterRuleSet struct {
	StringKeyRateLimiterRuleSet
}

func NewNodeRateLimiterRuleSet(
	rules map[string]RateLimiterRuleMap,
) NodeRateLimiterRuleSet {
	return NodeRateLimiterRuleSet{StringKeyRateLimiterRuleSet: newStringKeyRateLimiterRuleSet(rules)}
}

func (rs NodeRateLimiterRuleSet) Rule(
	_ net.Addr, handler string, hint RateLimitRuleHint,
) (_ string, _ RateLimiterRule, _ string, _ bool) {
	if len(rs.rules) < 1 {
		return "", RateLimiterRule{}, "", false
	}

	var node string

	switch {
	case hint.Node == nil:
		return "", RateLimiterRule{}, "", false
	default:
		node = hint.Node.String()
	}

	rule, found := rs.rule(node, handler)

	return "", rule, "", found
}

type SuffrageRateLimiterRuleSet struct {
	IsInConsensusNodesFunc func() (util.Hash, func(base.Address) (found bool), error)
	rules                  RateLimiterRuleMap
	updatedAt              int64
	sync.RWMutex
}

func NewSuffrageRateLimiterRuleSet(rule RateLimiterRuleMap) *SuffrageRateLimiterRuleSet {
	return &SuffrageRateLimiterRuleSet{rules: rule, updatedAt: time.Now().UnixNano()}
}

func (*SuffrageRateLimiterRuleSet) IsValid([]byte) error {
	return nil
}

func (rs *SuffrageRateLimiterRuleSet) UpdatedAt() int64 {
	return rs.updatedAt
}

func (rs *SuffrageRateLimiterRuleSet) Rule(
	_ net.Addr, handler string, hint RateLimitRuleHint,
) (statehash string, rule RateLimiterRule, _ string, _ bool) {
	if rs.rules.IsEmpty() {
		return "", rule, "", false
	}

	var node base.Address

	switch {
	case hint.Node == nil:
		return "", rule, "", false
	default:
		node = hint.Node
	}

	switch st, exists, err := rs.IsInConsensusNodesFunc(); {
	case err != nil, !exists(node):
		return "", rule, "", false
	default:
		l, found := rs.rules.Rule(handler)

		return st.String(), l, "", found
	}
}

type ClientIDRateLimiterRuleSet struct {
	StringKeyRateLimiterRuleSet
}

func NewClientIDRateLimiterRuleSet(
	rules map[string]RateLimiterRuleMap,
) ClientIDRateLimiterRuleSet {
	return ClientIDRateLimiterRuleSet{StringKeyRateLimiterRuleSet: newStringKeyRateLimiterRuleSet(rules)}
}

func (rs ClientIDRateLimiterRuleSet) Rule(
	_ net.Addr, handler string, hint RateLimitRuleHint,
) (_ string, rule RateLimiterRule, _ string, _ bool) {
	if len(rs.rules) < 1 {
		return "", rule, "", false
	}

	if len(hint.ClientID) < 1 {
		return "", rule, "", false
	}

	l, found := rs.rule(hint.ClientID, handler)

	return "", l, fmt.Sprintf(`{"client_id":%q}`, hint.ClientID), found
}

type addrPool struct {
	l              *util.ShardedMap[string, util.LockedMap[string, *RateLimiter]]
	addrs          *util.ShardedMap[string, base.Address]
	lastAccessedAt *util.ShardedMap[string, time.Time]
	addrsQueue     *rateLimitAddrsQueue
}

func newAddrPool(poolsize []uint64) (*addrPool, error) {
	l, err := util.NewDeepShardedMap[string, util.LockedMap[string, *RateLimiter]](
		poolsize,
		func() util.LockedMap[string, util.LockedMap[string, *RateLimiter]] {
			return util.NewSingleLockedMap[string, util.LockedMap[string, *RateLimiter]]()
		},
	)
	if err != nil {
		return nil, err
	}

	addrs, err := util.NewDeepShardedMap[string, base.Address](poolsize, nil)
	if err != nil {
		return nil, err
	}

	lastAccessedAt, err := util.NewDeepShardedMap[string, time.Time](poolsize, nil)
	if err != nil {
		return nil, err
	}

	return &addrPool{
		l:              l,
		addrs:          addrs,
		lastAccessedAt: lastAccessedAt,
		addrsQueue:     newRateLimitAddrsQueue(),
	}, nil
}

func (p *addrPool) addNode(addr string, node base.Address) bool {
	var created bool

	_ = p.l.Get(addr, func(_ util.LockedMap[string, *RateLimiter], found bool) error {
		if !found {
			return nil
		}

		_, created, _ = p.addrs.Set(addr, func(_ base.Address, found bool) (base.Address, error) {
			if found {
				return nil, util.ErrLockedSetIgnore
			}

			_ = p.lastAccessedAt.SetValue(addr, time.Now())

			return node, nil
		})

		return nil
	})

	return created
}

func (p *addrPool) rateLimiter(
	addr,
	handler string,
	hint RateLimitRuleHint,
	f func(l *RateLimiter, found, created bool, hint RateLimitRuleHint) (*RateLimiter, error),
) *RateLimiter {
	var l *RateLimiter

	_ = p.l.GetOrCreate(
		addr,
		func(i util.LockedMap[string, *RateLimiter], _ bool) error {
			_ = p.lastAccessedAt.SetValue(addr, time.Now())

			var created bool

			if i.Len() < 1 {
				created = true

				p.addrsQueue.Add(addr)
			}

			switch j, _ := p.addrs.Value(addr); {
			case j == nil: // NOTE hint.Node preserved
			default:
				hint.Node = j
			}

			l, _, _ = i.Set(
				handler,
				func(l *RateLimiter, found bool) (*RateLimiter, error) {
					return f(l, found, created, hint)
				},
			)

			return nil
		},
		func() (util.LockedMap[string, *RateLimiter], error) {
			return util.NewSingleLockedMap[string, *RateLimiter](), nil
		},
	)

	return l
}

func (p *addrPool) remove(addr string) bool {
	removed, _ := p.l.Remove(addr, func(util.LockedMap[string, *RateLimiter], bool) error {
		_ = p.lastAccessedAt.RemoveValue(addr)

		_ = p.addrs.RemoveValue(addr)
		_ = p.addrsQueue.Remove(addr)

		return nil
	})

	return removed
}

func (p *addrPool) shrink(ctx context.Context, expire time.Time, maxAddrs uint64) (removed uint64) {
	p.lastAccessedAt.TraverseMap(
		func(m util.LockedMap[string, time.Time]) bool {
			return util.AwareContext(ctx, func(context.Context) error {
				var gathered []string

				m.Traverse(func(addr string, accessed time.Time) bool {
					if accessed.Before(expire) {
						gathered = append(gathered, addr)
					}

					return true
				})

				for i := range gathered {
					_ = p.remove(gathered[i])
				}

				removed += uint64(len(gathered))

				return nil
			}) == nil
		},
	)

	removed += p.shrinkAddrsQueue(maxAddrs)

	return removed
}

func (p *addrPool) shrinkAddrsQueue(maxAddrs uint64) uint64 {
	var removed uint64

	for uint64(p.addrsQueue.Len()) > maxAddrs {
		switch addr := p.addrsQueue.Pop(); {
		case len(addr) < 1:
			continue
		default:
			if p.remove(addr) {
				removed++
			}
		}
	}

	return removed
}

type rateLimitAddrsQueue struct {
	l *list.List
	m map[string]*list.Element
	sync.RWMutex
}

func newRateLimitAddrsQueue() *rateLimitAddrsQueue {
	return &rateLimitAddrsQueue{
		l: list.New(),
		m: map[string]*list.Element{},
	}
}

func (h *rateLimitAddrsQueue) Len() int {
	h.RLock()
	defer h.RUnlock()

	return h.l.Len()
}

func (h *rateLimitAddrsQueue) Add(addr string) {
	h.Lock()
	defer h.Unlock()

	if i, found := h.m[addr]; found {
		_ = h.l.Remove(i)
	}

	h.m[addr] = h.l.PushBack(addr)
}

func (h *rateLimitAddrsQueue) Remove(addr string) bool {
	h.Lock()
	defer h.Unlock()

	switch i, found := h.m[addr]; {
	case found:
		_ = h.l.Remove(i)
		delete(h.m, addr)

		return true
	default:
		return false
	}
}

func (h *rateLimitAddrsQueue) Pop() string {
	h.Lock()
	defer h.Unlock()

	e := h.l.Front()
	if e != nil {
		_ = h.l.Remove(e)
	}

	var addr string

	if e != nil {
		addr = e.Value.(string) //nolint:forcetypeassert //...

		delete(h.m, addr)
	}

	return addr
}

func humanizeRateLimiter(limit rate.Limit, burst int) string {
	switch {
	case limit == rate.Inf:
		return "nolimit"
	case limit == 0, burst < 1:
		return "0"
	default:
		return fmt.Sprintf("%d/%s",
			burst,
			loadLimitDuration(limit, burst).String(),
		)
	}
}

func rateLimitHandlerFunc(
	ratelimiter *RateLimitHandler,
	findPrefix func([32]byte) (string, bool),
) func(quicstream.Handler) quicstream.Handler {
	return func(handler quicstream.Handler) quicstream.Handler {
		return func(ctx context.Context, addr net.Addr, r io.Reader, w io.WriteCloser) (context.Context, error) {
			var prefix string

			if b, ok := ctx.Value(quicstream.PrefixHandlerPrefixContextKey).([32]byte); ok {
				switch i, found := findPrefix(b); {
				case found:
					prefix = i
				default:
					return handler(ctx, addr, r, w)
				}
			}

			return ratelimiter.Func(
				context.WithValue(ctx, RateLimiterLimiterPrefixContextKey, prefix),
				addr,
				func(ctx context.Context) (context.Context, error) {
					return handler(ctx, addr, r, w)
				},
			)
		}
	}
}

func rateLimitHeaderHandlerFunc[T quicstreamheader.RequestHeader](
	ratelimiter *RateLimitHandler,
	findPrefix func([32]byte) (string, bool),
	handler quicstreamheader.Handler[T],
) quicstreamheader.Handler[T] {
	return func(ctx context.Context, addr net.Addr, broker *quicstreamheader.HandlerBroker, header T) (
		context.Context, error,
	) {
		var prefix string

		if b, ok := ctx.Value(quicstream.PrefixHandlerPrefixContextKey).([32]byte); ok {
			switch i, found := findPrefix(b); {
			case found:
				prefix = i
			default:
				return handler(ctx, addr, broker, header)
			}
		}

		nctx := context.WithValue(ctx, RateLimiterLimiterPrefixContextKey, prefix)

		if i, ok := (interface{})(header).(interface{ ClientID() string }); ok {
			nctx = context.WithValue(nctx, RateLimiterClientIDContextKey, i.ClientID())
		}

		return ratelimiter.Func(
			nctx,
			addr,
			func(ctx context.Context) (context.Context, error) {
				return handler(ctx, addr, broker, header)
			},
		)
	}
}

func makeLimit(d time.Duration, burst int) rate.Limit {
	return rate.Every(d / time.Duration(burst))
}

func loadLimitDuration(l rate.Limit, burst int) time.Duration {
	return time.Duration(
		(float64(burst) / float64(l) * float64(time.Second)),
	)
}

type RateLimiterResult struct {
	Addr        net.Addr          `json:"addr,omitempty"`
	Hint        RateLimitRuleHint `json:"hint,omitempty"`
	Limiter     string            `json:"limiter,omitempty"`
	RulesetType string            `json:"ruleset_type,omitempty"`
	RulesetDesc string            `json:"ruleset_desc,omitempty"`
	Prefix      string            `json:"prefix,omitempty"`
	Tokens      float64           `json:"tokens,omitempty"`
	Allowed     bool              `json:"allowed,omitempty"`
}
