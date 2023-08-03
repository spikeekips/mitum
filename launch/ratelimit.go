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

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	isaacnetwork "github.com/spikeekips/mitum/isaac/network"
	"github.com/spikeekips/mitum/network/quicstream"
	"github.com/spikeekips/mitum/util"
	"golang.org/x/time/rate"
)

var (
	ErrRateLimited     = util.NewIDError("over rate limit")
	defaultRateLimiter = RateLimiterRule{
		Limit: rate.Every(time.Second * 3), Burst: 3, //nolint:gomnd //...
	}
	defaultRateLimitHandlerPoolSizes = []uint64{1 << 7, 1 << 7, 1 << 7}
)

type RateLimiter struct {
	*rate.Limiter
	updatedAt time.Time
	checksum  string
	nolimit   bool
	sync.RWMutex
}

// NewRateLimiter make new *RateLimiter;
// - if limit is zero or burst is zero, all events will be rejected
// - if limit is rate.Inf, no limit
func NewRateLimiter(limit rate.Limit, burst int, checksum string) *RateLimiter {
	r := &RateLimiter{
		updatedAt: time.Now(),
		checksum:  checksum,
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

func (r *RateLimiter) UpdatedAt() time.Time {
	r.RLock()
	defer r.RUnlock()

	return r.updatedAt
}

func (r *RateLimiter) Update(limit rate.Limit, burst int, checksum string) *RateLimiter {
	r.Lock()
	defer r.Unlock()

	if r.Limit() != limit || r.Burst() != burst {
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

	if r.checksum != checksum {
		r.checksum = checksum
	}

	r.updatedAt = time.Now()

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

type RateLimitHandlerArgs struct {
	GetLastSuffrageFunc func(context.Context) (statehash util.Hash, _ base.Suffrage, found bool, _ error)
	Rules               *RateLimiterRules
	PoolSizes           []uint64
	// ExpireAddr sets the expire duration for idle addr. if addr is over
	// ExpireAddr, it will be removed.
	ExpireAddr           time.Duration
	ShrinkInterval       time.Duration
	LastSuffrageInterval time.Duration
	// MaxAddrs limits the number of network addresses; if new address over
	// MaxAddrs, the oldes addr will be removed.
	MaxAddrs uint64
}

func NewRateLimitHandlerArgs() *RateLimitHandlerArgs {
	return &RateLimitHandlerArgs{
		ExpireAddr:           time.Second * 33, //nolint:gomnd //...
		ShrinkInterval:       time.Second * 33, //nolint:gomnd // long enough
		LastSuffrageInterval: time.Second * 2,  //nolint:gomnd //...
		PoolSizes:            defaultRateLimitHandlerPoolSizes,
		GetLastSuffrageFunc: func(context.Context) (util.Hash, base.Suffrage, bool, error) {
			return nil, nil, false, nil
		},
		Rules:    NewRateLimiterRules(NewRateLimiterRuleMap(&defaultRateLimiter, nil)),
		MaxAddrs: math.MaxUint32,
	}
}

type RateLimitHandler struct {
	*util.ContextDaemon
	args         *RateLimitHandlerArgs
	rules        *RateLimiterRules
	pool         *addrPool
	lastSuffrage *util.Locked[[2]interface{}]
}

func NewRateLimitHandler(args *RateLimitHandlerArgs) (*RateLimitHandler, error) {
	pool, err := newAddrPool(args.PoolSizes)
	if err != nil {
		return nil, err
	}

	r := &RateLimitHandler{
		args:         args,
		pool:         pool,
		rules:        args.Rules,
		lastSuffrage: util.EmptyLocked[[2]interface{}](),
	}

	r.ContextDaemon = util.NewContextDaemon(r.start)

	return r, nil
}

func (r *RateLimitHandler) HandlerFunc(handlerPrefix string, handler quicstream.Handler) quicstream.Handler {
	return func(ctx context.Context, addr net.Addr, ir io.Reader, iw io.WriteCloser) (context.Context, error) {
		if l, allowed := r.allow(addr, handlerPrefix); !allowed {
			return ctx, ErrRateLimited.Errorf("prefix=%q limit=%v burst=%d", handlerPrefix, l.Limit(), l.Burst())
		}

		nctx, err := handler(ctx, addr, ir, iw)

		if nctx != nil {
			if node, ok := nctx.Value(isaacnetwork.ContextKeyNodeChallengedNode).(base.Address); ok {
				_ = r.AddNode(addr, node)
			}
		}

		return nctx, err
	}
}

func (r *RateLimitHandler) AddNode(addr net.Addr, node base.Address) bool {
	return r.pool.addNode(addr.String(), node)
}

func (r *RateLimitHandler) allow(addr net.Addr, handler string) (*RateLimiter, bool) {
	l := r.pool.rateLimiter(addr.String(), handler,
		r.rateLimiterFunc(addr, handler),
	)

	return l, l.Allow()
}

func (r *RateLimitHandler) shrink(ctx context.Context) (removed uint64) {
	point := time.Now().Add(r.args.ExpireAddr * -1)

	return r.pool.shrink(ctx, point, r.args.MaxAddrs)
}

func (r *RateLimitHandler) start(ctx context.Context) error {
	sticker := time.NewTicker(r.args.ShrinkInterval)
	defer sticker.Stop()

	lticker := time.NewTicker(r.args.LastSuffrageInterval)
	defer lticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-sticker.C:
			r.shrink(ctx)
		case <-lticker.C:
			_ = r.checkLastSuffrage(ctx)
		}
	}
}

func (r *RateLimitHandler) checkLastSuffrage(ctx context.Context) error {
	switch st, suf, found, err := r.args.GetLastSuffrageFunc(ctx); {
	case err != nil:
		return err
	case !found:
		return nil
	default:
		var updated bool

		_, _ = r.lastSuffrage.Set(func(i [2]interface{}, isempty bool) ([2]interface{}, error) {
			if isempty {
				updated = true

				return [2]interface{}{st, suf}, nil
			}

			if prev, ok := i[0].(util.Hash); ok && prev.Equal(st) {
				return [2]interface{}{}, util.ErrLockedSetIgnore.WithStack()
			}

			updated = true

			return [2]interface{}{st, suf}, nil
		})

		if updated {
			_ = r.rules.SetSuffrage(suf, st)
		}

		return nil
	}
}

func (r *RateLimitHandler) isOldNodeInSuffrage(node base.Address, checksum string) bool {
	i, isempty := r.lastSuffrage.Value()
	if isempty {
		return false
	}

	switch suf, ok := i[1].(base.Suffrage); {
	case !ok:
		return false
	case !suf.Exists(node):
		return len(checksum) > 0
	}

	switch i, ok := i[0].(util.Hash); {
	case !ok:
		return false
	default:
		return i.String() != checksum
	}
}

func (r *RateLimitHandler) rateLimiterFunc(
	addr net.Addr,
	handler string,
) func(_ *RateLimiter, found, created bool, node base.Address) (*RateLimiter, error) {
	return func(l *RateLimiter, found, created bool, node base.Address) (*RateLimiter, error) {
		if !found || l.UpdatedAt().Before(r.rules.UpdatedAt()) {
			// NOTE if RateLimiter is older than rule updated

			checksum, rule := r.rules.Rule(addr, node, handler)

			if !found {
				return NewRateLimiter(rule.Limit, rule.Burst, checksum), nil
			}

			_ = l.Update(rule.Limit, rule.Burst, checksum)

			return nil, util.ErrLockedSetIgnore.WithStack()
		}

		if found && node != nil {
			if r.isOldNodeInSuffrage(node, l.Checksum()) { // NOTE check node is in suffrage
				checksum, rule := r.rules.Rule(addr, node, handler)

				_ = l.Update(rule.Limit, rule.Burst, checksum)

				return nil, util.ErrLockedSetIgnore.WithStack()
			}
		}

		return nil, util.ErrLockedSetIgnore.WithStack()
	}
}

type RateLimiterRule struct {
	Limit rate.Limit
	Burst int
}

type RateLimiterRules struct {
	updatedAt  time.Time
	suffrage   RateLimiterRuleSet
	nodes      RateLimiterRuleSet
	nets       RateLimiterRuleSet
	defaultMap RateLimiterRuleMap
	sync.RWMutex
}

func NewRateLimiterRules(defaultMap RateLimiterRuleMap) *RateLimiterRules {
	return &RateLimiterRules{
		defaultMap: defaultMap,
		updatedAt:  time.Now(),
	}
}

func (r *RateLimiterRules) Rule(addr net.Addr, node base.Address, handler string) (string, RateLimiterRule) {
	checksum, l := r.rule(addr, node, handler)

	return checksum, l
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
	r.updatedAt = time.Now()

	return nil
}

func (r *RateLimiterRules) SetSuffrage(suf base.Suffrage, st util.Hash) error {
	i, ok := r.suffrage.(rulesetSuffrageSetter)
	if !ok {
		return errors.Errorf("suffrage rule set does not have SetSuffrage()")
	}

	_ = i.SetSuffrage(suf, st)

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

	if _, ok := l.(rulesetSuffrageSetter); !ok {
		return errors.Errorf("expected rulesetSuffrageSetter, but %T", l)
	}

	r.suffrage = l
	r.updatedAt = time.Now()

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
	r.updatedAt = time.Now()

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
	r.updatedAt = time.Now()

	return nil
}

func (r *RateLimiterRules) UpdatedAt() time.Time {
	r.RLock()
	defer r.RUnlock()

	return r.updatedAt
}

func (r *RateLimiterRules) rule(addr net.Addr, node base.Address, handler string) (string, RateLimiterRule) {
	r.RLock()
	defer r.RUnlock()

	if node != nil && r.nodes != nil {
		if checksum, l, found := r.nodes.Rule(node, handler); found {
			return checksum, l
		}
	}

	if r.nets != nil {
		if checksum, l, found := r.nets.Rule(addr, handler); found {
			return checksum, l
		}
	}

	if node != nil && r.suffrage != nil {
		if checksum, l, found := r.suffrage.Rule(node, handler); found {
			return checksum, l
		}
	}

	if l, found := r.defaultMap.Rule(handler); found {
		return "", l
	}

	return "", defaultRateLimiter
}

func (r *RateLimiterRules) IsValid([]byte) error {
	if r.defaultMap.IsEmpty() {
		return util.ErrInvalid.Errorf("empty default rule map")
	}

	return nil
}

type RateLimiterRuleSet interface {
	Rule(_ interface{}, handler string) (checksum string, _ RateLimiterRule, found bool)
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
	rules  map[ /* ipnet */ string]RateLimiterRuleMap
	ipnets []*net.IPNet
}

func NewNetRateLimiterRuleSet() NetRateLimiterRuleSet {
	return NetRateLimiterRuleSet{
		rules: map[string]RateLimiterRuleMap{},
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

func (rs *NetRateLimiterRuleSet) Add(ipnet *net.IPNet, rule RateLimiterRuleMap) *NetRateLimiterRuleSet {
	rs.ipnets = append(rs.ipnets, ipnet)
	rs.rules[ipnet.String()] = rule

	return rs
}

func (rs NetRateLimiterRuleSet) Rule(i interface{}, handler string) (string, RateLimiterRule, bool) {
	l, found := rs.rule(i, handler)

	return "", l, found
}

func (rs NetRateLimiterRuleSet) rule(i interface{}, handler string) (rule RateLimiterRule, _ bool) {
	var ip net.IP

	switch t := i.(type) {
	case *net.UDPAddr:
		ip = t.IP
	case net.IP:
		ip = t
	default:
		return rule, false
	}

	for i := range rs.ipnets {
		in := rs.ipnets[i]
		if !in.Contains(ip) {
			continue
		}

		m, found := rs.rules[in.String()]
		if !found {
			return rule, false
		}

		l, found := m.Rule(handler)
		if !found {
			return rule, false
		}

		return l, true
	}

	return rule, false
}

type NodeRateLimiterRuleSet struct {
	rules map[ /* node address */ string]RateLimiterRuleMap
}

func NewNodeRateLimiterRuleSet(
	rules map[string]RateLimiterRuleMap,
) NodeRateLimiterRuleSet {
	return NodeRateLimiterRuleSet{rules: rules}
}

func (NodeRateLimiterRuleSet) IsValid([]byte) error {
	return nil
}

func (rs NodeRateLimiterRuleSet) Rule(i interface{}, handler string) (string, RateLimiterRule, bool) {
	l, found := rs.rule(i, handler)

	return "", l, found
}

func (rs NodeRateLimiterRuleSet) rule(i interface{}, handler string) (rule RateLimiterRule, _ bool) {
	if len(rs.rules) < 1 {
		return rule, false
	}

	var node string

	switch t := i.(type) {
	case base.Address:
		node = t.String()
	case fmt.Stringer:
		node = t.String()
	case string:
		node = t
	default:
		return rule, false
	}

	m, found := rs.rules[node]
	if !found {
		return rule, false
	}

	return m.Rule(handler)
}

type SuffrageRateLimiterRuleSet struct {
	suf   base.Suffrage
	st    util.Hash
	rules RateLimiterRuleMap
	sync.RWMutex
}

func NewSuffrageRateLimiterRuleSet(rule RateLimiterRuleMap) *SuffrageRateLimiterRuleSet {
	return &SuffrageRateLimiterRuleSet{rules: rule}
}

func (*SuffrageRateLimiterRuleSet) IsValid([]byte) error {
	return nil
}

func (rs *SuffrageRateLimiterRuleSet) SetSuffrage(suf base.Suffrage, st util.Hash) bool {
	rs.Lock()
	defer rs.Unlock()

	if rs.st != nil && rs.st.Equal(st) {
		return false
	}

	rs.suf = suf
	rs.st = st

	return true
}

func (rs *SuffrageRateLimiterRuleSet) inSuffrage(node base.Address) bool {
	rs.RLock()
	defer rs.RUnlock()

	if rs.suf == nil {
		return false
	}

	return rs.suf.Exists(node)
}

func (rs *SuffrageRateLimiterRuleSet) Rule(i interface{}, handler string) (string, RateLimiterRule, bool) {
	l, found := rs.rule(i, handler)

	return rs.st.String(), l, found
}

func (rs *SuffrageRateLimiterRuleSet) rule(i interface{}, handler string) (rule RateLimiterRule, _ bool) {
	if rs.rules.IsEmpty() {
		return rule, false
	}

	node, ok := i.(base.Address)
	if !ok {
		return rule, false
	}

	if !rs.inSuffrage(node) {
		return rule, false
	}

	l, found := rs.rules.Rule(handler)

	return l, found
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
				return nil, util.ErrLockedSetIgnore.WithStack()
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
	f func(l *RateLimiter, found, created bool, node base.Address) (*RateLimiter, error),
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

			node, _ := p.addrs.Value(addr)

			l, _, _ = i.Set(
				handler,
				func(l *RateLimiter, found bool) (*RateLimiter, error) {
					return f(l, found, created, node)
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

type rulesetSuffrageSetter interface {
	SetSuffrage(base.Suffrage, util.Hash) bool
}
