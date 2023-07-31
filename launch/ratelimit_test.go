package launch

import (
	"context"
	"math"
	mathrand "math/rand"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/network/quicstream"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
	"golang.org/x/time/rate"
)

type testRateLimiter struct {
	suite.Suite
}

func (t *testRateLimiter) TestAllow() {
	l := NewRateLimiter(rate.Every(time.Millisecond*333), 3, "")

	for range make([]int, 3) {
		t.True(l.Allow())
	}
	t.False(l.Allow())

	<-time.After(time.Millisecond * 333)
	t.True(l.Allow())
}

func TestRateLimiter(t *testing.T) {
	suite.Run(t, new(testRateLimiter))
}

type testRateLimitHandler struct {
	suite.Suite
}

func (t *testRateLimitHandler) printL(
	h *RateLimitHandler,
	f func(addr, prefix string, r *RateLimiter) bool,
) {
	h.pool.l.Traverse(func(addr string, m util.LockedMap[string, *RateLimiter]) bool {
		m.Traverse(func(prefix string, r *RateLimiter) bool {
			t.T().Logf("addr=%q prefix=%q limit=%v burst=%v tokens=%0.2f createdAt=%v checksum=%q", addr, prefix, r.Limit(), r.Burst(), r.Tokens(), r.UpdatedAt(), r.Checksum())

			return f(addr, prefix, r)
		})

		return true
	})
}

func (t *testRateLimitHandler) newargs() *RateLimitHandlerArgs {
	args := NewRateLimitHandlerArgs()
	args.ExpireAddr = time.Second
	args.PoolSizes = []uint64{2, 2}

	return args
}

func (t *testRateLimitHandler) TestNew() {
	h, err := NewRateLimitHandler(t.newargs())
	t.NoError(err)

	addr := quicstream.RandomUDPAddr()
	t.T().Log("addr:", addr)

	l, allowed := h.allow(addr, util.UUID().String())
	t.NotNil(l)
	t.True(allowed)

	t.printL(h, func(addr, prefix string, r *RateLimiter) bool {
		t.T().Logf("addr=%q prefix=%q limit=%v burst=%v createdAt=%v", addr, prefix, r.Limit(), r.Burst(), r.UpdatedAt())

		return true
	})
}

func (t *testRateLimitHandler) TestNotAllow() {
	h, err := NewRateLimitHandler(t.newargs())
	t.NoError(err)

	addr0 := quicstream.RandomUDPAddr()
	addr0.IP = net.IPv6zero
	t.T().Log("addr0:", addr0)
	addr1 := quicstream.RandomUDPAddr()
	addr1.IP = net.IPv6interfacelocalallnodes
	t.T().Log("addr1:", addr1)

	prefix := util.UUID().String()

	ruleset := NewNetRateLimiterRuleSet()
	ruleset.Add(
		&net.IPNet{IP: addr0.IP, Mask: net.CIDRMask(64, 128)},
		map[string]RateLimiterRule{
			prefix: {Limit: rate.Every(time.Minute), Burst: 1},
		},
	)

	t.NoError(h.Rules().SetNetRuleSet(ruleset))

	t.Run("check allow", func() {
		l, allowed := h.allow(addr0, prefix)
		t.NotNil(l)
		t.True(allowed)

		l, allowed = h.allow(addr1, prefix)
		t.NotNil(l)
		t.True(allowed)

		t.printL(h, func(addr, prefix string, r *RateLimiter) bool {
			if addr == addr0.String() {
				t.Equal(rate.Every(time.Minute), r.Limit())
				t.Equal(1, r.Burst())
				t.True(r.Tokens() < 1)
			}

			if addr == addr1.String() {
				t.Equal(h.Rules().DefaultLimiter().Limit, r.Limit())
				t.Equal(h.Rules().DefaultLimiter().Burst, r.Burst())
				t.True(r.Tokens() > 0)
			}

			return true
		})
	})

	t.Run("check again", func() {
		l, allowed := h.allow(addr0, prefix)
		t.NotNil(l)
		t.False(allowed)

		l, allowed = h.allow(addr1, prefix)
		t.NotNil(l)
		t.True(allowed)

		t.printL(h, func(addr, prefix string, r *RateLimiter) bool {
			if addr == addr0.String() {
				t.Equal(rate.Every(time.Minute), r.Limit())
				t.Equal(1, r.Burst())
				t.True(r.Tokens() < 1)
			}

			if addr == addr1.String() {
				t.Equal(h.Rules().DefaultLimiter().Limit, r.Limit())
				t.Equal(h.Rules().DefaultLimiter().Burst, r.Burst())
				t.True(r.Tokens() > 0)
			}

			return true
		})
	})
}

func (t *testRateLimitHandler) TestRuleSetUpdated() {
	h, err := NewRateLimitHandler(t.newargs())
	t.NoError(err)

	addr := quicstream.RandomUDPAddr()
	t.T().Log("addr:", addr)

	prefix := util.UUID().String()

	for range make([]int, h.Rules().DefaultLimiter().Burst) {
		l, allowed := h.allow(addr, prefix)
		t.NotNil(l)
		t.True(allowed)
	}

	t.printL(h, func(addr, prefix string, r *RateLimiter) bool {
		t.Equal(h.Rules().DefaultLimiter().Limit, r.Limit())
		t.Equal(h.Rules().DefaultLimiter().Burst, r.Burst())
		t.True(r.Tokens() < 1)

		return true
	})

	newburst := h.Rules().DefaultLimiter().Burst + 1

	t.T().Log("ruleset updated; rate limiters will be resetted")
	ruleset := NewNetRateLimiterRuleSet()
	ruleset.Add(
		&net.IPNet{IP: addr.IP, Mask: net.CIDRMask(64, 128)},
		map[string]RateLimiterRule{
			prefix: {Limit: rate.Every(time.Minute), Burst: newburst},
		},
	)

	t.NoError(h.Rules().SetNetRuleSet(ruleset))

	t.T().Log("check allow")
	l, allowed := h.allow(addr, prefix)
	t.NotNil(l)
	t.True(allowed)

	t.printL(h, func(addr, prefix string, r *RateLimiter) bool {
		t.Equal(rate.Every(time.Minute), r.Limit())
		t.Equal(newburst, r.Burst())
		t.True(r.Tokens() >= 0, "tokens=%0.9f")

		return true
	})
}

func (t *testRateLimitHandler) TestSuffrageNode() {
	args := t.newargs()
	args.Rules = NewRateLimiterRules(RateLimiterRule{Limit: rate.Every(time.Second), Burst: 1})

	h, err := NewRateLimitHandler(args)
	t.NoError(err)

	prefix := util.UUID().String()

	sufst := valuehash.RandomSHA256()
	suf, members := isaac.NewTestSuffrage(2)

	args.GetLastSuffrageFunc = func(context.Context) (util.Hash, base.Suffrage, bool, error) {
		return sufst, suf, true, nil
	}

	node := members[0].Address()
	nodeLimiter := RateLimiterRule{Limit: rate.Every(time.Second * 3), Burst: 3}

	ruleset := NewSuffrageRateLimiterRuleSet(suf, map[string]RateLimiterRule{
		prefix: nodeLimiter,
	}, RateLimiterRule{})
	t.NoError(h.Rules().SetSuffrageRuleSet(ruleset))

	t.NoError(h.checkLastSuffrage(context.Background()))

	addr := quicstream.RandomUDPAddr()
	t.T().Log("addr:", addr)

	t.Run("check allow; node not in suffrage", func() {
		l, allowed := h.allow(addr, prefix)
		t.NotNil(l)
		t.True(allowed)

		t.printL(h, func(addr, prefix string, r *RateLimiter) bool {
			t.Equal(h.Rules().DefaultLimiter().Limit, r.Limit())
			t.Equal(h.Rules().DefaultLimiter().Burst, r.Burst())
			t.True(r.Tokens() < 1)

			return true
		})

		l, allowed = h.allow(addr, prefix)
		t.NotNil(l)
		t.False(allowed)
	})

	t.Run("add node", func() {
		t.True(h.AddNode(addr, node))
	})

	t.Run("check allow; node in suffrage", func() {
		l, allowed := h.allow(addr, prefix)
		t.NotNil(l)
		t.True(allowed)

		t.printL(h, func(addr, prefix string, r *RateLimiter) bool {
			t.Equal(nodeLimiter.Limit, r.Limit())
			t.Equal(nodeLimiter.Burst, r.Burst())
			t.True(r.Tokens() > 0)

			return true
		})
	})

	t.Run("check allow; node out of suffrage", func() {
		nsufst := valuehash.RandomSHA256()
		nsuf, _ := isaac.NewSuffrage(suf.Nodes()[1:])

		args.GetLastSuffrageFunc = func(context.Context) (util.Hash, base.Suffrage, bool, error) {
			return nsufst, nsuf, true, nil
		}

		t.NoError(h.checkLastSuffrage(context.Background()))

		l, allowed := h.allow(addr, prefix)
		t.NotNil(l)
		t.True(allowed)

		t.printL(h, func(addr, prefix string, r *RateLimiter) bool {
			t.Equal(h.Rules().DefaultLimiter().Limit, r.Limit())
			t.Equal(h.Rules().DefaultLimiter().Burst, r.Burst())
			t.True(r.Tokens() < 1)

			return true
		})

		l, allowed = h.allow(addr, prefix)
		t.NotNil(l)
		t.False(allowed)
	})

	t.Run("check allow; node in suffrage back", func() {
		args.GetLastSuffrageFunc = func(context.Context) (util.Hash, base.Suffrage, bool, error) {
			return sufst, suf, true, nil
		}

		t.NoError(h.checkLastSuffrage(context.Background()))

		l, allowed := h.allow(addr, prefix)
		t.NotNil(l)
		t.True(allowed)

		t.printL(h, func(addr, prefix string, r *RateLimiter) bool {
			t.Equal(nodeLimiter.Limit, r.Limit())
			t.Equal(nodeLimiter.Burst, r.Burst())
			t.True(r.Tokens() > 0)

			return true
		})
	})
}

func (t *testRateLimitHandler) TestConcurrent() {
	sufst := valuehash.RandomSHA256()
	suf, members := isaac.NewTestSuffrage(3)

	nodes := make([]string, len(members)+3)
	for i := range members {
		nodes[i] = members[i].Address().String()
	}

	for i := len(members); i < len(nodes); i++ {
		nodes[i] = base.RandomAddress("").String()
	}

	nodeAddrs := map[string]net.Addr{}
	for i := range nodes {
		node := nodes[i]
		addr := quicstream.RandomUDPAddr()

		nodeAddrs[node] = addr
		t.T().Logf("node=%q addr=%q", node, addr)
	}

	prefixes := make([]string, 3)
	for i := range prefixes {
		prefixes[i] = util.UUID().String()
	}

	args := t.newargs()
	args.ExpireAddr = time.Millisecond * 11
	// args.ShrinkInterval = time.Millisecond * 11
	args.Rules = NewRateLimiterRules(RateLimiterRule{Limit: rate.Every(time.Second), Burst: math.MaxInt})
	args.MaxAddrs = 3

	args.GetLastSuffrageFunc = func(context.Context) (util.Hash, base.Suffrage, bool, error) {
		return sufst, suf, true, nil
	}

	h, err := NewRateLimitHandler(args)
	t.NoError(err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	t.NoError(h.Start(ctx))
	defer h.Stop()

	for i := range members {
		member := members[i].Address()
		addr := nodeAddrs[member.String()]
		prefix := prefixes[mathrand.Intn(len(prefixes))]

		_, allowed := h.allow(addr, prefix)
		t.True(allowed)

		t.True(h.AddNode(addr, member))
	}

	rules := map[string]RateLimiterRule{}
	for i := range prefixes {
		rules[prefixes[i]] = RateLimiterRule{Limit: rate.Every(time.Second), Burst: math.MaxInt}
	}

	ruleset := NewSuffrageRateLimiterRuleSet(suf, rules, RateLimiterRule{})
	t.NoError(h.Rules().SetSuffrageRuleSet(ruleset))
	t.NoError(h.checkLastSuffrage(context.Background()))

	var tloglock sync.Mutex
	tlog := func(a ...interface{}) {
		tloglock.Lock()
		defer tloglock.Unlock()

		t.T().Log(a...)
	}

	t.Run("check allow and shrink", func() {
		worker, err := util.NewErrgroupWorker(ctx, int64(1<<10))
		t.NoError(err)
		defer worker.Close()

		go func() {
			for i := range make([]int, 1<<13) {
				prefix := prefixes[mathrand.Intn(len(prefixes))]
				node := nodes[mathrand.Intn(len(nodes))]
				addr := nodeAddrs[node]

				_ = worker.NewJob(func(context.Context, uint64) error {
					_, allowed := h.allow(addr, prefix)
					if !allowed {
						return errors.Errorf("not allowed")
					}

					<-time.After(time.Millisecond * 33)

					return nil
				})

				if i%1000 == 0 {
					_ = worker.NewJob(func(_ context.Context, i uint64) error {
						removed := h.shrink(ctx)
						tlog("\t> shrink:", i, removed)

						return nil
					})
				}
			}

			worker.Done()
		}()

		t.NoError(worker.Wait())
	})

	t.printL(h, func(addr, prefix string, r *RateLimiter) bool { return true })
}

func (t *testRateLimitHandler) TestMaxAddr() {
	args := t.newargs()
	args.MaxAddrs = 3

	h, err := NewRateLimitHandler(args)
	t.NoError(err)

	prevs := make([]string, args.MaxAddrs)
	for i := range make([]int, args.MaxAddrs) {
		addr := quicstream.RandomUDPAddr()
		_, allowed := h.allow(addr, util.UUID().String())
		t.True(allowed)

		removed := h.pool.shrinkAddrsHistory(args.MaxAddrs)

		t.T().Logf("history=%d removed=%d", h.pool.addrsHistory.Len(), removed)

		prevs[i] = addr.String()
	}

	t.Run("over max", func() {
		for range make([]int, 3) {
			_, allowed := h.allow(quicstream.RandomUDPAddr(), util.UUID().String())
			t.True(allowed)

			removed := h.pool.shrinkAddrsHistory(args.MaxAddrs)

			t.T().Logf("history=%d removed=%d", h.pool.addrsHistory.Len(), removed)

			t.T().Log("history:", h.pool.addrsHistory.Len())
			t.Equal(int(args.MaxAddrs), h.pool.addrsHistory.Len())
		}
	})

	t.Run("previous addrs removed", func() {
		for i := range prevs {
			addr := prevs[i]

			t.False(h.pool.l.Exists(addr), "l")
			t.False(h.pool.addrNodes.Exists(addr), "addrNodes")
			t.False(h.pool.lastAccessedAt.Exists(addr), "lastAccessedAt")
		}
	})
}

func TestRateLimitHandler(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testRateLimitHandler))
}
