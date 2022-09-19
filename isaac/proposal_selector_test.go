package isaac

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bluele/gcache"
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
)

type dummyProposalPool struct {
	sync.RWMutex
	facthashs gcache.Cache
	points    gcache.Cache
}

func newDummyProposalPool(size int) *dummyProposalPool {
	return &dummyProposalPool{
		facthashs: gcache.New(size).LRU().Build(),
		points:    gcache.New(size).LRU().Build(),
	}
}

func (p *dummyProposalPool) Proposal(facthash util.Hash) (base.ProposalSignFact, bool, error) {
	p.RLock()
	defer p.RUnlock()

	return p.get(facthash.String())
}

func (p *dummyProposalPool) ProposalBytes(facthash util.Hash) (hint.Hint, []byte, []byte, bool, error) {
	p.RLock()
	defer p.RUnlock()

	pr, found, err := p.get(facthash.String())
	if err != nil || !found {
		return hint.Hint{}, nil, nil, false, err
	}

	b, err := util.MarshalJSON(pr)
	if err != nil {
		return hint.Hint{}, nil, nil, false, err
	}

	return jsonenc.JSONEncoderHint, nil, b, true, nil
}

func (p *dummyProposalPool) ProposalByPoint(point base.Point, proposer base.Address) (base.ProposalSignFact, bool, error) {
	p.RLock()
	defer p.RUnlock()

	switch i, err := p.points.Get(p.pointkey(point, proposer)); {
	case errors.Is(err, gcache.KeyNotFoundError):
		return nil, false, nil
	case err != nil:
		return nil, false, nil
	case i == nil:
		return nil, false, nil
	default:
		return p.get(i.(string))
	}
}

func (p *dummyProposalPool) SetProposal(pr base.ProposalSignFact) (bool, error) {
	p.Lock()
	defer p.Unlock()

	facthash := pr.Fact().Hash().String()
	if p.facthashs.Has(facthash) {
		return false, nil
	}

	_ = p.facthashs.Set(facthash, pr)
	_ = p.points.Set(p.pointkey(pr.Point(), pr.ProposalFact().Proposer()), facthash)

	return true, nil
}

func (p *dummyProposalPool) get(facthash string) (base.ProposalSignFact, bool, error) {
	switch i, err := p.facthashs.Get(facthash); {
	case errors.Is(err, gcache.KeyNotFoundError):
		return nil, false, nil
	case err != nil:
		return nil, false, nil
	case i == nil:
		return nil, false, nil
	default:
		return i.(base.ProposalSignFact), true, nil
	}
}

func (p *dummyProposalPool) pointkey(point base.Point, proposer base.Address) string {
	return fmt.Sprintf("%d-%d-%s",
		point.Height(),
		point.Round(),
		proposer.String(),
	)
}

type testBaseProposalSelector struct {
	BaseTestBallots
}

func (t *testBaseProposalSelector) SetupTest() {
	t.BaseTestBallots.SetupTest()
}

func (t *testBaseProposalSelector) newNodes(n int, extra ...base.Node) []base.Node {
	nodes := make([]base.Node, n+1)

	for i := range nodes {
		nodes[i] = RandomLocalNode()
	}

	for i := range extra {
		nodes[n+i] = extra[i]
	}

	return nodes
}

func (t *testBaseProposalSelector) TestNew() {
	nodes := t.newNodes(2, t.Local)

	pool := newDummyProposalPool(10)
	p := NewBaseProposalSelector(
		t.Local,
		t.LocalParams,
		NewBlockBasedProposerSelector(
			func(base.Height) (util.Hash, error) {
				return valuehash.NewBytes([]byte("abc")), nil
			},
		),
		NewProposalMaker(t.Local, t.LocalParams, func(context.Context, base.Height) ([]util.Hash, error) { return nil, nil }, pool),
		func(base.Height) ([]base.Node, bool, error) {
			return nodes, true, nil
		},
		func(ctx context.Context, point base.Point, proposer base.Address) (base.ProposalSignFact, error) {
			for i := range nodes {
				n := nodes[i]
				if !n.Address().Equal(proposer) {
					continue
				}

				return NewProposalMaker(n.(base.LocalNode), t.LocalParams, func(context.Context, base.Height) ([]util.Hash, error) { return nil, nil }, pool).New(ctx, point)
			}

			return nil, errors.Errorf("proposer not found in suffrage")
		},
		pool,
	)

	t.T().Logf("available nodes: %d", len(nodes))

	point := base.RawPoint(66, 11)
	pr, err := p.Select(context.Background(), point)
	t.NoError(err)
	t.NotNil(pr)

	t.NoError(pr.IsValid(t.LocalParams.NetworkID()))

	t.Equal(point, pr.Point())

	t.T().Logf("selected proposer: %q", pr.ProposalFact().Proposer())
	t.True(nodes[2].Address().Equal(pr.ProposalFact().Proposer()))
}

func (t *testBaseProposalSelector) TestOneNode() {
	nodes := []base.Node{t.Local}

	pool := newDummyProposalPool(10)
	p := NewBaseProposalSelector(
		t.Local,
		t.LocalParams,
		NewBlockBasedProposerSelector(
			func(base.Height) (util.Hash, error) {
				return valuehash.RandomSHA512(), nil
			},
		),
		NewProposalMaker(t.Local, t.LocalParams, func(context.Context, base.Height) ([]util.Hash, error) { return nil, nil }, pool),
		func(base.Height) ([]base.Node, bool, error) {
			return nodes, true, nil
		},
		func(_ context.Context, point base.Point, proposer base.Address) (base.ProposalSignFact, error) {
			return nil, errors.Errorf("proposer not found in suffrage")
		},
		pool,
	)

	t.T().Logf("available nodes: %d", len(nodes))

	point := base.RawPoint(66, 11)
	pr, err := p.Select(context.Background(), point)
	t.NoError(err)
	t.NotNil(pr)

	t.NoError(pr.IsValid(t.LocalParams.NetworkID()))

	t.Equal(point, pr.Point())

	t.T().Logf("selected proposer: %q", pr.ProposalFact().Proposer())
	t.True(t.Local.Address().Equal(pr.ProposalFact().Proposer()))
}

func (t *testBaseProposalSelector) TestUnknownSuffrage() {
	pool := newDummyProposalPool(10)
	p := NewBaseProposalSelector(
		t.Local,
		t.LocalParams,
		NewBlockBasedProposerSelector(
			func(base.Height) (util.Hash, error) {
				return valuehash.RandomSHA512(), nil
			},
		),
		NewProposalMaker(t.Local, t.LocalParams, func(context.Context, base.Height) ([]util.Hash, error) { return nil, nil }, pool),
		func(base.Height) ([]base.Node, bool, error) {
			return nil, false, nil
		},
		func(_ context.Context, point base.Point, proposer base.Address) (base.ProposalSignFact, error) {
			return nil, errors.Errorf("proposer not found in suffrage")
		},
		pool,
	)

	point := base.RawPoint(66, 11)
	pr, err := p.Select(context.Background(), point)
	t.Error(err)
	t.Nil(pr)
	t.ErrorContains(err, "suffrage not found for height")
}

func (t *testBaseProposalSelector) TestUnknownManifestHash() {
	nodes := t.newNodes(2, t.Local)

	pool := newDummyProposalPool(10)
	p := NewBaseProposalSelector(
		t.Local,
		t.LocalParams,
		NewBlockBasedProposerSelector(
			func(base.Height) (util.Hash, error) {
				return nil, util.ErrNotFound.Errorf("hahaha")
			},
		),
		NewProposalMaker(t.Local, t.LocalParams, func(context.Context, base.Height) ([]util.Hash, error) { return nil, nil }, pool),
		func(base.Height) ([]base.Node, bool, error) {
			return nodes, true, nil
		},
		func(_ context.Context, point base.Point, proposer base.Address) (base.ProposalSignFact, error) {
			return nil, errors.Errorf("proposer not found in suffrage")
		},
		pool,
	)

	t.T().Logf("available nodes: %d", len(nodes))

	point := base.RawPoint(66, 11)
	pr, err := p.Select(context.Background(), point)
	t.Error(err)
	t.Nil(pr)
	t.True(errors.Is(err, util.ErrNotFound))
	t.ErrorContains(err, "hahaha")
}

func (t *testBaseProposalSelector) TestFailedToReqeustByContext() {
	nodes := t.newNodes(3)
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].Address().String() < nodes[j].Address().String()
	})

	var touched int64

	pool := newDummyProposalPool(10)
	p := NewBaseProposalSelector(
		t.Local,
		t.LocalParams,
		NewFixedProposerSelector(
			func(base.Point, []base.Node) (base.Node, error) {
				if atomic.LoadInt64(&touched) < 1 {
					atomic.AddInt64(&touched, 1)

					return nodes[2], nil
				}

				return nodes[1], nil
			},
		),
		NewProposalMaker(t.Local, t.LocalParams, func(context.Context, base.Height) ([]util.Hash, error) { return nil, nil }, pool),
		func(base.Height) ([]base.Node, bool, error) {
			return nodes, true, nil
		},
		func(ctx context.Context, point base.Point, proposer base.Address) (base.ProposalSignFact, error) {
			if proposer.Equal(nodes[2].Address()) {
				return nil, context.Canceled
			}

			for i := range nodes {
				n := nodes[i]
				if !n.Address().Equal(proposer) {
					continue
				}

				return NewProposalMaker(n.(base.LocalNode), t.LocalParams, func(context.Context, base.Height) ([]util.Hash, error) { return nil, nil }, pool).New(ctx, point)
			}

			return nil, errors.Errorf("proposer not found in suffrage")
		},
		pool,
	)

	t.T().Logf("available nodes: %d", len(nodes))
	for i := range nodes {
		t.T().Logf("available node: %d, %v", i, nodes[i].Address())
	}

	point := base.RawPoint(66, 11)
	pr, err := p.Select(context.Background(), point)
	t.NoError(err)
	t.NotNil(pr)

	t.NoError(pr.IsValid(t.LocalParams.NetworkID()))

	t.Equal(point, pr.Point())

	t.T().Logf("expected selected proposer: %q, but it's dead", nodes[2].Address())
	t.T().Logf("selected proposer: %q", pr.ProposalFact().Proposer())
	t.True(nodes[1].Address().Equal(pr.ProposalFact().Proposer()))
}

func (t *testBaseProposalSelector) TestAllFailedToReqeust() {
	nodes := t.newNodes(3)

	pool := newDummyProposalPool(10)
	p := NewBaseProposalSelector(
		t.Local,
		t.LocalParams,
		NewBlockBasedProposerSelector(
			func(base.Height) (util.Hash, error) {
				return valuehash.NewBytes([]byte("abc")), nil
			},
		),
		NewProposalMaker(t.Local, t.LocalParams, func(context.Context, base.Height) ([]util.Hash, error) { return nil, nil }, pool),
		func(base.Height) ([]base.Node, bool, error) {
			return nodes, true, nil
		},
		func(_ context.Context, point base.Point, proposer base.Address) (base.ProposalSignFact, error) {
			return nil, errors.Errorf("proposer not found in suffrage")
		},
		pool,
	)

	t.T().Logf("available nodes: %d", len(nodes))

	point := base.RawPoint(66, 11)
	pr, err := p.Select(context.Background(), point)
	t.Error(err)
	t.Nil(pr)

	t.ErrorContains(err, "no valid nodes left")
}

func (t *testBaseProposalSelector) TestContextCanceled() {
	nodes := t.newNodes(3)

	requestdelay := time.Second
	_ = t.LocalParams.SetTimeoutRequestProposal(time.Millisecond * 10)

	pool := newDummyProposalPool(10)
	p := NewBaseProposalSelector(
		t.Local,
		t.LocalParams,
		NewBlockBasedProposerSelector(
			func(base.Height) (util.Hash, error) {
				return valuehash.NewBytes([]byte("abc")), nil
			},
		),
		NewProposalMaker(t.Local, t.LocalParams, func(context.Context, base.Height) ([]util.Hash, error) { return nil, nil }, pool),
		func(base.Height) ([]base.Node, bool, error) {
			return nodes, true, nil
		},
		func(ctx context.Context, point base.Point, proposer base.Address) (base.ProposalSignFact, error) {
			done := make(chan struct{}, 1)

			var pr base.ProposalSignFact
			var err error
			go func() {
				select {
				case <-ctx.Done():
					return
				case <-time.After(requestdelay):
				}

				for i := range nodes {
					n := nodes[i]
					if !n.Address().Equal(proposer) {
						continue
					}

					pr, err = NewProposalMaker(n.(base.LocalNode), t.LocalParams, func(context.Context, base.Height) ([]util.Hash, error) { return nil, nil }, pool).New(ctx, point)

					done <- struct{}{}

					break
				}
			}()

			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-done:
				switch {
				case err != nil:
					return nil, err
				case pr == nil:
					return nil, errors.Errorf("proposer not found in suffrage")
				default:
					return pr, nil
				}
			}
		},
		pool,
	)

	t.T().Logf("available nodes: %d", len(nodes))

	point := base.RawPoint(66, 11)
	pr, err := p.Select(context.Background(), point)
	t.Error(err)
	t.Nil(pr)

	t.ErrorContains(err, "no valid nodes left")
}

func (t *testBaseProposalSelector) TestMainContextCanceled() {
	nodes := t.newNodes(3)

	requestdelay := time.Second * 10
	_ = t.LocalParams.SetTimeoutRequestProposal(requestdelay * 2)

	pool := newDummyProposalPool(10)
	p := NewBaseProposalSelector(
		t.Local,
		t.LocalParams,
		NewBlockBasedProposerSelector(
			func(base.Height) (util.Hash, error) {
				return valuehash.NewBytes([]byte("abc")), nil
			},
		),
		NewProposalMaker(t.Local, t.LocalParams, func(context.Context, base.Height) ([]util.Hash, error) { return nil, nil }, pool),
		func(base.Height) ([]base.Node, bool, error) {
			return nodes, true, nil
		},
		func(ctx context.Context, point base.Point, proposer base.Address) (base.ProposalSignFact, error) {
			done := make(chan struct{}, 1)

			var pr base.ProposalSignFact
			var err error
			go func() {
				<-time.After(requestdelay)

				for i := range nodes {
					n := nodes[i]
					if !n.Address().Equal(proposer) {
						continue
					}

					pr, err = NewProposalMaker(n.(base.LocalNode), t.LocalParams, func(context.Context, base.Height) ([]util.Hash, error) { return nil, nil }, pool).New(ctx, point)

					done <- struct{}{}

					break
				}
			}()

			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-done:
				switch {
				case err != nil:
					return nil, err
				case pr == nil:
					return nil, errors.Errorf("proposer not found in suffrage")
				default:
					return pr, nil
				}
			}
		},
		pool,
	)

	t.T().Logf("available nodes: %d", len(nodes))

	ctx, cancel := context.WithCancel(context.Background())

	point := base.RawPoint(66, 11)

	done := make(chan struct{}, 1)

	var pr base.ProposalSignFact
	var err error
	go func() {
		pr, err = p.Select(ctx, point)

		done <- struct{}{}
	}()

	<-time.After(time.Millisecond * 300)
	cancel()

	<-done

	t.Nil(pr)
	t.True(errors.Is(err, context.Canceled))
	t.ErrorContains(err, "context canceled")
}

func TestBaseProposalSelector(tt *testing.T) {
	suite.Run(tt, new(testBaseProposalSelector))
}
