package isaac

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/bluele/gcache"
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
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

func (p *dummyProposalPool) Proposal(facthash util.Hash) (base.ProposalSignedFact, bool, error) {
	p.RLock()
	defer p.RUnlock()

	return p.get(facthash.String())
}

func (p *dummyProposalPool) ProposalByPoint(point base.Point, proposer base.Address) (base.ProposalSignedFact, bool, error) {
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

func (p *dummyProposalPool) SetProposal(pr base.ProposalSignedFact) (bool, error) {
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

func (p *dummyProposalPool) get(facthash string) (base.ProposalSignedFact, bool, error) {
	switch i, err := p.facthashs.Get(facthash); {
	case errors.Is(err, gcache.KeyNotFoundError):
		return nil, false, nil
	case err != nil:
		return nil, false, nil
	case i == nil:
		return nil, false, nil
	default:
		return i.(base.ProposalSignedFact), true, nil
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

func (t *testBaseProposalSelector) TestNew() {
	suf, nodes := NewTestSuffrage(2, t.Local)

	pool := newDummyProposalPool(10)
	p := NewBaseProposalSelector(
		t.Local,
		t.NodePolicy,
		NewBlockBasedProposerSelector(
			func(base.Height) (util.Hash, error) {
				return valuehash.NewBytes([]byte("abc")), nil
			},
		),
		NewProposalMaker(t.Local, t.NodePolicy, func(context.Context) ([]util.Hash, error) { return nil, nil }, pool),
		func(base.Height) (base.Suffrage, bool, error) {
			return suf, true, nil
		},
		func() []base.Address {
			return nil
		},
		func(ctx context.Context, point base.Point, proposer base.Address) (base.ProposalSignedFact, error) {
			for i := range nodes {
				n := nodes[i]
				if !n.Address().Equal(proposer) {
					continue
				}

				return NewProposalMaker(n, t.NodePolicy, func(context.Context) ([]util.Hash, error) { return nil, nil }, pool).New(ctx, point)
			}

			return nil, errors.Errorf("proposer not found in suffrage")
		},
		pool,
	)

	t.T().Logf("suffrage len: %d", suf.Len())
	for i := range nodes {
		t.T().Logf("suffrage node: %q", nodes[i].Address())
	}

	point := base.RawPoint(66, 11)
	pr, err := p.Select(context.Background(), point)
	t.NoError(err)
	t.NotNil(pr)

	t.NoError(pr.IsValid(t.NodePolicy.NetworkID()))

	t.Equal(point, pr.Point())

	t.T().Logf("selected proposer: %q", pr.ProposalFact().Proposer())
	t.True(nodes[2].Address().Equal(pr.ProposalFact().Proposer()))
}

func (t *testBaseProposalSelector) TestOneNode() {
	suf, _ := NewTestSuffrage(0, t.Local)

	pool := newDummyProposalPool(10)
	p := NewBaseProposalSelector(
		t.Local,
		t.NodePolicy,
		NewBlockBasedProposerSelector(
			func(base.Height) (util.Hash, error) {
				return valuehash.RandomSHA512(), nil
			},
		),
		NewProposalMaker(t.Local, t.NodePolicy, func(context.Context) ([]util.Hash, error) { return nil, nil }, pool),
		func(base.Height) (base.Suffrage, bool, error) {
			return suf, true, nil
		},
		func() []base.Address {
			return nil
		},
		func(_ context.Context, point base.Point, proposer base.Address) (base.ProposalSignedFact, error) {
			return nil, errors.Errorf("proposer not found in suffrage")
		},
		pool,
	)

	t.T().Logf("suffrage len: %d", suf.Len())

	point := base.RawPoint(66, 11)
	pr, err := p.Select(context.Background(), point)
	t.NoError(err)
	t.NotNil(pr)

	t.NoError(pr.IsValid(t.NodePolicy.NetworkID()))

	t.Equal(point, pr.Point())

	t.T().Logf("selected proposer: %q", pr.ProposalFact().Proposer())
	t.True(t.Local.Address().Equal(pr.ProposalFact().Proposer()))
}

func (t *testBaseProposalSelector) TestUnknownSuffrage() {
	suf, _ := NewTestSuffrage(2, t.Local)

	pool := newDummyProposalPool(10)
	p := NewBaseProposalSelector(
		t.Local,
		t.NodePolicy,
		NewBlockBasedProposerSelector(
			func(base.Height) (util.Hash, error) {
				return valuehash.RandomSHA512(), nil
			},
		),
		NewProposalMaker(t.Local, t.NodePolicy, func(context.Context) ([]util.Hash, error) { return nil, nil }, pool),
		func(base.Height) (base.Suffrage, bool, error) {
			return nil, false, nil
		},
		func() []base.Address {
			return nil
		},
		func(_ context.Context, point base.Point, proposer base.Address) (base.ProposalSignedFact, error) {
			return nil, errors.Errorf("proposer not found in suffrage")
		},
		pool,
	)

	t.T().Logf("suffrage len: %d", suf.Len())

	point := base.RawPoint(66, 11)
	pr, err := p.Select(context.Background(), point)
	t.Error(err)
	t.Nil(pr)
	t.Contains(err.Error(), "suffrage not found for height")
}

func (t *testBaseProposalSelector) TestUnknownManifestHash() {
	suf, _ := NewTestSuffrage(2, t.Local)

	pool := newDummyProposalPool(10)
	p := NewBaseProposalSelector(
		t.Local,
		t.NodePolicy,
		NewBlockBasedProposerSelector(
			func(base.Height) (util.Hash, error) {
				return nil, util.NotFoundError.Errorf("hahaha")
			},
		),
		NewProposalMaker(t.Local, t.NodePolicy, func(context.Context) ([]util.Hash, error) { return nil, nil }, pool),
		func(base.Height) (base.Suffrage, bool, error) {
			return suf, true, nil
		},
		func() []base.Address {
			return nil
		},
		func(_ context.Context, point base.Point, proposer base.Address) (base.ProposalSignedFact, error) {
			return nil, errors.Errorf("proposer not found in suffrage")
		},
		pool,
	)

	t.T().Logf("suffrage len: %d", suf.Len())

	point := base.RawPoint(66, 11)
	pr, err := p.Select(context.Background(), point)
	t.Error(err)
	t.Nil(pr)
	t.True(errors.Is(err, util.NotFoundError))
	t.Contains(err.Error(), "hahaha")
}

func (t *testBaseProposalSelector) TestDeadNode() {
	suf, nodes := NewTestSuffrage(2, t.Local)

	pool := newDummyProposalPool(10)
	p := NewBaseProposalSelector(
		t.Local,
		t.NodePolicy,
		NewBlockBasedProposerSelector(
			func(base.Height) (util.Hash, error) {
				return valuehash.NewBytes([]byte("abc")), nil
			},
		),
		NewProposalMaker(t.Local, t.NodePolicy, func(context.Context) ([]util.Hash, error) { return nil, nil }, pool),
		func(base.Height) (base.Suffrage, bool, error) {
			return suf, true, nil
		},
		func() []base.Address {
			return []base.Address{nodes[2].Address()}
		},
		func(ctx context.Context, point base.Point, proposer base.Address) (base.ProposalSignedFact, error) {
			for i := range nodes {
				n := nodes[i]
				if !n.Address().Equal(proposer) {
					continue
				}

				return NewProposalMaker(n, t.NodePolicy, func(context.Context) ([]util.Hash, error) { return nil, nil }, pool).New(ctx, point)
			}

			return nil, errors.Errorf("proposer not found in suffrage")
		},
		pool,
	)

	t.T().Logf("suffrage len: %d", suf.Len())
	for i := range nodes {
		t.T().Logf("suffrage node: %q", nodes[i].Address())
	}

	point := base.RawPoint(66, 11)
	pr, err := p.Select(context.Background(), point)
	t.NoError(err)
	t.NotNil(pr)

	t.NoError(pr.IsValid(t.NodePolicy.NetworkID()))

	t.Equal(point, pr.Point())

	for i := range nodes {
		t.T().Logf("000suffrage node: %q", nodes[i].Address())
	}

	t.T().Logf("expected selected proposer: %q, but it's dead", nodes[2].Address())
	t.T().Logf("selected proposer: %q", pr.ProposalFact().Proposer())
	t.True(nodes[1].Address().Equal(pr.ProposalFact().Proposer()))
}

func (t *testBaseProposalSelector) TestFailedToReqeustByContext() {
	suf, nodes := NewTestSuffrage(2, t.Local)

	pool := newDummyProposalPool(10)
	p := NewBaseProposalSelector(
		t.Local,
		t.NodePolicy,
		NewBlockBasedProposerSelector(
			func(base.Height) (util.Hash, error) {
				return valuehash.NewBytes([]byte("abc")), nil
			},
		),
		NewProposalMaker(t.Local, t.NodePolicy, func(context.Context) ([]util.Hash, error) { return nil, nil }, pool),
		func(base.Height) (base.Suffrage, bool, error) {
			return suf, true, nil
		},
		func() []base.Address {
			return nil
		},
		func(ctx context.Context, point base.Point, proposer base.Address) (base.ProposalSignedFact, error) {
			if proposer.Equal(nodes[2].Address()) {
				return nil, context.Canceled
			}

			for i := range nodes {
				n := nodes[i]
				if !n.Address().Equal(proposer) {
					continue
				}

				return NewProposalMaker(n, t.NodePolicy, func(context.Context) ([]util.Hash, error) { return nil, nil }, pool).New(ctx, point)
			}

			return nil, errors.Errorf("proposer not found in suffrage")
		},
		pool,
	)

	t.T().Logf("suffrage len: %d", suf.Len())
	for i := range nodes {
		t.T().Logf("suffrage node: %q", nodes[i].Address())
	}

	point := base.RawPoint(66, 11)
	pr, err := p.Select(context.Background(), point)
	t.NoError(err)
	t.NotNil(pr)

	t.NoError(pr.IsValid(t.NodePolicy.NetworkID()))

	t.Equal(point, pr.Point())

	t.T().Logf("expected selected proposer: %q, but it's dead", nodes[2].Address())
	t.T().Logf("selected proposer: %q", pr.ProposalFact().Proposer())
	t.True(nodes[1].Address().Equal(pr.ProposalFact().Proposer()))
}

func (t *testBaseProposalSelector) TestAllFailedToReqeust() {
	suf, nodes := NewTestSuffrage(3)

	pool := newDummyProposalPool(10)
	p := NewBaseProposalSelector(
		t.Local,
		t.NodePolicy,
		NewBlockBasedProposerSelector(
			func(base.Height) (util.Hash, error) {
				return valuehash.NewBytes([]byte("abc")), nil
			},
		),
		NewProposalMaker(t.Local, t.NodePolicy, func(context.Context) ([]util.Hash, error) { return nil, nil }, pool),
		func(base.Height) (base.Suffrage, bool, error) {
			return suf, true, nil
		},
		func() []base.Address {
			return nil
		},
		func(_ context.Context, point base.Point, proposer base.Address) (base.ProposalSignedFact, error) {
			return nil, errors.Errorf("proposer not found in suffrage")
		},
		pool,
	)

	t.T().Logf("suffrage len: %d", suf.Len())
	for i := range nodes {
		t.T().Logf("suffrage node: %q", nodes[i].Address())
	}

	point := base.RawPoint(66, 11)
	pr, err := p.Select(context.Background(), point)
	t.Error(err)
	t.Nil(pr)

	t.Contains(err.Error(), "no valid nodes left")
}

func (t *testBaseProposalSelector) TestContextCanceled() {
	suf, nodes := NewTestSuffrage(3)

	requestdelay := time.Second
	_ = t.NodePolicy.SetTimeoutRequestProposal(time.Millisecond * 10)

	pool := newDummyProposalPool(10)
	p := NewBaseProposalSelector(
		t.Local,
		t.NodePolicy,
		NewBlockBasedProposerSelector(
			func(base.Height) (util.Hash, error) {
				return valuehash.NewBytes([]byte("abc")), nil
			},
		),
		NewProposalMaker(t.Local, t.NodePolicy, func(context.Context) ([]util.Hash, error) { return nil, nil }, pool),
		func(base.Height) (base.Suffrage, bool, error) {
			return suf, true, nil
		},
		func() []base.Address {
			return nil
		},
		func(ctx context.Context, point base.Point, proposer base.Address) (base.ProposalSignedFact, error) {
			done := make(chan struct{}, 1)

			var pr base.ProposalSignedFact
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

					pr, err = NewProposalMaker(n, t.NodePolicy, func(context.Context) ([]util.Hash, error) { return nil, nil }, pool).New(ctx, point)

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

	t.T().Logf("suffrage len: %d", suf.Len())
	for i := range nodes {
		t.T().Logf("suffrage node: %q", nodes[i].Address())
	}

	point := base.RawPoint(66, 11)
	pr, err := p.Select(context.Background(), point)
	t.Error(err)
	t.Nil(pr)

	t.Contains(err.Error(), "no valid nodes left")
}

func (t *testBaseProposalSelector) TestMainContextCanceled() {
	suf, nodes := NewTestSuffrage(3)

	requestdelay := time.Second * 10
	_ = t.NodePolicy.SetTimeoutRequestProposal(requestdelay * 2)

	pool := newDummyProposalPool(10)
	p := NewBaseProposalSelector(
		t.Local,
		t.NodePolicy,
		NewBlockBasedProposerSelector(
			func(base.Height) (util.Hash, error) {
				return valuehash.NewBytes([]byte("abc")), nil
			},
		),
		NewProposalMaker(t.Local, t.NodePolicy, func(context.Context) ([]util.Hash, error) { return nil, nil }, pool),
		func(base.Height) (base.Suffrage, bool, error) {
			return suf, true, nil
		},
		func() []base.Address {
			return nil
		},
		func(ctx context.Context, point base.Point, proposer base.Address) (base.ProposalSignedFact, error) {
			done := make(chan struct{}, 1)

			var pr base.ProposalSignedFact
			var err error
			go func() {
				<-time.After(requestdelay)

				for i := range nodes {
					n := nodes[i]
					if !n.Address().Equal(proposer) {
						continue
					}

					pr, err = NewProposalMaker(n, t.NodePolicy, func(context.Context) ([]util.Hash, error) { return nil, nil }, pool).New(ctx, point)

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

	t.T().Logf("suffrage len: %d", suf.Len())
	for i := range nodes {
		t.T().Logf("suffrage node: %q", nodes[i].Address())
	}

	ctx, cancel := context.WithCancel(context.Background())

	point := base.RawPoint(66, 11)

	done := make(chan struct{}, 1)

	var pr base.ProposalSignedFact
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
	t.Contains(err.Error(), "context canceled")
}

func TestBaseProposalSelector(tt *testing.T) {
	suite.Run(tt, new(testBaseProposalSelector))
}
