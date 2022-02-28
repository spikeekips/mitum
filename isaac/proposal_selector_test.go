package isaac

import (
	"context"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
)

type testBaseProposalSelector struct {
	baseTestHandler
}

func (t *testBaseProposalSelector) SetupTest() {
	t.baseTestHandler.SetupTest()
}

func (t *testBaseProposalSelector) TestNew() {
	suf, nodes := newTestSuffrage(2, t.local)

	p := NewBaseProposalSelector(
		t.local,
		t.policy,
		NewBlockBasedProposerSelector(
			func(base.Height) (util.Hash, error) {
				return valuehash.NewBytes([]byte("abc")), nil
			},
		),
		NewProposalMaker(t.local, t.policy, func(context.Context) ([]util.Hash, error) { return nil, nil }),
		func(base.Height) base.Suffrage {
			return suf
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

				return NewProposalMaker(n, t.policy, func(context.Context) ([]util.Hash, error) { return nil, nil }).New(ctx, point)
			}

			return nil, errors.Errorf("proposer not found in suffrage")
		},
		nil,
	)

	t.T().Logf("suffrage len: %d", suf.Len())
	for i := range nodes {
		t.T().Logf("suffrage node: %q", nodes[i].Address())
	}

	point := base.RawPoint(66, 11)
	pr, err := p.Select(context.Background(), point)
	t.NoError(err)
	t.NotNil(pr)

	t.NoError(pr.IsValid(t.policy.NetworkID()))

	t.Equal(point, pr.Point())

	t.T().Logf("selected proposer: %q", pr.ProposalFact().Proposer())
	t.True(nodes[2].Address().Equal(pr.ProposalFact().Proposer()))
}

func (t *testBaseProposalSelector) TestOneNode() {
	suf, _ := newTestSuffrage(0, t.local)

	p := NewBaseProposalSelector(
		t.local,
		t.policy,
		NewBlockBasedProposerSelector(
			func(base.Height) (util.Hash, error) {
				return valuehash.RandomSHA512(), nil
			},
		),
		NewProposalMaker(t.local, t.policy, func(context.Context) ([]util.Hash, error) { return nil, nil }),
		func(base.Height) base.Suffrage {
			return suf
		},
		func() []base.Address {
			return nil
		},
		func(_ context.Context, point base.Point, proposer base.Address) (base.ProposalSignedFact, error) {
			return nil, errors.Errorf("proposer not found in suffrage")
		},
		nil,
	)

	t.T().Logf("suffrage len: %d", suf.Len())

	point := base.RawPoint(66, 11)
	pr, err := p.Select(context.Background(), point)
	t.NoError(err)
	t.NotNil(pr)

	t.NoError(pr.IsValid(t.policy.NetworkID()))

	t.Equal(point, pr.Point())

	t.T().Logf("selected proposer: %q", pr.ProposalFact().Proposer())
	t.True(t.local.Address().Equal(pr.ProposalFact().Proposer()))
}

func (t *testBaseProposalSelector) TestUnknownSuffrage() {
	suf, _ := newTestSuffrage(2, t.local)

	p := NewBaseProposalSelector(
		t.local,
		t.policy,
		NewBlockBasedProposerSelector(
			func(base.Height) (util.Hash, error) {
				return valuehash.RandomSHA512(), nil
			},
		),
		NewProposalMaker(t.local, t.policy, func(context.Context) ([]util.Hash, error) { return nil, nil }),
		func(base.Height) base.Suffrage {
			return nil
		},
		func() []base.Address {
			return nil
		},
		func(_ context.Context, point base.Point, proposer base.Address) (base.ProposalSignedFact, error) {
			return nil, errors.Errorf("proposer not found in suffrage")
		},
		nil,
	)

	t.T().Logf("suffrage len: %d", suf.Len())

	point := base.RawPoint(66, 11)
	pr, err := p.Select(context.Background(), point)
	t.Error(err)
	t.Nil(pr)
	t.Contains(err.Error(), "failed to get suffrage for height")
}

func (t *testBaseProposalSelector) TestUnknownManifestHash() {
	suf, _ := newTestSuffrage(2, t.local)

	p := NewBaseProposalSelector(
		t.local,
		t.policy,
		NewBlockBasedProposerSelector(
			func(base.Height) (util.Hash, error) {
				return nil, util.NotFoundError.Errorf("hahaha")
			},
		),
		NewProposalMaker(t.local, t.policy, func(context.Context) ([]util.Hash, error) { return nil, nil }),
		func(base.Height) base.Suffrage {
			return suf
		},
		func() []base.Address {
			return nil
		},
		func(_ context.Context, point base.Point, proposer base.Address) (base.ProposalSignedFact, error) {
			return nil, errors.Errorf("proposer not found in suffrage")
		},
		nil,
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
	suf, nodes := newTestSuffrage(2, t.local)

	p := NewBaseProposalSelector(
		t.local,
		t.policy,
		NewBlockBasedProposerSelector(
			func(base.Height) (util.Hash, error) {
				return valuehash.NewBytes([]byte("abc")), nil
			},
		),
		NewProposalMaker(t.local, t.policy, func(context.Context) ([]util.Hash, error) { return nil, nil }),
		func(base.Height) base.Suffrage {
			return suf
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

				return NewProposalMaker(n, t.policy, func(context.Context) ([]util.Hash, error) { return nil, nil }).New(ctx, point)
			}

			return nil, errors.Errorf("proposer not found in suffrage")
		},
		nil,
	)

	t.T().Logf("suffrage len: %d", suf.Len())
	for i := range nodes {
		t.T().Logf("suffrage node: %q", nodes[i].Address())
	}

	point := base.RawPoint(66, 11)
	pr, err := p.Select(context.Background(), point)
	t.NoError(err)
	t.NotNil(pr)

	t.NoError(pr.IsValid(t.policy.NetworkID()))

	t.Equal(point, pr.Point())

	for i := range nodes {
		t.T().Logf("000suffrage node: %q", nodes[i].Address())
	}

	t.T().Logf("expected selected proposer: %q, but it's dead", nodes[2].Address())
	t.T().Logf("selected proposer: %q", pr.ProposalFact().Proposer())
	t.True(nodes[1].Address().Equal(pr.ProposalFact().Proposer()))
}

func (t *testBaseProposalSelector) TestFailedToReqeustByContext() {
	suf, nodes := newTestSuffrage(2, t.local)

	p := NewBaseProposalSelector(
		t.local,
		t.policy,
		NewBlockBasedProposerSelector(
			func(base.Height) (util.Hash, error) {
				return valuehash.NewBytes([]byte("abc")), nil
			},
		),
		NewProposalMaker(t.local, t.policy, func(context.Context) ([]util.Hash, error) { return nil, nil }),
		func(base.Height) base.Suffrage {
			return suf
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

				return NewProposalMaker(n, t.policy, func(context.Context) ([]util.Hash, error) { return nil, nil }).New(ctx, point)
			}

			return nil, errors.Errorf("proposer not found in suffrage")
		},
		nil,
	)

	t.T().Logf("suffrage len: %d", suf.Len())
	for i := range nodes {
		t.T().Logf("suffrage node: %q", nodes[i].Address())
	}

	point := base.RawPoint(66, 11)
	pr, err := p.Select(context.Background(), point)
	t.NoError(err)
	t.NotNil(pr)

	t.NoError(pr.IsValid(t.policy.NetworkID()))

	t.Equal(point, pr.Point())

	t.T().Logf("expected selected proposer: %q, but it's dead", nodes[2].Address())
	t.T().Logf("selected proposer: %q", pr.ProposalFact().Proposer())
	t.True(nodes[1].Address().Equal(pr.ProposalFact().Proposer()))
}

func (t *testBaseProposalSelector) TestAllFailedToReqeust() {
	suf, nodes := newTestSuffrage(3)

	p := NewBaseProposalSelector(
		t.local,
		t.policy,
		NewBlockBasedProposerSelector(
			func(base.Height) (util.Hash, error) {
				return valuehash.NewBytes([]byte("abc")), nil
			},
		),
		NewProposalMaker(t.local, t.policy, func(context.Context) ([]util.Hash, error) { return nil, nil }),
		func(base.Height) base.Suffrage {
			return suf
		},
		func() []base.Address {
			return nil
		},
		func(_ context.Context, point base.Point, proposer base.Address) (base.ProposalSignedFact, error) {
			return nil, errors.Errorf("proposer not found in suffrage")
		},
		nil,
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
	suf, nodes := newTestSuffrage(3)

	requestdelay := time.Second
	_ = t.policy.SetTimeoutRequestProposal(time.Millisecond * 10)

	p := NewBaseProposalSelector(
		t.local,
		t.policy,
		NewBlockBasedProposerSelector(
			func(base.Height) (util.Hash, error) {
				return valuehash.NewBytes([]byte("abc")), nil
			},
		),
		NewProposalMaker(t.local, t.policy, func(context.Context) ([]util.Hash, error) { return nil, nil }),
		func(base.Height) base.Suffrage {
			return suf
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

					pr, err = NewProposalMaker(n, t.policy, func(context.Context) ([]util.Hash, error) { return nil, nil }).New(ctx, point)

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
		nil,
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
	suf, nodes := newTestSuffrage(3)

	requestdelay := time.Second * 10
	_ = t.policy.SetTimeoutRequestProposal(requestdelay * 2)

	p := NewBaseProposalSelector(
		t.local,
		t.policy,
		NewBlockBasedProposerSelector(
			func(base.Height) (util.Hash, error) {
				return valuehash.NewBytes([]byte("abc")), nil
			},
		),
		NewProposalMaker(t.local, t.policy, func(context.Context) ([]util.Hash, error) { return nil, nil }),
		func(base.Height) base.Suffrage {
			return suf
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

					pr, err = NewProposalMaker(n, t.policy, func(context.Context) ([]util.Hash, error) { return nil, nil }).New(ctx, point)

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
		nil,
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
