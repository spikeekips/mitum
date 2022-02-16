package isaac

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
)

type testProposalProcessors struct {
	baseTestStateHandler
}

func (t *testProposalProcessors) TestProcess() {
	point := base.NewPoint(base.Height(33), base.Round(44))

	pr := t.prpool.get(point)

	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())

	pp := NewDummyProposalProcessor(manifest)

	savech := make(chan base.ACCEPTVoteproof, 1)
	pp.saveerr = func(avp base.ACCEPTVoteproof) error {
		savech <- avp

		return nil
	}

	pps := newProposalProcessors(
		pp.make,
		func(_ context.Context, facthash util.Hash) (base.ProposalFact, error) {
			return t.prpool.factByHash(facthash)
		},
	)

	facthash := pr.SignedFact().Fact().Hash()

	t.T().Log("process")
	rmanifest, err := pps.process(context.Background(), facthash)
	t.NoError(err)

	base.CompareManifest(t.Assert(), manifest, rmanifest)

	t.T().Log("save")
	avp, _ := t.voteproofsPair(
		point.Decrease(),
		point,
		nil,
		facthash,
		nil,
		[]*LocalNode{t.local},
	)
	t.NoError(pps.save(context.Background(), facthash, avp))

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait save"))
	case ravp := <-savech:
		base.CompareVoteproof(t.Assert(), avp, ravp)
	}

	t.NoError(pps.close())
	t.Nil(pps.processor())
}

func (t *testProposalProcessors) TestAlreadyProcessing() {
	point := base.NewPoint(base.Height(33), base.Round(44))

	pr := t.prpool.get(point)

	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())

	pp := NewDummyProposalProcessor(manifest)

	processch := make(chan bool, 1)
	pp.processerr = func() error {
		processch <- true

		return nil
	}

	pps := newProposalProcessors(
		pp.make,
		func(_ context.Context, facthash util.Hash) (base.ProposalFact, error) {
			return t.prpool.factByHash(facthash)
		},
	)
	pps.SetLogging(logging.TestNilLogging)

	facthash := pr.SignedFact().Fact().Hash()

	t.T().Log("process")
	go func() {
		_, err := pps.process(context.Background(), facthash)
		t.NoError(err)
	}()

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait result"))
	case <-processch:
	}

	t.T().Log("try process again")
	_, err := pps.process(context.Background(), facthash)
	t.NoError(err)

	t.NotNil(pps.processor())
	t.True(pr.SignedFact().Fact().Hash().Equal(pps.processor().proposal().Hash()))
}

func (t *testProposalProcessors) TestCancelPrevious() {
	point := base.NewPoint(base.Height(33), base.Round(44))

	pr := t.prpool.get(point)
	nextpr := t.prpool.get(point.NextRound())

	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())

	pp := NewDummyProposalProcessor(manifest)

	processch := make(chan bool, 1)
	pp.processerr = func() error {
		processch <- true

		return nil
	}
	cancelch := make(chan bool, 1)
	pp.cancelerr = func() error {
		cancelch <- true

		return nil
	}

	nextpp := NewDummyProposalProcessor(nil)

	pps := newProposalProcessors(
		func(fact base.ProposalFact) proposalProcessor {
			switch fact.Point().Point {
			case point:
				pp.fact = fact
				return pp
			case point.NextRound():
				nextpp.fact = fact
				return nextpp
			default:
				return nil
			}
		},
		func(_ context.Context, facthash util.Hash) (base.ProposalFact, error) {
			return t.prpool.factByHash(facthash)
		},
	)

	t.T().Log("process")
	go func() {
		_, err := pps.process(context.Background(), pr.SignedFact().Fact().Hash())
		t.NoError(err)
	}()

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait result"))
	case <-processch:
	}

	t.T().Log("process another")
	_, err := pps.process(context.Background(), nextpr.SignedFact().Fact().Hash())
	t.NoError(err)
	t.NotNil(pps.processor())

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait cancel"))
	case <-cancelch:
	}
}

func (t *testProposalProcessors) TestFailedToFetchFact() {
	point := base.NewPoint(base.Height(33), base.Round(44))

	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())

	pp := NewDummyProposalProcessor(manifest)

	pps := newProposalProcessors(
		pp.make,
		func(context.Context, util.Hash) (base.ProposalFact, error) {
			return nil, util.NotFoundError.Errorf("hehehe")
		},
	)

	t.T().Log("process")
	_, err := pps.process(context.Background(), valuehash.RandomSHA256())
	t.Error(err)
	t.True(errors.Is(err, util.NotFoundError))
	t.Contains(err.Error(), "hehehe")
}

func (t *testProposalProcessors) TestFailedToFetchFactCanceled() {
	point := base.NewPoint(base.Height(33), base.Round(44))

	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())

	pp := NewDummyProposalProcessor(manifest)

	pps := newProposalProcessors(
		pp.make,
		func(context.Context, util.Hash) (base.ProposalFact, error) {
			return nil, context.Canceled
		},
	)

	t.T().Log("process")
	_, err := pps.process(context.Background(), valuehash.RandomSHA256())
	t.Error(err)
	t.True(errors.Is(err, context.Canceled))
	t.Contains(err.Error(), "canceled")
}

func (t *testProposalProcessors) TestRetryFetchFact() {
	point := base.NewPoint(base.Height(33), base.Round(44))

	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())

	pp := NewDummyProposalProcessor(manifest)

	var try int64
	pps := newProposalProcessors(
		pp.make,
		func(context.Context, util.Hash) (base.ProposalFact, error) {
			if atomic.LoadInt64(&try) > 2 {
				return nil, context.Canceled
			}

			atomic.AddInt64(&try, 1)

			return nil, RetryProposalProcessorError.Call()
		},
	)
	pps.limit = 4
	pps.retryinterval = time.Millisecond * 10

	t.T().Log("process")
	_, err := pps.process(context.Background(), valuehash.RandomSHA256())
	t.Error(err)
	t.True(errors.Is(err, context.Canceled))
	t.Contains(err.Error(), "canceled")

	t.True(atomic.LoadInt64(&try) > 2)
}

func (t *testProposalProcessors) TestRetryFetchFactOverLimit() {
	point := base.NewPoint(base.Height(33), base.Round(44))

	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())

	pp := NewDummyProposalProcessor(manifest)

	var try int64
	pps := newProposalProcessors(
		pp.make,
		func(context.Context, util.Hash) (base.ProposalFact, error) {
			atomic.AddInt64(&try, 1)

			return nil, RetryProposalProcessorError.Call()
		},
	)
	pps.limit = 3
	pps.retryinterval = time.Millisecond * 10

	t.T().Log("process")
	_, err := pps.process(context.Background(), valuehash.RandomSHA256())
	t.Error(err)
	t.Contains(err.Error(), "too many retry")

	t.True(atomic.LoadInt64(&try) > 2)
}

func (t *testProposalProcessors) TestProcessError() {
	point := base.NewPoint(base.Height(33), base.Round(44))

	pr := t.prpool.get(point)

	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())

	pp := NewDummyProposalProcessor(manifest)

	pp.processerr = func() error {
		return errors.New("hihihi")
	}

	pps := newProposalProcessors(
		pp.make,
		func(_ context.Context, facthash util.Hash) (base.ProposalFact, error) {
			return t.prpool.factByHash(facthash)
		},
	)

	facthash := pr.SignedFact().Fact().Hash()

	t.T().Log("process")
	_, err := pps.process(context.Background(), facthash)

	t.Error(err)
	t.Contains(err.Error(), "hihihi")
}

func (t *testProposalProcessors) TestProcessIgnoreError() {
	point := base.NewPoint(base.Height(33), base.Round(44))

	pr := t.prpool.get(point)

	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())

	pp := NewDummyProposalProcessor(manifest)

	pp.processerr = func() error {
		return IgnoreErrorProposalProcessorError.Call()
	}

	pps := newProposalProcessors(
		pp.make,
		func(_ context.Context, facthash util.Hash) (base.ProposalFact, error) {
			return t.prpool.factByHash(facthash)
		},
	)

	facthash := pr.SignedFact().Fact().Hash()

	t.T().Log("process")
	rmanifest, err := pps.process(context.Background(), facthash)
	t.NoError(err)
	t.Nil(rmanifest)
}

func (t *testProposalProcessors) TestProcessContextCanceled() {
	point := base.NewPoint(base.Height(33), base.Round(44))

	pr := t.prpool.get(point)

	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())

	pp := NewDummyProposalProcessor(manifest)

	pp.processerr = func() error {
		return context.Canceled
	}

	pps := newProposalProcessors(
		pp.make,
		func(_ context.Context, facthash util.Hash) (base.ProposalFact, error) {
			return t.prpool.factByHash(facthash)
		},
	)

	facthash := pr.SignedFact().Fact().Hash()

	t.T().Log("process")
	rmanifest, err := pps.process(context.Background(), facthash)

	t.True(errors.Is(err, context.Canceled))
	t.Nil(rmanifest)
}

func (t *testProposalProcessors) TestProcessRetry() {
	point := base.NewPoint(base.Height(33), base.Round(44))

	pr := t.prpool.get(point)

	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())

	pp := NewDummyProposalProcessor(manifest)

	var try int64
	pp.processerr = func() error {
		atomic.AddInt64(&try, 1)
		return RetryProposalProcessorError.Call()
	}

	pps := newProposalProcessors(
		pp.make,
		func(_ context.Context, facthash util.Hash) (base.ProposalFact, error) {
			return t.prpool.factByHash(facthash)
		},
	)
	pps.limit = 3
	pps.retryinterval = time.Millisecond * 10

	facthash := pr.SignedFact().Fact().Hash()

	t.T().Log("process")
	rmanifest, err := pps.process(context.Background(), facthash)

	t.Nil(rmanifest)
	t.Contains(err.Error(), "too many retry")

	t.True(atomic.LoadInt64(&try) > 2)
}

func (t *testProposalProcessors) TestSaveError() {
	point := base.NewPoint(base.Height(33), base.Round(44))

	pr := t.prpool.get(point)

	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())

	pp := NewDummyProposalProcessor(manifest)

	var try int64
	pp.saveerr = func(base.ACCEPTVoteproof) error {
		atomic.AddInt64(&try, 1)

		return RetryProposalProcessorError.Call()
	}

	pps := newProposalProcessors(
		pp.make,
		func(_ context.Context, facthash util.Hash) (base.ProposalFact, error) {
			return t.prpool.factByHash(facthash)
		},
	)
	pps.limit = 3
	pps.retryinterval = time.Millisecond * 10

	facthash := pr.SignedFact().Fact().Hash()

	t.T().Log("process")
	rmanifest, err := pps.process(context.Background(), facthash)
	t.NoError(err)

	base.CompareManifest(t.Assert(), manifest, rmanifest)

	t.T().Log("save")
	avp, _ := t.voteproofsPair(
		point.Decrease(),
		point,
		nil,
		facthash,
		nil,
		[]*LocalNode{t.local},
	)
	err = pps.save(context.Background(), facthash, avp)
	t.Error(err)
	t.Contains(err.Error(), "too many retry")

	t.NoError(pps.close())
	t.Nil(pps.processor())

	t.True(atomic.LoadInt64(&try) > 2)
}

func TestProposalProcessors(t *testing.T) {
	suite.Run(t, new(testProposalProcessors))
}
