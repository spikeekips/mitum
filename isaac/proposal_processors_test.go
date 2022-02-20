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
	baseStateTestHandler
}

func (t *testProposalProcessors) TestProcess() {
	point := base.RawPoint(33, 44)

	pr := t.prpool.get(point)

	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())

	pp := NewDummyProposalProcessor()

	savech := make(chan base.ACCEPTVoteproof, 1)
	pp.processerr = func(_ context.Context, _ base.ProposalFact) (base.Manifest, error) {
		return manifest, nil
	}
	pp.saveerr = func(_ context.Context, avp base.ACCEPTVoteproof) error {
		savech <- avp

		return nil
	}

	pps := newProposalProcessors(
		pp.make,
		func(_ context.Context, facthash util.Hash) (base.ProposalFact, error) {
			return t.prpool.factByHash(facthash)
		},
	)

	facthash := pr.Fact().Hash()

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
	point := base.RawPoint(33, 44)

	pr := t.prpool.get(point)

	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())

	pp := NewDummyProposalProcessor()

	processch := make(chan bool, 1)
	pp.processerr = func(context.Context, base.ProposalFact) (base.Manifest, error) {
		processch <- true

		return manifest, nil
	}

	pps := newProposalProcessors(
		pp.make,
		func(_ context.Context, facthash util.Hash) (base.ProposalFact, error) {
			return t.prpool.factByHash(facthash)
		},
	)
	pps.SetLogging(logging.TestNilLogging)

	facthash := pr.Fact().Hash()

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
	t.True(pr.Fact().Hash().Equal(pps.processor().proposal().Hash()))
}

func (t *testProposalProcessors) TestCancelPrevious() {
	point := base.RawPoint(33, 44)

	pr := t.prpool.get(point)
	nextpr := t.prpool.get(point.NextRound())

	pp := NewDummyProposalProcessor()

	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())

	processch := make(chan bool, 1)
	pp.processerr = func(context.Context, base.ProposalFact) (base.Manifest, error) {
		processch <- true

		return manifest, nil
	}
	cancelch := make(chan bool, 1)
	pp.cancelerr = func() error {
		cancelch <- true

		return nil
	}

	pps := newProposalProcessors(
		pp.make,
		func(_ context.Context, facthash util.Hash) (base.ProposalFact, error) {
			return t.prpool.factByHash(facthash)
		},
	)

	t.T().Log("process")
	go func() {
		_, err := pps.process(context.Background(), pr.Fact().Hash())
		t.NoError(err)
	}()

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait result"))
	case <-processch:
	}

	t.T().Log("process another")
	_, err := pps.process(context.Background(), nextpr.Fact().Hash())
	t.NoError(err)
	t.NotNil(pps.processor())

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait cancel"))
	case <-cancelch:
	}
}

func (t *testProposalProcessors) TestFailedToFetchFact() {
	pps := newProposalProcessors(
		NewDummyProposalProcessor().make,
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
	pps := newProposalProcessors(
		NewDummyProposalProcessor().make,
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
	var try int64
	pps := newProposalProcessors(
		NewDummyProposalProcessor().make,
		func(context.Context, util.Hash) (base.ProposalFact, error) {
			if atomic.LoadInt64(&try) > 2 {
				return nil, context.Canceled
			}

			atomic.AddInt64(&try, 1)

			return nil, retryProposalProcessorError.Call()
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
	var try int64
	pps := newProposalProcessors(
		NewDummyProposalProcessor().make,
		func(context.Context, util.Hash) (base.ProposalFact, error) {
			atomic.AddInt64(&try, 1)

			return nil, retryProposalProcessorError.Call()
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
	point := base.RawPoint(33, 44)
	pr := t.prpool.get(point)

	pp := NewDummyProposalProcessor()
	pp.processerr = func(context.Context, base.ProposalFact) (base.Manifest, error) {
		return nil, errors.New("hihihi")
	}

	pps := newProposalProcessors(
		pp.make,
		func(_ context.Context, facthash util.Hash) (base.ProposalFact, error) {
			return t.prpool.factByHash(facthash)
		},
	)

	facthash := pr.Fact().Hash()

	t.T().Log("process")
	_, err := pps.process(context.Background(), facthash)

	t.Error(err)
	t.Contains(err.Error(), "hihihi")
}

func (t *testProposalProcessors) TestProcessIgnoreError() {
	point := base.RawPoint(33, 44)

	pr := t.prpool.get(point)

	pp := NewDummyProposalProcessor()

	pp.processerr = func(context.Context, base.ProposalFact) (base.Manifest, error) {
		return nil, ignoreErrorProposalProcessorError.Call()
	}

	pps := newProposalProcessors(
		pp.make,
		func(_ context.Context, facthash util.Hash) (base.ProposalFact, error) {
			return t.prpool.factByHash(facthash)
		},
	)

	facthash := pr.Fact().Hash()

	t.T().Log("process")
	rmanifest, err := pps.process(context.Background(), facthash)
	t.NoError(err)
	t.Nil(rmanifest)
}

func (t *testProposalProcessors) TestProcessContextCanceled() {
	point := base.RawPoint(33, 44)

	pr := t.prpool.get(point)

	pp := NewDummyProposalProcessor()

	pp.processerr = func(context.Context, base.ProposalFact) (base.Manifest, error) {
		return nil, context.Canceled
	}

	pps := newProposalProcessors(
		pp.make,
		func(_ context.Context, facthash util.Hash) (base.ProposalFact, error) {
			return t.prpool.factByHash(facthash)
		},
	)

	facthash := pr.Fact().Hash()

	t.T().Log("process")
	rmanifest, err := pps.process(context.Background(), facthash)

	t.True(errors.Is(err, context.Canceled))
	t.Nil(rmanifest)
}

func (t *testProposalProcessors) TestProcessRetry() {
	point := base.RawPoint(33, 44)

	pr := t.prpool.get(point)

	pp := NewDummyProposalProcessor()

	var try int64
	pp.processerr = func(context.Context, base.ProposalFact) (base.Manifest, error) {
		atomic.AddInt64(&try, 1)
		return nil, retryProposalProcessorError.Call()
	}

	pps := newProposalProcessors(
		pp.make,
		func(_ context.Context, facthash util.Hash) (base.ProposalFact, error) {
			return t.prpool.factByHash(facthash)
		},
	)
	pps.limit = 3
	pps.retryinterval = time.Millisecond * 10

	facthash := pr.Fact().Hash()

	t.T().Log("process")
	rmanifest, err := pps.process(context.Background(), facthash)

	t.Nil(rmanifest)
	t.Contains(err.Error(), "too many retry")

	t.True(atomic.LoadInt64(&try) > 2)
}

func (t *testProposalProcessors) TestSaveError() {
	point := base.RawPoint(33, 44)

	pr := t.prpool.get(point)

	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())

	pp := NewDummyProposalProcessor()

	var try int64
	pp.processerr = func(context.Context, base.ProposalFact) (base.Manifest, error) {
		return manifest, nil
	}

	pp.saveerr = func(context.Context, base.ACCEPTVoteproof) error {
		atomic.AddInt64(&try, 1)

		return retryProposalProcessorError.Call()
	}

	pps := newProposalProcessors(
		pp.make,
		func(_ context.Context, facthash util.Hash) (base.ProposalFact, error) {
			return t.prpool.factByHash(facthash)
		},
	)
	pps.limit = 3
	pps.retryinterval = time.Millisecond * 10

	facthash := pr.Fact().Hash()

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
