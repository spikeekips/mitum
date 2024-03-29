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
	BaseTestBallots
}

func (t *testProposalProcessors) TestProcess() {
	point := base.RawPoint(33, 44)

	pr := t.PRPool.Get(point)

	previous := base.NewDummyManifest(point.Height()-1, valuehash.RandomSHA256())
	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())

	pp := NewDummyProposalProcessor()

	savech := make(chan base.ACCEPTVoteproof, 1)
	pp.Processerr = func(context.Context, base.ProposalFact, base.INITVoteproof) (base.Manifest, error) {
		return manifest, nil
	}
	pp.Saveerr = func(_ context.Context, avp base.ACCEPTVoteproof) (base.BlockMap, error) {
		savech <- avp

		return nil, nil
	}

	pps := NewProposalProcessors(
		pp.Make,
		func(_ context.Context, _ base.Point, facthash util.Hash) (base.ProposalSignFact, error) {
			return t.PRPool.ByHash(facthash)
		},
	)

	facthash := pr.Fact().Hash()

	t.T().Log("process")
	processf, err := pps.Process(context.Background(), point, facthash, previous, nil)
	t.NoError(err)
	t.NotNil(processf)

	rmanifest, err := processf(context.Background())
	t.NoError(err)

	base.EqualManifest(t.Assert(), manifest, rmanifest)

	t.T().Log("save")
	avp, _ := t.VoteproofsPair(
		point.PrevHeight(),
		point,
		nil,
		facthash,
		nil,
		[]base.LocalNode{t.Local},
	)
	_, err = pps.Save(context.Background(), facthash, avp)
	t.NoError(err)

	select {
	case <-time.After(time.Second * 2):
		t.Fail("timeout to wait save")
	case ravp := <-savech:
		base.EqualVoteproof(t.Assert(), avp, ravp)
	}

	t.NoError(pps.Cancel())
	t.Nil(pps.Processor())
}

func (t *testProposalProcessors) TestAlreadyProcessing() {
	point := base.RawPoint(33, 44)

	pr := t.PRPool.Get(point)

	previous := base.NewDummyManifest(point.Height()-1, valuehash.RandomSHA256())
	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())

	pp := NewDummyProposalProcessor()

	processch := make(chan bool, 1)
	pp.Processerr = func(context.Context, base.ProposalFact, base.INITVoteproof) (base.Manifest, error) {
		processch <- true

		return manifest, nil
	}

	pps := NewProposalProcessors(
		pp.Make,
		func(_ context.Context, _ base.Point, facthash util.Hash) (base.ProposalSignFact, error) {
			return t.PRPool.ByHash(facthash)
		},
	)
	pps.SetLogging(logging.TestNilLogging)

	facthash := pr.Fact().Hash()

	t.T().Log("process")
	processf, err := pps.Process(context.Background(), point, facthash, previous, nil)
	t.NoError(err)
	t.NotNil(processf)

	_, err = processf(context.Background())
	t.NoError(err)

	select {
	case <-time.After(time.Second * 2):
		t.Fail("timeout to wait result")
	case <-processch:
	}

	t.T().Log("try process again")
	bprocessf, err := pps.Process(context.Background(), point, facthash, previous, nil)
	t.NoError(err)
	t.Nil(bprocessf)

	t.NotNil(pps.Processor())
	t.True(pr.Fact().Hash().Equal(pps.Processor().Proposal().Fact().Hash()))
}

func (t *testProposalProcessors) TestCancelPrevious() {
	point := base.RawPoint(33, 44)

	pr := t.PRPool.Get(point)
	nextpr := t.PRPool.Get(point.NextRound())

	pp := NewDummyProposalProcessor()

	previous := base.NewDummyManifest(point.Height()-1, valuehash.RandomSHA256())
	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())

	processch := make(chan bool, 1)
	pp.Processerr = func(context.Context, base.ProposalFact, base.INITVoteproof) (base.Manifest, error) {
		processch <- true

		return manifest, nil
	}
	cancelch := make(chan bool, 1)
	pp.Cancelerr = func() error {
		cancelch <- true

		return nil
	}

	pps := NewProposalProcessors(
		pp.Make,
		func(_ context.Context, _ base.Point, facthash util.Hash) (base.ProposalSignFact, error) {
			return t.PRPool.ByHash(facthash)
		},
	)

	t.T().Log("process")
	go func() {
		processf, err := pps.Process(context.Background(), point, pr.Fact().Hash(), previous, nil)
		if err != nil {
			panic(err)
		}

		_, err = processf(context.Background())
		if err != nil {
			panic(err)
		}
	}()

	select {
	case <-time.After(time.Second * 2):
		t.Fail("timeout to wait result")
	case <-processch:
	}

	t.T().Log("process another")
	_, err := pps.Process(context.Background(), point, nextpr.Fact().Hash(), previous, nil)
	t.NoError(err)
	t.NotNil(pps.Processor())

	select {
	case <-time.After(time.Second * 2):
		t.Fail("timeout to wait cancel")
	case <-cancelch:
	}
}

func (t *testProposalProcessors) TestFailedToFetchFact() {
	point := base.RawPoint(33, 44)

	pps := NewProposalProcessors(
		NewDummyProposalProcessor().Make,
		func(context.Context, base.Point, util.Hash) (base.ProposalSignFact, error) {
			return nil, util.ErrNotFound.Errorf("hehehe")
		},
	)
	pps.retrylimit = 1
	pps.retryinterval = 1

	t.T().Log("process")
	processf, err := pps.Process(context.Background(), point, valuehash.RandomSHA256(), nil, nil)
	t.Error(err)
	t.Nil(processf)

	t.ErrorIs(err, util.ErrNotFound)
	t.ErrorContains(err, "hehehe")
}

func (t *testProposalProcessors) TestFailedToFetchFactCanceled() {
	point := base.RawPoint(33, 44)

	pps := NewProposalProcessors(
		NewDummyProposalProcessor().Make,
		func(context.Context, base.Point, util.Hash) (base.ProposalSignFact, error) {
			return nil, context.Canceled
		},
	)
	pps.retrylimit = 1
	pps.retryinterval = 1

	t.T().Log("process")
	processf, err := pps.Process(context.Background(), point, valuehash.RandomSHA256(), nil, nil)
	t.Error(err)
	t.Nil(processf)

	t.ErrorIs(err, context.Canceled)
	t.ErrorContains(err, "canceled")
}

func (t *testProposalProcessors) TestRetryFetchFact() {
	point := base.RawPoint(33, 44)

	var try int64
	pps := NewProposalProcessors(
		NewDummyProposalProcessor().Make,
		func(context.Context, base.Point, util.Hash) (base.ProposalSignFact, error) {
			if atomic.LoadInt64(&try) > 2 {
				return nil, context.Canceled
			}

			atomic.AddInt64(&try, 1)

			return nil, errors.Errorf("findme")
		},
	)
	pps.retrylimit = 4
	pps.retryinterval = time.Millisecond * 10

	t.T().Log("process")
	_, err := pps.Process(context.Background(), point, valuehash.RandomSHA256(), nil, nil)
	t.Error(err)
	t.ErrorIs(err, context.Canceled)
	t.ErrorContains(err, "canceled")

	t.True(atomic.LoadInt64(&try) > 2)
}

func (t *testProposalProcessors) TestRetryFetchFactOverLimit() {
	point := base.RawPoint(33, 44)

	var try int64
	pps := NewProposalProcessors(
		NewDummyProposalProcessor().Make,
		func(context.Context, base.Point, util.Hash) (base.ProposalSignFact, error) {
			atomic.AddInt64(&try, 1)

			return nil, errors.Errorf("findme")
		},
	)
	pps.retrylimit = 3
	pps.retryinterval = time.Millisecond * 10

	t.T().Log("process")
	_, err := pps.Process(context.Background(), point, valuehash.RandomSHA256(), nil, nil)
	t.Error(err)
	t.ErrorContains(err, "findme")

	t.True(atomic.LoadInt64(&try) > 2)
}

func (t *testProposalProcessors) TestProcessError() {
	point := base.RawPoint(33, 44)
	previous := base.NewDummyManifest(point.Height()-1, valuehash.RandomSHA256())
	pr := t.PRPool.Get(point)

	pp := NewDummyProposalProcessor()
	pp.Processerr = func(context.Context, base.ProposalFact, base.INITVoteproof) (base.Manifest, error) {
		return nil, errors.New("hihihi")
	}

	pps := NewProposalProcessors(
		pp.Make,
		func(_ context.Context, _ base.Point, facthash util.Hash) (base.ProposalSignFact, error) {
			return t.PRPool.ByHash(facthash)
		},
	)

	facthash := pr.Fact().Hash()

	t.T().Log("process")
	processf, err := pps.Process(context.Background(), point, facthash, previous, nil)
	t.NoError(err)

	_, err = processf(context.Background())
	t.Error(err)
	t.ErrorContains(err, "hihihi")
}

func (t *testProposalProcessors) TestProcessIgnoreError() {
	point := base.RawPoint(33, 44)
	previous := base.NewDummyManifest(point.Height()-1, valuehash.RandomSHA256())

	pr := t.PRPool.Get(point)

	pp := NewDummyProposalProcessor()

	pp.Processerr = func(context.Context, base.ProposalFact, base.INITVoteproof) (base.Manifest, error) {
		return nil, ErrIgnoreErrorProposalProcessor.WithStack()
	}

	pps := NewProposalProcessors(
		pp.Make,
		func(_ context.Context, _ base.Point, facthash util.Hash) (base.ProposalSignFact, error) {
			return t.PRPool.ByHash(facthash)
		},
	)

	facthash := pr.Fact().Hash()

	t.T().Log("process")
	processf, err := pps.Process(context.Background(), point, facthash, previous, nil)
	t.NoError(err)

	rmanifest, err := processf(context.Background())
	t.NoError(err)
	t.Nil(rmanifest)
}

func (t *testProposalProcessors) TestProcessContextCanceled() {
	point := base.RawPoint(33, 44)
	previous := base.NewDummyManifest(point.Height()-1, valuehash.RandomSHA256())

	pr := t.PRPool.Get(point)

	pp := NewDummyProposalProcessor()

	pp.Processerr = func(context.Context, base.ProposalFact, base.INITVoteproof) (base.Manifest, error) {
		return nil, context.Canceled
	}

	pps := NewProposalProcessors(
		pp.Make,
		func(_ context.Context, _ base.Point, facthash util.Hash) (base.ProposalSignFact, error) {
			return t.PRPool.ByHash(facthash)
		},
	)

	facthash := pr.Fact().Hash()

	t.T().Log("process")
	processf, err := pps.Process(context.Background(), point, facthash, previous, nil)
	t.NoError(err)

	rmanifest, err := processf(context.Background())
	t.Error(err)

	t.ErrorIs(err, context.Canceled)
	t.ErrorIs(err, ErrNotProposalProcessorProcessed)
	t.Nil(rmanifest)
}

func (t *testProposalProcessors) TestSaveError() {
	point := base.RawPoint(33, 44)

	pr := t.PRPool.Get(point)

	previous := base.NewDummyManifest(point.Height()-1, valuehash.RandomSHA256())
	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())

	pp := NewDummyProposalProcessor()

	pp.Processerr = func(context.Context, base.ProposalFact, base.INITVoteproof) (base.Manifest, error) {
		return manifest, nil
	}

	pp.Saveerr = func(context.Context, base.ACCEPTVoteproof) (base.BlockMap, error) {
		return nil, errors.Errorf("findme")
	}

	pps := NewProposalProcessors(
		pp.Make,
		func(_ context.Context, _ base.Point, facthash util.Hash) (base.ProposalSignFact, error) {
			return t.PRPool.ByHash(facthash)
		},
	)

	facthash := pr.Fact().Hash()

	t.T().Log("process")
	processf, err := pps.Process(context.Background(), point, facthash, previous, nil)
	t.NoError(err)
	t.NotNil(processf)

	rmanifest, err := processf(context.Background())
	t.NoError(err)

	base.EqualManifest(t.Assert(), manifest, rmanifest)

	t.T().Log("save")
	avp, _ := t.VoteproofsPair(
		point.PrevHeight(),
		point,
		nil,
		facthash,
		nil,
		[]base.LocalNode{t.Local},
	)
	_, err = pps.Save(context.Background(), facthash, avp)
	t.Error(err)
	t.ErrorContains(err, "findme")

	t.NoError(pps.Cancel())
	t.Nil(pps.Processor())
}

func TestProposalProcessors(t *testing.T) {
	suite.Run(t, new(testProposalProcessors))
}
