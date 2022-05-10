package isaacnetwork

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	isaacdatabase "github.com/spikeekips/mitum/isaac/database"
	"github.com/spikeekips/mitum/network/quictransport"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type testQuicstreamNodeNetworkHandlers struct {
	isaacdatabase.BaseTestDatabase
	isaac.BaseTestBallots
}

func (t *testQuicstreamNodeNetworkHandlers) SetupTest() {
	t.BaseTestDatabase.SetupTest()
	t.BaseTestBallots.SetupTest()
}

func (t *testQuicstreamNodeNetworkHandlers) SetupSuite() {
	t.BaseTestDatabase.SetupSuite()

	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: RequestProposalBodyHint, Instance: RequestProposalBody{}}))
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: ProposalBodyHint, Instance: ProposalBody{}}))
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: isaac.SuffrageCandidateHint, Instance: isaac.SuffrageCandidate{}}))
}

func (t *testQuicstreamNodeNetworkHandlers) TestClient() {
	c := newBaseNodeNetworkClient(t.Encs, t.Enc, nil)

	_ = (interface{})(c).(isaac.NodeNetworkClient)
}

func (t *testQuicstreamNodeNetworkHandlers) TestRequestProposal() {
	pool := t.NewPool()
	defer pool.Close()

	proposalMaker := isaac.NewProposalMaker(
		t.Local,
		t.NodePolicy,
		func(context.Context) ([]util.Hash, error) {
			return []util.Hash{valuehash.RandomSHA256(), valuehash.RandomSHA256()}, nil
		},
		pool,
	)

	handlers := NewQuicstreamNodeNetworkHandlers(t.Local, t.Encs, t.Enc, pool, proposalMaker)
	send := func(ctx context.Context, ci quictransport.ConnInfo, prefix string, b []byte) (io.ReadCloser, error) {
		if prefix != HandlerPrefixRequestProposal {
			return nil, errors.Errorf("unknown request, %q", prefix)
		}

		r := bytes.NewBuffer(b)
		w := bytes.NewBuffer(nil)

		if err := handlers.RequestProposal(nil, r, w); err != nil {
			return nil, errors.Wrap(err, "failed to handle request")
		}

		return io.NopCloser(w), nil
	}

	ci := quictransport.NewBaseConnInfo(nil, true)
	c := newBaseNodeNetworkClient(t.Encs, t.Enc, send)

	t.Run("local is proposer", func() {
		point := base.RawPoint(33, 1)
		pr, found, err := c.RequestProposal(context.Background(), ci, point, t.Local.Address())
		t.NoError(err)
		t.True(found)

		t.Equal(point, pr.Point())
		t.True(t.Local.Address().Equal(pr.ProposalFact().Proposer()))
		t.NoError(base.IsValidProposalSignedFact(pr, t.NodePolicy.NetworkID()))
	})

	t.Run("local is not proposer", func() {
		point := base.RawPoint(33, 1)
		proposer := base.RandomAddress("")
		pr, found, err := c.RequestProposal(context.Background(), ci, point, proposer)
		t.NoError(err)
		t.False(found)
		t.Nil(pr)
	})
}

func (t *testQuicstreamNodeNetworkHandlers) TestProposal() {
	pool := t.NewPool()
	defer pool.Close()

	proposalMaker := isaac.NewProposalMaker(
		t.Local,
		t.NodePolicy,
		func(context.Context) ([]util.Hash, error) {
			return []util.Hash{valuehash.RandomSHA256(), valuehash.RandomSHA256()}, nil
		},
		pool,
	)

	point := base.RawPoint(33, 1)
	pr, err := proposalMaker.New(context.Background(), point)
	t.NoError(err)
	_, err = pool.SetProposal(pr)
	t.NoError(err)

	handlers := NewQuicstreamNodeNetworkHandlers(t.Local, t.Encs, t.Enc, pool, proposalMaker)
	send := func(ctx context.Context, ci quictransport.ConnInfo, prefix string, b []byte) (io.ReadCloser, error) {
		if prefix != HandlerPrefixProposal {
			return nil, errors.Errorf("unknown request, %q", prefix)
		}

		r := bytes.NewBuffer(b)
		w := bytes.NewBuffer(nil)

		if err := handlers.Proposal(nil, r, w); err != nil {
			return nil, errors.Wrap(err, "failed to handle request")
		}

		return io.NopCloser(w), nil
	}

	ci := quictransport.NewBaseConnInfo(nil, true)
	c := newBaseNodeNetworkClient(t.Encs, t.Enc, send)

	t.Run("found", func() {
		pr, found, err := c.Proposal(context.Background(), ci, pr.Fact().Hash())
		t.NoError(err)
		t.True(found)

		t.Equal(point, pr.Point())
		t.True(t.Local.Address().Equal(pr.ProposalFact().Proposer()))
		t.NoError(base.IsValidProposalSignedFact(pr, t.NodePolicy.NetworkID()))
	})

	t.Run("unknown", func() {
		pr, found, err := c.Proposal(context.Background(), ci, valuehash.RandomSHA256())
		t.NoError(err)
		t.False(found)
		t.Nil(pr)
	})

	t.Run("nil proposal fact hash", func() {
		pr, found, err := c.Proposal(context.Background(), ci, nil)
		t.Error(err)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "invalid ProposalBody")
		t.False(found)
		t.Nil(pr)
	})
}

func TestQuicstreamNodeNetworkHandlers(t *testing.T) {
	defer goleak.VerifyNone(t,
		goleak.IgnoreTopFunction("github.com/syndtr/goleveldb/leveldb.(*DB).mpoolDrain"),
	)

	suite.Run(t, new(testQuicstreamNodeNetworkHandlers))
}
