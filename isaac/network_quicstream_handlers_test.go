package isaac

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/network/quictransport"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type testQuicstreamNodeNetworkHandlers struct {
	baseTestDatabase
	baseTestHandler
	ci quictransport.ConnInfo
}

func (t *testQuicstreamNodeNetworkHandlers) SetupTest() {
	t.baseTestDatabase.SetupTest()
	t.baseTestHandler.SetupTest()
}

func (t *testQuicstreamNodeNetworkHandlers) SetupSuite() {
	t.baseTestDatabase.SetupSuite()

	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: RequestProposalBodyHint, Instance: RequestProposalBody{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: ProposalBodyHint, Instance: ProposalBody{}}))
}

func (t *testQuicstreamNodeNetworkHandlers) TestClient() {
	c := newBaseNodeNetworkClient(t.encs, t.enc, nil)

	_ = (interface{})(c).(NodeNetworkClient)
}

func (t *testQuicstreamNodeNetworkHandlers) TestRequestProposal() {
	pool := t.newPool()
	defer pool.Close()

	proposalMaker := NewProposalMaker(
		t.local,
		t.policy,
		func(context.Context) ([]util.Hash, error) {
			return []util.Hash{valuehash.RandomSHA256(), valuehash.RandomSHA256()}, nil
		},
		pool,
	)

	handlers := NewQuicstreamNodeNetworkHandlers(t.local, t.encs, t.enc, pool, proposalMaker, nil)
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
	c := newBaseNodeNetworkClient(t.encs, t.enc, send)

	t.Run("local is proposer", func() {
		point := base.RawPoint(33, 1)
		pr, found, err := c.RequestProposal(context.Background(), ci, point, t.local.Address())
		t.NoError(err)
		t.True(found)

		t.Equal(point, pr.Point())
		t.True(t.local.Address().Equal(pr.ProposalFact().Proposer()))
		t.NoError(base.IsValidProposalSignedFact(pr, t.policy.NetworkID()))
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
	pool := t.newPool()
	defer pool.Close()

	proposalMaker := NewProposalMaker(
		t.local,
		t.policy,
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

	handlers := NewQuicstreamNodeNetworkHandlers(t.local, t.encs, t.enc, pool, proposalMaker, nil)
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
	c := newBaseNodeNetworkClient(t.encs, t.enc, send)

	t.Run("found", func() {
		pr, found, err := c.Proposal(context.Background(), ci, pr.Fact().Hash())
		t.NoError(err)
		t.True(found)

		t.Equal(point, pr.Point())
		t.True(t.local.Address().Equal(pr.ProposalFact().Proposer()))
		t.NoError(base.IsValidProposalSignedFact(pr, t.policy.NetworkID()))
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
		t.True(errors.Is(err, util.InvalidError))
		t.Contains(err.Error(), "invalid ProposalBody")
		t.False(found)
		t.Nil(pr)
	})
}

func (t *testQuicstreamNodeNetworkHandlers) TestLastSuffrageState() {
	_, nodes := t.locals(3)
	height := base.Height(33)

	_, stv := t.suffrageState(height, base.Height(22), nodes)
	manifest := base.NewDummyManifest(height, valuehash.RandomSHA256())

	pool := t.newPool()
	defer pool.Close()

	handlers := NewQuicstreamNodeNetworkHandlers(t.local, t.encs, t.enc, pool, nil, nil)
	send := func(ctx context.Context, ci quictransport.ConnInfo, prefix string, b []byte) (io.ReadCloser, error) {
		if prefix != HandlerPrefixLastSuffrage {
			return nil, errors.Errorf("unknown request, %q", prefix)
		}

		r := bytes.NewBuffer(b)
		w := bytes.NewBuffer(nil)

		if err := handlers.LastSuffrage(nil, r, w); err != nil {
			return nil, errors.Wrap(err, "failed to handle request")
		}

		return io.NopCloser(w), nil
	}

	ci := quictransport.NewBaseConnInfo(nil, true)
	c := newBaseNodeNetworkClient(t.encs, t.enc, send)

	t.Run("found", func() {
		handlers.lastSuffragef = func() (base.Manifest, base.SuffrageStateValue, bool, error) {
			return manifest, stv, true, nil
		}

		rm, rstv, found, err := c.LastSuffrage(context.Background(), ci)
		t.NoError(err)
		t.True(found)

		base.EqualManifest(t.Assert(), manifest, rm)
		t.True(stv.Equal(rstv))
	})

	t.Run("not found", func() {
		handlers.lastSuffragef = func() (base.Manifest, base.SuffrageStateValue, bool, error) {
			return nil, nil, false, nil
		}

		rm, rstv, found, err := c.LastSuffrage(context.Background(), ci)
		t.NoError(err)
		t.False(found)
		t.Nil(rm)
		t.Nil(rstv)
	})
}

func TestQuicstreamNodeNetworkHandlers(t *testing.T) {
	defer goleak.VerifyNone(t,
		goleak.IgnoreTopFunction("github.com/syndtr/goleveldb/leveldb.(*DB).mpoolDrain"),
	)

	suite.Run(t, new(testQuicstreamNodeNetworkHandlers))
}
