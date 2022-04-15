package isaac

import (
	"context"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
)

type testNewINITOnINITVoteproofConsensusHandler struct {
	baseTestConsensusHandler
}

func (t *testNewINITOnINITVoteproofConsensusHandler) TestHigherHeight() {
	point := base.RawPoint(33, 44)
	suf, nodes := NewTestSuffrage(2, t.Local)

	st, closefunc, _, ivp := t.newStateWithINITVoteproof(point, suf)
	defer closefunc()

	prch := make(chan util.Hash, 1)
	st.pps.getproposal = func(_ context.Context, facthash util.Hash) (base.ProposalSignedFact, error) {
		pr, err := t.prpool.byHash(facthash)
		if err != nil {
			return nil, err
		}

		if pr.Point().Equal(point) {
			prch <- facthash
		}

		return pr, nil
	}

	st.proposalSelector = DummyProposalSelector(func(ctx context.Context, p base.Point) (base.ProposalSignedFact, error) {
		return nil, errors.Errorf("hahaah")
	})

	sctx := newConsensusSwitchContext(StateJoining, ivp)

	deferred, err := st.enter(sctx)
	t.NoError(err)
	deferred()

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait"))

		return
	case <-prch:
	}

	t.T().Log("new higher init voteproof")
	newpoint := point.Next()
	fact := t.prpool.getfact(point)
	_, newivp := t.VoteproofsPair(newpoint.Decrease(), newpoint, nil, nil, fact.Hash(), nodes)

	err = st.newVoteproof(newivp)
	t.Error(err)

	var ssctx syncingSwitchContext
	t.True(errors.As(err, &ssctx))
	t.Equal(ssctx.height, newpoint.Height()-1)
}

func (t *testNewINITOnINITVoteproofConsensusHandler) TestNextRoundButAlreadyFinished() {
	point := base.RawPoint(33, 44)
	suf, nodes := NewTestSuffrage(2, t.Local)

	st, closefunc, _, ivp := t.newStateWithINITVoteproof(point, suf)
	defer closefunc()

	prch := make(chan util.Hash, 1)
	st.pps.getproposal = func(_ context.Context, facthash util.Hash) (base.ProposalSignedFact, error) {
		pr, err := t.prpool.byHash(facthash)
		if err != nil {
			return nil, err
		}

		if pr.Point().Equal(point) {
			prch <- facthash
		}

		return pr, nil
	}

	st.proposalSelector = DummyProposalSelector(func(ctx context.Context, p base.Point) (base.ProposalSignedFact, error) {
		return nil, errors.Errorf("hahaah")
	})

	sctx := newConsensusSwitchContext(StateJoining, ivp)

	deferred, err := st.enter(sctx)
	t.NoError(err)
	deferred()

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait"))

		return
	case <-prch:
	}

	t.T().Log("next round init voteproof")
	newpoint := point.NextRound()
	fact := t.prpool.getfact(point)
	_, newivp := t.VoteproofsPair(newpoint.Decrease(), newpoint, nil, nil, fact.Hash(), nodes)

	t.NoError(st.newVoteproof(newivp))
}

func (t *testNewINITOnINITVoteproofConsensusHandler) TestDrawBeforePreviousBlockNotMatched() {
	point := base.RawPoint(33, 44)
	suf, nodes := NewTestSuffrage(2, t.Local)

	st, closefunc, pp, ivp := t.newStateWithINITVoteproof(point, suf)
	defer closefunc()

	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	pp.processerr = func(context.Context, base.ProposalFact, base.INITVoteproof) (base.Manifest, error) {
		return manifest, nil
	}
	savedch := make(chan base.ACCEPTVoteproof, 1)
	pp.saveerr = func(_ context.Context, avp base.ACCEPTVoteproof) error {
		savedch <- avp
		return nil
	}

	ballotch := make(chan base.Ballot, 1)
	st.broadcastBallotFunc = func(bl base.Ballot) error {
		switch {
		case bl.Point().Point.Equal(point.Next()):
			ballotch <- bl
		case bl.Point().Point.Equal(point.Next().NextRound()):
			ballotch <- bl
		}

		return nil
	}

	st.proposalSelector = DummyProposalSelector(func(ctx context.Context, p base.Point) (base.ProposalSignedFact, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			return t.prpool.get(p), nil
		}
	})

	sctx := newConsensusSwitchContext(StateJoining, ivp)

	deferred, err := st.enter(sctx)
	t.NoError(err)
	deferred()

	nextavp, drawivp := t.VoteproofsPair(point, point.Next(), manifest.Hash(), t.prpool.hash(point), nil, nodes)
	t.NoError(st.newVoteproof(nextavp))

	t.T().Log("wait new block saved")
	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait save proposal processor"))

		return
	case <-savedch:
	}

	t.T().Log("wait next init ballot")
	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait next init ballot"))

		return
	case bl := <-ballotch:
		t.Equal(point.Next(), bl.Point().Point)
	}

	drawivp.SetResult(base.VoteResultDraw).finish()

	t.NoError(st.newVoteproof(drawivp))

	t.T().Log("new draw init voteproof")
	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait next round init ballot"))

		return
	case bl := <-ballotch:
		t.Equal(point.Next().NextRound(), bl.Point().Point)
	}

	_, newivp := t.VoteproofsPair(point, drawivp.Point().Point.NextRound(), valuehash.RandomSHA256(), nil, t.prpool.hash(drawivp.Point().Point.NextRound()), nodes)
	err = st.newVoteproof(newivp)

	var ssctx syncingSwitchContext
	t.True(errors.As(err, &ssctx))
	t.Equal(ssctx.height, newivp.Point().Height()-1)
}

func (t *testNewINITOnINITVoteproofConsensusHandler) TestDrawBefore() {
	point := base.RawPoint(33, 44)
	suf, nodes := NewTestSuffrage(2, t.Local)

	st, closefunc, pp, ivp := t.newStateWithINITVoteproof(point, suf)
	defer closefunc()

	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	pp.processerr = func(context.Context, base.ProposalFact, base.INITVoteproof) (base.Manifest, error) {
		return manifest, nil
	}

	savedch := make(chan base.ACCEPTVoteproof, 1)
	pp.saveerr = func(_ context.Context, avp base.ACCEPTVoteproof) error {
		savedch <- avp
		return nil
	}

	ballotch := make(chan base.Ballot, 1)
	st.broadcastBallotFunc = func(bl base.Ballot) error {
		switch {
		case bl.Point().Point.Equal(point.Next()):
			ballotch <- bl
		case bl.Point().Point.Equal(point.Next().NextRound()):
			ballotch <- bl
		}

		return nil
	}

	st.proposalSelector = DummyProposalSelector(func(ctx context.Context, p base.Point) (base.ProposalSignedFact, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			return t.prpool.get(p), nil
		}
	})

	sctx := newConsensusSwitchContext(StateJoining, ivp)

	deferred, err := st.enter(sctx)
	t.NoError(err)
	deferred()

	nextavp, drawivp := t.VoteproofsPair(point, point.Next(), nil, t.prpool.hash(point), nil, nodes)
	t.NoError(st.newVoteproof(nextavp))

	t.T().Log("wait new block saved")
	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait save proposal processor"))

		return
	case <-savedch:
	}

	t.T().Log("wait next init ballot")
	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait next init ballot"))

		return
	case bl := <-ballotch:
		t.Equal(point.Next(), bl.Point().Point)
	}

	drawivp.SetResult(base.VoteResultDraw).finish()

	t.NoError(st.newVoteproof(drawivp))

	t.T().Log("new draw init voteproof")
	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait next round init ballot"))

		return
	case bl := <-ballotch:
		t.Equal(point.Next().NextRound(), bl.Point().Point)
	}

	_, newivp := t.VoteproofsPair(point, drawivp.Point().Point.NextRound(), nextavp.BallotMajority().NewBlock(), nil, t.prpool.hash(drawivp.Point().Point.NextRound()), nodes)
	t.NoError(st.newVoteproof(newivp))

	t.T().Log("new init voteproof")
	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait next round init ballot"))

		return
	case bl := <-ballotch:
		t.Equal(drawivp.Point().Point.NextRound(), bl.Point().Point)
	}
}

func (t *testNewINITOnINITVoteproofConsensusHandler) TestDrawAndDrawAgain() {
	point := base.RawPoint(33, 44)
	suf, nodes := NewTestSuffrage(2, t.Local)

	st, closefunc, pp, ivp := t.newStateWithINITVoteproof(point, suf)
	defer closefunc()

	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	pp.processerr = func(context.Context, base.ProposalFact, base.INITVoteproof) (base.Manifest, error) {
		return manifest, nil
	}
	savedch := make(chan base.ACCEPTVoteproof, 1)
	pp.saveerr = func(_ context.Context, avp base.ACCEPTVoteproof) error {
		savedch <- avp
		return nil
	}

	ballotch := make(chan base.Ballot, 1)
	st.broadcastBallotFunc = func(bl base.Ballot) error {
		switch {
		case bl.Point().Point.Equal(point.Next()):
			ballotch <- bl
		case bl.Point().Point.Equal(point.Next().NextRound()):
			ballotch <- bl
		case bl.Point().Point.Equal(point.Next().NextRound().NextRound()):
			ballotch <- bl
		}

		return nil
	}

	st.proposalSelector = DummyProposalSelector(func(ctx context.Context, p base.Point) (base.ProposalSignedFact, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			return t.prpool.get(p), nil
		}
	})

	sctx := newConsensusSwitchContext(StateJoining, ivp)

	deferred, err := st.enter(sctx)
	t.NoError(err)
	deferred()

	nextavp, drawivp := t.VoteproofsPair(point, point.Next(), manifest.Hash(), t.prpool.hash(point), nil, nodes)
	t.NoError(st.newVoteproof(nextavp))

	t.T().Log("wait new block saved")
	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait save proposal processor"))

		return
	case <-savedch:
	}

	t.T().Log("wait next init ballot")
	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait next init ballot"))

		return
	case bl := <-ballotch:
		t.Equal(point.Next(), bl.Point().Point)
	}

	drawivp.SetResult(base.VoteResultDraw).finish()

	t.NoError(st.newVoteproof(drawivp))

	t.T().Log("new draw init voteproof")
	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait next round init ballot"))

		return
	case bl := <-ballotch:
		t.Equal(point.Next().NextRound(), bl.Point().Point)
	}

	_, newivp := t.VoteproofsPair(point, drawivp.Point().Point.NextRound(), nil, nil, t.prpool.hash(drawivp.Point().Point.NextRound()), nodes)
	newivp.SetResult(base.VoteResultDraw).finish()

	t.NoError(st.newVoteproof(newivp))

	t.T().Log("new draw init voteproof again")
	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait next round init ballot"))

		return
	case bl := <-ballotch:
		t.Equal(drawivp.Point().Point.NextRound().NextRound(), bl.Point().Point)
	}
}

func TestNewINITOnINITVoteproofConsensusHandler(t *testing.T) {
	suite.Run(t, new(testNewINITOnINITVoteproofConsensusHandler))
}

type testNewINITOnACCEPTVoteproofConsensusHandler struct {
	baseTestConsensusHandler
}

func (t *testNewINITOnACCEPTVoteproofConsensusHandler) TestExpected() {
	point := base.RawPoint(33, 44)
	suf, nodes := NewTestSuffrage(2, t.Local)

	st, closefunc, pp, ivp := t.newStateWithINITVoteproof(point, suf)
	defer closefunc()
	st.SetLogging(logging.TestNilLogging)

	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	pp.processerr = func(context.Context, base.ProposalFact, base.INITVoteproof) (base.Manifest, error) {
		return manifest, nil
	}

	savedch := make(chan base.ACCEPTVoteproof, 1)
	pp.saveerr = func(_ context.Context, avp base.ACCEPTVoteproof) error {
		savedch <- avp
		return nil
	}

	ballotch := make(chan base.Ballot, 1)
	st.broadcastBallotFunc = func(bl base.Ballot) error {
		if p := bl.Point(); p.Point.Equal(point.Next()) && p.Stage() == base.StageACCEPT {
			ballotch <- bl
		}

		return nil
	}

	st.proposalSelector = DummyProposalSelector(func(ctx context.Context, p base.Point) (base.ProposalSignedFact, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			return t.prpool.get(p), nil
		}
	})

	sctx := newConsensusSwitchContext(StateJoining, ivp)

	deferred, err := st.enter(sctx)
	t.NoError(err)
	deferred()

	nextavp, nextivp := t.VoteproofsPair(point, point.Next(), manifest.Hash(), t.prpool.hash(point), t.prpool.hash(point.Next()), nodes)
	t.NoError(st.newVoteproof(nextavp))

	t.T().Log("wait new block saved")
	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait save proposal processor"))

		return
	case <-savedch:
	}

	t.T().Log("new init voteproof; wrong previous block")

	t.NoError(st.newVoteproof(nextivp))

	t.T().Log("wait next accept ballot")
	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait next accept ballot"))

		return
	case bl := <-ballotch:
		t.Equal(point.Next(), bl.Point().Point)
	}
}

func (t *testNewINITOnACCEPTVoteproofConsensusHandler) TestHigherHeight() {
	point := base.RawPoint(33, 44)
	suf, nodes := NewTestSuffrage(2, t.Local)

	st, closefunc, pp, ivp := t.newStateWithINITVoteproof(point, suf)
	defer closefunc()

	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	pp.processerr = func(context.Context, base.ProposalFact, base.INITVoteproof) (base.Manifest, error) {
		return manifest, nil
	}

	savedch := make(chan base.ACCEPTVoteproof, 1)
	pp.saveerr = func(_ context.Context, avp base.ACCEPTVoteproof) error {
		savedch <- avp
		return nil
	}

	ballotch := make(chan base.Ballot, 1)
	st.broadcastBallotFunc = func(bl base.Ballot) error {
		switch {
		case bl.Point().Point.Equal(point.Next()):
			ballotch <- bl
		case bl.Point().Point.Equal(point.Next().NextRound()):
			ballotch <- bl
		}

		return nil
	}

	st.proposalSelector = DummyProposalSelector(func(ctx context.Context, p base.Point) (base.ProposalSignedFact, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			return t.prpool.get(p), nil
		}
	})

	sctx := newConsensusSwitchContext(StateJoining, ivp)

	deferred, err := st.enter(sctx)
	t.NoError(err)
	deferred()

	nextavp, _ := t.VoteproofsPair(point, point.Next(), manifest.Hash(), t.prpool.hash(point), nil, nodes)
	t.NoError(st.newVoteproof(nextavp))

	t.T().Log("wait new block saved")
	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait save proposal processor"))

		return
	case <-savedch:
	}

	_, newivp := t.VoteproofsPair(point.Next(), point.Next().Next(), nil, nil, t.prpool.hash(point.Next().Next()), nodes)
	err = st.newVoteproof(newivp)

	var ssctx syncingSwitchContext
	t.True(errors.As(err, &ssctx))
	t.Equal(ssctx.height, newivp.Point().Height()-1)
}

func (t *testNewINITOnACCEPTVoteproofConsensusHandler) TestPreviousBlockNotMatch() {
	point := base.RawPoint(33, 44)
	suf, nodes := NewTestSuffrage(2, t.Local)

	st, closefunc, pp, ivp := t.newStateWithINITVoteproof(point, suf)
	defer closefunc()

	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	pp.processerr = func(context.Context, base.ProposalFact, base.INITVoteproof) (base.Manifest, error) {
		return manifest, nil
	}

	savedch := make(chan base.ACCEPTVoteproof, 1)
	pp.saveerr = func(_ context.Context, avp base.ACCEPTVoteproof) error {
		savedch <- avp
		return nil
	}

	ballotch := make(chan base.Ballot, 1)
	st.broadcastBallotFunc = func(bl base.Ballot) error {
		switch {
		case bl.Point().Point.Equal(point.Next()):
			ballotch <- bl
		case bl.Point().Point.Equal(point.Next().NextRound()):
			ballotch <- bl
		}

		return nil
	}

	st.proposalSelector = DummyProposalSelector(func(ctx context.Context, p base.Point) (base.ProposalSignedFact, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			return t.prpool.get(p), nil
		}
	})

	sctx := newConsensusSwitchContext(StateJoining, ivp)

	deferred, err := st.enter(sctx)
	t.NoError(err)
	deferred()

	nextavp, _ := t.VoteproofsPair(point, point.Next(), manifest.Hash(), t.prpool.hash(point), nil, nodes)
	t.NoError(st.newVoteproof(nextavp))

	t.T().Log("wait new block saved")
	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait save proposal processor"))

		return
	case <-savedch:
	}

	t.T().Log("new init voteproof; wrong previous block")

	ifact := t.NewINITBallotFact(point.Next(), valuehash.RandomSHA256(), t.prpool.hash(point.Next()))
	newivp, err := t.NewINITVoteproof(ifact, t.Local, nodes)
	t.NoError(err)

	err = st.newVoteproof(newivp)

	var ssctx syncingSwitchContext
	t.True(errors.As(err, &ssctx))
	t.Equal(ssctx.height, newivp.Point().Height()-1)
}

func TestNewINITOnACCEPTVoteproofConsensusHandler(t *testing.T) {
	suite.Run(t, new(testNewINITOnACCEPTVoteproofConsensusHandler))
}
