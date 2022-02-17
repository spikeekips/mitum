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

type testNewINITVoteproofOnINITVoteproofConsensusHandler struct {
	baseTestConsensusHandler
}

func (t *testNewINITVoteproofOnINITVoteproofConsensusHandler) TestHigherHeight() {
	nodes := t.nodes(3)

	point := base.NewPoint(base.Height(33), base.Round(44))
	fact := t.prpool.getfact(point)

	manifest := base.NewDummyManifest(fact.Point().Height(), valuehash.RandomSHA256())
	pp := NewDummyProposalProcessor(manifest)

	st, closefunc := t.newState()
	defer closefunc()

	prch := make(chan util.Hash, 1)
	st.pps.makenew = pp.make
	st.pps.getfact = func(_ context.Context, facthash util.Hash) (base.ProposalFact, error) {
		pr, err := t.prpool.factByHash(facthash)
		if err != nil {
			return nil, err
		}

		if fact.Hash().Equal(facthash) {
			prch <- facthash
		}

		return pr, nil
	}

	st.broadcastBallotFunc = func(bl base.Ballot, tolocal bool) error {
		return nil
	}

	st.proposalSelector = DummyProposalSelector(func(p base.Point) (base.Proposal, error) {
		return nil, errors.Errorf("hahaah")
	})

	_, ivp := t.voteproofsPair(point.Decrease(), point, nil, nil, fact.Hash(), nodes)
	t.True(st.setLastVoteproof(ivp))
	sctx := newConsensusSwitchContext(StateJoining, ivp)

	deferred, err := st.enter(sctx)
	t.NoError(err)
	t.NoError(deferred())

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait"))

		return
	case <-prch:
	}

	t.T().Log("new higher init voteproof")
	newpoint := point.Next()
	_, newivp := t.voteproofsPair(newpoint.Decrease(), newpoint, nil, nil, fact.Hash(), nodes)

	err = st.newVoteproof(newivp)
	t.Error(err)

	var ssctx syncingSwitchContext
	t.True(errors.As(err, &ssctx))
	t.Equal(ssctx.height, newpoint.Height()-1)
}

func (t *testNewINITVoteproofOnINITVoteproofConsensusHandler) TestNextRoundButAlreadyFinished() {
	nodes := t.nodes(3)

	point := base.NewPoint(base.Height(33), base.Round(44))
	fact := t.prpool.getfact(point)

	manifest := base.NewDummyManifest(fact.Point().Height(), valuehash.RandomSHA256())
	pp := NewDummyProposalProcessor(manifest)

	st, closefunc := t.newState()
	defer closefunc()

	prch := make(chan util.Hash, 1)
	st.pps.makenew = pp.make
	st.pps.getfact = func(_ context.Context, facthash util.Hash) (base.ProposalFact, error) {
		pr, err := t.prpool.factByHash(facthash)
		if err != nil {
			return nil, err
		}

		if fact.Hash().Equal(facthash) {
			prch <- facthash
		}

		return pr, nil
	}

	st.broadcastBallotFunc = func(bl base.Ballot, tolocal bool) error {
		return nil
	}

	st.proposalSelector = DummyProposalSelector(func(p base.Point) (base.Proposal, error) {
		return nil, errors.Errorf("hahaah")
	})

	_, ivp := t.voteproofsPair(point.Decrease(), point, nil, nil, fact.Hash(), nodes)
	t.True(st.setLastVoteproof(ivp))
	sctx := newConsensusSwitchContext(StateJoining, ivp)

	deferred, err := st.enter(sctx)
	t.NoError(err)
	t.NoError(deferred())

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait"))

		return
	case <-prch:
	}

	t.T().Log("next round init voteproof")
	newpoint := point.NextRound()
	_, newivp := t.voteproofsPair(newpoint.Decrease(), newpoint, nil, nil, fact.Hash(), nodes)

	t.NoError(st.newVoteproof(newivp))
}

func (t *testNewINITVoteproofOnINITVoteproofConsensusHandler) TestDrawBeforePreviousBlockNotMatched() {
	nodes := t.nodes(3)

	point := base.NewPoint(base.Height(33), base.Round(44))

	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	pp := NewDummyProposalProcessor(manifest)

	savedch := make(chan base.ACCEPTVoteproof, 1)
	pp.saveerr = func(avp base.ACCEPTVoteproof) error {
		savedch <- avp
		return nil
	}

	st, closefunc := t.newState()
	defer closefunc()

	st.pps.makenew = pp.make
	st.pps.getfact = func(_ context.Context, facthash util.Hash) (base.ProposalFact, error) {
		return t.prpool.factByHash(facthash)
	}

	ballotch := make(chan base.Ballot, 1)
	st.broadcastBallotFunc = func(bl base.Ballot, tolocal bool) error {
		switch {
		case bl.Point().Point == point.Next():
			ballotch <- bl
		case bl.Point().Point == point.Next().NextRound():
			ballotch <- bl
		}

		return nil
	}

	st.proposalSelector = DummyProposalSelector(func(p base.Point) (base.Proposal, error) {
		return t.prpool.get(p), nil
	})

	_, ivp := t.voteproofsPair(point.Decrease(), point, nil, nil, t.prpool.hash(point), nodes)
	t.True(st.setLastVoteproof(ivp))
	sctx := newConsensusSwitchContext(StateJoining, ivp)

	deferred, err := st.enter(sctx)
	t.NoError(err)
	t.NoError(deferred())

	nextavp, drawivp := t.voteproofsPair(point, point.Next(), nil, t.prpool.hash(point), nil, nodes)
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

	drawivp.SetMajority(nil)
	drawivp.SetResult(base.VoteResultDraw)
	drawivp.finish()

	t.NoError(st.newVoteproof(drawivp))

	t.T().Log("new draw init voteproof")
	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait next round init ballot"))

		return
	case bl := <-ballotch:
		t.Equal(point.Next().NextRound(), bl.Point().Point)
	}

	_, newivp := t.voteproofsPair(point, drawivp.Point().Point.NextRound(), valuehash.RandomSHA256(), nil, t.prpool.hash(drawivp.Point().Point.NextRound()), nodes)
	err = st.newVoteproof(newivp)

	var ssctx syncingSwitchContext
	t.True(errors.As(err, &ssctx))
	t.Equal(ssctx.height, newivp.Point().Height()-1)
}

func (t *testNewINITVoteproofOnINITVoteproofConsensusHandler) TestDrawBefore() {
	nodes := t.nodes(3)

	point := base.NewPoint(base.Height(33), base.Round(44))

	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	pp := NewDummyProposalProcessor(manifest)

	savedch := make(chan base.ACCEPTVoteproof, 1)
	pp.saveerr = func(avp base.ACCEPTVoteproof) error {
		savedch <- avp
		return nil
	}

	st, closefunc := t.newState()
	defer closefunc()

	st.pps.makenew = pp.make
	st.pps.getfact = func(_ context.Context, facthash util.Hash) (base.ProposalFact, error) {
		return t.prpool.factByHash(facthash)
	}

	ballotch := make(chan base.Ballot, 1)
	st.broadcastBallotFunc = func(bl base.Ballot, tolocal bool) error {
		switch {
		case bl.Point().Point == point.Next():
			ballotch <- bl
		case bl.Point().Point == point.Next().NextRound():
			ballotch <- bl
		}

		return nil
	}

	st.proposalSelector = DummyProposalSelector(func(p base.Point) (base.Proposal, error) {
		return t.prpool.get(p), nil
	})

	_, ivp := t.voteproofsPair(point.Decrease(), point, nil, nil, t.prpool.hash(point), nodes)
	t.True(st.setLastVoteproof(ivp))
	sctx := newConsensusSwitchContext(StateJoining, ivp)

	deferred, err := st.enter(sctx)
	t.NoError(err)
	t.NoError(deferred())

	nextavp, drawivp := t.voteproofsPair(point, point.Next(), nil, t.prpool.hash(point), nil, nodes)
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

	drawivp.SetMajority(nil)
	drawivp.SetResult(base.VoteResultDraw)
	drawivp.finish()

	t.NoError(st.newVoteproof(drawivp))

	t.T().Log("new draw init voteproof")
	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait next round init ballot"))

		return
	case bl := <-ballotch:
		t.Equal(point.Next().NextRound(), bl.Point().Point)
	}

	_, newivp := t.voteproofsPair(point, drawivp.Point().Point.NextRound(), nextavp.BallotMajority().NewBlock(), nil, t.prpool.hash(drawivp.Point().Point.NextRound()), nodes)
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

func (t *testNewINITVoteproofOnINITVoteproofConsensusHandler) TestDrawAndDrawAgain() {
	nodes := t.nodes(3)

	point := base.NewPoint(base.Height(33), base.Round(44))

	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	pp := NewDummyProposalProcessor(manifest)

	savedch := make(chan base.ACCEPTVoteproof, 1)
	pp.saveerr = func(avp base.ACCEPTVoteproof) error {
		savedch <- avp
		return nil
	}

	st, closefunc := t.newState()
	defer closefunc()

	st.pps.makenew = pp.make
	st.pps.getfact = func(_ context.Context, facthash util.Hash) (base.ProposalFact, error) {
		return t.prpool.factByHash(facthash)
	}

	ballotch := make(chan base.Ballot, 1)
	st.broadcastBallotFunc = func(bl base.Ballot, tolocal bool) error {
		switch {
		case bl.Point().Point == point.Next():
			ballotch <- bl
		case bl.Point().Point == point.Next().NextRound():
			ballotch <- bl
		case bl.Point().Point == point.Next().NextRound().NextRound():
			ballotch <- bl
		}

		return nil
	}

	st.proposalSelector = DummyProposalSelector(func(p base.Point) (base.Proposal, error) {
		return t.prpool.get(p), nil
	})

	_, ivp := t.voteproofsPair(point.Decrease(), point, nil, nil, t.prpool.hash(point), nodes)
	t.True(st.setLastVoteproof(ivp))
	sctx := newConsensusSwitchContext(StateJoining, ivp)

	deferred, err := st.enter(sctx)
	t.NoError(err)
	t.NoError(deferred())

	nextavp, drawivp := t.voteproofsPair(point, point.Next(), nil, t.prpool.hash(point), nil, nodes)
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

	drawivp.SetMajority(nil)
	drawivp.SetResult(base.VoteResultDraw)
	drawivp.finish()

	t.NoError(st.newVoteproof(drawivp))

	t.T().Log("new draw init voteproof")
	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait next round init ballot"))

		return
	case bl := <-ballotch:
		t.Equal(point.Next().NextRound(), bl.Point().Point)
	}

	_, newivp := t.voteproofsPair(point, drawivp.Point().Point.NextRound(), nil, nil, t.prpool.hash(drawivp.Point().Point.NextRound()), nodes)
	newivp.SetMajority(nil)
	newivp.SetResult(base.VoteResultDraw)
	newivp.finish()

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

func TestNewINITVoteproofOnINITVoteproofConsensusHandler(t *testing.T) {
	suite.Run(t, new(testNewINITVoteproofOnINITVoteproofConsensusHandler))
}

type testNewINITVoteproofOnACCEPTVoteproofConsensusHandler struct {
	baseTestConsensusHandler
}

func (t *testNewINITVoteproofOnACCEPTVoteproofConsensusHandler) TestExpected() {
	nodes := t.nodes(3)

	point := base.NewPoint(base.Height(33), base.Round(44))

	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	pp := NewDummyProposalProcessor(manifest)

	savedch := make(chan base.ACCEPTVoteproof, 1)
	pp.saveerr = func(avp base.ACCEPTVoteproof) error {
		savedch <- avp
		return nil
	}

	st, closefunc := t.newState()
	defer closefunc()
	st.SetLogging(logging.TestNilLogging)

	st.pps.makenew = pp.make
	st.pps.getfact = func(_ context.Context, facthash util.Hash) (base.ProposalFact, error) {
		return t.prpool.factByHash(facthash)
	}

	ballotch := make(chan base.Ballot, 1)
	st.broadcastBallotFunc = func(bl base.Ballot, tolocal bool) error {
		if p := bl.Point(); p.Point == point.Next() && p.Stage() == base.StageACCEPT {
			ballotch <- bl
		}

		return nil
	}

	st.proposalSelector = DummyProposalSelector(func(p base.Point) (base.Proposal, error) {
		return t.prpool.get(p), nil
	})

	_, ivp := t.voteproofsPair(point.Decrease(), point, nil, nil, t.prpool.hash(point), nodes)
	t.True(st.setLastVoteproof(ivp))
	sctx := newConsensusSwitchContext(StateJoining, ivp)

	deferred, err := st.enter(sctx)
	t.NoError(err)
	t.NoError(deferred())

	nextavp, nextivp := t.voteproofsPair(point, point.Next(), nil, t.prpool.hash(point), t.prpool.hash(point.Next()), nodes)
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

func (t *testNewINITVoteproofOnACCEPTVoteproofConsensusHandler) TestHigherHeight() {
	nodes := t.nodes(3)

	point := base.NewPoint(base.Height(33), base.Round(44))

	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	pp := NewDummyProposalProcessor(manifest)

	savedch := make(chan base.ACCEPTVoteproof, 1)
	pp.saveerr = func(avp base.ACCEPTVoteproof) error {
		savedch <- avp
		return nil
	}

	st, closefunc := t.newState()
	defer closefunc()

	st.pps.makenew = pp.make
	st.pps.getfact = func(_ context.Context, facthash util.Hash) (base.ProposalFact, error) {
		return t.prpool.factByHash(facthash)
	}

	ballotch := make(chan base.Ballot, 1)
	st.broadcastBallotFunc = func(bl base.Ballot, tolocal bool) error {
		switch {
		case bl.Point().Point == point.Next():
			ballotch <- bl
		case bl.Point().Point == point.Next().NextRound():
			ballotch <- bl
		}

		return nil
	}

	st.proposalSelector = DummyProposalSelector(func(p base.Point) (base.Proposal, error) {
		return t.prpool.get(p), nil
	})

	_, ivp := t.voteproofsPair(point.Decrease(), point, nil, nil, t.prpool.hash(point), nodes)
	t.True(st.setLastVoteproof(ivp))
	sctx := newConsensusSwitchContext(StateJoining, ivp)

	deferred, err := st.enter(sctx)
	t.NoError(err)
	t.NoError(deferred())

	nextavp, _ := t.voteproofsPair(point, point.Next(), nil, t.prpool.hash(point), nil, nodes)
	t.NoError(st.newVoteproof(nextavp))

	t.T().Log("wait new block saved")
	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait save proposal processor"))

		return
	case <-savedch:
	}

	_, newivp := t.voteproofsPair(point.Next(), point.Next().Next(), nil, nil, t.prpool.hash(point.Next().Next()), nodes)
	err = st.newVoteproof(newivp)

	var ssctx syncingSwitchContext
	t.True(errors.As(err, &ssctx))
	t.Equal(ssctx.height, newivp.Point().Height()-1)
}

func (t *testNewINITVoteproofOnACCEPTVoteproofConsensusHandler) TestPreviousBlockNotMatch() {
	nodes := t.nodes(3)

	point := base.NewPoint(base.Height(33), base.Round(44))

	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	pp := NewDummyProposalProcessor(manifest)

	savedch := make(chan base.ACCEPTVoteproof, 1)
	pp.saveerr = func(avp base.ACCEPTVoteproof) error {
		savedch <- avp
		return nil
	}

	st, closefunc := t.newState()
	defer closefunc()

	st.pps.makenew = pp.make
	st.pps.getfact = func(_ context.Context, facthash util.Hash) (base.ProposalFact, error) {
		return t.prpool.factByHash(facthash)
	}

	ballotch := make(chan base.Ballot, 1)
	st.broadcastBallotFunc = func(bl base.Ballot, tolocal bool) error {
		switch {
		case bl.Point().Point == point.Next():
			ballotch <- bl
		case bl.Point().Point == point.Next().NextRound():
			ballotch <- bl
		}

		return nil
	}

	st.proposalSelector = DummyProposalSelector(func(p base.Point) (base.Proposal, error) {
		return t.prpool.get(p), nil
	})

	_, ivp := t.voteproofsPair(point.Decrease(), point, nil, nil, t.prpool.hash(point), nodes)
	t.True(st.setLastVoteproof(ivp))
	sctx := newConsensusSwitchContext(StateJoining, ivp)

	deferred, err := st.enter(sctx)
	t.NoError(err)
	t.NoError(deferred())

	nextavp, _ := t.voteproofsPair(point, point.Next(), nil, t.prpool.hash(point), nil, nodes)
	t.NoError(st.newVoteproof(nextavp))

	t.T().Log("wait new block saved")
	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait save proposal processor"))

		return
	case <-savedch:
	}

	t.T().Log("new init voteproof; wrong previous block")

	ifact := t.newINITBallotFact(point.Next(), valuehash.RandomSHA256(), t.prpool.hash(point.Next()))
	newivp, err := t.newINITVoteproof(ifact, t.local, nodes)
	t.NoError(err)

	err = st.newVoteproof(newivp)

	var ssctx syncingSwitchContext
	t.True(errors.As(err, &ssctx))
	t.Equal(ssctx.height, newivp.Point().Height()-1)
}

func TestNewINITVoteproofOnACCEPTVoteproofConsensusHandler(t *testing.T) {
	suite.Run(t, new(testNewINITVoteproofOnACCEPTVoteproofConsensusHandler))
}
