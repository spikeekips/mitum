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

type testNewACCEPTVoteproofOnINITVoteproofConsensusHandler struct {
	baseTestConsensusHandler
}

func (t *testNewACCEPTVoteproofOnINITVoteproofConsensusHandler) TestExpected() {
	nodes := t.nodes(3)

	point := base.NewPoint(base.Height(33), base.Round(44))
	fact := t.newProposalFact(point, []util.Hash{valuehash.RandomSHA256()})
	pr := t.newProposal(t.local, fact)

	manifest := base.NewDummyManifest(fact.Point().Height(), valuehash.RandomSHA256())
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
		if facthash.Equal(fact.Hash()) {
			return fact, nil
		}

		return nil, util.NotFoundError.Errorf("fact not found")
	}

	ballotch := make(chan base.Ballot, 1)
	st.broadcastBallotFunc = func(bl base.Ballot, tolocal bool) error {
		if bl.Point().Point == point.Next() {
			ballotch <- bl
		}

		return nil
	}

	nextpr := t.newProposal(t.local, t.newProposalFact(point, []util.Hash{valuehash.RandomSHA256()}))
	st.proposalSelector = DummyProposalSelector(func(p base.Point) (base.Proposal, error) {
		switch {
		case p == point:
			return pr, nil
		case p == point.Next():
			return nextpr, nil
		default:
			return nil, errors.Errorf("hahaah")
		}
	})

	_, ivp := t.voteproofsPair(point.Decrease(), point, nil, nil, fact.Hash(), nodes)
	sctx := newConsensusSwitchContext(StateJoining, ivp)

	deferred, err := st.enter(sctx)
	t.NoError(err)
	t.NoError(deferred())

	nextavp, _ := t.voteproofsPair(point, point.Next(), nil, fact.Hash(), nil, nodes)
	t.NoError(st.newVoteproof(nextavp))

	t.T().Log("wait new block saved")
	var avp base.ACCEPTVoteproof
	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait save proposal processor"))

		return
	case avp = <-savedch:
		base.CompareVoteproof(t.Assert(), nextavp, avp)
		base.CompareVoteproof(t.Assert(), avp, st.lastACCEPTVoteproof())
	}

	t.T().Log("wait next init ballot")
	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait next init ballot"))

		return
	case bl := <-ballotch:
		t.Equal(point.Next(), bl.Point().Point)

		rbl, ok := bl.(base.INITBallot)
		t.True(ok)

		rfact := rbl.BallotSignedFact().BallotFact()
		t.Equal(avp.BallotMajority().NewBlock(), rfact.PreviousBlock())
		t.True(nextpr.SignedFact().Fact().Hash().Equal(rfact.Proposal()))
	}
}

func (t *testNewACCEPTVoteproofOnINITVoteproofConsensusHandler) TestOld() {
	nodes := t.nodes(3)

	point := base.NewPoint(base.Height(33), base.Round(44))
	fact := t.newProposalFact(point, []util.Hash{valuehash.RandomSHA256()})

	manifest := base.NewDummyManifest(fact.Point().Height(), valuehash.RandomSHA256())
	pp := NewDummyProposalProcessor(manifest)

	st, closefunc := t.newState()
	defer closefunc()

	st.pps.makenew = pp.make
	st.pps.getfact = func(_ context.Context, facthash util.Hash) (base.ProposalFact, error) {
		if facthash.Equal(fact.Hash()) {
			return fact, nil
		}

		return nil, util.NotFoundError.Errorf("fact not found")
	}

	st.broadcastBallotFunc = func(bl base.Ballot, tolocal bool) error {
		return nil
	}

	_, ivp := t.voteproofsPair(point.Decrease(), point, nil, nil, fact.Hash(), nodes)
	sctx := newConsensusSwitchContext(StateJoining, ivp)

	deferred, err := st.enter(sctx)
	t.NoError(err)
	t.NoError(deferred())

	nextavp, _ := t.voteproofsPair(point.Decrease(), point.Next(), nil, fact.Hash(), nil, nodes)
	t.NoError(st.newVoteproof(nextavp))
	t.Nil(st.lastACCEPTVoteproof())
}

func (t *testNewACCEPTVoteproofOnINITVoteproofConsensusHandler) TestHiger() {
	nodes := t.nodes(3)

	point := base.NewPoint(base.Height(33), base.Round(44))
	fact := t.newProposalFact(point, []util.Hash{valuehash.RandomSHA256()})

	manifest := base.NewDummyManifest(fact.Point().Height(), valuehash.RandomSHA256())
	pp := NewDummyProposalProcessor(manifest)

	st, closefunc := t.newState()
	defer closefunc()

	st.pps.makenew = pp.make
	st.pps.getfact = func(_ context.Context, facthash util.Hash) (base.ProposalFact, error) {
		if facthash.Equal(fact.Hash()) {
			return fact, nil
		}

		return nil, util.NotFoundError.Errorf("fact not found")
	}

	st.broadcastBallotFunc = func(bl base.Ballot, tolocal bool) error {
		return nil
	}

	_, ivp := t.voteproofsPair(point.Decrease(), point, nil, nil, fact.Hash(), nodes)
	sctx := newConsensusSwitchContext(StateJoining, ivp)

	deferred, err := st.enter(sctx)
	t.NoError(err)
	t.NoError(deferred())

	nextavp, _ := t.voteproofsPair(point.Next(), point.Next(), nil, fact.Hash(), nil, nodes)
	err = st.newVoteproof(nextavp)
	t.Error(err)
	t.NotNil(st.lastACCEPTVoteproof())

	var nsctx syncingSwitchContext
	t.True(errors.As(err, &nsctx))
}

func (t *testNewACCEPTVoteproofOnINITVoteproofConsensusHandler) TestDraw() {
	nodes := t.nodes(3)

	point := base.NewPoint(base.Height(33), base.Round(44))
	fact := t.newProposalFact(point, []util.Hash{valuehash.RandomSHA256()})
	pr := t.newProposal(t.local, fact)

	manifest := base.NewDummyManifest(fact.Point().Height(), valuehash.RandomSHA256())
	pp := NewDummyProposalProcessor(manifest)

	st, closefunc := t.newState()
	defer closefunc()

	st.pps.makenew = pp.make
	st.pps.getfact = func(_ context.Context, facthash util.Hash) (base.ProposalFact, error) {
		if facthash.Equal(fact.Hash()) {
			return fact, nil
		}

		return nil, util.NotFoundError.Errorf("fact not found")
	}

	nextpr := t.newProposal(t.local, t.newProposalFact(point.NextRound(), []util.Hash{valuehash.RandomSHA256()}))
	st.proposalSelector = DummyProposalSelector(func(p base.Point) (base.Proposal, error) {
		switch {
		case p == point:
			return pr, nil
		case p == point.NextRound():
			return nextpr, nil
		default:
			return nil, errors.Errorf("hahaah")
		}
	})

	ballotch := make(chan base.Ballot, 1)
	st.broadcastBallotFunc = func(bl base.Ballot, tolocal bool) error {
		if bl.Point().Point.Compare(point) < 1 {
			return nil
		}

		ballotch <- bl

		return nil
	}

	_, ivp := t.voteproofsPair(point.Decrease(), point, nil, nil, fact.Hash(), nodes)
	sctx := newConsensusSwitchContext(StateJoining, ivp)

	deferred, err := st.enter(sctx)
	t.NoError(err)
	t.NoError(deferred())

	nextavp, _ := t.voteproofsPair(point, point.Next(), nil, fact.Hash(), nil, nodes)
	nextavp.SetMajority(nil)
	nextavp.SetResult(base.VoteResultDraw)

	t.NoError(st.newVoteproof(nextavp))
	t.NotNil(st.lastACCEPTVoteproof())

	t.T().Log("will wait init ballot of next round")

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait next round init ballot"))

		return
	case bl := <-ballotch:
		t.Equal(point.NextRound(), bl.Point().Point)

		rbl, ok := bl.(base.INITBallot)
		t.True(ok)

		rfact := rbl.BallotSignedFact().BallotFact()
		t.Equal(ivp.BallotMajority().PreviousBlock(), rfact.PreviousBlock())
		t.True(nextpr.SignedFact().Fact().Hash().Equal(rfact.Proposal()))
	}
}

func (t *testNewACCEPTVoteproofOnINITVoteproofConsensusHandler) TestDrawFailedProposalSelector() {
	nodes := t.nodes(3)

	point := base.NewPoint(base.Height(33), base.Round(44))
	fact := t.newProposalFact(point, []util.Hash{valuehash.RandomSHA256()})

	manifest := base.NewDummyManifest(fact.Point().Height(), valuehash.RandomSHA256())
	pp := NewDummyProposalProcessor(manifest)

	st, closefunc := t.newState()
	defer closefunc()

	st.pps.makenew = pp.make
	st.pps.getfact = func(_ context.Context, facthash util.Hash) (base.ProposalFact, error) {
		if facthash.Equal(fact.Hash()) {
			return fact, nil
		}

		return nil, util.NotFoundError.Errorf("fact not found")
	}

	st.proposalSelector = DummyProposalSelector(func(point base.Point) (base.Proposal, error) {
		return nil, errors.Errorf("hahaha")
	})

	st.broadcastBallotFunc = func(bl base.Ballot, tolocal bool) error {
		return nil
	}

	sctxch := make(chan stateSwitchContext, 1)
	st.switchStateFunc = func(sctx stateSwitchContext) error {
		sctxch <- sctx

		return nil
	}

	_, ivp := t.voteproofsPair(point.Decrease(), point, nil, nil, fact.Hash(), nodes)
	sctx := newConsensusSwitchContext(StateJoining, ivp)

	deferred, err := st.enter(sctx)
	t.NoError(err)
	t.NoError(deferred())

	nextavp, _ := t.voteproofsPair(point, point.Next(), nil, fact.Hash(), nil, nodes)
	nextavp.SetMajority(nil)
	nextavp.SetResult(base.VoteResultDraw)

	t.NoError(st.newVoteproof(nextavp))
	t.NotNil(st.lastACCEPTVoteproof())

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait switch context"))

		return
	case nsctx := <-sctxch:
		var bsctx brokenSwitchContext
		t.True(errors.As(nsctx, &bsctx))
		t.Contains(bsctx.Error(), "hahaha")
	}
}

func (t *testNewACCEPTVoteproofOnINITVoteproofConsensusHandler) TestNotProposalProcessorProcessedError() {
	nodes := t.nodes(3)

	point := base.NewPoint(base.Height(33), base.Round(44))
	fact := t.newProposalFact(point, []util.Hash{valuehash.RandomSHA256()})
	pr := t.newProposal(t.local, fact)

	manifest := base.NewDummyManifest(fact.Point().Height(), valuehash.RandomSHA256())
	pp := NewDummyProposalProcessor(manifest)

	pp.saveerr = func(avp base.ACCEPTVoteproof) error {
		if avp.Point().Point == point {
			return NotProposalProcessorProcessedError.Errorf("hehehe")
		}

		return nil
	}

	st, closefunc := t.newState()
	defer closefunc()

	st.pps.makenew = pp.make
	st.pps.getfact = func(_ context.Context, facthash util.Hash) (base.ProposalFact, error) {
		if facthash.Equal(fact.Hash()) {
			return fact, nil
		}

		return nil, util.NotFoundError.Errorf("fact not found")
	}

	st.proposalSelector = DummyProposalSelector(func(point base.Point) (base.Proposal, error) {
		return pr, nil
	})

	sctxch := make(chan stateSwitchContext, 1)
	st.switchStateFunc = func(sctx stateSwitchContext) error {
		sctxch <- sctx

		return nil
	}

	_, ivp := t.voteproofsPair(point.Decrease(), point, nil, nil, fact.Hash(), nodes)
	sctx := newConsensusSwitchContext(StateJoining, ivp)

	deferred, err := st.enter(sctx)
	t.NoError(err)
	t.NoError(deferred())

	nextavp, _ := t.voteproofsPair(point, point.Next(), nil, fact.Hash(), nil, nodes)
	t.NoError(st.newVoteproof(nextavp))

	t.T().Log("wait new block saved, but it will be failed; wait to move syncing")

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait switch context"))

		return
	case nsctx := <-sctxch:
		var ssctx syncingSwitchContext
		t.True(errors.As(nsctx, &ssctx))
		t.Equal(ssctx.height, point.Height())
	}
}

func (t *testNewACCEPTVoteproofOnINITVoteproofConsensusHandler) TestSaveBlockError() {
	nodes := t.nodes(3)

	point := base.NewPoint(base.Height(33), base.Round(44))
	fact := t.newProposalFact(point, []util.Hash{valuehash.RandomSHA256()})
	pr := t.newProposal(t.local, fact)

	manifest := base.NewDummyManifest(fact.Point().Height(), valuehash.RandomSHA256())
	pp := NewDummyProposalProcessor(manifest)

	pp.saveerr = func(avp base.ACCEPTVoteproof) error {
		if avp.Point().Point == point {
			return errors.Errorf("hehehe")
		}

		return nil
	}

	st, closefunc := t.newState()
	defer closefunc()

	st.pps.makenew = pp.make
	st.pps.getfact = func(_ context.Context, facthash util.Hash) (base.ProposalFact, error) {
		if facthash.Equal(fact.Hash()) {
			return fact, nil
		}

		return nil, util.NotFoundError.Errorf("fact not found")
	}

	st.proposalSelector = DummyProposalSelector(func(point base.Point) (base.Proposal, error) {
		return pr, nil
	})

	sctxch := make(chan stateSwitchContext, 1)
	st.switchStateFunc = func(sctx stateSwitchContext) error {
		sctxch <- sctx

		return nil
	}

	_, ivp := t.voteproofsPair(point.Decrease(), point, nil, nil, fact.Hash(), nodes)
	sctx := newConsensusSwitchContext(StateJoining, ivp)

	deferred, err := st.enter(sctx)
	t.NoError(err)
	t.NoError(deferred())

	nextavp, _ := t.voteproofsPair(point, point.Next(), nil, fact.Hash(), nil, nodes)
	t.NoError(st.newVoteproof(nextavp))

	t.T().Log("wait new block saved, but it will be failed; wait to move syncing")

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait switch context"))

		return
	case nsctx := <-sctxch:
		var bsctx brokenSwitchContext
		t.True(errors.As(nsctx, &bsctx))
		t.Contains(bsctx.Error(), "hehehe")
	}
}

func (t *testNewACCEPTVoteproofOnINITVoteproofConsensusHandler) TestHigherAndDraw() {
	nodes := t.nodes(3)

	point := base.NewPoint(base.Height(33), base.Round(44))
	fact := t.newProposalFact(point, []util.Hash{valuehash.RandomSHA256()})
	pr := t.newProposal(t.local, fact)

	manifest := base.NewDummyManifest(fact.Point().Height(), valuehash.RandomSHA256())
	pp := NewDummyProposalProcessor(manifest)

	st, closefunc := t.newState()
	defer closefunc()

	st.pps.makenew = pp.make
	st.pps.getfact = func(_ context.Context, facthash util.Hash) (base.ProposalFact, error) {
		if facthash.Equal(fact.Hash()) {
			return fact, nil
		}

		return nil, util.NotFoundError.Errorf("fact not found")
	}

	st.proposalSelector = DummyProposalSelector(func(p base.Point) (base.Proposal, error) {
		if p == point {
			return pr, nil
		}

		return nil, errors.Errorf("hahaah")
	})

	_, ivp := t.voteproofsPair(point.Decrease(), point, nil, nil, fact.Hash(), nodes)
	sctx := newConsensusSwitchContext(StateJoining, ivp)

	deferred, err := st.enter(sctx)
	t.NoError(err)
	t.NoError(deferred())

	nextavp, _ := t.voteproofsPair(point.Next(), point.Next().Next(), nil, fact.Hash(), nil, nodes)
	nextavp.SetMajority(nil)
	nextavp.SetResult(base.VoteResultDraw)

	err = st.newVoteproof(nextavp)
	t.NotNil(st.lastACCEPTVoteproof())

	var ssctx syncingSwitchContext
	t.True(errors.As(err, &ssctx))

	t.Equal(nextavp.Point().Height(), ssctx.height)
}

func (t *testNewACCEPTVoteproofOnINITVoteproofConsensusHandler) TestHigherRoundDraw() {
	nodes := t.nodes(3)

	point := base.NewPoint(base.Height(33), base.Round(44))
	fact := t.newProposalFact(point, []util.Hash{valuehash.RandomSHA256()})
	pr := t.newProposal(t.local, fact)

	manifest := base.NewDummyManifest(fact.Point().Height(), valuehash.RandomSHA256())
	pp := NewDummyProposalProcessor(manifest)

	st, closefunc := t.newState()
	defer closefunc()

	st.pps.makenew = pp.make
	st.pps.getfact = func(_ context.Context, facthash util.Hash) (base.ProposalFact, error) {
		if facthash.Equal(fact.Hash()) {
			return fact, nil
		}

		return nil, util.NotFoundError.Errorf("fact not found")
	}

	nextpr := t.newProposal(t.local, t.newProposalFact(point.NextRound().NextRound().NextRound(), []util.Hash{valuehash.RandomSHA256()}))
	st.proposalSelector = DummyProposalSelector(func(p base.Point) (base.Proposal, error) {
		switch {
		case p == point:
			return pr, nil
		case p == nextpr.Point().Point:
			return nextpr, nil
		default:
			return nil, errors.Errorf("hahaah")
		}
	})

	ballotch := make(chan base.Ballot, 1)
	st.broadcastBallotFunc = func(bl base.Ballot, tolocal bool) error {
		if bl.Point().Point.Compare(point) < 1 {
			return nil
		}

		ballotch <- bl

		return nil
	}

	_, ivp := t.voteproofsPair(point.Decrease(), point, nil, nil, fact.Hash(), nodes)
	sctx := newConsensusSwitchContext(StateJoining, ivp)

	deferred, err := st.enter(sctx)
	t.NoError(err)
	t.NoError(deferred())

	nextavp, _ := t.voteproofsPair(point.NextRound().NextRound(), point.Next(), nil, fact.Hash(), nil, nodes)
	nextavp.SetMajority(nil)
	nextavp.SetResult(base.VoteResultDraw)

	t.NoError(st.newVoteproof(nextavp))
	t.NotNil(st.lastACCEPTVoteproof())

	t.T().Log("will wait init ballot of next round")

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait next round init ballot"))

		return
	case bl := <-ballotch:
		t.Equal(nextpr.Point().Point, bl.Point().Point)

		rbl, ok := bl.(base.INITBallot)
		t.True(ok)

		rfact := rbl.BallotSignedFact().BallotFact()
		t.Equal(ivp.BallotMajority().PreviousBlock(), rfact.PreviousBlock())
		t.True(nextpr.SignedFact().Fact().Hash().Equal(rfact.Proposal()))
	}
}

func TestNewACCEPTVoteproofOnINITVoteproofConsensusHandler(t *testing.T) {
	suite.Run(t, new(testNewACCEPTVoteproofOnINITVoteproofConsensusHandler))
}

type testNewACCEPTVoteproofOnACCEPTVoteproofConsensusHandler struct {
	baseTestConsensusHandler
}

func (t *testNewACCEPTVoteproofOnACCEPTVoteproofConsensusHandler) TestHigerHeight() {
	nodes := t.nodes(3)

	point := base.NewPoint(base.Height(33), base.Round(44))
	fact := t.newProposalFact(point, []util.Hash{valuehash.RandomSHA256()})

	manifest := base.NewDummyManifest(fact.Point().Height(), valuehash.RandomSHA256())
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
		if facthash.Equal(fact.Hash()) {
			return fact, nil
		}

		return nil, util.NotFoundError.Errorf("fact not found")
	}

	nextpr := t.newProposal(t.local, t.newProposalFact(point.Next(), []util.Hash{valuehash.RandomSHA256()}))
	st.proposalSelector = DummyProposalSelector(func(p base.Point) (base.Proposal, error) {
		if p == nextpr.Point().Point {
			return nextpr, nil
		}

		return nil, errors.Errorf("hahaah")
	})

	_, ivp := t.voteproofsPair(point.Decrease(), point, nil, nil, fact.Hash(), nodes)
	sctx := newConsensusSwitchContext(StateJoining, ivp)

	deferred, err := st.enter(sctx)
	t.NoError(err)
	t.NoError(deferred())

	avp, _ := t.voteproofsPair(point, point.Next(), nil, fact.Hash(), nil, nodes)
	t.NoError(st.newVoteproof(avp))

	t.T().Log("wait new block saved")
	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait save proposal processor"))

		return
	case ravp := <-savedch:
		base.CompareVoteproof(t.Assert(), avp, ravp)
		base.CompareVoteproof(t.Assert(), ravp, st.lastACCEPTVoteproof())
	}

	t.T().Log("new accept voteproof; higher height")

	newavp, _ := t.voteproofsPair(point.Next().Next(), point.Next(), nil, nil, nil, nodes)
	err = st.newVoteproof(newavp)

	var ssctx syncingSwitchContext
	t.True(errors.As(err, &ssctx))
	t.Equal(ssctx.height, newavp.Point().Height())
}

func (t *testNewACCEPTVoteproofOnACCEPTVoteproofConsensusHandler) TestDrawAndHigerHeight() {
	nodes := t.nodes(3)

	point := base.NewPoint(base.Height(33), base.Round(44))
	fact := t.newProposalFact(point, []util.Hash{valuehash.RandomSHA256()})

	manifest := base.NewDummyManifest(fact.Point().Height(), valuehash.RandomSHA256())
	pp := NewDummyProposalProcessor(manifest)

	st, closefunc := t.newState()
	defer closefunc()

	st.pps.makenew = pp.make
	st.pps.getfact = func(_ context.Context, facthash util.Hash) (base.ProposalFact, error) {
		if facthash.Equal(fact.Hash()) {
			return fact, nil
		}

		return nil, util.NotFoundError.Errorf("fact not found")
	}

	nextprch := make(chan base.Point, 1)
	nextpr := t.newProposal(t.local, t.newProposalFact(point.NextRound(), []util.Hash{valuehash.RandomSHA256()}))
	st.proposalSelector = DummyProposalSelector(func(p base.Point) (base.Proposal, error) {
		if p == nextpr.Point().Point {
			nextprch <- p
			return nextpr, nil
		}

		return nil, errors.Errorf("hahaah")
	})

	_, ivp := t.voteproofsPair(point.Decrease(), point, nil, nil, fact.Hash(), nodes)
	sctx := newConsensusSwitchContext(StateJoining, ivp)

	deferred, err := st.enter(sctx)
	t.NoError(err)
	t.NoError(deferred())

	avp, _ := t.voteproofsPair(point, point.Next(), nil, fact.Hash(), nil, nodes)
	avp.SetMajority(nil)
	avp.SetResult(base.VoteResultDraw)
	t.NoError(st.newVoteproof(avp))

	t.T().Log("wait new block saved")
	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait save proposal processor"))

		return
	case p := <-nextprch:
		t.Equal(point.NextRound(), p)
	}

	t.T().Log("new accept voteproof; higher height")

	newavp, _ := t.voteproofsPair(point.Next(), point.Next().Next(), nil, nil, nil, nodes)
	err = st.newVoteproof(newavp)

	var ssctx syncingSwitchContext
	t.True(errors.As(err, &ssctx))
	t.Equal(ssctx.height, newavp.Point().Height())
}

func (t *testNewACCEPTVoteproofOnACCEPTVoteproofConsensusHandler) TestDrawAndHigerRound() {
	nodes := t.nodes(3)

	point := base.NewPoint(base.Height(33), base.Round(44))
	fact := t.newProposalFact(point, []util.Hash{valuehash.RandomSHA256()})

	manifest := base.NewDummyManifest(fact.Point().Height(), valuehash.RandomSHA256())
	pp := NewDummyProposalProcessor(manifest)

	st, closefunc := t.newState()
	defer closefunc()

	st.pps.makenew = pp.make
	st.pps.getfact = func(_ context.Context, facthash util.Hash) (base.ProposalFact, error) {
		if facthash.Equal(fact.Hash()) {
			return fact, nil
		}

		return nil, util.NotFoundError.Errorf("fact not found")
	}

	nextprch := make(chan base.Point, 1)
	nextpr := t.newProposal(t.local, t.newProposalFact(point.NextRound(), []util.Hash{valuehash.RandomSHA256()}))
	st.proposalSelector = DummyProposalSelector(func(p base.Point) (base.Proposal, error) {
		if p == nextpr.Point().Point {
			nextprch <- p
			return nextpr, nil
		}

		return nil, errors.Errorf("hahaah")
	})

	_, ivp := t.voteproofsPair(point.Decrease(), point, nil, nil, fact.Hash(), nodes)
	sctx := newConsensusSwitchContext(StateJoining, ivp)

	deferred, err := st.enter(sctx)
	t.NoError(err)
	t.NoError(deferred())

	avp, _ := t.voteproofsPair(point, point.Next(), nil, fact.Hash(), nil, nodes)
	avp.SetMajority(nil)
	avp.SetResult(base.VoteResultDraw)
	t.NoError(st.newVoteproof(avp))

	t.T().Log("wait new block saved")
	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait save proposal processor"))

		return
	case p := <-nextprch:
		t.Equal(point.NextRound(), p)
	}

	t.T().Log("new accept voteproof; higher height")

	newavp, _ := t.voteproofsPair(point.NextRound(), point.Next(), nil, nil, nil, nodes)
	err = st.newVoteproof(newavp)

	var ssctx syncingSwitchContext
	t.True(errors.As(err, &ssctx))
	t.Equal(ssctx.height, newavp.Point().Height())
}

func (t *testNewACCEPTVoteproofOnACCEPTVoteproofConsensusHandler) TestDrawAndDrawAgain() {
	nodes := t.nodes(3)

	point := base.NewPoint(base.Height(33), base.Round(44))
	fact := t.newProposalFact(point, []util.Hash{valuehash.RandomSHA256()})
	pr := t.newProposal(t.local, fact)

	manifest := base.NewDummyManifest(fact.Point().Height(), valuehash.RandomSHA256())
	pp := NewDummyProposalProcessor(manifest)

	st, closefunc := t.newState()
	defer closefunc()

	st.pps.makenew = pp.make
	st.pps.getfact = func(_ context.Context, facthash util.Hash) (base.ProposalFact, error) {
		if facthash.Equal(fact.Hash()) {
			return fact, nil
		}

		return nil, util.NotFoundError.Errorf("fact not found")
	}

	newprch := make(chan base.Point, 1)
	newpr := t.newProposal(t.local, t.newProposalFact(point.NextRound(), []util.Hash{valuehash.RandomSHA256()}))
	nextpr := t.newProposal(t.local, t.newProposalFact(point.NextRound().NextRound(), []util.Hash{valuehash.RandomSHA256()}))
	st.proposalSelector = DummyProposalSelector(func(p base.Point) (base.Proposal, error) {
		switch p {
		case pr.Point().Point:
			return pr, nil
		case nextpr.Point().Point:
			return nextpr, nil
		case newpr.Point().Point:
			newprch <- p
			return newpr, nil
		}

		return nil, errors.Errorf("hahaah")
	})

	ballotch := make(chan base.Ballot, 1)
	st.broadcastBallotFunc = func(bl base.Ballot, tolocal bool) error {
		if bl.Point().Point.Compare(point.NextRound().NextRound()) == 0 {
			ballotch <- bl
		}

		return nil
	}

	_, ivp := t.voteproofsPair(point.Decrease(), point, nil, nil, fact.Hash(), nodes)
	sctx := newConsensusSwitchContext(StateJoining, ivp)

	deferred, err := st.enter(sctx)
	t.NoError(err)
	t.NoError(deferred())

	avp, _ := t.voteproofsPair(point, point.Next(), nil, fact.Hash(), nil, nodes)
	avp.SetMajority(nil)
	avp.SetResult(base.VoteResultDraw)
	t.NoError(st.newVoteproof(avp))

	t.T().Log("wait new block saved")
	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait"))

		return
	case p := <-newprch:
		t.Equal(point.NextRound(), p)
	}

	t.T().Log("new accept voteproof; higher height")

	newavp, _ := t.voteproofsPair(point.NextRound(), point.Next(), nil, nil, nil, nodes)
	newavp.SetMajority(nil)
	newavp.SetResult(base.VoteResultDraw)

	t.NoError(st.newVoteproof(newavp))

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait accept ballot"))

		return
	case bl := <-ballotch:
		t.NoError(bl.IsValid(t.policy.NetworkID()))

		rbl, ok := bl.(base.INITBallot)
		t.True(ok)

		t.Equal(nextpr.Point().Point, bl.Point().Point)

		rfact := rbl.BallotSignedFact().BallotFact()
		t.True(nextpr.SignedFact().Fact().Hash().Equal(rfact.Proposal()))
	}
}

func TestNewACCEPTVoteproofOnACCEPTVoteproofConsensusHandler(t *testing.T) {
	suite.Run(t, new(testNewACCEPTVoteproofOnACCEPTVoteproofConsensusHandler))
}
