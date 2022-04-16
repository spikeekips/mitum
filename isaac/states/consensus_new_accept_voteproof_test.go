package isaacstates

import (
	"context"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
)

type testNewACCEPTOnINITVoteproofConsensusHandler struct {
	baseTestConsensusHandler
}

func (t *testNewACCEPTOnINITVoteproofConsensusHandler) TestExpected() {
	point := base.RawPoint(33, 44)
	suf, nodes := isaac.NewTestSuffrage(2, t.Local)

	st, closefunc, pp, ivp := t.newStateWithINITVoteproof(point, suf)
	defer closefunc()
	st.SetLogging(logging.TestNilLogging)

	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	pp.Processerr = func(context.Context, base.ProposalFact, base.INITVoteproof) (base.Manifest, error) {
		return manifest, nil
	}
	savedch := make(chan base.ACCEPTVoteproof, 1)
	pp.Saveerr = func(_ context.Context, avp base.ACCEPTVoteproof) error {
		savedch <- avp
		return nil
	}

	ballotch := make(chan base.Ballot, 1)
	st.broadcastBallotFunc = func(bl base.Ballot) error {
		if bl.Point().Point.Equal(point.Next()) {
			ballotch <- bl
		}

		return nil
	}

	nextpr := t.PRPool.Get(point.Next())

	sctx := newConsensusSwitchContext(StateJoining, ivp)

	deferred, err := st.enter(sctx)
	t.NoError(err)
	deferred()

	fact := t.PRPool.GetFact(point)
	nextavp, _ := t.VoteproofsPair(point, point.Next(), manifest.Hash(), fact.Hash(), nil, nodes)
	t.NoError(st.newVoteproof(nextavp))

	t.T().Log("wait new block saved")
	var avp base.ACCEPTVoteproof
	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait save proposal processor"))

		return
	case avp = <-savedch:
		base.EqualVoteproof(t.Assert(), nextavp, avp)
		base.EqualVoteproof(t.Assert(), avp, st.lastVoteproof().accept())
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
		t.True(nextpr.Fact().Hash().Equal(rfact.Proposal()))
	}
}

func (t *testNewACCEPTOnINITVoteproofConsensusHandler) TestOld() {
	point := base.RawPoint(33, 44)
	suf, nodes := isaac.NewTestSuffrage(2, t.Local)

	st, closefunc, _, ivp := t.newStateWithINITVoteproof(point, suf)
	defer closefunc()

	sctx := newConsensusSwitchContext(StateJoining, ivp)

	deferred, err := st.enter(sctx)
	t.NoError(err)
	deferred()

	fact := t.PRPool.GetFact(point)
	nextavp, _ := t.VoteproofsPair(point.Decrease(), point.Next(), nil, fact.Hash(), nil, nodes)
	t.NoError(st.newVoteproof(nextavp))
	t.Nil(st.lastVoteproof().accept())
}

func (t *testNewACCEPTOnINITVoteproofConsensusHandler) TestHiger() {
	point := base.RawPoint(33, 44)
	suf, nodes := isaac.NewTestSuffrage(2, t.Local)

	st, closefunc, _, ivp := t.newStateWithINITVoteproof(point, suf)
	defer closefunc()

	sctx := newConsensusSwitchContext(StateJoining, ivp)

	deferred, err := st.enter(sctx)
	t.NoError(err)
	deferred()

	fact := t.PRPool.GetFact(point)
	nextavp, _ := t.VoteproofsPair(point.Next(), point.Next(), nil, fact.Hash(), nil, nodes)
	err = st.newVoteproof(nextavp)
	t.Error(err)
	t.NotNil(st.lastVoteproof().accept())

	var nsctx syncingSwitchContext
	t.True(errors.As(err, &nsctx))
}

func (t *testNewACCEPTOnINITVoteproofConsensusHandler) TestDraw() {
	point := base.RawPoint(33, 44)
	suf, nodes := isaac.NewTestSuffrage(2, t.Local)

	st, closefunc, _, ivp := t.newStateWithINITVoteproof(point, suf)
	defer closefunc()

	nextpr := t.PRPool.Get(point.NextRound())

	ballotch := make(chan base.Ballot, 1)
	st.broadcastBallotFunc = func(bl base.Ballot) error {
		if bl.Point().Point.Compare(point) < 1 {
			return nil
		}

		ballotch <- bl

		return nil
	}

	sctx := newConsensusSwitchContext(StateJoining, ivp)

	deferred, err := st.enter(sctx)
	t.NoError(err)
	deferred()

	fact := t.PRPool.GetFact(point)
	nextavp, _ := t.VoteproofsPair(point, point.Next(), nil, fact.Hash(), nil, nodes)
	nextavp.SetResult(base.VoteResultDraw).Finish()

	t.NoError(st.newVoteproof(nextavp))
	t.NotNil(st.lastVoteproof().accept())

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
		t.True(nextpr.Fact().Hash().Equal(rfact.Proposal()))
	}
}

func (t *testNewACCEPTOnINITVoteproofConsensusHandler) TestDrawFailedProposalSelector() {
	point := base.RawPoint(33, 44)
	suf, nodes := isaac.NewTestSuffrage(2, t.Local)

	st, closefunc, pp, ivp := t.newStateWithINITVoteproof(point, suf)
	defer closefunc()

	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	pp.Processerr = func(context.Context, base.ProposalFact, base.INITVoteproof) (base.Manifest, error) {
		return manifest, nil
	}

	st.proposalSelector = isaac.DummyProposalSelector(func(ctx context.Context, point base.Point) (base.ProposalSignedFact, error) {
		return nil, errors.Errorf("hahaha")
	})

	sctxch := make(chan switchContext, 1)
	st.switchStateFunc = func(sctx switchContext) error {
		sctxch <- sctx

		return nil
	}

	sctx := newConsensusSwitchContext(StateJoining, ivp)

	deferred, err := st.enter(sctx)
	t.NoError(err)
	deferred()

	fact := t.PRPool.GetFact(point)
	nextavp, _ := t.VoteproofsPair(point, point.Next(), nil, fact.Hash(), nil, nodes)
	nextavp.SetResult(base.VoteResultDraw).Finish()

	t.NoError(st.newVoteproof(nextavp))
	t.NotNil(st.lastVoteproof().accept())

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

func (t *testNewACCEPTOnINITVoteproofConsensusHandler) TestNotProposalProcessorProcessedError() {
	point := base.RawPoint(33, 44)
	suf, nodes := isaac.NewTestSuffrage(2, t.Local)

	st, closefunc, pp, ivp := t.newStateWithINITVoteproof(point, suf)
	defer closefunc()

	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	pp.Processerr = func(context.Context, base.ProposalFact, base.INITVoteproof) (base.Manifest, error) {
		return manifest, nil
	}

	pp.Saveerr = func(_ context.Context, avp base.ACCEPTVoteproof) error {
		if avp.Point().Point.Equal(point) {
			return isaac.NotProposalProcessorProcessedError.Errorf("hehehe")
		}

		return nil
	}

	sctx := newConsensusSwitchContext(StateJoining, ivp)

	deferred, err := st.enter(sctx)
	t.NoError(err)
	deferred()

	fact := t.PRPool.GetFact(point)
	nextavp, _ := t.VoteproofsPair(point, point.Next(), nil, fact.Hash(), nil, nodes)

	t.T().Log("wait new block saved, but it will be failed; wait to move syncing")

	err = st.newVoteproof(nextavp)

	var ssctx syncingSwitchContext
	t.True(errors.As(err, &ssctx))
	t.Equal(ssctx.height, point.Height())
}

func (t *testNewACCEPTOnINITVoteproofConsensusHandler) TestSaveBlockError() {
	point := base.RawPoint(33, 44)
	suf, nodes := isaac.NewTestSuffrage(2, t.Local)

	st, closefunc, pp, ivp := t.newStateWithINITVoteproof(point, suf)
	defer closefunc()

	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	pp.Processerr = func(context.Context, base.ProposalFact, base.INITVoteproof) (base.Manifest, error) {
		return manifest, nil
	}

	pp.Saveerr = func(_ context.Context, avp base.ACCEPTVoteproof) error {
		if avp.Point().Point.Equal(point) {
			return errors.Errorf("hehehe")
		}

		return nil
	}

	sctx := newConsensusSwitchContext(StateJoining, ivp)

	deferred, err := st.enter(sctx)
	t.NoError(err)
	deferred()

	fact := t.PRPool.GetFact(point)
	nextavp, _ := t.VoteproofsPair(point, point.Next(), nil, fact.Hash(), nil, nodes)

	err = st.newVoteproof(nextavp)

	t.T().Log("wait new block saved, but it will be failed; wait to move syncing")

	var bsctx brokenSwitchContext
	t.True(errors.As(err, &bsctx))
	t.Contains(bsctx.Error(), "hehehe")
}

func (t *testNewACCEPTOnINITVoteproofConsensusHandler) TestHigherAndDraw() {
	point := base.RawPoint(33, 44)
	suf, nodes := isaac.NewTestSuffrage(2, t.Local)

	st, closefunc, _, ivp := t.newStateWithINITVoteproof(point, suf)
	defer closefunc()

	sctx := newConsensusSwitchContext(StateJoining, ivp)

	deferred, err := st.enter(sctx)
	t.NoError(err)
	deferred()

	fact := t.PRPool.GetFact(point)
	nextavp, _ := t.VoteproofsPair(point.Next(), point.Next().Next(), nil, fact.Hash(), nil, nodes)
	nextavp.SetResult(base.VoteResultDraw).Finish()

	err = st.newVoteproof(nextavp)
	t.NotNil(st.lastVoteproof().accept())

	var ssctx syncingSwitchContext
	t.True(errors.As(err, &ssctx))

	t.Equal(nextavp.Point().Height(), ssctx.height)
}

func (t *testNewACCEPTOnINITVoteproofConsensusHandler) TestHigherRoundDraw() {
	point := base.RawPoint(33, 44)
	suf, nodes := isaac.NewTestSuffrage(2, t.Local)

	st, closefunc, _, ivp := t.newStateWithINITVoteproof(point, suf)
	defer closefunc()

	nextpr := t.PRPool.Get(point.NextRound().NextRound().NextRound())

	ballotch := make(chan base.Ballot, 1)
	st.broadcastBallotFunc = func(bl base.Ballot) error {
		if bl.Point().Point.Compare(point) < 1 {
			return nil
		}

		ballotch <- bl

		return nil
	}

	sctx := newConsensusSwitchContext(StateJoining, ivp)

	deferred, err := st.enter(sctx)
	t.NoError(err)
	deferred()

	fact := t.PRPool.GetFact(point)
	nextavp, _ := t.VoteproofsPair(point.NextRound().NextRound(), point.Next(), nil, fact.Hash(), nil, nodes)
	nextavp.SetResult(base.VoteResultDraw).Finish()

	t.NoError(st.newVoteproof(nextavp))
	t.NotNil(st.lastVoteproof().accept())

	t.T().Log("will wait init ballot of next round")

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait next round init ballot"))

		return
	case bl := <-ballotch:
		t.Equal(nextpr.Point(), bl.Point().Point)

		rbl, ok := bl.(base.INITBallot)
		t.True(ok)

		rfact := rbl.BallotSignedFact().BallotFact()
		t.Equal(ivp.BallotMajority().PreviousBlock(), rfact.PreviousBlock())
		t.True(nextpr.Fact().Hash().Equal(rfact.Proposal()))
	}
}

func TestNewACCEPTOnINITVoteproofConsensusHandler(t *testing.T) {
	suite.Run(t, new(testNewACCEPTOnINITVoteproofConsensusHandler))
}

type testNewACCEPTOnACCEPTVoteproofConsensusHandler struct {
	baseTestConsensusHandler
}

func (t *testNewACCEPTOnACCEPTVoteproofConsensusHandler) TestHigerHeight() {
	point := base.RawPoint(33, 44)
	suf, nodes := isaac.NewTestSuffrage(2, t.Local)

	st, closefunc, pp, ivp := t.newStateWithINITVoteproof(point, suf)
	defer closefunc()

	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	pp.Processerr = func(context.Context, base.ProposalFact, base.INITVoteproof) (base.Manifest, error) {
		return manifest, nil
	}
	savedch := make(chan base.ACCEPTVoteproof, 1)
	pp.Saveerr = func(_ context.Context, avp base.ACCEPTVoteproof) error {
		savedch <- avp
		return nil
	}

	_ = t.PRPool.Get(point.Next())
	fact := t.PRPool.GetFact(point)

	sctx := newConsensusSwitchContext(StateJoining, ivp)

	deferred, err := st.enter(sctx)
	t.NoError(err)
	deferred()

	avp, _ := t.VoteproofsPair(point, point.Next(), manifest.Hash(), fact.Hash(), nil, nodes)
	t.NoError(st.newVoteproof(avp))

	t.T().Log("wait new block saved")
	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait save proposal processor"))

		return
	case ravp := <-savedch:
		base.EqualVoteproof(t.Assert(), avp, ravp)
		base.EqualVoteproof(t.Assert(), ravp, st.lastVoteproof().accept())
	}

	t.T().Log("new accept voteproof; higher height")

	newavp, _ := t.VoteproofsPair(point.Next().Next(), point.Next(), nil, nil, nil, nodes)
	err = st.newVoteproof(newavp)

	var ssctx syncingSwitchContext
	t.True(errors.As(err, &ssctx))
	t.Equal(ssctx.height, newavp.Point().Height())
}

func (t *testNewACCEPTOnACCEPTVoteproofConsensusHandler) TestDrawAndHigherHeight() {
	point := base.RawPoint(33, 44)
	suf, nodes := isaac.NewTestSuffrage(2, t.Local)

	st, closefunc, _, ivp := t.newStateWithINITVoteproof(point, suf)
	defer closefunc()

	_ = t.PRPool.Get(point.NextRound())

	nextprch := make(chan base.Point, 1)
	st.proposalSelector = isaac.DummyProposalSelector(func(ctx context.Context, p base.Point) (base.ProposalSignedFact, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			pr := t.PRPool.ByPoint(p)
			if pr != nil {
				if p.Equal(point.NextRound()) {
					nextprch <- p
				}

				return pr, nil
			}
			return nil, util.NotFoundError.Call()
		}
	})

	sctx := newConsensusSwitchContext(StateJoining, ivp)

	deferred, err := st.enter(sctx)
	t.NoError(err)
	deferred()

	fact := t.PRPool.GetFact(point)
	avp, _ := t.VoteproofsPair(point, point.Next(), nil, fact.Hash(), nil, nodes)
	avp.SetResult(base.VoteResultDraw).Finish()

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

	newavp, _ := t.VoteproofsPair(point.Next(), point.Next().Next(), nil, nil, nil, nodes)
	err = st.newVoteproof(newavp)

	var ssctx syncingSwitchContext
	t.True(errors.As(err, &ssctx))
	t.Equal(ssctx.height, newavp.Point().Height())
}

func (t *testNewACCEPTOnACCEPTVoteproofConsensusHandler) TestDrawAndHigherRound() {
	point := base.RawPoint(33, 44)
	suf, nodes := isaac.NewTestSuffrage(2, t.Local)

	st, closefunc, _, ivp := t.newStateWithINITVoteproof(point, suf)
	defer closefunc()

	_ = t.PRPool.Get(point.NextRound())

	nextprch := make(chan base.Point, 1)
	st.proposalSelector = isaac.DummyProposalSelector(func(ctx context.Context, p base.Point) (base.ProposalSignedFact, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			pr := t.PRPool.ByPoint(p)
			if pr != nil {
				if p.Equal(point.NextRound()) {
					nextprch <- p
				}

				return pr, nil
			}
			return nil, util.NotFoundError.Call()
		}
	})

	sctx := newConsensusSwitchContext(StateJoining, ivp)

	deferred, err := st.enter(sctx)
	t.NoError(err)
	deferred()

	fact := t.PRPool.GetFact(point)
	avp, _ := t.VoteproofsPair(point, point.Next(), nil, fact.Hash(), nil, nodes)
	avp.SetResult(base.VoteResultDraw).Finish()

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

	newavp, _ := t.VoteproofsPair(point.NextRound(), point.Next(), nil, nil, nil, nodes)
	err = st.newVoteproof(newavp)

	var ssctx syncingSwitchContext
	t.True(errors.As(err, &ssctx))
	t.Equal(ssctx.height, newavp.Point().Height())
}

func (t *testNewACCEPTOnACCEPTVoteproofConsensusHandler) TestDrawAndDrawAgain() {
	point := base.RawPoint(33, 44)
	suf, nodes := isaac.NewTestSuffrage(2, t.Local)

	st, closefunc, _, ivp := t.newStateWithINITVoteproof(point, suf)
	defer closefunc()

	_ = t.PRPool.Get(point.NextRound())
	nextpr := t.PRPool.Get(point.NextRound().NextRound())

	newprch := make(chan base.Point, 1)
	st.proposalSelector = isaac.DummyProposalSelector(func(ctx context.Context, p base.Point) (base.ProposalSignedFact, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			pr := t.PRPool.ByPoint(p)
			if pr != nil {
				if p.Equal(point.NextRound()) {
					newprch <- p
				}

				return pr, nil
			}
			return nil, util.NotFoundError.Call()
		}
	})

	ballotch := make(chan base.Ballot, 1)
	st.broadcastBallotFunc = func(bl base.Ballot) error {
		if bl.Point().Point.Compare(point.NextRound().NextRound()) == 0 {
			ballotch <- bl
		}

		return nil
	}

	sctx := newConsensusSwitchContext(StateJoining, ivp)

	deferred, err := st.enter(sctx)
	t.NoError(err)
	deferred()

	fact := t.PRPool.GetFact(point)
	avp, _ := t.VoteproofsPair(point, point.Next(), nil, fact.Hash(), nil, nodes)
	avp.SetResult(base.VoteResultDraw).Finish()

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

	newavp, _ := t.VoteproofsPair(point.NextRound(), point.Next(), nil, nil, nil, nodes)
	newavp.SetResult(base.VoteResultDraw).Finish()

	t.NoError(st.newVoteproof(newavp))

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait accept ballot"))

		return
	case bl := <-ballotch:
		t.NoError(bl.IsValid(t.Policy.NetworkID()))

		rbl, ok := bl.(base.INITBallot)
		t.True(ok)

		t.Equal(nextpr.Point(), bl.Point().Point)

		rfact := rbl.BallotSignedFact().BallotFact()
		t.True(nextpr.Fact().Hash().Equal(rfact.Proposal()))
	}
}

func TestNewACCEPTOnACCEPTVoteproofConsensusHandler(t *testing.T) {
	suite.Run(t, new(testNewACCEPTOnACCEPTVoteproofConsensusHandler))
}