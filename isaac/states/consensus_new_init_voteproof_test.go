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

type testNewINITOnINITVoteproofConsensusHandler struct {
	baseTestConsensusHandler
}

func (t *testNewINITOnINITVoteproofConsensusHandler) TestHigherHeight() {
	point := base.RawPoint(33, 44)
	suf, nodes := isaac.NewTestSuffrage(2, t.Local)

	st, closefunc, pp, ivp := t.newStateWithINITVoteproof(point, suf)
	defer closefunc()

	t.LocalParams.SetWaitPreparingINITBallot(time.Nanosecond)

	pp.Processerr = func(context.Context, base.ProposalFact, base.INITVoteproof) (base.Manifest, error) {
		return base.NewDummyManifest(point.Height(), valuehash.RandomSHA256()), nil
	}

	prch := make(chan util.Hash, 1)
	st.args.ProposalProcessors.SetGetProposal(func(_ context.Context, _ base.Point, facthash util.Hash) (base.ProposalSignFact, error) {
		pr, err := t.PRPool.ByHash(facthash)
		if err != nil {
			return nil, err
		}

		if pr.Point().Equal(point) {
			prch <- facthash
		}

		return pr, nil
	})

	st.args.ProposalSelectFunc = func(context.Context, base.Point, util.Hash, time.Duration) (base.ProposalSignFact, error) {
		return nil, errors.Errorf("hahaah")
	}

	sctx, _ := newConsensusSwitchContext(StateJoining, ivp)

	deferred, err := st.enter(StateJoining, sctx)
	t.NoError(err)
	deferred()

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait"))

		return
	case <-prch:
	}

	t.T().Log("new higher init voteproof")
	newpoint := point.NextHeight()
	fact := t.PRPool.GetFact(point)
	_, newivp := t.VoteproofsPair(newpoint.PrevHeight(), newpoint, nil, nil, fact.Hash(), nodes)

	err = st.newVoteproof(newivp)
	t.Error(err)

	var ssctx SyncingSwitchContext
	t.True(errors.As(err, &ssctx))
	t.Equal(ssctx.height, newpoint.Height()-1)
}

func (t *testNewINITOnINITVoteproofConsensusHandler) TestNextRoundMajorityButAlreadyFinished() {
	point := base.RawPoint(33, 44)
	suf, nodes := isaac.NewTestSuffrage(2, t.Local)

	st, closefunc, pp, ivp := t.newStateWithINITVoteproof(point, suf)
	defer closefunc()

	t.LocalParams.SetWaitPreparingINITBallot(time.Nanosecond)

	pp.Processerr = func(_ context.Context, fact base.ProposalFact, _ base.INITVoteproof) (base.Manifest, error) {
		return base.NewDummyManifest(fact.Point().Height(), valuehash.RandomSHA256()), nil
	}

	prch := make(chan util.Hash, 1)
	st.args.ProposalProcessors.SetGetProposal(func(_ context.Context, _ base.Point, facthash util.Hash) (base.ProposalSignFact, error) {
		pr, err := t.PRPool.ByHash(facthash)
		if err != nil {
			return nil, err
		}

		prch <- facthash

		return pr, nil
	})

	st.args.ProposalSelectFunc = func(context.Context, base.Point, util.Hash, time.Duration) (base.ProposalSignFact, error) {
		return nil, errors.Errorf("hahaah")
	}

	sctx, _ := newConsensusSwitchContext(StateJoining, ivp)

	deferred, err := st.enter(StateJoining, sctx)
	t.NoError(err)
	deferred()

	var processed util.Hash

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait"))

		return
	case processed = <-prch:
		t.NotNil(processed)
		t.True(ivp.Majority().(base.INITBallotFact).Proposal().Equal(processed))
	}

	t.T().Log("next round init voteproof")
	newpoint := point.NextRound()
	newfact := t.PRPool.GetFact(newpoint)
	_, newivp := t.VoteproofsPair(newpoint.PrevHeight(), newpoint, ivp.Majority().(base.INITBallotFact).PreviousBlock(), nil, newfact.Hash(), nodes)

	t.NoError(st.newVoteproof(newivp))

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait new processed proposal"))

		return
	case newprocessed := <-prch:
		t.NotNil(processed)
		t.True(newfact.Hash().Equal(newprocessed))
	}
}

func (t *testNewINITOnINITVoteproofConsensusHandler) TestNextRoundButAlreadyFinished() {
	point := base.RawPoint(33, 44)
	suf, nodes := isaac.NewTestSuffrage(2, t.Local)

	st, closefunc, pp, ivp := t.newStateWithINITVoteproof(point, suf)
	defer closefunc()

	t.LocalParams.SetWaitPreparingINITBallot(time.Nanosecond)

	pp.Processerr = func(_ context.Context, fact base.ProposalFact, _ base.INITVoteproof) (base.Manifest, error) {
		return base.NewDummyManifest(fact.Point().Height(), valuehash.RandomSHA256()), nil
	}

	prch := make(chan util.Hash, 1)
	st.args.ProposalProcessors.SetGetProposal(func(_ context.Context, _ base.Point, facthash util.Hash) (base.ProposalSignFact, error) {
		pr, err := t.PRPool.ByHash(facthash)
		if err != nil {
			return nil, err
		}

		prch <- facthash

		return pr, nil
	})

	st.args.ProposalSelectFunc = func(ctx context.Context, p base.Point, _ util.Hash, _ time.Duration) (base.ProposalSignFact, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			return t.PRPool.Get(p), nil
		}
	}

	nextpoint := point.NextRound()

	ballotch := make(chan base.Ballot, 1)
	st.ballotBroadcaster = NewDummyBallotBroadcaster(t.Local.Address(), func(bl base.Ballot) error {
		if bl.Point().Point.Equal(nextpoint.NextRound()) {
			ballotch <- bl
		}

		return nil
	})

	sctx, _ := newConsensusSwitchContext(StateJoining, ivp)

	deferred, err := st.enter(StateJoining, sctx)
	t.NoError(err)
	deferred()

	var processed util.Hash

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait"))

		return
	case processed = <-prch:
		t.NotNil(processed)
		t.True(ivp.Majority().(base.INITBallotFact).Proposal().Equal(processed))
	}

	t.T().Log("next round init voteproof")
	newfact := t.PRPool.GetFact(nextpoint)
	_, newivp := t.VoteproofsPair(nextpoint.PrevHeight(), nextpoint, ivp.Majority().(base.INITBallotFact).PreviousBlock(), nil, newfact.Hash(), nodes)
	newivp.SetMajority(nil)

	t.Equal(newivp.Result(), base.VoteResultDraw)
	t.Nil(newivp.Majority())

	t.NoError(st.newVoteproof(newivp))

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait new next round init ballot"))

		return
	case bl := <-ballotch:
		t.NotNil(bl)
		t.Equal(bl.Point().Point, nextpoint.NextRound())
	}
}

func (t *testNewINITOnINITVoteproofConsensusHandler) TestDrawBeforePreviousBlockNotMatched() {
	point := base.RawPoint(33, 44)
	suf, nodes := isaac.NewTestSuffrage(2, t.Local)

	st, closefunc, pp, ivp := t.newStateWithINITVoteproof(point, suf)
	defer closefunc()

	t.LocalParams.SetWaitPreparingINITBallot(time.Nanosecond)

	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	pp.Processerr = func(context.Context, base.ProposalFact, base.INITVoteproof) (base.Manifest, error) {
		return manifest, nil
	}
	savedch := make(chan base.ACCEPTVoteproof, 1)
	pp.Saveerr = func(_ context.Context, avp base.ACCEPTVoteproof) (base.BlockMap, error) {
		savedch <- avp
		return nil, nil
	}

	ballotch := make(chan base.Ballot, 1)
	st.ballotBroadcaster = NewDummyBallotBroadcaster(t.Local.Address(), func(bl base.Ballot) error {
		switch {
		case bl.Point().Point.Equal(point.NextHeight()):
			ballotch <- bl
		case bl.Point().Point.Equal(point.NextHeight().NextRound()):
			ballotch <- bl
		}

		return nil
	})

	st.args.ProposalSelectFunc = func(ctx context.Context, p base.Point, _ util.Hash, _ time.Duration) (base.ProposalSignFact, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			return t.PRPool.Get(p), nil
		}
	}

	sctx, _ := newConsensusSwitchContext(StateJoining, ivp)

	deferred, err := st.enter(StateJoining, sctx)
	t.NoError(err)
	deferred()

	nextavp, drawivp := t.VoteproofsPair(point, point.NextHeight(), manifest.Hash(), t.PRPool.Hash(point), nil, nodes)
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
		t.Equal(point.NextHeight(), bl.Point().Point)
	}

	drawivp.SetResult(base.VoteResultDraw).Finish()

	t.NoError(st.newVoteproof(drawivp))

	t.T().Log("new draw init voteproof")
	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait next round init ballot"))

		return
	case bl := <-ballotch:
		t.Equal(point.NextHeight().NextRound(), bl.Point().Point)
	}

	_, newivp := t.VoteproofsPair(point, drawivp.Point().Point.NextRound(), valuehash.RandomSHA256(), nil, t.PRPool.Hash(drawivp.Point().Point.NextRound()), nodes)
	err = st.newVoteproof(newivp)

	var ssctx SyncingSwitchContext
	t.True(errors.As(err, &ssctx))
	t.Equal(ssctx.height, newivp.Point().Height()-1)
}

func (t *testNewINITOnINITVoteproofConsensusHandler) TestDrawBefore() {
	point := base.RawPoint(33, 44)
	suf, nodes := isaac.NewTestSuffrage(2, t.Local)

	st, closefunc, pp, ivp := t.newStateWithINITVoteproof(point, suf)
	defer closefunc()

	t.LocalParams.SetWaitPreparingINITBallot(time.Nanosecond)

	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	pp.Processerr = func(context.Context, base.ProposalFact, base.INITVoteproof) (base.Manifest, error) {
		return manifest, nil
	}

	savedch := make(chan base.ACCEPTVoteproof, 1)
	pp.Saveerr = func(_ context.Context, avp base.ACCEPTVoteproof) (base.BlockMap, error) {
		savedch <- avp
		return nil, nil
	}

	ballotch := make(chan base.Ballot, 1)
	st.ballotBroadcaster = NewDummyBallotBroadcaster(t.Local.Address(), func(bl base.Ballot) error {
		switch {
		case bl.Point().Point.Equal(point.NextHeight()):
			ballotch <- bl
		case bl.Point().Point.Equal(point.NextHeight().NextRound()):
			ballotch <- bl
		}

		return nil
	})

	st.args.ProposalSelectFunc = func(ctx context.Context, p base.Point, _ util.Hash, _ time.Duration) (base.ProposalSignFact, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			return t.PRPool.Get(p), nil
		}
	}

	confirmedch := make(chan base.Height, 1)
	st.args.WhenNewBlockConfirmed = func(height base.Height) {
		if height >= point.Height() {
			confirmedch <- height
		}
	}

	sctx, _ := newConsensusSwitchContext(StateJoining, ivp)

	deferred, err := st.enter(StateJoining, sctx)
	t.NoError(err)
	deferred()

	nextavp, drawivp := t.VoteproofsPair(point, point.NextHeight(), nil, t.PRPool.Hash(point), nil, nodes)
	t.NoError(st.newVoteproof(nextavp))

	t.T().Log("wait new block saved:", point)
	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait save proposal processor"))

		return
	case <-savedch:
	}

	t.T().Log("wait next init ballot:", point.NextHeight())
	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait next init ballot"))

		return
	case bl := <-ballotch:
		t.Equal(point.NextHeight(), bl.Point().Point)
	}

	drawivp.SetResult(base.VoteResultDraw).Finish()

	t.T().Log("new draw init voteproof:", drawivp.Point())
	t.NoError(st.newVoteproof(drawivp))

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait next round init ballot"))

		return
	case bl := <-ballotch:
		t.Equal(point.NextHeight().NextRound(), bl.Point().Point)
	}

	_, newivp := t.VoteproofsPair(point, drawivp.Point().Point.NextRound(), nextavp.BallotMajority().NewBlock(), nil, t.PRPool.Hash(drawivp.Point().Point.NextRound()), nodes)
	t.NoError(st.newVoteproof(newivp))

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait to be confirmed"))
	case height := <-confirmedch:
		t.Equal(newivp.Point().Height()-1, height)
	}

	t.T().Log("next init voteproof:", newivp.Point())
	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait next round init ballot"))

		return
	case bl := <-ballotch:
		t.T().Log("next accept ballot:", bl.Point())
		t.Equal(drawivp.Point().Point.NextRound(), bl.Point().Point)
	}
}

func (t *testNewINITOnINITVoteproofConsensusHandler) TestDrawAndDrawAgain() {
	point := base.RawPoint(33, 44)
	suf, nodes := isaac.NewTestSuffrage(2, t.Local)

	st, closefunc, pp, ivp := t.newStateWithINITVoteproof(point, suf)
	defer closefunc()

	t.LocalParams.SetWaitPreparingINITBallot(time.Nanosecond)

	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	pp.Processerr = func(context.Context, base.ProposalFact, base.INITVoteproof) (base.Manifest, error) {
		return manifest, nil
	}
	savedch := make(chan base.ACCEPTVoteproof, 1)
	pp.Saveerr = func(_ context.Context, avp base.ACCEPTVoteproof) (base.BlockMap, error) {
		savedch <- avp
		return nil, nil
	}

	ballotch := make(chan base.Ballot, 1)
	st.ballotBroadcaster = NewDummyBallotBroadcaster(t.Local.Address(), func(bl base.Ballot) error {
		switch {
		case bl.Point().Point.Equal(point.NextHeight()):
			ballotch <- bl
		case bl.Point().Point.Equal(point.NextHeight().NextRound()):
			ballotch <- bl
		case bl.Point().Point.Equal(point.NextHeight().NextRound().NextRound()):
			ballotch <- bl
		}

		return nil
	})

	st.args.ProposalSelectFunc = func(ctx context.Context, p base.Point, _ util.Hash, _ time.Duration) (base.ProposalSignFact, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			return t.PRPool.Get(p), nil
		}
	}

	sctx, _ := newConsensusSwitchContext(StateJoining, ivp)

	deferred, err := st.enter(StateJoining, sctx)
	t.NoError(err)
	deferred()

	nextavp, drawivp := t.VoteproofsPair(point, point.NextHeight(), manifest.Hash(), t.PRPool.Hash(point), nil, nodes)
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
		t.Equal(point.NextHeight(), bl.Point().Point)
	}

	drawivp.SetResult(base.VoteResultDraw).Finish()

	t.NoError(st.newVoteproof(drawivp))

	t.T().Log("new draw init voteproof")
	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait next round init ballot"))

		return
	case bl := <-ballotch:
		t.Equal(point.NextHeight().NextRound(), bl.Point().Point)
	}

	_, newivp := t.VoteproofsPair(point, drawivp.Point().Point.NextRound(), nil, nil, t.PRPool.Hash(drawivp.Point().Point.NextRound()), nodes)
	newivp.SetResult(base.VoteResultDraw).Finish()

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
	suf, nodes := isaac.NewTestSuffrage(2, t.Local)

	st, closefunc, pp, ivp := t.newStateWithINITVoteproof(point, suf)
	defer closefunc()
	st.SetLogging(logging.TestNilLogging)

	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	pp.Processerr = func(context.Context, base.ProposalFact, base.INITVoteproof) (base.Manifest, error) {
		return manifest, nil
	}

	savedch := make(chan base.ACCEPTVoteproof, 1)
	pp.Saveerr = func(_ context.Context, avp base.ACCEPTVoteproof) (base.BlockMap, error) {
		savedch <- avp
		return nil, nil
	}

	ballotch := make(chan base.Ballot, 1)
	st.ballotBroadcaster = NewDummyBallotBroadcaster(t.Local.Address(), func(bl base.Ballot) error {
		if p := bl.Point(); p.Point.Equal(point.NextHeight()) && p.Stage() == base.StageACCEPT {
			ballotch <- bl
		}

		return nil
	})

	prpool := t.PRPool
	st.args.ProposalSelectFunc = func(ctx context.Context, p base.Point, _ util.Hash, _ time.Duration) (base.ProposalSignFact, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			return prpool.Get(p), nil
		}
	}

	sctx, _ := newConsensusSwitchContext(StateJoining, ivp)

	deferred, err := st.enter(StateJoining, sctx)
	t.NoError(err)
	deferred()

	nextavp, nextivp := t.VoteproofsPair(point, point.NextHeight(), manifest.Hash(), t.PRPool.Hash(point), t.PRPool.Hash(point.NextHeight()), nodes)
	t.NoError(st.newVoteproof(nextavp))

	t.T().Log("wait new block saved")
	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait save proposal processor"))

		return
	case <-savedch:
	}

	t.T().Log("new init voteproof")

	t.NoError(st.newVoteproof(nextivp))

	t.T().Log("wait next accept ballot")
	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait next accept ballot"))

		return
	case bl := <-ballotch:
		t.Equal(point.NextHeight(), bl.Point().Point)
	}
}

func (t *testNewINITOnACCEPTVoteproofConsensusHandler) TestHigherHeight() {
	point := base.RawPoint(33, 44)
	suf, nodes := isaac.NewTestSuffrage(2, t.Local)

	st, closefunc, pp, ivp := t.newStateWithINITVoteproof(point, suf)
	defer closefunc()

	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	pp.Processerr = func(context.Context, base.ProposalFact, base.INITVoteproof) (base.Manifest, error) {
		return manifest, nil
	}

	savedch := make(chan base.ACCEPTVoteproof, 1)
	pp.Saveerr = func(_ context.Context, avp base.ACCEPTVoteproof) (base.BlockMap, error) {
		savedch <- avp
		return nil, nil
	}

	ballotch := make(chan base.Ballot, 1)
	st.ballotBroadcaster = NewDummyBallotBroadcaster(t.Local.Address(), func(bl base.Ballot) error {
		switch {
		case bl.Point().Point.Equal(point.NextHeight()):
			ballotch <- bl
		case bl.Point().Point.Equal(point.NextHeight().NextRound()):
			ballotch <- bl
		}

		return nil
	})

	prpool := t.PRPool
	st.args.ProposalSelectFunc = func(ctx context.Context, p base.Point, _ util.Hash, _ time.Duration) (base.ProposalSignFact, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			return prpool.Get(p), nil
		}
	}

	sctx, _ := newConsensusSwitchContext(StateJoining, ivp)

	deferred, err := st.enter(StateJoining, sctx)
	t.NoError(err)
	deferred()

	nextavp, _ := t.VoteproofsPair(point, point.NextHeight(), manifest.Hash(), t.PRPool.Hash(point), nil, nodes)
	t.NoError(st.newVoteproof(nextavp))

	t.T().Log("wait new block saved")
	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait save proposal processor"))

		return
	case <-savedch:
	}

	_, newivp := t.VoteproofsPair(point.NextHeight(), point.NextHeight().NextHeight(), nil, nil, t.PRPool.Hash(point.NextHeight().NextHeight()), nodes)
	err = st.newVoteproof(newivp)

	var ssctx SyncingSwitchContext
	t.True(errors.As(err, &ssctx))
	t.Equal(ssctx.height, newivp.Point().Height()-1)
}

func (t *testNewINITOnACCEPTVoteproofConsensusHandler) TestPreviousBlockNotMatch() {
	point := base.RawPoint(33, 44)
	suf, nodes := isaac.NewTestSuffrage(2, t.Local)

	st, closefunc, pp, ivp := t.newStateWithINITVoteproof(point, suf)
	defer closefunc()

	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	pp.Processerr = func(context.Context, base.ProposalFact, base.INITVoteproof) (base.Manifest, error) {
		return manifest, nil
	}

	savedch := make(chan base.ACCEPTVoteproof, 1)
	pp.Saveerr = func(_ context.Context, avp base.ACCEPTVoteproof) (base.BlockMap, error) {
		savedch <- avp
		return nil, nil
	}

	ballotch := make(chan base.Ballot, 1)
	st.ballotBroadcaster = NewDummyBallotBroadcaster(t.Local.Address(), func(bl base.Ballot) error {
		switch {
		case bl.Point().Point.Equal(point.NextHeight()):
			ballotch <- bl
		case bl.Point().Point.Equal(point.NextHeight().NextRound()):
			ballotch <- bl
		}

		return nil
	})

	st.args.ProposalSelectFunc = func(ctx context.Context, p base.Point, _ util.Hash, _ time.Duration) (base.ProposalSignFact, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			return t.PRPool.Get(p), nil
		}
	}

	sctx, _ := newConsensusSwitchContext(StateJoining, ivp)

	deferred, err := st.enter(StateJoining, sctx)
	t.NoError(err)
	deferred()

	nextavp, _ := t.VoteproofsPair(point, point.NextHeight(), manifest.Hash(), t.PRPool.Hash(point), nil, nodes)
	t.NoError(st.newVoteproof(nextavp))

	t.T().Log("wait new block saved")
	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait save proposal processor"))

		return
	case <-savedch:
	}

	t.T().Log("new init voteproof; wrong previous block")

	ifact := t.NewINITBallotFact(point.NextHeight(), valuehash.RandomSHA256(), t.PRPool.Hash(point.NextHeight()))
	newivp, err := t.NewINITVoteproof(ifact, t.Local, nodes)
	t.NoError(err)

	err = st.newVoteproof(newivp)

	var ssctx SyncingSwitchContext
	t.True(errors.As(err, &ssctx))
	t.Equal(ssctx.height, newivp.Point().Height()-1)
}

func (t *testNewINITOnACCEPTVoteproofConsensusHandler) TestNotInConsensusNodes() {
	point := base.RawPoint(33, 44)
	suf, nodes := isaac.NewTestSuffrage(2, t.Local)

	st, closefunc, pp, ivp := t.newStateWithINITVoteproof(point, suf)
	defer closefunc()

	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	pp.Processerr = func(context.Context, base.ProposalFact, base.INITVoteproof) (base.Manifest, error) {
		return manifest, nil
	}

	savedch := make(chan base.ACCEPTVoteproof, 1)
	pp.Saveerr = func(_ context.Context, avp base.ACCEPTVoteproof) (base.BlockMap, error) {
		savedch <- avp
		return nil, nil
	}

	ballotch := make(chan base.Ballot, 1)
	st.ballotBroadcaster = NewDummyBallotBroadcaster(t.Local.Address(), func(bl base.Ballot) error {
		if p := bl.Point(); p.Point.Equal(point.NextHeight()) && p.Stage() == base.StageACCEPT {
			ballotch <- bl
		}

		return nil
	})

	sctxch := make(chan switchContext, 2)
	st.switchStateFunc = func(sctx switchContext) error {
		sctxch <- sctx

		return nil
	}

	st.args.VoteFunc = func(bl base.Ballot) (bool, error) {
		return false, errFailedToVoteNotInConsensus.Errorf("hehehe")
	}

	prpool := t.PRPool
	st.args.ProposalSelectFunc = func(ctx context.Context, p base.Point, _ util.Hash, _ time.Duration) (base.ProposalSignFact, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			return prpool.Get(p), nil
		}
	}

	sctx, _ := newConsensusSwitchContext(StateJoining, ivp)

	deferred, err := st.enter(StateJoining, sctx)
	t.NoError(err)
	deferred()

	nextavp, nextivp := t.VoteproofsPair(point, point.NextHeight(), manifest.Hash(), t.PRPool.Hash(point), t.PRPool.Hash(point.NextHeight()), nodes)
	t.NoError(st.newVoteproof(nextavp))

	t.T().Log("wait new block saved")
	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait save proposal processor"))

		return
	case <-savedch:
	}

	t.T().Log("new init voteproof")

	t.NoError(st.newVoteproof(nextivp))

	t.T().Log("wait next accept ballot")
	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait next accept ballot"))

		return
	case bl := <-ballotch:
		t.Equal(point.NextHeight(), bl.Point().Point)
	}

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait to switch syncing state"))

		return
	case sctx := <-sctxch:
		var ssctx SyncingSwitchContext
		t.True(errors.As(sctx, &ssctx))
		t.Equal(nextavp.Point().Height()-1, ssctx.height)
	}
}

func (t *testNewINITOnACCEPTVoteproofConsensusHandler) TestProcessContextCanceled() {
	point := base.RawPoint(33, 44)
	suf, nodes := isaac.NewTestSuffrage(2, t.Local)

	st, closefunc, pp, ivp := t.newStateWithINITVoteproof(point, suf)
	defer closefunc()
	st.SetLogging(logging.TestNilLogging)

	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	pp.Processerr = func(_ context.Context, _ base.ProposalFact, ivp base.INITVoteproof) (base.Manifest, error) {
		if ivp.Point().Height() == point.NextHeight().Height() {
			return nil, context.Canceled
		}

		return manifest, nil
	}

	savedch := make(chan base.ACCEPTVoteproof, 1)
	pp.Saveerr = func(_ context.Context, avp base.ACCEPTVoteproof) (base.BlockMap, error) {
		savedch <- avp
		return nil, nil
	}

	ballotch := make(chan base.Ballot, 1)
	st.ballotBroadcaster = NewDummyBallotBroadcaster(t.Local.Address(), func(bl base.Ballot) error {
		if p := bl.Point(); p.Point.Equal(point.NextHeight()) && p.Stage() == base.StageACCEPT {
			ballotch <- bl
		}

		return nil
	})

	prpool := t.PRPool
	st.args.ProposalSelectFunc = func(ctx context.Context, p base.Point, _ util.Hash, _ time.Duration) (base.ProposalSignFact, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			return prpool.Get(p), nil
		}
	}

	sctx, _ := newConsensusSwitchContext(StateJoining, ivp)

	deferred, err := st.enter(StateJoining, sctx)
	t.NoError(err)
	deferred()

	nextavp, nextivp := t.VoteproofsPair(point, point.NextHeight(), manifest.Hash(), t.PRPool.Hash(point), t.PRPool.Hash(point.NextHeight()), nodes)
	t.NoError(st.newVoteproof(nextavp))

	t.T().Log("wait new block saved")
	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait save proposal processor"))

		return
	case <-savedch:
	}

	t.T().Log("new init voteproof")

	t.NoError(st.newVoteproof(nextivp))

	t.T().Log("wait next accept ballot")
	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait next accept ballot"))

		return
	case bl := <-ballotch:
		t.Equal(point.NextHeight(), bl.Point().Point)

		abl, ok := bl.(base.ACCEPTBallot)
		t.True(ok)
		t.True(nextivp.BallotMajority().Proposal().Equal(abl.BallotSignFact().BallotFact().Proposal()))
	}
}

func TestNewINITOnACCEPTVoteproofConsensusHandler(t *testing.T) {
	suite.Run(t, new(testNewINITOnACCEPTVoteproofConsensusHandler))
}
