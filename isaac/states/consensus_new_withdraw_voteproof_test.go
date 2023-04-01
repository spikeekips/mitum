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

type testWithdrawsConsensusHandler struct {
	baseTestConsensusHandler
}

func (t *testWithdrawsConsensusHandler) TestEnterWithSuffrageConfirmVoteproof() {
	point := base.RawPoint(33, 44)
	suf, nodes := isaac.NewTestSuffrage(2, t.Local)
	withdrawnode := nodes[2]

	st, closefunc, pp, origivp := t.newStateWithINITVoteproof(point, suf)
	defer closefunc()
	st.SetLogging(logging.TestNilLogging)

	processedch := make(chan base.Manifest, 1)
	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	pp.Processerr = func(context.Context, base.ProposalFact, base.INITVoteproof) (base.Manifest, error) {
		processedch <- manifest

		return manifest, nil
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

	t.T().Log("prepare new init voteproof")

	origifact := origivp.BallotMajority()

	withdraws := t.Withdraws(point.Height()-1, []base.Address{withdrawnode.Address()}, nodes[:2])
	withdrawfacts := make([]util.Hash, len(withdraws))
	for i := range withdraws {
		withdrawfacts[i] = withdraws[i].Fact().Hash()
	}

	sfact := isaac.NewSuffrageConfirmBallotFact(point, origifact.PreviousBlock(), origifact.Proposal(), withdrawfacts)
	ivp, err := t.NewINITWithdrawVoteproof(sfact, t.Local, nodes[:2], withdraws)
	t.NoError(err)

	t.T().Log("new suffrage confirm init voteproof", ivp.Point())

	sctx, _ := newConsensusSwitchContext(StateJoining, ivp)

	deferred, err := st.enter(StateJoining, sctx)
	t.NoError(err)
	deferred()

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait suffrage confirm init ballot"))

		return
	case m := <-processedch:
		t.NotNil(m)

		base.EqualManifest(t.Assert(), manifest, m)

		t.T().Log("expected manifest processed from suffrage confirm init voteproof,", manifest.Height())
	}
}

func (t *testWithdrawsConsensusHandler) TestSuffrageConfirmAfterEnteringINITVoteproof() {
	point := base.RawPoint(33, 44)
	suf, nodes := isaac.NewTestSuffrage(2, t.Local)
	withdrawnode := nodes[2]

	st, closefunc, pp, origivp := t.newStateWithINITVoteproof(point, suf)
	defer closefunc()
	st.SetLogging(logging.TestNilLogging)

	processedch := make(chan base.Manifest)
	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	pp.Processerr = func(context.Context, base.ProposalFact, base.INITVoteproof) (base.Manifest, error) {
		processedch <- manifest

		return manifest, nil
	}

	scballotch := make(chan base.Ballot, 1)
	st.ballotBroadcaster = NewDummyBallotBroadcaster(t.Local.Address(), func(bl base.Ballot) error {
		if p := bl.Point(); p.Point.Equal(point) && p.Stage() == base.StageINIT {
			scballotch <- bl
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

	t.T().Log("prepare new init voteproof")

	origifact := origivp.BallotMajority()

	withdraws := t.Withdraws(point.Height()-1, []base.Address{withdrawnode.Address()}, nodes[:2])
	withdrawfacts := make([]util.Hash, len(withdraws))
	for i := range withdraws {
		withdrawfacts[i] = withdraws[i].Fact().Hash()
	}

	ifact := isaac.NewINITBallotFact(point, origifact.PreviousBlock(), origifact.Proposal(), withdrawfacts)
	ivp, err := t.NewINITWithdrawVoteproof(ifact, t.Local, nodes[:2], withdraws)
	t.NoError(err)

	t.True(st.forceSetLastVoteproof(ivp))

	t.T().Log("new withdraw init voteproof", ivp.Point())

	sctx, _ := newConsensusSwitchContext(StateJoining, ivp)

	deferred, err := st.enter(StateJoining, sctx)
	t.NoError(err)
	deferred()

	var sfact isaac.SuffrageConfirmBallotFact

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait suffrage confirm init ballot"))

		return
	case bl := <-scballotch:
		t.NoError(bl.IsValid(t.LocalParams.NetworkID()))
		ibl, ok := bl.(base.INITBallot)
		t.True(ok)
		t.Equal(ivp.Point(), ibl.Point())

		i, ok := ibl.SignFact().Fact().(isaac.SuffrageConfirmBallotFact)
		t.True(ok)
		sfact = i

		sfactwithdraws := sfact.WithdrawFacts()

		t.Equal(len(withdrawfacts), len(sfactwithdraws))
		for i := range withdrawfacts {
			f := util.InSliceFunc(sfactwithdraws, func(j util.Hash) bool {
				return withdrawfacts[i].Equal(j)
			})
			t.False(f < 0)
		}

		t.T().Log("expected suffrage confirm init ballot broadcasted", bl.Point())
	}

	ivp, err = t.NewINITWithdrawVoteproof(sfact, t.Local, nodes[:2], withdraws)
	t.NoError(err)

	t.T().Log("new suffrage confirm init voteproof", ivp.Point())

	t.NoError(st.newVoteproof(ivp))

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait suffrage confirm init ballot"))

		return
	case m := <-processedch:
		t.NotNil(m)

		base.EqualManifest(t.Assert(), manifest, m)

		t.T().Log("expected manifest processed from suffrage confirm init voteproof,", manifest.Height())
	}
}

func (t *testWithdrawsConsensusHandler) TestSuffrageConfirmAfterACCEPTVoteproof() {
	point := base.RawPoint(33, 44)
	suf, nodes := isaac.NewTestSuffrage(2, t.Local)

	st, closefunc, pp, ivp := t.newStateWithINITVoteproof(point, suf)
	defer closefunc()
	st.SetLogging(logging.TestNilLogging)

	processedch := make(chan base.Manifest, 1)
	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	nextmanifest := base.NewDummyManifest(point.Height()+1, valuehash.RandomSHA256())
	pp.Processerr = func(_ context.Context, _ base.ProposalFact, ivp base.INITVoteproof) (base.Manifest, error) {
		switch {
		case ivp.Point().Height() == manifest.Height():
			processedch <- manifest

			return manifest, nil
		case ivp.Point().Height() == nextmanifest.Height():
			processedch <- nextmanifest

			return nextmanifest, nil
		default:
			return nil, errors.Errorf("unknown height")
		}
	}

	scballotch := make(chan base.Ballot, 1)
	st.ballotBroadcaster = NewDummyBallotBroadcaster(t.Local.Address(), func(bl base.Ballot) error {
		if p := bl.Point(); p.Point.Equal(point.NextHeight()) && p.Stage() == base.StageINIT {
			scballotch <- bl
		}

		return nil
	})

	savedch := make(chan base.ACCEPTVoteproof, 1)
	pp.Saveerr = func(_ context.Context, avp base.ACCEPTVoteproof) error {
		savedch <- avp
		return nil
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

	t.T().Log("1st init voteproof", ivp.Point())

	sctx, _ := newConsensusSwitchContext(StateJoining, ivp)

	deferred, err := st.enter(StateJoining, sctx)
	t.NoError(err)
	deferred()

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait suffrage confirm init ballot"))

		return
	case m := <-processedch:
		t.NotNil(m)

		base.EqualManifest(t.Assert(), manifest, m)

		t.T().Log("expected manifest processed from suffrage confirm init voteproof,", manifest.Height())
	}

	afact := isaac.NewACCEPTBallotFact(ivp.Point().Point, ivp.Majority().(base.INITBallotFact).Proposal(), manifest.Hash(), nil)
	avp, err := t.NewACCEPTVoteproof(afact, t.Local, nodes[:2])
	t.NoError(err)

	t.NoError(st.newVoteproof(avp))

	t.T().Log("wait new block saved")
	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait save proposal processor"))

		return
	case <-savedch:
	}

	nextpoint := point.NextHeight()

	withdrawnode := nodes[2]
	withdraws := t.Withdraws(nextpoint.Height()-1, []base.Address{withdrawnode.Address()}, nodes[:2])
	withdrawfacts := make([]util.Hash, len(withdraws))
	for i := range withdraws {
		withdrawfacts[i] = withdraws[i].Fact().Hash()
	}

	ifact := isaac.NewINITBallotFact(nextpoint, afact.NewBlock(), t.PRPool.Hash(nextpoint), withdrawfacts)
	nextivp, err := t.NewINITWithdrawVoteproof(ifact, t.Local, nodes[:2], withdraws)
	t.NoError(err)

	t.NoError(st.newVoteproof(nextivp))

	var sfact isaac.SuffrageConfirmBallotFact

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait suffrage confirm init ballot"))

		return
	case bl := <-scballotch:
		t.NoError(bl.IsValid(t.LocalParams.NetworkID()))
		ibl, ok := bl.(base.INITBallot)
		t.True(ok)
		t.Equal(nextivp.Point(), ibl.Point())

		i, ok := ibl.SignFact().Fact().(isaac.SuffrageConfirmBallotFact)
		t.True(ok)
		sfact = i

		t.T().Log("expected suffrage confirm init ballot broadcasted", sfact.Point())
	}

	confirmivp, err := t.NewINITWithdrawVoteproof(sfact, t.Local, nodes[:2], withdraws)
	t.NoError(err)

	t.T().Log("new suffrage confirm init voteproof", confirmivp.Point())

	t.NoError(st.newVoteproof(confirmivp))

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait suffrage confirm init ballot"))

		return
	case m := <-processedch:
		t.NotNil(m)

		base.EqualManifest(t.Assert(), nextmanifest, m)

		t.T().Log("expected manifest processed from suffrage confirm init voteproof,", manifest.Height())
	}
}

func (t *testWithdrawsConsensusHandler) prepareAfterACCEPT(
	point base.Point,
	numberOfNodes int,
) (
	base.Suffrage,
	[]base.LocalNode,
	*ConsensusHandler,
	*isaac.DummyProposalProcessor,
	func() (
		base.ACCEPTVoteproof,
		error,
	),
	func(), // close handler
) {
	t.LocalParams.SetWaitPreparingINITBallot(time.Nanosecond)

	suf, nodes := isaac.NewTestSuffrage(numberOfNodes-1, t.Local)

	st, closefunc, pp, ivp := t.newStateWithINITVoteproof(point, suf)
	st.SetLogging(logging.TestNilLogging)

	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())

	processedch := make(chan base.Manifest, 1)
	pp.Processerr = func(_ context.Context, _ base.ProposalFact, ivp base.INITVoteproof) (base.Manifest, error) {
		if ivp.Point().Height() == manifest.Height() {
			processedch <- manifest

			return manifest, nil
		}

		return nil, errors.Errorf("unknown height")
	}

	st.ballotBroadcaster = NewDummyBallotBroadcaster(t.Local.Address(), func(bl base.Ballot) error {
		return nil
	})

	savedch := make(chan base.ACCEPTVoteproof, 1)
	pp.Saveerr = func(_ context.Context, avp base.ACCEPTVoteproof) error {
		savedch <- avp

		return nil
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

	return suf, nodes, st, pp, func() (base.ACCEPTVoteproof, error) {
			t.T().Log("1st init voteproof", ivp.Point())

			sctx, _ := newConsensusSwitchContext(StateJoining, ivp)

			deferred, err := st.enter(StateJoining, sctx)
			t.NoError(err)
			deferred()

			select {
			case <-time.After(time.Second * 2):
				return nil, errors.Errorf("timeout to wait suffrage confirm init ballot")
			case m := <-processedch:
				t.NotNil(m)

				base.EqualManifest(t.Assert(), manifest, m)

				t.T().Log("expected manifest processed from suffrage confirm init voteproof,", manifest.Height())
			}

			afact := isaac.NewACCEPTBallotFact(ivp.Point().Point, ivp.Majority().(base.INITBallotFact).Proposal(), manifest.Hash(), nil)
			avp, err := t.NewACCEPTVoteproof(afact, t.Local, nodes[:2])
			t.NoError(err)

			t.NoError(st.newVoteproof(avp))

			t.T().Log("wait new block saved")
			select {
			case <-time.After(time.Second * 2):
				return nil, errors.Errorf("timeout to wait save proposal processor")
			case <-savedch:
			}

			return avp, nil
		},
		closefunc
}

func (t *testWithdrawsConsensusHandler) TestSuffrageConfirmAfterDrawINITVoteproof() {
	point := base.RawPoint(33, 44)
	nextpoint := point.NextHeight()

	_, nodes, st, pp, dof, closef := t.prepareAfterACCEPT(point, 3)
	defer closef()

	initballotch := make(chan base.Ballot, 1)
	st.ballotBroadcaster = NewDummyBallotBroadcaster(t.Local.Address(), func(bl base.Ballot) error {
		if p := bl.Point(); p.Point.Equal(nextpoint.NextRound()) && p.Stage() == base.StageINIT {
			initballotch <- bl
		}

		return nil
	})

	avp, err := dof()
	t.NoError(err)

	afact := avp.Majority().(base.ACCEPTBallotFact)

	nextmanifest := base.NewDummyManifest(nextpoint.Height(), valuehash.RandomSHA256())

	processedch := make(chan base.Manifest, 1)
	pp.Processerr = func(_ context.Context, _ base.ProposalFact, ivp base.INITVoteproof) (base.Manifest, error) {
		if ivp.Point().Height() == nextmanifest.Height() {
			processedch <- nextmanifest

			return nextmanifest, nil
		}

		return nil, errors.Errorf("unknown height")
	}

	t.T().Log("prepare next draw init voteproof:", nextpoint)

	withdrawnode := nodes[2]
	withdraws := t.Withdraws(nextpoint.Height()-1, []base.Address{withdrawnode.Address()}, nodes[:2])
	withdrawfacts := make([]util.Hash, len(withdraws))
	for i := range withdraws {
		withdrawfacts[i] = withdraws[i].Fact().Hash()
	}

	ifact := isaac.NewINITBallotFact(nextpoint, afact.NewBlock(), t.PRPool.Hash(nextpoint), withdrawfacts)
	nextdrawivp, err := t.NewINITVoteproof(ifact, t.Local, nodes)
	t.NoError(err)

	isfs := make([]base.BallotSignFact, len(nodes))
	for i := range nodes {
		n := nodes[i]

		var fs isaac.INITBallotSignFact

		if n.Address().Equal(withdrawnode.Address()) {
			fact := isaac.NewINITBallotFact(nextpoint, afact.NewBlock(), t.PRPool.Hash(nextpoint), nil)
			fs = isaac.NewINITBallotSignFact(fact)
		} else {
			fs = isaac.NewINITBallotSignFact(ifact)
		}

		t.NoError(fs.NodeSign(n.Privatekey(), t.LocalParams.NetworkID(), n.Address()))

		isfs[i] = fs
	}
	nextdrawivp.
		SetMajority(nil).
		SetSignFacts(isfs).
		Finish()

	t.NoError(nextdrawivp.IsValid(t.LocalParams.NetworkID()))

	t.T().Log("next draw init voteproof:", nextdrawivp.Point())
	t.NoError(st.newVoteproof(nextdrawivp))

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait suffrage confirm init ballot"))

		return
	case bl := <-initballotch:
		t.NoError(bl.IsValid(t.LocalParams.NetworkID()))
		ibl, ok := bl.(base.INITBallot)
		t.True(ok)
		t.Equal(nextdrawivp.Point().NextRound(), ibl.Point().Point, "%v != %v", nextdrawivp.Point().NextRound(), ibl.Point())

		_, ok = ibl.SignFact().Fact().(isaac.INITBallotFact)
		t.True(ok)

		t.T().Log("expected next round init ballot broadcasted", bl.Point())
	}

	sfact := isaac.NewSuffrageConfirmBallotFact(nextpoint, afact.NewBlock(), t.PRPool.Hash(nextpoint), withdrawfacts)
	confirmivp, err := t.NewINITWithdrawVoteproof(sfact, t.Local, nodes[:2], withdraws)
	t.NoError(err)

	t.T().Log("next suffrage confirm init voteproof:", confirmivp.Point())
	t.NoError(st.newVoteproof(confirmivp))

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait suffrage confirm init ballot"))

		return
	case m := <-processedch:
		t.NotNil(m)

		base.EqualManifest(t.Assert(), nextmanifest, m)

		t.T().Log("expected manifest processed from suffrage confirm init voteproof,", nextmanifest.Height())
	}
}

func (t *testWithdrawsConsensusHandler) TestReversalAfterDrawINITVoteproof() {
	point := base.RawPoint(33, 44)
	nextpoint := point.NextHeight()

	_, nodes, st, _, dof, closef := t.prepareAfterACCEPT(point, 3)
	defer closef()

	initballotch := make(chan base.Ballot, 3)
	scballotch := make(chan base.Ballot, 3)
	st.ballotBroadcaster = NewDummyBallotBroadcaster(t.Local.Address(), func(bl base.Ballot) error {
		p := bl.Point()
		if p.Stage() != base.StageINIT {
			return nil
		}

		switch {
		case p.Point.Equal(nextpoint.NextRound()):
			initballotch <- bl
		case p.Point.Equal(nextpoint):
			if _, ok := bl.SignFact().Fact().(isaac.SuffrageConfirmBallotFact); ok {
				scballotch <- bl
			}
		}

		return nil
	})

	avp, err := dof()
	t.NoError(err)

	afact := avp.Majority().(base.ACCEPTBallotFact)

	t.T().Log("prepare next draw init voteproof:", nextpoint)

	withdrawnode := nodes[2]
	withdraws := t.Withdraws(nextpoint.Height()-1, []base.Address{withdrawnode.Address()}, nodes[:2])
	withdrawfacts := make([]util.Hash, len(withdraws))
	for i := range withdraws {
		withdrawfacts[i] = withdraws[i].Fact().Hash()
	}

	ifact := isaac.NewINITBallotFact(nextpoint, afact.NewBlock(), t.PRPool.Hash(nextpoint), withdrawfacts)
	nextdrawivp, err := t.NewINITVoteproof(ifact, t.Local, nodes)
	t.NoError(err)
	nextmajorityivp, err := t.NewINITWithdrawVoteproof(ifact, t.Local, nodes, withdraws)
	t.NoError(err)

	drawsfs := make([]base.BallotSignFact, len(nodes))
	var majoritysfs []base.BallotSignFact
	for i := range nodes {
		n := nodes[i]

		var fs isaac.INITBallotSignFact

		if n.Address().Equal(withdrawnode.Address()) {
			fact := isaac.NewINITBallotFact(nextpoint, afact.NewBlock(), t.PRPool.Hash(nextpoint), nil)
			fs = isaac.NewINITBallotSignFact(fact)
		} else {
			fs = isaac.NewINITBallotSignFact(ifact)
		}

		t.NoError(fs.NodeSign(n.Privatekey(), t.LocalParams.NetworkID(), n.Address()))

		if !n.Address().Equal(withdrawnode.Address()) {
			majoritysfs = append(majoritysfs, fs)
		}

		drawsfs[i] = fs
	}
	nextdrawivp.
		SetMajority(nil).
		SetSignFacts(drawsfs).
		Finish()

	nextmajorityivp.
		SetMajority(ifact).
		SetSignFacts(majoritysfs).
		Finish()

	t.NoError(nextdrawivp.IsValid(t.LocalParams.NetworkID()))

	t.T().Log("next draw init voteproof:", nextdrawivp.Point())
	t.NoError(st.newVoteproof(nextdrawivp))

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait suffrage confirm init ballot"))

		return
	case bl := <-initballotch:
		t.NoError(bl.IsValid(t.LocalParams.NetworkID()))
		ibl, ok := bl.(base.INITBallot)
		t.True(ok)
		t.Equal(nextdrawivp.Point().NextRound(), ibl.Point().Point, "%v != %v", nextdrawivp.Point().NextRound(), ibl.Point())

		_, ok = ibl.SignFact().Fact().(isaac.INITBallotFact)
		t.True(ok)

		t.T().Log("expected next round init ballot broadcasted", bl.Point())
	}

	t.T().Log("reversal, next majority init voteproof:", nextmajorityivp.Point())
	t.NoError(st.newVoteproof(nextmajorityivp))

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait suffrage confirm init ballot"))

		return
	case bl := <-scballotch:
		t.NoError(bl.IsValid(t.LocalParams.NetworkID()))
		ibl, ok := bl.(base.INITBallot)
		t.True(ok)
		t.Equal(nextmajorityivp.Point().Point, ibl.Point().Point, "%v != %v", nextmajorityivp.Point().Point, ibl.Point())

		_, ok = ibl.SignFact().Fact().(isaac.SuffrageConfirmBallotFact)
		t.True(ok, "expected SuffrageConfirmBallotFact, but %T", ibl.SignFact().Fact())

		t.T().Log("expected suffrage confirm init ballot broadcasted", bl.Point())
	}
}

func (t *testWithdrawsConsensusHandler) TestEnterINITStuckVoteproof() {
	point := base.RawPoint(33, 44)
	suf, nodes := isaac.NewTestSuffrage(2, t.Local)

	t.LocalParams = t.LocalParams.SetWaitPreparingINITBallot(time.Nanosecond)

	withdrawnode := nodes[2]

	st, closefunc, _, origivp := t.newStateWithINITVoteproof(point, suf)
	defer closefunc()

	ballotch := make(chan base.Ballot, 1)
	st.ballotBroadcaster = NewDummyBallotBroadcaster(t.Local.Address(), func(bl base.Ballot) error {
		ballotch <- bl

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

	t.T().Log("prepare new init stuck voteproof")

	withdraws := t.Withdraws(point.Height(), []base.Address{withdrawnode.Address()}, nodes[:2])
	withdrawfacts := make([]util.Hash, len(withdraws))
	for i := range withdraws {
		withdrawfacts[i] = withdraws[i].Fact().Hash()
	}

	sfs := util.FilterSlice(origivp.SignFacts(), func(i base.BallotSignFact) bool {
		return !i.Node().Equal(withdrawnode.Address())
	})

	ivp := isaac.NewINITStuckVoteproof(origivp.Point().Point)
	ivp.
		SetMajority(origivp.Majority()).
		SetSignFacts(sfs)
	ivp.SetWithdraws(withdraws)
	ivp.Finish()

	// NOTE set last accept voteproof as last voteproofs
	lvps := st.lastVoteproofs()
	st.forceSetLastVoteproof(lvps.ACCEPT())

	sctx, _ := newConsensusSwitchContext(StateJoining, ivp)

	deferred, err := st.enter(StateJoining, sctx)
	t.NoError(err)
	deferred()

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("wait next init ballot, but failed"))

		return
	case bl := <-ballotch:
		t.NotNil(bl)

		t.Equal(base.NewStagePoint(point.NextRound(), base.StageINIT), bl.Point())
	}
}

func (t *testWithdrawsConsensusHandler) TestINITStuckVoteproof() {
	point := base.RawPoint(33, 44)

	_, nodes, st, _, dof, closef := t.prepareAfterACCEPT(point, 3)
	defer closef()

	withdrawnode := nodes[2]

	nextinitballotch := make(chan base.Ballot, 1)
	nextroundballotch := make(chan base.Ballot, 1)
	st.ballotBroadcaster = NewDummyBallotBroadcaster(t.Local.Address(), func(bl base.Ballot) error {
		switch {
		case bl.Point().Point.Equal(point) && bl.Point().Stage() == base.StageACCEPT:
		case bl.Point().Point.Equal(point.NextHeight()) && bl.Point().Stage() == base.StageINIT:
			nextinitballotch <- bl
		case bl.Point().Point.Equal(point.NextHeight().NextRound()) && bl.Point().Stage() == base.StageINIT:
			nextroundballotch <- bl
		}

		return nil
	})

	withdraws := t.Withdraws(point.NextHeight().Height(), []base.Address{withdrawnode.Address()}, nodes[:2])
	st.args.SuffrageVotingFindFunc = func(context.Context, base.Height, base.Suffrage) (
		[]base.SuffrageWithdrawOperation, error,
	) {
		return withdraws, nil
	}

	dof()

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("wait next init ballot, but failed"))

		return
	case bl := <-nextinitballotch:
		ifact := bl.SignFact().Fact().(base.INITBallotFact)
		origivp, err := t.NewINITVoteproof(ifact, t.Local, nodes[:2])
		t.NoError(err)

		t.T().Log("next init ballot broadcasted; prepare new init stuck voteproof")

		withdrawfacts := make([]util.Hash, len(withdraws))
		for i := range withdraws {
			withdrawfacts[i] = withdraws[i].Fact().Hash()
		}

		sfs := util.FilterSlice(origivp.SignFacts(), func(i base.BallotSignFact) bool {
			return !i.Node().Equal(withdrawnode.Address())
		})

		stuckivp := isaac.NewINITStuckVoteproof(origivp.Point().Point)
		stuckivp.
			SetMajority(ifact).
			SetSignFacts(sfs)
		stuckivp.SetWithdraws(withdraws)
		stuckivp.Finish()

		t.NoError(stuckivp.IsValid(t.LocalParams.NetworkID()))

		t.T().Log("process new stuck voteproof")

		t.NoError(st.newVoteproof(stuckivp))
	}

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("wait next round init ballot, but failed"))

		return
	case bl := <-nextroundballotch:
		t.NoError(bl.IsValid(t.LocalParams.NetworkID()))

		t.Equal(point.NextHeight().NextRound(), bl.Point().Point)

		wbl, ok := bl.(base.HasWithdraws)
		t.True(ok)
		t.NotEmpty(wbl.Withdraws())
		t.Equal(1, len(wbl.Withdraws()))

		base.EqualOperation(t.Assert(), withdraws[0], wbl.Withdraws()[0])
	}
}

func (t *testWithdrawsConsensusHandler) TestINITStuckVoteproofEnterSyncing() {
	point := base.RawPoint(33, 44)

	_, nodes, st, _, dof, closef := t.prepareAfterACCEPT(point, 3)
	defer closef()

	withdrawnode := nodes[2]

	nextinitballotch := make(chan base.Ballot, 1)
	st.ballotBroadcaster = NewDummyBallotBroadcaster(t.Local.Address(), func(bl base.Ballot) error {
		switch {
		case bl.Point().Point.Equal(point) && bl.Point().Stage() == base.StageACCEPT:
		case bl.Point().Point.Equal(point.NextHeight()) && bl.Point().Stage() == base.StageINIT:
			nextinitballotch <- bl
		}

		return nil
	})

	withdraws := t.Withdraws(point.NextHeight().Height(), []base.Address{withdrawnode.Address()}, nodes[:2])
	st.args.SuffrageVotingFindFunc = func(context.Context, base.Height, base.Suffrage) (
		[]base.SuffrageWithdrawOperation, error,
	) {
		return withdraws, nil
	}

	dof()

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("wait next init ballot, but failed"))

		return
	case bl := <-nextinitballotch:
		nextpoint := bl.Point().Point.NextHeight()

		ifact := isaac.NewINITBallotFact(nextpoint, valuehash.RandomSHA256(), valuehash.RandomSHA256(), nil)
		origivp, err := t.NewINITVoteproof(ifact, t.Local, nodes[:2])
		t.NoError(err)

		t.T().Log("next init ballot broadcasted; prepare new higher init stuck voteproof")

		withdrawfacts := make([]util.Hash, len(withdraws))
		for i := range withdraws {
			withdrawfacts[i] = withdraws[i].Fact().Hash()
		}

		sfs := util.FilterSlice(origivp.SignFacts(), func(i base.BallotSignFact) bool {
			return !i.Node().Equal(withdrawnode.Address())
		})

		stuckivp := isaac.NewINITStuckVoteproof(origivp.Point().Point)
		stuckivp.
			SetMajority(ifact).
			SetSignFacts(sfs)
		stuckivp.SetWithdraws(withdraws)
		stuckivp.Finish()

		t.NoError(stuckivp.IsValid(t.LocalParams.NetworkID()))

		t.T().Log("process new stuck voteproof")

		err = st.newVoteproof(stuckivp)
		t.Error(err)

		t.T().Log("switching to syncing state")
		var ssctx SyncingSwitchContext
		t.True(errors.As(err, &ssctx))
		t.Equal(nextpoint.Height()-1, ssctx.height)
	}
}

func (t *testWithdrawsConsensusHandler) TestACCEPTStuckVoteproof() {
	point := base.RawPoint(33, 44)
	suf, nodes := isaac.NewTestSuffrage(2, t.Local)
	withdrawnode := nodes[2]

	t.LocalParams = t.LocalParams.SetWaitPreparingINITBallot(time.Nanosecond)

	st, closefunc, pp, ivp := t.newStateWithINITVoteproof(point, suf)
	defer closefunc()

	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	pp.Processerr = func(context.Context, base.ProposalFact, base.INITVoteproof) (base.Manifest, error) {
		return manifest, nil
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

	acceptballotch := make(chan base.Ballot, 1)
	nextroundballotch := make(chan base.Ballot, 1)
	st.ballotBroadcaster = NewDummyBallotBroadcaster(t.Local.Address(), func(bl base.Ballot) error {
		switch {
		case bl.Point().Point.Equal(point) && bl.Point().Stage() == base.StageACCEPT:
			acceptballotch <- bl
		case bl.Point().Point.Equal(point.NextRound()) && bl.Point().Stage() == base.StageINIT:
			nextroundballotch <- bl
		}

		return nil
	})

	t.T().Log("prepare new accept stuck voteproof")

	withdraws := t.Withdraws(point.Height(), []base.Address{withdrawnode.Address()}, nodes[:2])
	withdrawfacts := make([]util.Hash, len(withdraws))
	for i := range withdraws {
		withdrawfacts[i] = withdraws[i].Fact().Hash()
	}

	st.args.SuffrageVotingFindFunc = func(context.Context, base.Height, base.Suffrage) (
		[]base.SuffrageWithdrawOperation, error,
	) {
		return withdraws, nil
	}

	sctx, _ := newConsensusSwitchContext(StateJoining, ivp)

	deferred, err := st.enter(StateJoining, sctx)
	t.NoError(err)
	deferred()

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("wait accept ballot, but failed"))

		return
	case bl := <-acceptballotch:
		t.NotNil(bl)

		t.Equal(base.NewStagePoint(point, base.StageACCEPT), bl.Point())

		t.T().Log("new accept stuck voteproof")

		fact := bl.SignFact().Fact().(isaac.ACCEPTBallotFact)
		origavp, err := t.NewACCEPTVoteproof(fact, t.Local, nodes[:2])
		t.NoError(err)

		sfs := util.FilterSlice(origavp.SignFacts(), func(i base.BallotSignFact) bool {
			return !i.Node().Equal(withdrawnode.Address())
		})

		stuckavp := isaac.NewACCEPTStuckVoteproof(origavp.Point().Point)
		stuckavp.
			SetMajority(origavp.Majority()).
			SetSignFacts(sfs)
		stuckavp.SetWithdraws(withdraws)
		stuckavp.Finish()

		t.NoError(stuckavp.IsValid(t.LocalParams.NetworkID()))

		t.T().Log("process new accept stuck voteproof")

		t.NoError(st.newVoteproof(stuckavp))
	}

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("wait next round ballot, but failed"))

		return
	case bl := <-nextroundballotch:
		t.NotNil(bl)

		t.Equal(point.NextRound(), bl.Point().Point)

		wbl, ok := bl.(base.HasWithdraws)
		t.True(ok)
		t.NotEmpty(wbl.Withdraws())
		t.Equal(1, len(wbl.Withdraws()))

		base.EqualOperation(t.Assert(), withdraws[0], wbl.Withdraws()[0])
	}
}

func (t *testWithdrawsConsensusHandler) TestACCEPTStuckVoteproofEnterSyncing() {
	point := base.RawPoint(33, 44)
	suf, nodes := isaac.NewTestSuffrage(2, t.Local)
	withdrawnode := nodes[2]

	st, closefunc, pp, ivp := t.newStateWithINITVoteproof(point, suf)
	defer closefunc()

	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	pp.Processerr = func(context.Context, base.ProposalFact, base.INITVoteproof) (base.Manifest, error) {
		return manifest, nil
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

	acceptballotch := make(chan base.Ballot, 1)
	nextroundballotch := make(chan base.Ballot, 1)
	st.ballotBroadcaster = NewDummyBallotBroadcaster(t.Local.Address(), func(bl base.Ballot) error {
		switch {
		case bl.Point().Point.Equal(point) && bl.Point().Stage() == base.StageACCEPT:
			acceptballotch <- bl
		case bl.Point().Point.Equal(point.NextRound()) && bl.Point().Stage() == base.StageINIT:
			nextroundballotch <- bl
		}

		return nil
	})

	t.T().Log("prepare new accept stuck voteproof")

	withdraws := t.Withdraws(point.Height(), []base.Address{withdrawnode.Address()}, nodes[:2])
	withdrawfacts := make([]util.Hash, len(withdraws))
	for i := range withdraws {
		withdrawfacts[i] = withdraws[i].Fact().Hash()
	}

	st.args.SuffrageVotingFindFunc = func(context.Context, base.Height, base.Suffrage) (
		[]base.SuffrageWithdrawOperation, error,
	) {
		return withdraws, nil
	}

	sctx, _ := newConsensusSwitchContext(StateJoining, ivp)

	deferred, err := st.enter(StateJoining, sctx)
	t.NoError(err)
	deferred()

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("wait accept ballot, but failed"))

		return
	case bl := <-acceptballotch:
		t.NotNil(bl)

		t.Equal(base.NewStagePoint(point, base.StageACCEPT), bl.Point())

		t.T().Log("new higher accept stuck voteproof")

		nextpoint := bl.Point().Point.NextHeight()

		fact := isaac.NewACCEPTBallotFact(nextpoint, valuehash.RandomSHA256(), valuehash.RandomSHA256(), nil)
		origavp, err := t.NewACCEPTVoteproof(fact, t.Local, nodes[:2])
		t.NoError(err)

		sfs := util.FilterSlice(origavp.SignFacts(), func(i base.BallotSignFact) bool {
			return !i.Node().Equal(withdrawnode.Address())
		})

		stuckavp := isaac.NewACCEPTStuckVoteproof(origavp.Point().Point)
		stuckavp.
			SetMajority(origavp.Majority()).
			SetSignFacts(sfs)
		stuckavp.SetWithdraws(withdraws)
		stuckavp.Finish()

		t.NoError(stuckavp.IsValid(t.LocalParams.NetworkID()))

		t.T().Log("process new higher accept stuck voteproof")

		err = st.newVoteproof(stuckavp)
		t.Error(err)

		t.T().Log("switching to syncing state")
		var ssctx SyncingSwitchContext
		t.True(errors.As(err, &ssctx))
		t.Equal(nextpoint.Height()-1, ssctx.height)
	}
}

func TestWithdrawsConsensusHandler(t *testing.T) {
	suite.Run(t, new(testWithdrawsConsensusHandler))
}
