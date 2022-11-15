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

func (t *testWithdrawsConsensusHandler) TestEnterWithSIGNVoteproof() {
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
	st.proposalSelector = isaac.DummyProposalSelector(func(ctx context.Context, p base.Point) (base.ProposalSignFact, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			return prpool.Get(p), nil
		}
	})

	t.T().Log("prepare new init voteproof")

	origifact := origivp.BallotMajority()

	withdraws := t.Withdraws(point.Height()-1, []base.Address{withdrawnode.Address()}, nodes[:2])
	withdrawfacts := make([]util.Hash, len(withdraws))
	for i := range withdraws {
		withdrawfacts[i] = withdraws[i].Fact().Hash()
	}

	sfact := isaac.NewSIGNBallotFact(point, origifact.PreviousBlock(), origifact.Proposal(), withdrawfacts)
	ivp, err := t.NewINITVoteproof(sfact, t.Local, nodes[:2])
	t.NoError(err)

	_ = ivp.SetWithdraws(withdraws)

	t.T().Log("new sign init voteproof", ivp.Point())

	sctx := newConsensusSwitchContext(StateJoining, ivp)

	deferred, err := st.enter(StateJoining, sctx)
	t.NoError(err)
	deferred()

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait sign init ballot"))

		return
	case m := <-processedch:
		t.NotNil(m)

		base.EqualManifest(t.Assert(), manifest, m)

		t.T().Log("expected manifest processed from sign init voteproof,", manifest.Height())
	}
}

func (t *testWithdrawsConsensusHandler) TestSIGNAfterEnteringINITVoteproof() {
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

	signballotch := make(chan base.Ballot, 1)
	st.broadcastBallotFunc = func(bl base.Ballot) error {
		if p := bl.Point(); p.Point.Equal(point) && p.Stage() == base.StageINIT {
			signballotch <- bl
		}

		return nil
	}

	prpool := t.PRPool
	st.proposalSelector = isaac.DummyProposalSelector(func(ctx context.Context, p base.Point) (base.ProposalSignFact, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			return prpool.Get(p), nil
		}
	})

	t.T().Log("prepare new init voteproof")

	origifact := origivp.BallotMajority()

	withdraws := t.Withdraws(point.Height()-1, []base.Address{withdrawnode.Address()}, nodes[:2])
	withdrawfacts := make([]util.Hash, len(withdraws))
	for i := range withdraws {
		withdrawfacts[i] = withdraws[i].Fact().Hash()
	}

	ifact := isaac.NewINITBallotFact(point, origifact.PreviousBlock(), origifact.Proposal(), withdrawfacts)
	ivp, err := t.NewINITVoteproof(ifact, t.Local, nodes[:2])
	t.NoError(err)

	_ = ivp.SetWithdraws(withdraws)

	t.True(st.forceSetLastVoteproof(ivp))

	t.T().Log("new withdraw init voteproof", ivp.Point())

	sctx := newConsensusSwitchContext(StateJoining, ivp)

	deferred, err := st.enter(StateJoining, sctx)
	t.NoError(err)
	deferred()

	var sfact isaac.SIGNBallotFact

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait sign init ballot"))

		return
	case bl := <-signballotch:
		t.NoError(bl.IsValid(t.LocalParams.NetworkID()))
		ibl, ok := bl.(base.INITBallot)
		t.True(ok)
		t.Equal(ivp.Point(), ibl.Point())

		i, ok := ibl.SignFact().Fact().(isaac.SIGNBallotFact)
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

		t.T().Log("expected sign init ballot broadcasted", bl.Point())
	}

	ivp, err = t.NewINITVoteproof(sfact, t.Local, nodes[:2])
	t.NoError(err)

	_ = ivp.SetWithdraws(withdraws)

	t.T().Log("new sign init voteproof", ivp.Point())

	t.NoError(st.newVoteproof(ivp))

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait sign init ballot"))

		return
	case m := <-processedch:
		t.NotNil(m)

		base.EqualManifest(t.Assert(), manifest, m)

		t.T().Log("expected manifest processed from sign init voteproof,", manifest.Height())
	}
}

func (t *testWithdrawsConsensusHandler) TestSIGNAfterACCEPTVoteproof() {
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

	signballotch := make(chan base.Ballot, 1)
	st.broadcastBallotFunc = func(bl base.Ballot) error {
		if p := bl.Point(); p.Point.Equal(point.NextHeight()) && p.Stage() == base.StageINIT {
			signballotch <- bl
		}

		return nil
	}

	savedch := make(chan base.ACCEPTVoteproof, 1)
	pp.Saveerr = func(_ context.Context, avp base.ACCEPTVoteproof) error {
		savedch <- avp
		return nil
	}

	prpool := t.PRPool
	st.proposalSelector = isaac.DummyProposalSelector(func(ctx context.Context, p base.Point) (base.ProposalSignFact, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			return prpool.Get(p), nil
		}
	})

	t.T().Log("1st init voteproof", ivp.Point())

	sctx := newConsensusSwitchContext(StateJoining, ivp)

	deferred, err := st.enter(StateJoining, sctx)
	t.NoError(err)
	deferred()

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait sign init ballot"))

		return
	case m := <-processedch:
		t.NotNil(m)

		base.EqualManifest(t.Assert(), manifest, m)

		t.T().Log("expected manifest processed from sign init voteproof,", manifest.Height())
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
	nextivp, err := t.NewINITVoteproof(ifact, t.Local, nodes[:2])
	t.NoError(err)

	_ = nextivp.SetWithdraws(withdraws)

	t.NoError(st.newVoteproof(nextivp))

	var sfact isaac.SIGNBallotFact

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait sign init ballot"))

		return
	case bl := <-signballotch:
		t.NoError(bl.IsValid(t.LocalParams.NetworkID()))
		ibl, ok := bl.(base.INITBallot)
		t.True(ok)
		t.Equal(nextivp.Point(), ibl.Point())

		i, ok := ibl.SignFact().Fact().(isaac.SIGNBallotFact)
		t.True(ok)
		sfact = i

		t.T().Log("expected sign init ballot broadcasted", sfact.Point())
	}

	signivp, err := t.NewINITVoteproof(sfact, t.Local, nodes[:2])
	t.NoError(err)

	_ = signivp.SetWithdraws(withdraws)

	t.T().Log("new sign init voteproof", signivp.Point())

	t.NoError(st.newVoteproof(signivp))

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait sign init ballot"))

		return
	case m := <-processedch:
		t.NotNil(m)

		base.EqualManifest(t.Assert(), nextmanifest, m)

		t.T().Log("expected manifest processed from sign init voteproof,", manifest.Height())
	}
}

func (t *testWithdrawsConsensusHandler) prepareAfterACCEPT(
	point base.Point,
	numberOfNodes int,
) (
	base.Suffrage,
	[]isaac.LocalNode,
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

	st.broadcastBallotFunc = func(bl base.Ballot) error {
		return nil
	}

	savedch := make(chan base.ACCEPTVoteproof, 1)
	pp.Saveerr = func(_ context.Context, avp base.ACCEPTVoteproof) error {
		savedch <- avp

		return nil
	}

	prpool := t.PRPool
	st.proposalSelector = isaac.DummyProposalSelector(func(ctx context.Context, p base.Point) (base.ProposalSignFact, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			return prpool.Get(p), nil
		}
	})

	return suf, nodes, st, pp, func() (base.ACCEPTVoteproof, error) {
			t.T().Log("1st init voteproof", ivp.Point())

			sctx := newConsensusSwitchContext(StateJoining, ivp)

			deferred, err := st.enter(StateJoining, sctx)
			t.NoError(err)
			deferred()

			select {
			case <-time.After(time.Second * 2):
				return nil, errors.Errorf("timeout to wait sign init ballot")
			case m := <-processedch:
				t.NotNil(m)

				base.EqualManifest(t.Assert(), manifest, m)

				t.T().Log("expected manifest processed from sign init voteproof,", manifest.Height())
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

func (t *testWithdrawsConsensusHandler) TestSIGNAfterDrawINITVoteproof() {
	point := base.RawPoint(33, 44)
	nextpoint := point.NextHeight()

	_, nodes, st, pp, dof, closef := t.prepareAfterACCEPT(point, 3)
	defer closef()

	initballotch := make(chan base.Ballot, 1)
	st.broadcastBallotFunc = func(bl base.Ballot) error {
		if p := bl.Point(); p.Point.Equal(nextpoint.NextRound()) && p.Stage() == base.StageINIT {
			initballotch <- bl
		}

		return nil
	}

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
			fs = isaac.NewINITBallotSignFact(n.Address(), fact)
		} else {
			fs = isaac.NewINITBallotSignFact(n.Address(), ifact)
		}

		t.NoError(fs.Sign(n.Privatekey(), t.LocalParams.NetworkID()))

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
		t.NoError(errors.Errorf("timeout to wait sign init ballot"))

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

	sfact := isaac.NewSIGNBallotFact(nextpoint, afact.NewBlock(), t.PRPool.Hash(nextpoint), withdrawfacts)
	signivp, err := t.NewINITVoteproof(sfact, t.Local, nodes[:2])
	t.NoError(err)
	_ = signivp.SetWithdraws(withdraws)

	t.T().Log("next sign init voteproof:", signivp.Point())
	t.NoError(st.newVoteproof(signivp))

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait sign init ballot"))

		return
	case m := <-processedch:
		t.NotNil(m)

		base.EqualManifest(t.Assert(), nextmanifest, m)

		t.T().Log("expected manifest processed from sign init voteproof,", nextmanifest.Height())
	}
}

func (t *testWithdrawsConsensusHandler) TestReversalAfterDrawINITVoteproof() {
	point := base.RawPoint(33, 44)
	nextpoint := point.NextHeight()

	_, nodes, st, _, dof, closef := t.prepareAfterACCEPT(point, 3)
	defer closef()

	initballotch := make(chan base.Ballot, 3)
	signballotch := make(chan base.Ballot, 3)
	st.broadcastBallotFunc = func(bl base.Ballot) error {
		p := bl.Point()
		if p.Stage() != base.StageINIT {
			return nil
		}

		switch {
		case p.Point.Equal(nextpoint.NextRound()):
			initballotch <- bl
		case p.Point.Equal(nextpoint):
			if _, ok := bl.SignFact().Fact().(isaac.SIGNBallotFact); ok {
				signballotch <- bl
			}
		}

		return nil
	}

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
	nextmajorityivp, err := t.NewINITVoteproof(ifact, t.Local, nodes)
	t.NoError(err)

	drawsfs := make([]base.BallotSignFact, len(nodes))
	var majoritysfs []base.BallotSignFact
	for i := range nodes {
		n := nodes[i]

		var fs isaac.INITBallotSignFact

		if n.Address().Equal(withdrawnode.Address()) {
			fact := isaac.NewINITBallotFact(nextpoint, afact.NewBlock(), t.PRPool.Hash(nextpoint), nil)
			fs = isaac.NewINITBallotSignFact(n.Address(), fact)
		} else {
			fs = isaac.NewINITBallotSignFact(n.Address(), ifact)
		}

		t.NoError(fs.Sign(n.Privatekey(), t.LocalParams.NetworkID()))

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
		SetWithdraws(withdraws).
		Finish()

	t.NoError(nextdrawivp.IsValid(t.LocalParams.NetworkID()))

	t.T().Log("next draw init voteproof:", nextdrawivp.Point())
	t.NoError(st.newVoteproof(nextdrawivp))

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait sign init ballot"))

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
		t.NoError(errors.Errorf("timeout to wait sign init ballot"))

		return
	case bl := <-signballotch:
		t.NoError(bl.IsValid(t.LocalParams.NetworkID()))
		ibl, ok := bl.(base.INITBallot)
		t.True(ok)
		t.Equal(nextmajorityivp.Point().Point, ibl.Point().Point, "%v != %v", nextmajorityivp.Point().Point, ibl.Point())

		_, ok = ibl.SignFact().Fact().(isaac.SIGNBallotFact)
		t.True(ok, "expected SIGNBallotFact, but %T", ibl.SignFact().Fact())

		t.T().Log("expected sign init ballot broadcasted", bl.Point())
	}
}

func TestWithdrawsConsensusHandler(t *testing.T) {
	suite.Run(t, new(testWithdrawsConsensusHandler))
}
