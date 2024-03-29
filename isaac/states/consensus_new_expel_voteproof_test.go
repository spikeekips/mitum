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
	"golang.org/x/exp/slices"
)

type testExpelsConsensusHandler struct {
	baseTestConsensusHandler
}

func (t *testExpelsConsensusHandler) TestEnterWithSuffrageConfirmVoteproof() {
	point := base.RawPoint(33, 44)
	suf, nodes := isaac.NewTestSuffrage(2, t.Local)
	expelnode := nodes[2]

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

	expels := t.Expels(point.Height()-1, []base.Address{expelnode.Address()}, nodes[:2])
	expelfacts := make([]util.Hash, len(expels))
	for i := range expels {
		expelfacts[i] = expels[i].Fact().Hash()
	}

	sfact := isaac.NewSuffrageConfirmBallotFact(point, origifact.PreviousBlock(), origifact.Proposal(), expelfacts)
	ivp, err := t.NewINITExpelVoteproof(sfact, t.Local, nodes[:2], expels)
	t.NoError(err)

	t.T().Log("new suffrage confirm init voteproof", ivp.Point())

	sctx, _ := newConsensusSwitchContext(StateJoining, ivp)

	deferred, err := st.enter(StateJoining, sctx)
	t.NoError(err)
	deferred()

	select {
	case <-time.After(time.Second * 2):
		t.Fail("timeout to wait suffrage confirm init ballot")

		return
	case m := <-processedch:
		t.NotNil(m)

		base.EqualManifest(t.Assert(), manifest, m)

		t.T().Log("expected manifest processed from suffrage confirm init voteproof,", manifest.Height())
	}
}

func (t *testExpelsConsensusHandler) TestSuffrageConfirmAfterEnteringINITVoteproof() {
	point := base.RawPoint(33, 44)
	suf, nodes := isaac.NewTestSuffrage(2, t.Local)
	expelnode := nodes[2]

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

	expels := t.Expels(point.Height()-1, []base.Address{expelnode.Address()}, nodes[:2])
	expelfacts := make([]util.Hash, len(expels))
	for i := range expels {
		expelfacts[i] = expels[i].Fact().Hash()
	}

	ifact := isaac.NewINITBallotFact(point, origifact.PreviousBlock(), origifact.Proposal(), expelfacts)
	ivp, err := t.NewINITExpelVoteproof(ifact, t.Local, nodes[:2], expels)
	t.NoError(err)

	t.True(st.forceSetLastVoteproof(ivp))

	t.T().Log("new expel init voteproof", ivp.Point())

	sctx, _ := newConsensusSwitchContext(StateJoining, ivp)

	deferred, err := st.enter(StateJoining, sctx)
	t.NoError(err)
	deferred()

	var sfact isaac.SuffrageConfirmBallotFact

	select {
	case <-time.After(time.Second * 2):
		t.Fail("timeout to wait suffrage confirm init ballot")

		return
	case bl := <-scballotch:
		t.NoError(bl.IsValid(t.LocalParams.NetworkID()))
		ibl, ok := bl.(base.INITBallot)
		t.True(ok)
		t.Equal(ivp.Point(), ibl.Point())

		i, ok := ibl.SignFact().Fact().(isaac.SuffrageConfirmBallotFact)
		t.True(ok)
		sfact = i

		sfactexpels := sfact.ExpelFacts()

		t.Equal(len(expelfacts), len(sfactexpels))
		for i := range expelfacts {
			f := slices.IndexFunc(sfactexpels, func(j util.Hash) bool {
				return expelfacts[i].Equal(j)
			})
			t.False(f < 0)
		}

		t.T().Log("expected suffrage confirm init ballot broadcasted", bl.Point())
	}

	ivp, err = t.NewINITExpelVoteproof(sfact, t.Local, nodes[:2], expels)
	t.NoError(err)

	t.T().Log("new suffrage confirm init voteproof", ivp.Point())

	t.NoError(st.newVoteproof(ivp))

	select {
	case <-time.After(time.Second * 2):
		t.Fail("timeout to wait suffrage confirm init ballot")

		return
	case m := <-processedch:
		t.NotNil(m)

		base.EqualManifest(t.Assert(), manifest, m)

		t.T().Log("expected manifest processed from suffrage confirm init voteproof,", manifest.Height())
	}
}

func (t *testExpelsConsensusHandler) TestSuffrageConfirmAfterACCEPTVoteproof() {
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
		if _, ok := bl.SignFact().Fact().(isaac.SuffrageConfirmBallotFact); !ok {
			return nil
		}

		if p := bl.Point(); p.Point.Equal(point.NextHeight()) && p.Stage() == base.StageINIT {
			scballotch <- bl
		}

		return nil
	})

	savedch := make(chan base.ACCEPTVoteproof, 1)
	pp.Saveerr = func(_ context.Context, avp base.ACCEPTVoteproof) (base.BlockMap, error) {
		savedch <- avp
		return nil, nil
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
		t.Fail("timeout to wait suffrage confirm init ballot")

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
		t.Fail("timeout to wait save proposal processor")

		return
	case <-savedch:
	}

	nextpoint := point.NextHeight()

	expelnode := nodes[2]
	expels := t.Expels(nextpoint.Height()-1, []base.Address{expelnode.Address()}, nodes[:2])
	expelfacts := make([]util.Hash, len(expels))
	for i := range expels {
		expelfacts[i] = expels[i].Fact().Hash()
	}

	ifact := isaac.NewINITBallotFact(nextpoint, afact.NewBlock(), t.PRPool.Hash(nextpoint), expelfacts)
	nextivp, err := t.NewINITExpelVoteproof(ifact, t.Local, nodes[:2], expels)
	t.NoError(err)

	t.NoError(st.newVoteproof(nextivp))

	var sfact isaac.SuffrageConfirmBallotFact

	select {
	case <-time.After(time.Second * 2):
		t.Fail("timeout to wait suffrage confirm init ballot")

		return
	case bl := <-scballotch:
		t.NoError(bl.IsValid(t.LocalParams.NetworkID()))
		ibl, ok := bl.(base.INITBallot)
		t.True(ok)
		t.Equal(nextivp.Point(), ibl.Point())

		i, ok := ibl.SignFact().Fact().(isaac.SuffrageConfirmBallotFact)
		t.True(ok)
		sfact = i

		t.T().Logf("expected suffrage confirm init ballot broadcasted, %v %T", sfact.Point(), ibl.SignFact().Fact())
	}

	confirmivp, err := t.NewINITExpelVoteproof(sfact, t.Local, nodes[:2], expels)
	t.NoError(err)

	t.T().Log("new suffrage confirm init voteproof", confirmivp.Point())

	t.NoError(st.newVoteproof(confirmivp))

	select {
	case <-time.After(time.Second * 2):
		t.Fail("timeout to wait suffrage confirm init ballot")

		return
	case m := <-processedch:
		t.NotNil(m)

		base.EqualManifest(t.Assert(), nextmanifest, m)

		t.T().Log("expected manifest processed from suffrage confirm init voteproof,", manifest.Height())
	}
}

func (t *testExpelsConsensusHandler) prepareAfterACCEPT(
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
	pp.Saveerr = func(_ context.Context, avp base.ACCEPTVoteproof) (base.BlockMap, error) {
		savedch <- avp

		return nil, nil
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

func (t *testExpelsConsensusHandler) TestSuffrageConfirmAfterDrawINITVoteproof() {
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

	expelnode := nodes[2]
	expels := t.Expels(nextpoint.Height()-1, []base.Address{expelnode.Address()}, nodes[:2])
	expelfacts := make([]util.Hash, len(expels))
	for i := range expels {
		expelfacts[i] = expels[i].Fact().Hash()
	}

	ifact := isaac.NewINITBallotFact(nextpoint, afact.NewBlock(), t.PRPool.Hash(nextpoint), expelfacts)
	nextdrawivp, err := t.NewINITVoteproof(ifact, t.Local, nodes)
	t.NoError(err)

	isfs := make([]base.BallotSignFact, len(nodes))
	for i := range nodes {
		n := nodes[i]

		var fs isaac.INITBallotSignFact

		if n.Address().Equal(expelnode.Address()) {
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
		t.Fail("timeout to wait suffrage confirm init ballot")

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

	sfact := isaac.NewSuffrageConfirmBallotFact(nextpoint, afact.NewBlock(), t.PRPool.Hash(nextpoint), expelfacts)
	confirmivp, err := t.NewINITExpelVoteproof(sfact, t.Local, nodes[:2], expels)
	t.NoError(err)

	t.T().Log("next suffrage confirm init voteproof:", confirmivp.Point())
	t.NoError(st.newVoteproof(confirmivp))

	select {
	case <-time.After(time.Second * 2):
		t.Fail("timeout to wait suffrage confirm init ballot")

		return
	case m := <-processedch:
		t.NotNil(m)

		base.EqualManifest(t.Assert(), nextmanifest, m)

		t.T().Log("expected manifest processed from suffrage confirm init voteproof,", nextmanifest.Height())
	}
}

func (t *testExpelsConsensusHandler) TestReversalAfterDrawINITVoteproof() {
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

	expelnode := nodes[2]
	expels := t.Expels(nextpoint.Height()-1, []base.Address{expelnode.Address()}, nodes[:2])
	expelfacts := make([]util.Hash, len(expels))
	for i := range expels {
		expelfacts[i] = expels[i].Fact().Hash()
	}

	ifact := isaac.NewINITBallotFact(nextpoint, afact.NewBlock(), t.PRPool.Hash(nextpoint), expelfacts)
	nextdrawivp, err := t.NewINITVoteproof(ifact, t.Local, nodes)
	t.NoError(err)
	nextmajorityivp, err := t.NewINITExpelVoteproof(ifact, t.Local, nodes, expels)
	t.NoError(err)

	drawsfs := make([]base.BallotSignFact, len(nodes))
	var majoritysfs []base.BallotSignFact
	for i := range nodes {
		n := nodes[i]

		var fs isaac.INITBallotSignFact

		if n.Address().Equal(expelnode.Address()) {
			fact := isaac.NewINITBallotFact(nextpoint, afact.NewBlock(), t.PRPool.Hash(nextpoint), nil)
			fs = isaac.NewINITBallotSignFact(fact)
		} else {
			fs = isaac.NewINITBallotSignFact(ifact)
		}

		t.NoError(fs.NodeSign(n.Privatekey(), t.LocalParams.NetworkID(), n.Address()))

		if !n.Address().Equal(expelnode.Address()) {
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
		t.Fail("timeout to wait suffrage confirm init ballot")

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
		t.Fail("timeout to wait suffrage confirm init ballot")

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

func (t *testExpelsConsensusHandler) TestEnterINITStuckVoteproof() {
	point := base.RawPoint(33, 44)
	suf, nodes := isaac.NewTestSuffrage(2, t.Local)

	t.LocalParams.SetWaitPreparingINITBallot(time.Nanosecond)

	expelnode := nodes[2]

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

	expels := t.Expels(point.Height(), []base.Address{expelnode.Address()}, nodes[:2])
	expelfacts := make([]util.Hash, len(expels))
	for i := range expels {
		expelfacts[i] = expels[i].Fact().Hash()
	}

	sfs := util.FilterSlice(origivp.SignFacts(), func(i base.BallotSignFact) bool {
		return !i.Node().Equal(expelnode.Address())
	})

	ivp := isaac.NewINITStuckVoteproof(origivp.Point().Point)
	ivp.
		SetMajority(origivp.Majority()).
		SetSignFacts(sfs)
	ivp.SetExpels(expels)
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
		t.Fail("wait next init ballot, but failed")

		return
	case bl := <-ballotch:
		t.NotNil(bl)

		t.Equal(base.NewStagePoint(point.NextRound(), base.StageINIT), bl.Point())
	}
}

func (t *testExpelsConsensusHandler) TestINITStuckVoteproof() {
	point := base.RawPoint(33, 44)

	_, nodes, st, _, dof, closef := t.prepareAfterACCEPT(point, 3)
	defer closef()

	expelnode := nodes[2]

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

	expels := t.Expels(point.NextHeight().Height(), []base.Address{expelnode.Address()}, nodes[:2])
	st.args.SuffrageVotingFindFunc = func(context.Context, base.Height, base.Suffrage) (
		[]base.SuffrageExpelOperation, error,
	) {
		return expels, nil
	}

	dof()

	select {
	case <-time.After(time.Second * 2):
		t.Fail("wait next init ballot, but failed")

		return
	case bl := <-nextinitballotch:
		ifact := bl.SignFact().Fact().(base.INITBallotFact)
		origivp, err := t.NewINITVoteproof(ifact, t.Local, nodes[:2])
		t.NoError(err)

		t.T().Log("next init ballot broadcasted; prepare new init stuck voteproof")

		expelfacts := make([]util.Hash, len(expels))
		for i := range expels {
			expelfacts[i] = expels[i].Fact().Hash()
		}

		sfs := util.FilterSlice(origivp.SignFacts(), func(i base.BallotSignFact) bool {
			return !i.Node().Equal(expelnode.Address())
		})

		stuckivp := isaac.NewINITStuckVoteproof(origivp.Point().Point)
		stuckivp.
			SetMajority(ifact).
			SetSignFacts(sfs)
		stuckivp.SetExpels(expels)
		stuckivp.Finish()

		t.NoError(stuckivp.IsValid(t.LocalParams.NetworkID()))

		t.T().Log("process new stuck voteproof")

		t.NoError(st.newVoteproof(stuckivp))
	}

	select {
	case <-time.After(time.Second * 2):
		t.Fail("wait next round init ballot, but failed")

		return
	case bl := <-nextroundballotch:
		t.NoError(bl.IsValid(t.LocalParams.NetworkID()))

		t.Equal(point.NextHeight().NextRound(), bl.Point().Point)

		wbl, ok := bl.(base.HasExpels)
		t.True(ok)
		t.NotEmpty(wbl.Expels())
		t.Equal(1, len(wbl.Expels()))

		base.EqualOperation(t.Assert(), expels[0], wbl.Expels()[0])
	}
}

func (t *testExpelsConsensusHandler) TestINITStuckVoteproofEnterSyncing() {
	point := base.RawPoint(33, 44)

	_, nodes, st, _, dof, closef := t.prepareAfterACCEPT(point, 3)
	defer closef()

	expelnode := nodes[2]

	nextinitballotch := make(chan base.Ballot, 1)
	st.ballotBroadcaster = NewDummyBallotBroadcaster(t.Local.Address(), func(bl base.Ballot) error {
		switch {
		case bl.Point().Point.Equal(point) && bl.Point().Stage() == base.StageACCEPT:
		case bl.Point().Point.Equal(point.NextHeight()) && bl.Point().Stage() == base.StageINIT:
			nextinitballotch <- bl
		}

		return nil
	})

	expels := t.Expels(point.NextHeight().Height(), []base.Address{expelnode.Address()}, nodes[:2])
	st.args.SuffrageVotingFindFunc = func(context.Context, base.Height, base.Suffrage) (
		[]base.SuffrageExpelOperation, error,
	) {
		return expels, nil
	}

	dof()

	select {
	case <-time.After(time.Second * 2):
		t.Fail("wait next init ballot, but failed")

		return
	case bl := <-nextinitballotch:
		nextpoint := bl.Point().Point.NextHeight()

		ifact := isaac.NewINITBallotFact(nextpoint, valuehash.RandomSHA256(), valuehash.RandomSHA256(), nil)
		origivp, err := t.NewINITVoteproof(ifact, t.Local, nodes[:2])
		t.NoError(err)

		t.T().Log("next init ballot broadcasted; prepare new higher init stuck voteproof")

		expelfacts := make([]util.Hash, len(expels))
		for i := range expels {
			expelfacts[i] = expels[i].Fact().Hash()
		}

		sfs := util.FilterSlice(origivp.SignFacts(), func(i base.BallotSignFact) bool {
			return !i.Node().Equal(expelnode.Address())
		})

		stuckivp := isaac.NewINITStuckVoteproof(origivp.Point().Point)
		stuckivp.
			SetMajority(ifact).
			SetSignFacts(sfs)
		stuckivp.SetExpels(expels)
		stuckivp.Finish()

		t.NoError(stuckivp.IsValid(t.LocalParams.NetworkID()))

		t.T().Log("process new stuck voteproof")

		err = st.newVoteproof(stuckivp)
		t.Error(err)

		t.T().Log("switching to syncing state")
		var ssctx SyncingSwitchContext
		t.ErrorAs(err, &ssctx)
		t.Equal(nextpoint.Height()-1, ssctx.height)
	}
}

func (t *testExpelsConsensusHandler) TestACCEPTStuckVoteproof() {
	point := base.RawPoint(33, 44)
	suf, nodes := isaac.NewTestSuffrage(2, t.Local)
	expelnode := nodes[2]

	t.LocalParams.SetWaitPreparingINITBallot(time.Nanosecond)

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

	expels := t.Expels(point.Height(), []base.Address{expelnode.Address()}, nodes[:2])
	expelfacts := make([]util.Hash, len(expels))
	for i := range expels {
		expelfacts[i] = expels[i].Fact().Hash()
	}

	st.args.SuffrageVotingFindFunc = func(context.Context, base.Height, base.Suffrage) (
		[]base.SuffrageExpelOperation, error,
	) {
		return expels, nil
	}

	sctx, _ := newConsensusSwitchContext(StateJoining, ivp)

	deferred, err := st.enter(StateJoining, sctx)
	t.NoError(err)
	deferred()

	select {
	case <-time.After(time.Second * 2):
		t.Fail("wait accept ballot, but failed")

		return
	case bl := <-acceptballotch:
		t.NotNil(bl)

		t.Equal(base.NewStagePoint(point, base.StageACCEPT), bl.Point())

		t.T().Log("new accept stuck voteproof")

		fact := bl.SignFact().Fact().(isaac.ACCEPTBallotFact)
		origavp, err := t.NewACCEPTVoteproof(fact, t.Local, nodes[:2])
		t.NoError(err)

		sfs := util.FilterSlice(origavp.SignFacts(), func(i base.BallotSignFact) bool {
			return !i.Node().Equal(expelnode.Address())
		})

		stuckavp := isaac.NewACCEPTStuckVoteproof(origavp.Point().Point)
		stuckavp.
			SetMajority(origavp.Majority()).
			SetSignFacts(sfs)
		stuckavp.SetExpels(expels)
		stuckavp.Finish()

		t.NoError(stuckavp.IsValid(t.LocalParams.NetworkID()))

		t.T().Log("process new accept stuck voteproof")

		t.NoError(st.newVoteproof(stuckavp))
	}

	select {
	case <-time.After(time.Second * 2):
		t.Fail("wait next round ballot, but failed")

		return
	case bl := <-nextroundballotch:
		t.NotNil(bl)

		t.Equal(point.NextRound(), bl.Point().Point)

		wbl, ok := bl.(base.HasExpels)
		t.True(ok)
		t.NotEmpty(wbl.Expels())
		t.Equal(1, len(wbl.Expels()))

		base.EqualOperation(t.Assert(), expels[0], wbl.Expels()[0])
	}
}

func (t *testExpelsConsensusHandler) TestACCEPTStuckVoteproofEnterSyncing() {
	point := base.RawPoint(33, 44)
	suf, nodes := isaac.NewTestSuffrage(2, t.Local)
	expelnode := nodes[2]

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

	expels := t.Expels(point.Height(), []base.Address{expelnode.Address()}, nodes[:2])
	expelfacts := make([]util.Hash, len(expels))
	for i := range expels {
		expelfacts[i] = expels[i].Fact().Hash()
	}

	st.args.SuffrageVotingFindFunc = func(context.Context, base.Height, base.Suffrage) (
		[]base.SuffrageExpelOperation, error,
	) {
		return expels, nil
	}

	sctx, _ := newConsensusSwitchContext(StateJoining, ivp)

	deferred, err := st.enter(StateJoining, sctx)
	t.NoError(err)
	deferred()

	select {
	case <-time.After(time.Second * 2):
		t.Fail("wait accept ballot, but failed")

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
			return !i.Node().Equal(expelnode.Address())
		})

		stuckavp := isaac.NewACCEPTStuckVoteproof(origavp.Point().Point)
		stuckavp.
			SetMajority(origavp.Majority()).
			SetSignFacts(sfs)
		stuckavp.SetExpels(expels)
		stuckavp.Finish()

		t.NoError(stuckavp.IsValid(t.LocalParams.NetworkID()))

		t.T().Log("process new higher accept stuck voteproof")

		err = st.newVoteproof(stuckavp)
		t.Error(err)

		t.T().Log("switching to syncing state")
		var ssctx SyncingSwitchContext
		t.ErrorAs(err, &ssctx)
		t.Equal(nextpoint.Height()-1, ssctx.height)
	}
}

func TestExpelsConsensusHandler(t *testing.T) {
	suite.Run(t, new(testExpelsConsensusHandler))
}
