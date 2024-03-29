package isaacstates

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/network/quicstream"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/stretchr/testify/suite"
)

type testSyncingHandler struct {
	isaac.BaseTestBallots
	waitlock *util.Locked[time.Duration]
}

func (t *testSyncingHandler) SetupTest() {
	t.BaseTestBallots.SetupTest()

	t.waitlock = util.NewLocked[time.Duration](time.Second * 10)
}

func (t *testSyncingHandler) newState(finishch chan base.Height) (*SyncingHandler, func(bool)) {
	local := t.Local
	params := t.LocalParams

	args := NewSyncingHandlerArgs()

	waitlock := t.waitlock
	args.WaitStuckInterval = func() time.Duration {
		i, _ := waitlock.Value()

		return i
	}

	args.NewSyncerFunc = func(height base.Height) (isaac.Syncer, error) {
		syncer := newDummySyncer(height, finishch, nil)

		return syncer, nil
	}
	args.JoinMemberlistFunc = func(context.Context, base.Suffrage) error { return nil }
	args.LeaveMemberlistFunc = func() error { return nil }
	args.WhenNewBlockSavedFunc = func(base.Height) {}

	newhandler := NewNewSyncingHandlerType(params.NetworkID(), local, args)

	_ = newhandler.SetLogging(logging.TestNilLogging)
	timers, err := util.NewSimpleTimers(2, time.Millisecond*33)
	t.NoError(err)

	newhandler.switchStateFunc = func(switchContext) error {
		return nil
	}

	i, err := newhandler.new()
	t.NoError(err)

	st := i.(*SyncingHandler)

	st.bbt = newBallotBroadcastTimers(timers, func(context.Context, base.Ballot) error {
		return nil
	}, time.Second)

	return st, func(ignoreerror bool) {
		deferred, err := st.exit(nil)
		if !ignoreerror {
			t.NoError(err)
		}

		if deferred != nil {
			deferred()
		}

		_ = st.bbt.Stop()
	}
}

func (t *testSyncingHandler) TestNew() {
	st, closef := t.newState(nil)
	defer closef(false)

	_ = (interface{})(st).(handler)

	deferred, err := st.enter(StateJoining, newSyncingSwitchContext(StateJoining, base.Height(33)))
	t.NoError(err)
	deferred()

	t.NotNil(st.syncer)
	t.Equal(base.Height(33), st.syncer.(*dummySyncer).Top())

	t.NoError(st.syncer.(*dummySyncer).Cancel())
}

func (t *testSyncingHandler) TestExit() {
	t.Run("exit", func() {
		st, closef := t.newState(nil)
		defer closef(false)

		deferredenter, err := st.enter(StateJoining, newSyncingSwitchContext(StateJoining, base.Height(33)))
		t.NoError(err)
		deferredenter()

		t.NoError(st.syncer.(*dummySyncer).Cancel())

		deferredexit, err := st.exit(nil)
		t.NoError(err)
		deferredexit()
	})

	t.Run("error", func() {
		st, _ := t.newState(nil)

		point := base.RawPoint(33, 0)
		deferredenter, err := st.enter(StateJoining, newSyncingSwitchContext(StateJoining, point.Height()))
		t.NoError(err)
		deferredenter()

		syncer := st.syncer.(*dummySyncer)
		syncer.cancelf = func() error {
			return errors.Errorf("hehehe")
		}

		syncer.finish(point.Height())

		deferredexit, err := st.exit(nil)
		t.Nil(deferredexit)
		t.Error(err)
		t.ErrorContains(err, "hehehe")
	})

	t.Run("leave memberlist; not allowed consensus", func() {
		st, closef := t.newState(nil)
		defer closef(false)

		leftch := make(chan struct{}, 1)
		st.args.LeaveMemberlistFunc = func() error {
			leftch <- struct{}{}

			return nil
		}

		st.setAllowConsensus(false)
		deferredenter, err := st.enter(StateJoining, newSyncingSwitchContext(StateJoining, base.Height(33)))
		t.NoError(err)
		deferredenter()

		t.NoError(st.syncer.(*dummySyncer).Cancel())

		deferredexit, err := st.exit(nil)
		t.NoError(err)
		deferredexit()

		select {
		case <-time.After(time.Second * 1):
			t.Fail("failed to wait to leave memberlist")
		case <-leftch:
		}
	})
}

func (t *testSyncingHandler) TestNewHigherVoteproof() {
	t.Run("higher init voteproof", func() {
		st, _ := t.newState(nil)

		local := t.Local
		st.args.NodeInConsensusNodesFunc = func(_ base.Node, h base.Height) (base.Suffrage, bool, error) {
			suf, _ := isaac.NewSuffrage([]base.Node{local})
			return suf, true, nil
		}

		point := base.RawPoint(33, 2)
		deferred, err := st.enter(StateJoining, newSyncingSwitchContext(StateJoining, point.Height()))
		t.NoError(err)
		deferred()

		syncer := st.syncer.(*dummySyncer)

		ifact := t.NewINITBallotFact(point.NextHeight().NextHeight(), nil, nil)
		ivp, err := t.NewINITVoteproof(ifact, t.Local, []base.LocalNode{t.Local})
		t.NoError(err)

		t.Equal(point.Height(), syncer.Top())

		t.NoError(st.newVoteproof(ivp))
		t.Equal(ivp.Point().Height()-1, syncer.Top())
	})

	t.Run("higher accept voteproof", func() {
		st, _ := t.newState(nil)

		local := t.Local
		st.args.NodeInConsensusNodesFunc = func(_ base.Node, h base.Height) (base.Suffrage, bool, error) {
			suf, _ := isaac.NewSuffrage([]base.Node{local})
			return suf, true, nil
		}

		point := base.RawPoint(33, 2)
		deferred, err := st.enter(StateJoining, newSyncingSwitchContext(StateJoining, point.Height()))
		t.NoError(err)
		deferred()

		syncer := st.syncer.(*dummySyncer)

		afact := t.NewACCEPTBallotFact(point.NextHeight(), nil, nil)
		avp, err := t.NewACCEPTVoteproof(afact, t.Local, []base.LocalNode{t.Local})
		t.NoError(err)

		t.Equal(point.Height(), syncer.Top())

		t.NoError(st.newVoteproof(avp))
		t.Equal(avp.Point().Height(), syncer.Top())
	})
}

func (t *testSyncingHandler) TestNewLowerVoteproof() {
	t.Run("lower init voteproof", func() {
		st, _ := t.newState(nil)

		local := t.Local
		st.args.NodeInConsensusNodesFunc = func(_ base.Node, h base.Height) (base.Suffrage, bool, error) {
			suf, _ := isaac.NewSuffrage([]base.Node{local})
			return suf, true, nil
		}

		point := base.RawPoint(33, 2)
		deferred, err := st.enter(StateJoining, newSyncingSwitchContext(StateJoining, point.Height()))
		t.NoError(err)
		deferred()

		syncer := st.syncer.(*dummySyncer)

		ifact := t.NewINITBallotFact(point, nil, nil)
		ivp, err := t.NewINITVoteproof(ifact, t.Local, []base.LocalNode{t.Local})
		t.NoError(err)

		t.Equal(point.Height(), syncer.Top())

		t.NoError(st.newVoteproof(ivp))
		t.Equal(point.Height(), syncer.Top())
	})

	t.Run("lower accept voteproof", func() {
		st, _ := t.newState(nil)

		local := t.Local
		st.args.NodeInConsensusNodesFunc = func(_ base.Node, h base.Height) (base.Suffrage, bool, error) {
			suf, _ := isaac.NewSuffrage([]base.Node{local})
			return suf, true, nil
		}

		point := base.RawPoint(33, 2)
		deferred, err := st.enter(StateJoining, newSyncingSwitchContext(StateJoining, point.Height()))
		t.NoError(err)
		deferred()

		syncer := st.syncer.(*dummySyncer)

		afact := t.NewACCEPTBallotFact(point, nil, nil)
		avp, err := t.NewACCEPTVoteproof(afact, t.Local, []base.LocalNode{t.Local})
		t.NoError(err)

		t.Equal(point.Height(), syncer.Top())

		t.NoError(st.newVoteproof(avp))
		t.Equal(point.Height(), syncer.Top())
	})
}

func (t *testSyncingHandler) TestNewExpectedVoteproof() {
	t.Run("not yet finished", func() {
		st, _ := t.newState(nil)

		local := t.Local
		st.args.NodeInConsensusNodesFunc = func(_ base.Node, h base.Height) (base.Suffrage, bool, error) {
			suf, _ := isaac.NewSuffrage([]base.Node{local})
			return suf, true, nil
		}

		point := base.RawPoint(33, 2)
		deferred, err := st.enter(StateJoining, newSyncingSwitchContext(StateJoining, point.Height()))
		t.NoError(err)
		deferred()

		syncer := st.syncer.(*dummySyncer)

		ifact := t.NewINITBallotFact(point.NextHeight().NextHeight(), nil, nil)
		ivp, err := t.NewINITVoteproof(ifact, t.Local, []base.LocalNode{t.Local})
		t.NoError(err)

		t.NoError(st.newVoteproof(ivp))
		t.Equal(ivp.Point().Height()-1, syncer.Top())
	})

	t.Run("finished && init voteproof", func() {
		st, closef := t.newState(nil)
		defer closef(false)

		local := t.Local
		st.args.NodeInConsensusNodesFunc = func(_ base.Node, h base.Height) (base.Suffrage, bool, error) {
			suf, _ := isaac.NewSuffrage([]base.Node{local})
			return suf, true, nil
		}

		point := base.RawPoint(33, 2)
		deferred, err := st.enter(StateJoining, newSyncingSwitchContext(StateJoining, point.Height()))
		t.NoError(err)
		deferred()

		syncer := st.syncer.(*dummySyncer)

		syncer.finish(point.Height())

		ifact := t.NewINITBallotFact(point.NextHeight(), nil, nil)
		ivp, err := t.NewINITVoteproof(ifact, t.Local, []base.LocalNode{t.Local})
		t.NoError(err)

		err = st.newVoteproof(ivp)

		var csctx consensusSwitchContext
		t.ErrorAs(err, &csctx)
		base.EqualVoteproof(t.Assert(), ivp, csctx.vp)
	})

	t.Run("finished && draw accept voteproof", func() {
		st, closef := t.newState(nil)
		defer closef(false)

		local := t.Local
		st.args.NodeInConsensusNodesFunc = func(_ base.Node, h base.Height) (base.Suffrage, bool, error) {
			suf, _ := isaac.NewSuffrage([]base.Node{local})
			return suf, true, nil
		}

		point := base.RawPoint(33, 2)
		deferred, err := st.enter(StateJoining, newSyncingSwitchContext(StateJoining, point.Height()))
		t.NoError(err)
		deferred()

		syncer := st.syncer.(*dummySyncer)

		syncer.finish(point.Height())

		afact := t.NewACCEPTBallotFact(point.NextHeight(), nil, nil)
		avp, err := t.NewACCEPTVoteproof(afact, t.Local, []base.LocalNode{t.Local})
		t.NoError(err)

		avp.SetMajority(nil).Finish()

		err = st.newVoteproof(avp)

		var csctx consensusSwitchContext
		t.ErrorAs(err, &csctx)
		base.EqualVoteproof(t.Assert(), avp, csctx.vp)
	})
}

func (t *testSyncingHandler) TestFinishedWithLastVoteproof() {
	t.Run("finished, but last init voteproof is old", func() {
		st, closef := t.newState(nil)
		defer closef(false)

		local := t.Local
		st.args.NodeInConsensusNodesFunc = func(_ base.Node, h base.Height) (base.Suffrage, bool, error) {
			suf, _ := isaac.NewSuffrage([]base.Node{local})
			return suf, true, nil
		}

		sctxch := make(chan switchContext, 1)
		st.switchStateFunc = func(sctx switchContext) error {
			sctxch <- sctx

			return nil
		}

		point := base.RawPoint(33, 2)
		deferred, err := st.enter(StateJoining, newSyncingSwitchContext(StateJoining, point.Height()))
		t.NoError(err)
		deferred()

		syncer := st.syncer.(*dummySyncer)

		ifact := t.NewINITBallotFact(point, nil, nil)
		ivp, err := t.NewINITVoteproof(ifact, t.Local, []base.LocalNode{t.Local})
		t.NoError(err)

		st.setLastVoteproof(ivp)

		syncer.finish(point.Height())

		select {
		case <-time.After(time.Second * 1):
		case sctx := <-sctxch:
			t.Fail(fmt.Sprintf("unexpected switch state, next=%q err=%q", sctx.next(), sctx.Error()))
		}

		t.Equal(point.Height(), syncer.Top())
	})

	t.Run("finished, but last init voteproof is higher", func() {
		st, _ := t.newState(nil)

		local := t.Local
		st.args.NodeInConsensusNodesFunc = func(_ base.Node, h base.Height) (base.Suffrage, bool, error) {
			suf, _ := isaac.NewSuffrage([]base.Node{local})
			return suf, true, nil
		}

		sctxch := make(chan switchContext, 1)
		st.switchStateFunc = func(sctx switchContext) error {
			sctxch <- sctx

			return nil
		}

		point := base.RawPoint(33, 2)
		deferred, err := st.enter(StateJoining, newSyncingSwitchContext(StateJoining, point.Height()))
		t.NoError(err)
		deferred()

		syncer := st.syncer.(*dummySyncer)

		ifact := t.NewINITBallotFact(point.NextHeight().NextHeight(), nil, nil)
		ivp, err := t.NewINITVoteproof(ifact, t.Local, []base.LocalNode{t.Local})
		t.NoError(err)

		st.setLastVoteproof(ivp)

		syncer.finish(point.Height())

		select {
		case <-time.After(time.Second * 1):
		case <-sctxch:
			t.Fail("unexpected switch state")
		}

		t.Equal(ivp.Point().Height()-1, syncer.Top())
	})

	t.Run("finished, but last accept voteproof is old", func() {
		st, closef := t.newState(nil)
		defer closef(false)

		local := t.Local
		st.args.NodeInConsensusNodesFunc = func(_ base.Node, h base.Height) (base.Suffrage, bool, error) {
			suf, _ := isaac.NewSuffrage([]base.Node{local})
			return suf, true, nil
		}

		sctxch := make(chan switchContext, 1)
		st.switchStateFunc = func(sctx switchContext) error {
			sctxch <- sctx

			return nil
		}

		point := base.RawPoint(33, 2)
		deferred, err := st.enter(StateJoining, newSyncingSwitchContext(StateJoining, point.Height()))
		t.NoError(err)
		deferred()

		syncer := st.syncer.(*dummySyncer)

		afact := t.NewACCEPTBallotFact(point, nil, nil)
		avp, err := t.NewACCEPTVoteproof(afact, t.Local, []base.LocalNode{t.Local})
		t.NoError(err)
		st.setLastVoteproof(avp)

		syncer.finish(point.Height())

		select {
		case <-time.After(time.Second * 1):
		case <-sctxch:
			t.Fail("unexpected switch state")
		}
	})

	t.Run("finished, but last accept voteproof higher", func() {
		st, _ := t.newState(nil)

		local := t.Local
		st.args.NodeInConsensusNodesFunc = func(_ base.Node, h base.Height) (base.Suffrage, bool, error) {
			suf, _ := isaac.NewSuffrage([]base.Node{local})
			return suf, true, nil
		}

		sctxch := make(chan switchContext, 1)
		st.switchStateFunc = func(sctx switchContext) error {
			sctxch <- sctx

			return nil
		}

		point := base.RawPoint(33, 2)
		deferred, err := st.enter(StateJoining, newSyncingSwitchContext(StateJoining, point.Height()))
		t.NoError(err)
		deferred()

		syncer := st.syncer.(*dummySyncer)

		afact := t.NewACCEPTBallotFact(point.NextHeight(), nil, nil)
		avp, err := t.NewACCEPTVoteproof(afact, t.Local, []base.LocalNode{t.Local})
		t.NoError(err)
		st.setLastVoteproof(avp)

		syncer.finish(point.Height())

		select {
		case <-time.After(time.Second * 1):
		case <-sctxch:
			t.Fail("unexpected switch state")
		}

		t.Equal(avp.Point().Height(), syncer.Top())
	})

	t.Run("finished and expected last init voteproof", func() {
		st, closef := t.newState(nil)
		defer closef(false)

		local := t.Local
		st.args.NodeInConsensusNodesFunc = func(_ base.Node, h base.Height) (base.Suffrage, bool, error) {
			suf, _ := isaac.NewSuffrage([]base.Node{local})
			return suf, true, nil
		}

		finishedheightch := make(chan base.Height)
		st.args.WhenNewBlockSavedFunc = func(height base.Height) {
			finishedheightch <- height
		}

		sctxch := make(chan switchContext, 1)
		st.switchStateFunc = func(sctx switchContext) error {
			sctxch <- sctx

			return nil
		}

		point := base.RawPoint(33, 2)
		deferred, err := st.enter(StateJoining, newSyncingSwitchContext(StateJoining, point.Height()))
		t.NoError(err)
		deferred()

		syncer := st.syncer.(*dummySyncer)

		ifact := t.NewINITBallotFact(point.NextHeight(), nil, nil)
		ivp, err := t.NewINITVoteproof(ifact, t.Local, []base.LocalNode{t.Local})
		t.NoError(err)

		st.setLastVoteproof(ivp)

		syncer.finish(point.Height())

		select {
		case <-time.After(time.Second * 1):
			t.Fail("timeout to wait finished height")
		case height := <-finishedheightch:
			t.Equal(point.Height(), height)
		}

		select {
		case <-time.After(time.Second * 1):
			t.Fail("timeout to switch consensus state")
		case sctx := <-sctxch:
			var csctx consensusSwitchContext
			t.ErrorAs(sctx, &csctx)
			base.EqualVoteproof(t.Assert(), ivp, csctx.vp)
		}
	})
}

func (t *testSyncingHandler) TestFinishedButStuck() {
	t.Run("finished and expected last accept voteproof", func() {
		st, closef := t.newState(nil)
		defer closef(false)

		local := t.Local
		st.args.NodeInConsensusNodesFunc = func(_ base.Node, h base.Height) (base.Suffrage, bool, error) {
			suf, _ := isaac.NewSuffrage([]base.Node{local})
			return suf, true, nil
		}

		sctxch := make(chan switchContext, 1)
		st.switchStateFunc = func(sctx switchContext) error {
			sctxch <- sctx

			return nil
		}

		point := base.RawPoint(33, 2)
		deferred, err := st.enter(StateJoining, newSyncingSwitchContext(StateJoining, point.Height()))
		t.NoError(err)
		deferred()

		syncer := st.syncer.(*dummySyncer)

		afact := t.NewACCEPTBallotFact(point, nil, nil)
		avp, err := t.NewACCEPTVoteproof(afact, t.Local, []base.LocalNode{t.Local})
		t.NoError(err)

		st.setLastVoteproof(avp)

		t.waitlock.SetValue(time.Millisecond * 100)

		syncer.finish(point.Height())

		select {
		case <-time.After(time.Second * 1):
			t.Fail("timeout to switch joining state")
		case sctx := <-sctxch:
			var jsctx joiningSwitchContext
			t.ErrorAs(sctx, &jsctx)
			base.EqualVoteproof(t.Assert(), avp, jsctx.vp)
		}
	})

	t.Run("finished and expected last accept voteproof, but not in suffrage", func() {
		st, closef := t.newState(nil)
		defer closef(false)

		st.args.NodeInConsensusNodesFunc = func(_ base.Node, h base.Height) (base.Suffrage, bool, error) {
			suf, _ := isaac.NewSuffrage([]base.Node{base.RandomNode()})

			return suf, false, nil
		}

		sctxch := make(chan switchContext, 1)
		st.switchStateFunc = func(sctx switchContext) error {
			sctxch <- sctx

			return nil
		}

		point := base.RawPoint(33, 2)
		deferred, err := st.enter(StateJoining, newSyncingSwitchContext(StateJoining, point.Height()))
		t.NoError(err)
		deferred()

		syncer := st.syncer.(*dummySyncer)

		afact := t.NewACCEPTBallotFact(point, nil, nil)
		avp, err := t.NewACCEPTVoteproof(afact, t.Local, []base.LocalNode{t.Local})
		t.NoError(err)

		st.setLastVoteproof(avp)

		t.waitlock.SetValue(time.Millisecond * 100)

		syncer.finish(point.Height())

		select {
		case <-time.After(time.Second * 1):
		case sctx := <-sctxch:
			t.Fail(fmt.Sprintf("unexpected to switch state, %v", sctx.next()))
		}
	})

	t.Run("finished and expected last accept voteproof, but add new height", func() {
		st, _ := t.newState(nil)

		local := t.Local
		st.args.NodeInConsensusNodesFunc = func(_ base.Node, h base.Height) (base.Suffrage, bool, error) {
			suf, _ := isaac.NewSuffrage([]base.Node{local})
			return suf, true, nil
		}

		sctxch := make(chan switchContext, 1)
		st.switchStateFunc = func(sctx switchContext) error {
			sctxch <- sctx

			return nil
		}

		point := base.RawPoint(33, 2)
		deferred, err := st.enter(StateJoining, newSyncingSwitchContext(StateJoining, point.Height()))
		t.NoError(err)
		deferred()

		syncer := st.syncer.(*dummySyncer)

		afact := t.NewACCEPTBallotFact(point, nil, nil)
		avp, err := t.NewACCEPTVoteproof(afact, t.Local, []base.LocalNode{t.Local})
		t.NoError(err)

		st.setLastVoteproof(avp)

		t.waitlock.SetValue(time.Second)

		syncer.finish(point.Height())
		syncer.Add(point.NextHeight().Height())

		select {
		case <-time.After(time.Second * 2):
		case sctx := <-sctxch:
			t.Fail(fmt.Sprintf("unexpected; switched another state: -> %s, %+v", sctx.next(), sctx.Error()))
		}
	})

	t.Run("finished and expected last accept voteproof, with new voteproof", func() {
		st, _ := t.newState(nil)

		local := t.Local
		st.args.NodeInConsensusNodesFunc = func(_ base.Node, h base.Height) (base.Suffrage, bool, error) {
			suf, _ := isaac.NewSuffrage([]base.Node{local})
			return suf, true, nil
		}

		sctxch := make(chan switchContext, 1)
		st.switchStateFunc = func(sctx switchContext) error {
			sctxch <- sctx

			return nil
		}

		point := base.RawPoint(33, 2)
		deferred, err := st.enter(StateJoining, newSyncingSwitchContext(StateJoining, point.Height()))
		t.NoError(err)
		deferred()

		syncer := st.syncer.(*dummySyncer)

		afact := t.NewACCEPTBallotFact(point, nil, nil)
		avp, err := t.NewACCEPTVoteproof(afact, t.Local, []base.LocalNode{t.Local})
		t.NoError(err)

		st.setLastVoteproof(avp)

		t.waitlock.SetValue(time.Second)

		syncer.finish(point.Height())
		syncer.Add(point.NextHeight().Height())

		newavp, err := t.NewACCEPTVoteproof(t.NewACCEPTBallotFact(point.NextHeight(), nil, nil), t.Local, []base.LocalNode{t.Local})
		t.NoError(err)

		t.waitlock.SetValue(time.Millisecond * 100)

		st.setLastVoteproof(newavp)
		syncer.finish(newavp.Point().Height())

		select {
		case <-time.After(time.Second * 2):
			t.Fail("timeout to switch joining state")
		case sctx := <-sctxch:
			var jsctx joiningSwitchContext
			t.ErrorAs(sctx, &jsctx)
			base.EqualVoteproof(t.Assert(), newavp, jsctx.vp)
		}
	})
}

func (t *testSyncingHandler) TestSyncerErr() {
	st, closef := t.newState(nil)
	defer closef(false)

	sctxch := make(chan switchContext, 1)
	st.switchStateFunc = func(sctx switchContext) error {
		sctxch <- sctx

		return nil
	}

	point := base.RawPoint(33, 2)
	deferred, err := st.enter(StateJoining, newSyncingSwitchContext(StateJoining, point.Height()))
	t.NoError(err)
	deferred()

	syncer := st.syncer.(*dummySyncer)
	syncer.done(errors.Errorf("kekeke"))

	select {
	case <-time.After(time.Second * 2):
		t.Fail("timeout to switch joining state")
	case sctx := <-sctxch:
		var bsctx baseErrorSwitchContext
		t.ErrorAs(sctx, &bsctx)
		t.ErrorContains(bsctx.err, "kekeke")
	}
}

func (t *testSyncingHandler) TestStuckWithoutVoteproof() {
	t.Run("empty last voteproof", func() {
		st, closef := t.newState(nil)
		defer closef(true)
		t.waitlock.SetValue(time.Millisecond * 100)

		sctxch := make(chan switchContext, 1)
		st.switchStateFunc = func(sctx switchContext) error {
			sctxch <- sctx

			return nil
		}

		deferred, err := st.enter(StateJoining, newSyncingSwitchContext(StateJoining, base.Height(33)))
		t.NoError(err)
		deferred()

		select {
		case <-time.After(time.Second * 2):
		case <-sctxch:
			t.Fail("without last voteproof, keep waiting")
		}
	})

	t.Run("last voteproof", func() {
		st, closef := t.newState(nil)
		defer closef(false)
		t.waitlock.SetValue(time.Millisecond * 100)

		sctxch := make(chan switchContext, 1)
		st.switchStateFunc = func(sctx switchContext) error {
			sctxch <- sctx

			return nil
		}

		point := base.RawPoint(33, 2)

		ifact := t.NewINITBallotFact(point, nil, nil)
		ivp, err := t.NewINITVoteproof(ifact, t.Local, []base.LocalNode{t.Local})
		t.NoError(err)

		st.setLastVoteproof(ivp)

		deferred, err := st.enter(StateJoining, newSyncingSwitchContext(StateJoining, point.Height()))
		t.NoError(err)
		deferred()

		select {
		case <-time.After(time.Second * 2):
			t.Fail("failed to switch joining state")
		case sctx := <-sctxch:
			var jsctx joiningSwitchContext
			t.ErrorAs(sctx, &jsctx)
			base.EqualVoteproof(t.Assert(), ivp, jsctx.vp)
		}
	})

	t.Run("last voteproof, but not allow consensus", func() {
		st, closef := t.newState(nil)
		defer closef(false)
		t.waitlock.SetValue(time.Millisecond * 100)

		st.setAllowConsensus(false)

		sctxch := make(chan switchContext, 1)
		st.switchStateFunc = func(sctx switchContext) error {
			sctxch <- sctx

			return nil
		}

		point := base.RawPoint(33, 2)

		ifact := t.NewINITBallotFact(point, nil, nil)
		ivp, err := t.NewINITVoteproof(ifact, t.Local, []base.LocalNode{t.Local})
		t.NoError(err)

		st.setLastVoteproof(ivp)

		deferred, err := st.enter(StateJoining, newSyncingSwitchContext(StateJoining, point.Height()))
		t.NoError(err)
		deferred()

		select {
		case <-time.After(time.Second * 2):
		case <-sctxch:
			t.Fail("not allowed consensus, keep waiting")
		}
	})
}

func (t *testSyncingHandler) TestAskHandover() {
	t.Run("finished && allowed consensus", func() {
		st, closef := t.newState(nil)
		defer closef(false)

		local := t.Local
		st.args.NodeInConsensusNodesFunc = func(_ base.Node, h base.Height) (base.Suffrage, bool, error) {
			suf, _ := isaac.NewSuffrage([]base.Node{local})
			return suf, true, nil
		}

		askedch := make(chan struct{}, 1)
		st.askHandoverFunc = func() (bool, error) {
			askedch <- struct{}{}

			return true, nil
		}

		point := base.RawPoint(33, 2)
		deferred, err := st.enter(StateJoining, newSyncingSwitchContext(StateJoining, point.Height()))
		t.NoError(err)
		deferred()

		syncer := st.syncer.(*dummySyncer)

		syncer.finish(point.Height())

		ifact := t.NewINITBallotFact(point.NextHeight(), nil, nil)
		ivp, err := t.NewINITVoteproof(ifact, t.Local, []base.LocalNode{t.Local})
		t.NoError(err)

		err = st.newVoteproof(ivp)

		var csctx consensusSwitchContext
		t.ErrorAs(err, &csctx)
		base.EqualVoteproof(t.Assert(), ivp, csctx.vp)

		select {
		case <-time.After(time.Second * 1):
		case <-askedch:
			t.Fail("unexpected asking")
		}
	})

	t.Run("finished && not allowed consensus", func() {
		st, closef := t.newState(nil)
		defer closef(false)

		local := t.Local
		st.args.NodeInConsensusNodesFunc = func(_ base.Node, h base.Height) (base.Suffrage, bool, error) {
			suf, _ := isaac.NewSuffrage([]base.Node{local})
			return suf, true, nil
		}

		askedch := make(chan struct{}, 1)
		st.askHandoverFunc = func() (bool, error) {
			askedch <- struct{}{}

			return false, nil
		}

		st.setAllowConsensus(false)

		point := base.RawPoint(33, 2)
		deferred, err := st.enter(StateJoining, newSyncingSwitchContext(StateJoining, point.Height()))
		t.NoError(err)
		deferred()

		syncer := st.syncer.(*dummySyncer)

		syncer.finish(point.Height())

		ifact := t.NewINITBallotFact(point.NextHeight(), nil, nil)
		ivp, err := t.NewINITVoteproof(ifact, t.Local, []base.LocalNode{t.Local})
		t.NoError(err)

		t.NoError(st.newVoteproof(ivp))

		select {
		case <-time.After(time.Second * 1):
			t.Fail("failed to ask")
		case <-askedch:
		}
	})
}

func (t *testSyncingHandler) TestMovesToHandover() {
	t.Run("finished && init voteproof; not asked", func() {
		st, closef := t.newState(nil)
		defer closef(false)

		st.setAllowConsensus(false)

		local := t.Local
		st.args.NodeInConsensusNodesFunc = func(_ base.Node, h base.Height) (base.Suffrage, bool, error) {
			suf, _ := isaac.NewSuffrage([]base.Node{local})
			return suf, true, nil
		}

		args := NewHandoverYBrokerArgs(t.LocalParams.NetworkID())
		args.SyncDataFunc = func(context.Context, quicstream.ConnInfo, chan<- struct{}) error {
			// NOTE not ready
			return nil
		}
		args.AskRequestFunc = func(context.Context, quicstream.ConnInfo) (string, bool, error) {
			return "", false, nil
		}

		broker := NewHandoverYBroker(context.Background(), args, quicstream.ConnInfo{})
		st.handoverYBrokerFunc = func() *HandoverYBroker {
			return broker
		}

		point := base.RawPoint(33, 2)
		deferred, err := st.enter(StateJoining, newSyncingSwitchContext(StateJoining, point.Height()))
		t.NoError(err)
		deferred()

		syncer := st.syncer.(*dummySyncer)

		syncer.finish(point.Height())

		ifact := t.NewINITBallotFact(point.NextHeight(), nil, nil)
		ivp, err := t.NewINITVoteproof(ifact, t.Local, []base.LocalNode{t.Local})
		t.NoError(err)

		err = st.newVoteproof(ivp)
		t.NoError(err)
	})

	t.Run("finished && init voteproof; asked", func() {
		st, closef := t.newState(nil)
		defer closef(false)

		st.setAllowConsensus(false)

		local := t.Local
		st.args.NodeInConsensusNodesFunc = func(_ base.Node, h base.Height) (base.Suffrage, bool, error) {
			suf, _ := isaac.NewSuffrage([]base.Node{local})
			return suf, true, nil
		}

		args := NewHandoverYBrokerArgs(t.LocalParams.NetworkID())
		args.SyncDataFunc = func(_ context.Context, _ quicstream.ConnInfo, readych chan<- struct{}) error {
			readych <- struct{}{}

			return nil
		}

		handoverid := util.UUID().String()
		args.AskRequestFunc = func(context.Context, quicstream.ConnInfo) (string, bool, error) {
			return handoverid, false, nil
		}

		broker := NewHandoverYBroker(context.Background(), args, quicstream.ConnInfo{})
		broker.checkSyncedDataDone()

		st.handoverYBrokerFunc = func() *HandoverYBroker {
			return broker
		}

		point := base.RawPoint(33, 2)
		deferred, err := st.enter(StateJoining, newSyncingSwitchContext(StateJoining, point.Height()))
		t.NoError(err)
		deferred()

		syncer := st.syncer.(*dummySyncer)

		t.Run("finished and ask", func() {
			syncer.finish(point.Height())

			ifact := t.NewINITBallotFact(point.NextHeight(), nil, nil)
			ivp, err := t.NewINITVoteproof(ifact, t.Local, []base.LocalNode{t.Local})
			t.NoError(err)

			t.NoError(st.newVoteproof(ivp))
		})

		t.Run("finished and asked", func() {
			nextpoint := point.NextHeight()

			syncer.Add(nextpoint.Height())
			syncer.finish(nextpoint.Height())

			ifact := t.NewINITBallotFact(nextpoint.NextHeight(), nil, nil)
			ivp, err := t.NewINITVoteproof(ifact, t.Local, []base.LocalNode{t.Local})
			t.NoError(err)

			err = st.newVoteproof(ivp)

			var csctx consensusSwitchContext
			t.ErrorAs(err, &csctx)
			base.EqualVoteproof(t.Assert(), ivp, csctx.vp)
		})

		t.False(st.allowedConsensus())
	})

	t.Run("finished && init voteproof; asked and can move consensus", func() {
		st, closef := t.newState(nil)
		defer closef(false)

		st.setAllowConsensus(false)

		local := t.Local
		st.args.NodeInConsensusNodesFunc = func(_ base.Node, h base.Height) (base.Suffrage, bool, error) {
			suf, _ := isaac.NewSuffrage([]base.Node{local})
			return suf, true, nil
		}

		args := NewHandoverYBrokerArgs(t.LocalParams.NetworkID())
		args.SyncDataFunc = func(_ context.Context, _ quicstream.ConnInfo, readych chan<- struct{}) error {
			readych <- struct{}{}

			return nil
		}
		args.AskRequestFunc = func(context.Context, quicstream.ConnInfo) (string, bool, error) {
			return util.UUID().String(), true, nil // NOTE can move consensus
		}

		broker := NewHandoverYBroker(context.Background(), args, quicstream.ConnInfo{})
		st.handoverYBrokerFunc = func() *HandoverYBroker {
			broker.checkSyncedDataDone()

			return broker
		}

		point := base.RawPoint(33, 2)
		deferred, err := st.enter(StateJoining, newSyncingSwitchContext(StateJoining, point.Height()))
		t.NoError(err)
		deferred()

		syncer := st.syncer.(*dummySyncer)

		syncer.finish(point.Height())

		ifact := t.NewINITBallotFact(point.NextHeight(), nil, nil)
		ivp, err := t.NewINITVoteproof(ifact, t.Local, []base.LocalNode{t.Local})
		t.NoError(err)

		err = st.newVoteproof(ivp)

		var csctx consensusSwitchContext
		t.ErrorAs(err, &csctx)
		base.EqualVoteproof(t.Assert(), ivp, csctx.vp)

		t.True(st.allowedConsensus())
	})
}

func TestSyncingHandler(t *testing.T) {
	suite.Run(t, new(testSyncingHandler))
}

type dummySyncer struct {
	sync.RWMutex
	topHeight  base.Height
	doneHeight base.Height
	ch         chan base.Height
	donech     chan struct{}
	err        error
	canceled   bool
	cancelf    func() error
}

func newDummySyncer(prevheight base.Height, ch chan base.Height, donech chan struct{}) *dummySyncer {
	if ch == nil {
		ch = make(chan base.Height)
	}

	if donech == nil {
		donech = make(chan struct{})
	}

	return &dummySyncer{
		ch:         ch,
		donech:     donech,
		topHeight:  prevheight,
		doneHeight: prevheight,
	}
}

func (s *dummySyncer) Start(context.Context) error { return nil }
func (s *dummySyncer) Stop() error                 { return nil }

func (s *dummySyncer) Top() base.Height {
	s.RLock()
	defer s.RUnlock()

	return s.topHeight
}

func (s *dummySyncer) Add(h base.Height) bool {
	s.Lock()
	defer s.Unlock()

	if s.canceled {
		return false
	}

	if h <= s.topHeight {
		return false
	}

	s.topHeight = h

	return true
}

func (s *dummySyncer) finish(h base.Height) {
	s.Lock()
	defer s.Unlock()

	if h > s.topHeight {
		return
	}

	go func() {
		s.ch <- h
	}()

	s.doneHeight = h
}

func (s *dummySyncer) Finished() <-chan base.Height {
	return s.ch
}

func (s *dummySyncer) Done() <-chan struct{} {
	return s.donech
}

func (s *dummySyncer) Err() error {
	s.RLock()
	defer s.RUnlock()

	return s.err
}

func (s *dummySyncer) done(err error) {
	s.Lock()
	defer s.Unlock()

	s.err = err
	s.donech <- struct{}{}
}

func (s *dummySyncer) IsFinished() (base.Height, bool) {
	s.RLock()
	defer s.RUnlock()

	if s.err != nil {
		return s.topHeight, true
	}

	if s.canceled {
		return s.topHeight, true
	}

	return s.topHeight, s.topHeight == s.doneHeight
}

func (s *dummySyncer) Cancel() error {
	s.Lock()
	defer s.Unlock()

	if s.canceled {
		return nil
	}

	if s.cancelf != nil {
		if err := s.cancelf(); err != nil {
			return err
		}
	}

	s.canceled = true

	return nil
}
