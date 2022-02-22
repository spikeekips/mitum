package isaac

import (
	"sync"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/stretchr/testify/suite"
)

type testSyncingHandler struct {
	baseStateTestHandler
}

func (t *testSyncingHandler) newState(finishch chan base.Height) (*SyncingHandler, func()) {
	local := t.local
	policy := t.policy

	st := NewSyncingHandler(
		local,
		policy,
		nil,
		func(base.Height) base.Suffrage { return nil },
		func() syncer {
			return newDummySyncer(finishch)
		},
	)
	_ = st.SetLogging(logging.TestNilLogging)
	_ = st.setTimers(util.NewTimers([]util.TimerID{
		timerIDBroadcastINITBallot,
		timerIDBroadcastACCEPTBallot,
	}, false))

	st.broadcastBallotFunc = func(bl base.Ballot) error {
		return nil
	}
	st.switchStateFunc = func(stateSwitchContext) error {
		return nil
	}

	return st, func() {
		deferred, err := st.exit(nil)
		t.NoError(err)
		deferred()
	}
}

func (t *testSyncingHandler) TestNew() {
	st, closef := t.newState(nil)
	defer closef()

	_ = (interface{})(st).(stateHandler)

	deferred, err := st.enter(newSyncingSwitchContext(StateJoining, base.Height(33)))
	t.NoError(err)
	deferred()

	t.NotNil(st.syncer)
	t.Equal(base.Height(33), st.syncer.top())

	t.NoError(st.syncer.(*dummySyncer).cancel())
}

func (t *testSyncingHandler) TestExit() {
	t.Run("exit", func() {
		st, closef := t.newState(nil)
		defer closef()

		deferredenter, err := st.enter(newSyncingSwitchContext(StateJoining, base.Height(33)))
		t.NoError(err)
		deferredenter()

		t.NoError(st.syncer.(*dummySyncer).cancel())

		deferredexit, err := st.exit(nil)
		t.NoError(err)
		deferredexit()

		t.Nil(st.syncer)
	})

	t.Run("error", func() {
		st, _ := t.newState(nil)

		point := base.RawPoint(33, 0)
		deferredenter, err := st.enter(newSyncingSwitchContext(StateJoining, point.Height()))
		t.NoError(err)
		deferredenter()

		syncer := st.syncer.(*dummySyncer)
		syncer.cancelf = func() error {
			return errors.Errorf("hehehe")
		}

		syncer.done(point.Height())

		deferredexit, err := st.exit(nil)
		t.Nil(deferredexit)
		t.Error(err)
		t.Contains(err.Error(), "hehehe")
	})

	t.Run("can not cancel", func() {
		st, _ := t.newState(nil)

		deferredenter, err := st.enter(newSyncingSwitchContext(StateJoining, base.Height(33)))
		t.NoError(err)
		deferredenter()

		syncer := st.syncer.(*dummySyncer)
		syncer.cancelf = func() error {
			return syncerCanNotCancelError.Call()
		}

		deferredexit, err := st.exit(nil)
		t.Nil(deferredexit)
		t.Error(err)
		t.True(errors.Is(err, ignoreSwithingStateError))
	})
}

func (t *testSyncingHandler) TestNewHigherVoteproof() {
	t.Run("higher init voteproof", func() {
		st, _ := t.newState(nil)

		point := base.RawPoint(33, 2)
		deferred, err := st.enter(newSyncingSwitchContext(StateJoining, point.Height()))
		t.NoError(err)
		deferred()

		syncer := st.syncer.(*dummySyncer)

		ifact := t.newINITBallotFact(point.Next().Next(), nil, nil)
		ivp, err := t.newINITVoteproof(ifact, t.local, []*LocalNode{t.local})
		t.NoError(err)

		t.Equal(point.Height(), syncer.top())

		t.NoError(st.newVoteproof(ivp))
		t.Equal(ivp.Point().Height()-1, syncer.top())
	})

	t.Run("higher accept voteproof", func() {
		st, _ := t.newState(nil)

		point := base.RawPoint(33, 2)
		deferred, err := st.enter(newSyncingSwitchContext(StateJoining, point.Height()))
		t.NoError(err)
		deferred()

		syncer := st.syncer.(*dummySyncer)

		afact := t.newACCEPTBallotFact(point.Next(), nil, nil)
		avp, err := t.newACCEPTVoteproof(afact, t.local, []*LocalNode{t.local})
		t.NoError(err)

		t.Equal(point.Height(), syncer.top())

		t.NoError(st.newVoteproof(avp))
		t.Equal(avp.Point().Height(), syncer.top())
	})
}

func (t *testSyncingHandler) TestNewLowerVoteproof() {
	t.Run("lower init voteproof", func() {
		st, _ := t.newState(nil)

		point := base.RawPoint(33, 2)
		deferred, err := st.enter(newSyncingSwitchContext(StateJoining, point.Height()))
		t.NoError(err)
		deferred()

		syncer := st.syncer.(*dummySyncer)

		ifact := t.newINITBallotFact(point, nil, nil)
		ivp, err := t.newINITVoteproof(ifact, t.local, []*LocalNode{t.local})
		t.NoError(err)

		t.Equal(point.Height(), syncer.top())

		t.NoError(st.newVoteproof(ivp))
		t.Equal(point.Height(), syncer.top())
	})

	t.Run("lower accept voteproof", func() {
		st, _ := t.newState(nil)

		point := base.RawPoint(33, 2)
		deferred, err := st.enter(newSyncingSwitchContext(StateJoining, point.Height()))
		t.NoError(err)
		deferred()

		syncer := st.syncer.(*dummySyncer)

		afact := t.newACCEPTBallotFact(point, nil, nil)
		avp, err := t.newACCEPTVoteproof(afact, t.local, []*LocalNode{t.local})
		t.NoError(err)

		t.Equal(point.Height(), syncer.top())

		t.NoError(st.newVoteproof(avp))
		t.Equal(point.Height(), syncer.top())
	})
}

func (t *testSyncingHandler) TestNewExpectedINITVoteproof() {
	t.Run("not yet finished", func() {
		st, _ := t.newState(nil)

		point := base.RawPoint(33, 2)
		deferred, err := st.enter(newSyncingSwitchContext(StateJoining, point.Height()))
		t.NoError(err)
		deferred()

		syncer := st.syncer.(*dummySyncer)

		ifact := t.newINITBallotFact(point.Next(), nil, nil)
		ivp, err := t.newINITVoteproof(ifact, t.local, []*LocalNode{t.local})
		t.NoError(err)

		t.NoError(st.newVoteproof(ivp))
		t.Equal(point.Height(), syncer.top())
	})

	t.Run("finished", func() {
		st, closef := t.newState(nil)
		defer closef()

		point := base.RawPoint(33, 2)
		deferred, err := st.enter(newSyncingSwitchContext(StateJoining, point.Height()))
		t.NoError(err)
		deferred()

		syncer := st.syncer.(*dummySyncer)

		syncer.done(point.Height())

		ifact := t.newINITBallotFact(point.Next(), nil, nil)
		ivp, err := t.newINITVoteproof(ifact, t.local, []*LocalNode{t.local})
		t.NoError(err)

		err = st.newVoteproof(ivp)

		var csctx consensusSwitchContext
		t.True(errors.As(err, &csctx))
		base.CompareVoteproof(t.Assert(), ivp, csctx.ivp)
	})
}

func (t *testSyncingHandler) TestFinishedWithLastVoteproof() {
	t.Run("finished, but last init voteproof is old", func() {
		st, closef := t.newState(nil)
		defer closef()

		sctxch := make(chan stateSwitchContext, 1)
		st.switchStateFunc = func(sctx stateSwitchContext) error {
			sctxch <- sctx

			return nil
		}

		point := base.RawPoint(33, 2)
		deferred, err := st.enter(newSyncingSwitchContext(StateJoining, point.Height()))
		t.NoError(err)
		deferred()

		syncer := st.syncer.(*dummySyncer)

		ifact := t.newINITBallotFact(point, nil, nil)
		ivp, err := t.newINITVoteproof(ifact, t.local, []*LocalNode{t.local})
		t.NoError(err)

		st.setLastVoteproof(ivp)

		syncer.done(point.Height())

		select {
		case <-time.After(time.Second * 1):
		case <-sctxch:
			t.NoError(errors.Errorf("unexpected switch state"))
		}

		t.Equal(point.Height(), syncer.top())
	})

	t.Run("finished, but last init voteproof is higher", func() {
		st, _ := t.newState(nil)

		sctxch := make(chan stateSwitchContext, 1)
		st.switchStateFunc = func(sctx stateSwitchContext) error {
			sctxch <- sctx

			return nil
		}

		point := base.RawPoint(33, 2)
		deferred, err := st.enter(newSyncingSwitchContext(StateJoining, point.Height()))
		t.NoError(err)
		deferred()

		syncer := st.syncer.(*dummySyncer)

		ifact := t.newINITBallotFact(point.Next().Next(), nil, nil)
		ivp, err := t.newINITVoteproof(ifact, t.local, []*LocalNode{t.local})
		t.NoError(err)

		st.setLastVoteproof(ivp)

		syncer.done(point.Height())

		select {
		case <-time.After(time.Second * 1):
		case <-sctxch:
			t.NoError(errors.Errorf("unexpected switch state"))
		}

		t.Equal(ivp.Point().Height()-1, syncer.top())
	})

	t.Run("finished, but last accept voteproof is old", func() {
		st, closef := t.newState(nil)
		defer closef()

		sctxch := make(chan stateSwitchContext, 1)
		st.switchStateFunc = func(sctx stateSwitchContext) error {
			sctxch <- sctx

			return nil
		}

		point := base.RawPoint(33, 2)
		deferred, err := st.enter(newSyncingSwitchContext(StateJoining, point.Height()))
		t.NoError(err)
		deferred()

		syncer := st.syncer.(*dummySyncer)

		afact := t.newACCEPTBallotFact(point, nil, nil)
		avp, err := t.newACCEPTVoteproof(afact, t.local, []*LocalNode{t.local})
		t.NoError(err)
		st.setLastVoteproof(avp)

		syncer.done(point.Height())

		select {
		case <-time.After(time.Second * 1):
		case <-sctxch:
			t.NoError(errors.Errorf("unexpected switch state"))
		}
	})

	t.Run("finished, but last accept voteproof higher", func() {
		st, _ := t.newState(nil)

		sctxch := make(chan stateSwitchContext, 1)
		st.switchStateFunc = func(sctx stateSwitchContext) error {
			sctxch <- sctx

			return nil
		}

		point := base.RawPoint(33, 2)
		deferred, err := st.enter(newSyncingSwitchContext(StateJoining, point.Height()))
		t.NoError(err)
		deferred()

		syncer := st.syncer.(*dummySyncer)

		afact := t.newACCEPTBallotFact(point.Next(), nil, nil)
		avp, err := t.newACCEPTVoteproof(afact, t.local, []*LocalNode{t.local})
		t.NoError(err)
		st.setLastVoteproof(avp)

		syncer.done(point.Height())

		select {
		case <-time.After(time.Second * 1):
		case <-sctxch:
			t.NoError(errors.Errorf("unexpected switch state"))
		}

		t.Equal(avp.Point().Height(), syncer.top())
	})

	t.Run("finished and expected last init voteproof", func() {
		st, closef := t.newState(nil)
		defer closef()

		sctxch := make(chan stateSwitchContext, 1)
		st.switchStateFunc = func(sctx stateSwitchContext) error {
			sctxch <- sctx

			return nil
		}

		point := base.RawPoint(33, 2)
		deferred, err := st.enter(newSyncingSwitchContext(StateJoining, point.Height()))
		t.NoError(err)
		deferred()

		syncer := st.syncer.(*dummySyncer)

		ifact := t.newINITBallotFact(point.Next(), nil, nil)
		ivp, err := t.newINITVoteproof(ifact, t.local, []*LocalNode{t.local})
		t.NoError(err)

		st.setLastVoteproof(ivp)

		syncer.done(point.Height())

		select {
		case <-time.After(time.Second * 1):
			t.NoError(errors.Errorf("timeout to switch consensus state"))
		case sctx := <-sctxch:
			var csctx consensusSwitchContext
			t.True(errors.As(sctx, &csctx))
			base.CompareVoteproof(t.Assert(), ivp, csctx.ivp)
		}
	})
}

func (t *testSyncingHandler) TestFinishedButStuck() {
	t.Run("finished and expected last accept voteproof", func() {
		st, closef := t.newState(nil)
		defer closef()

		sctxch := make(chan stateSwitchContext, 1)
		st.switchStateFunc = func(sctx stateSwitchContext) error {
			sctxch <- sctx

			return nil
		}

		point := base.RawPoint(33, 2)
		deferred, err := st.enter(newSyncingSwitchContext(StateJoining, point.Height()))
		t.NoError(err)
		deferred()

		syncer := st.syncer.(*dummySyncer)

		afact := t.newACCEPTBallotFact(point, nil, nil)
		avp, err := t.newACCEPTVoteproof(afact, t.local, []*LocalNode{t.local})
		t.NoError(err)

		st.setLastVoteproof(avp)

		st.waitStuck = time.Millisecond * 100

		syncer.done(point.Height())

		select {
		case <-time.After(time.Second * 1):
			t.NoError(errors.Errorf("timeout to switch joining state"))
		case sctx := <-sctxch:
			var jsctx joiningSwitchContext
			t.True(errors.As(sctx, &jsctx))
			base.CompareVoteproof(t.Assert(), avp, jsctx.vp)
		}
	})

	t.Run("finished and expected last accept voteproof, but add new height", func() {
		st, _ := t.newState(nil)
		st.SetLogging(logging.TestLogging)

		sctxch := make(chan stateSwitchContext, 1)
		st.switchStateFunc = func(sctx stateSwitchContext) error {
			sctxch <- sctx

			return nil
		}

		point := base.RawPoint(33, 2)
		deferred, err := st.enter(newSyncingSwitchContext(StateJoining, point.Height()))
		t.NoError(err)
		deferred()

		syncer := st.syncer.(*dummySyncer)

		afact := t.newACCEPTBallotFact(point, nil, nil)
		avp, err := t.newACCEPTVoteproof(afact, t.local, []*LocalNode{t.local})
		t.NoError(err)

		st.setLastVoteproof(avp)

		st.waitStuck = time.Second

		syncer.done(point.Height())
		syncer.add(point.Next().Height())

		select {
		case <-time.After(time.Second * 2):
		case <-sctxch:
			t.NoError(errors.Errorf("switched joining state"))
		}
	})

	t.Run("finished and expected last accept voteproof, with new voteproof", func() {
		st, _ := t.newState(nil)
		st.SetLogging(logging.TestLogging)

		sctxch := make(chan stateSwitchContext, 1)
		st.switchStateFunc = func(sctx stateSwitchContext) error {
			sctxch <- sctx

			return nil
		}

		point := base.RawPoint(33, 2)
		deferred, err := st.enter(newSyncingSwitchContext(StateJoining, point.Height()))
		t.NoError(err)
		deferred()

		syncer := st.syncer.(*dummySyncer)

		afact := t.newACCEPTBallotFact(point, nil, nil)
		avp, err := t.newACCEPTVoteproof(afact, t.local, []*LocalNode{t.local})
		t.NoError(err)

		st.setLastVoteproof(avp)

		st.waitStuck = time.Second

		syncer.done(point.Height())
		syncer.add(point.Next().Height())

		newavp, err := t.newACCEPTVoteproof(t.newACCEPTBallotFact(point.Next(), nil, nil), t.local, []*LocalNode{t.local})
		t.NoError(err)

		st.waitStuck = time.Millisecond * 100

		st.setLastVoteproof(newavp)
		syncer.done(newavp.Point().Height())

		select {
		case <-time.After(time.Second * 2):
			t.NoError(errors.Errorf("timeout to switch joining state"))
		case sctx := <-sctxch:
			var jsctx joiningSwitchContext
			t.True(errors.As(sctx, &jsctx))
			base.CompareVoteproof(t.Assert(), newavp, jsctx.vp)
		}
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
	canceled   bool
	cancelf    func() error
}

func newDummySyncer(ch chan base.Height) *dummySyncer {
	if ch == nil {
		ch = make(chan base.Height)
	}
	return &dummySyncer{
		ch: ch,
	}
}

func (s *dummySyncer) top() base.Height {
	s.RLock()
	defer s.RUnlock()

	return s.topHeight
}

func (s *dummySyncer) add(h base.Height) bool {
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

func (s *dummySyncer) done(h base.Height) {
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

func (s *dummySyncer) finished() <-chan base.Height {
	return s.ch
}

func (s *dummySyncer) isFinished() bool {
	s.RLock()
	defer s.RUnlock()

	if s.canceled {
		return true
	}

	return s.topHeight == s.doneHeight
}

func (s *dummySyncer) cancel() error {
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
