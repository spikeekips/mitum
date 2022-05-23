package isaacstates

import (
	"sync"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/stretchr/testify/suite"
)

type testSyncingHandler struct {
	isaac.BaseTestBallots
}

func (t *testSyncingHandler) newState(finishch chan base.Height) (*SyncingHandler, func()) {
	local := t.Local
	policy := t.NodePolicy

	st := NewSyncingHandler(
		local,
		policy,
		nil,
		func(height base.Height) (isaac.Syncer, error) {
			syncer := newDummySyncer(finishch, nil)
			if !syncer.Add(height) {
				return nil, errors.Errorf("failed new syncer")
			}

			return syncer, nil
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
	st.switchStateFunc = func(switchContext) error {
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

	_ = (interface{})(st).(handler)

	deferred, err := st.enter(newSyncingSwitchContext(StateJoining, base.Height(33)))
	t.NoError(err)
	deferred()

	t.NotNil(st.syncer)
	t.Equal(base.Height(33), st.syncer.(*dummySyncer).Top())

	t.NoError(st.syncer.(*dummySyncer).Cancel())
}

func (t *testSyncingHandler) TestExit() {
	t.Run("exit", func() {
		st, closef := t.newState(nil)
		defer closef()

		deferredenter, err := st.enter(newSyncingSwitchContext(StateJoining, base.Height(33)))
		t.NoError(err)
		deferredenter()

		t.NoError(st.syncer.(*dummySyncer).Cancel())

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

		syncer.finish(point.Height())

		deferredexit, err := st.exit(nil)
		t.Nil(deferredexit)
		t.Error(err)
		t.ErrorContains(err, "hehehe")
	})

	t.Run("can not cancel", func() {
		st, _ := t.newState(nil)

		deferredenter, err := st.enter(newSyncingSwitchContext(StateJoining, base.Height(33)))
		t.NoError(err)
		deferredenter()

		syncer := st.syncer.(*dummySyncer)
		syncer.cancelf = func() error {
			return SyncerCanNotCancelError.Call()
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

		ifact := t.NewINITBallotFact(point.NextHeight().NextHeight(), nil, nil)
		ivp, err := t.NewINITVoteproof(ifact, t.Local, []isaac.LocalNode{t.Local})
		t.NoError(err)

		t.Equal(point.Height(), syncer.Top())

		t.NoError(st.newVoteproof(ivp))
		t.Equal(ivp.Point().Height()-1, syncer.Top())
	})

	t.Run("higher accept voteproof", func() {
		st, _ := t.newState(nil)

		point := base.RawPoint(33, 2)
		deferred, err := st.enter(newSyncingSwitchContext(StateJoining, point.Height()))
		t.NoError(err)
		deferred()

		syncer := st.syncer.(*dummySyncer)

		afact := t.NewACCEPTBallotFact(point.NextHeight(), nil, nil)
		avp, err := t.NewACCEPTVoteproof(afact, t.Local, []isaac.LocalNode{t.Local})
		t.NoError(err)

		t.Equal(point.Height(), syncer.Top())

		t.NoError(st.newVoteproof(avp))
		t.Equal(avp.Point().Height(), syncer.Top())
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

		ifact := t.NewINITBallotFact(point, nil, nil)
		ivp, err := t.NewINITVoteproof(ifact, t.Local, []isaac.LocalNode{t.Local})
		t.NoError(err)

		t.Equal(point.Height(), syncer.Top())

		t.NoError(st.newVoteproof(ivp))
		t.Equal(point.Height(), syncer.Top())
	})

	t.Run("lower accept voteproof", func() {
		st, _ := t.newState(nil)

		point := base.RawPoint(33, 2)
		deferred, err := st.enter(newSyncingSwitchContext(StateJoining, point.Height()))
		t.NoError(err)
		deferred()

		syncer := st.syncer.(*dummySyncer)

		afact := t.NewACCEPTBallotFact(point, nil, nil)
		avp, err := t.NewACCEPTVoteproof(afact, t.Local, []isaac.LocalNode{t.Local})
		t.NoError(err)

		t.Equal(point.Height(), syncer.Top())

		t.NoError(st.newVoteproof(avp))
		t.Equal(point.Height(), syncer.Top())
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

		ifact := t.NewINITBallotFact(point.NextHeight(), nil, nil)
		ivp, err := t.NewINITVoteproof(ifact, t.Local, []isaac.LocalNode{t.Local})
		t.NoError(err)

		t.NoError(st.newVoteproof(ivp))
		t.Equal(point.Height(), syncer.Top())
	})

	t.Run("finished", func() {
		st, closef := t.newState(nil)
		defer closef()

		point := base.RawPoint(33, 2)
		deferred, err := st.enter(newSyncingSwitchContext(StateJoining, point.Height()))
		t.NoError(err)
		deferred()

		syncer := st.syncer.(*dummySyncer)

		syncer.finish(point.Height())

		ifact := t.NewINITBallotFact(point.NextHeight(), nil, nil)
		ivp, err := t.NewINITVoteproof(ifact, t.Local, []isaac.LocalNode{t.Local})
		t.NoError(err)

		err = st.newVoteproof(ivp)

		var csctx consensusSwitchContext
		t.True(errors.As(err, &csctx))
		base.EqualVoteproof(t.Assert(), ivp, csctx.ivp)
	})
}

func (t *testSyncingHandler) TestFinishedWithLastVoteproof() {
	t.Run("finished, but last init voteproof is old", func() {
		st, closef := t.newState(nil)
		defer closef()

		sctxch := make(chan switchContext, 1)
		st.switchStateFunc = func(sctx switchContext) error {
			sctxch <- sctx

			return nil
		}

		point := base.RawPoint(33, 2)
		deferred, err := st.enter(newSyncingSwitchContext(StateJoining, point.Height()))
		t.NoError(err)
		deferred()

		syncer := st.syncer.(*dummySyncer)

		ifact := t.NewINITBallotFact(point, nil, nil)
		ivp, err := t.NewINITVoteproof(ifact, t.Local, []isaac.LocalNode{t.Local})
		t.NoError(err)

		st.setLastVoteproof(ivp)

		syncer.finish(point.Height())

		select {
		case <-time.After(time.Second * 1):
		case <-sctxch:
			t.NoError(errors.Errorf("unexpected switch state"))
		}

		t.Equal(point.Height(), syncer.Top())
	})

	t.Run("finished, but last init voteproof is higher", func() {
		st, _ := t.newState(nil)

		sctxch := make(chan switchContext, 1)
		st.switchStateFunc = func(sctx switchContext) error {
			sctxch <- sctx

			return nil
		}

		point := base.RawPoint(33, 2)
		deferred, err := st.enter(newSyncingSwitchContext(StateJoining, point.Height()))
		t.NoError(err)
		deferred()

		syncer := st.syncer.(*dummySyncer)

		ifact := t.NewINITBallotFact(point.NextHeight().NextHeight(), nil, nil)
		ivp, err := t.NewINITVoteproof(ifact, t.Local, []isaac.LocalNode{t.Local})
		t.NoError(err)

		st.setLastVoteproof(ivp)

		syncer.finish(point.Height())

		select {
		case <-time.After(time.Second * 1):
		case <-sctxch:
			t.NoError(errors.Errorf("unexpected switch state"))
		}

		t.Equal(ivp.Point().Height()-1, syncer.Top())
	})

	t.Run("finished, but last accept voteproof is old", func() {
		st, closef := t.newState(nil)
		defer closef()

		sctxch := make(chan switchContext, 1)
		st.switchStateFunc = func(sctx switchContext) error {
			sctxch <- sctx

			return nil
		}

		point := base.RawPoint(33, 2)
		deferred, err := st.enter(newSyncingSwitchContext(StateJoining, point.Height()))
		t.NoError(err)
		deferred()

		syncer := st.syncer.(*dummySyncer)

		afact := t.NewACCEPTBallotFact(point, nil, nil)
		avp, err := t.NewACCEPTVoteproof(afact, t.Local, []isaac.LocalNode{t.Local})
		t.NoError(err)
		st.setLastVoteproof(avp)

		syncer.finish(point.Height())

		select {
		case <-time.After(time.Second * 1):
		case <-sctxch:
			t.NoError(errors.Errorf("unexpected switch state"))
		}
	})

	t.Run("finished, but last accept voteproof higher", func() {
		st, _ := t.newState(nil)

		sctxch := make(chan switchContext, 1)
		st.switchStateFunc = func(sctx switchContext) error {
			sctxch <- sctx

			return nil
		}

		point := base.RawPoint(33, 2)
		deferred, err := st.enter(newSyncingSwitchContext(StateJoining, point.Height()))
		t.NoError(err)
		deferred()

		syncer := st.syncer.(*dummySyncer)

		afact := t.NewACCEPTBallotFact(point.NextHeight(), nil, nil)
		avp, err := t.NewACCEPTVoteproof(afact, t.Local, []isaac.LocalNode{t.Local})
		t.NoError(err)
		st.setLastVoteproof(avp)

		syncer.finish(point.Height())

		select {
		case <-time.After(time.Second * 1):
		case <-sctxch:
			t.NoError(errors.Errorf("unexpected switch state"))
		}

		t.Equal(avp.Point().Height(), syncer.Top())
	})

	t.Run("finished and expected last init voteproof", func() {
		st, closef := t.newState(nil)
		defer closef()

		sctxch := make(chan switchContext, 1)
		st.switchStateFunc = func(sctx switchContext) error {
			sctxch <- sctx

			return nil
		}

		point := base.RawPoint(33, 2)
		deferred, err := st.enter(newSyncingSwitchContext(StateJoining, point.Height()))
		t.NoError(err)
		deferred()

		syncer := st.syncer.(*dummySyncer)

		ifact := t.NewINITBallotFact(point.NextHeight(), nil, nil)
		ivp, err := t.NewINITVoteproof(ifact, t.Local, []isaac.LocalNode{t.Local})
		t.NoError(err)

		st.setLastVoteproof(ivp)

		syncer.finish(point.Height())

		select {
		case <-time.After(time.Second * 1):
			t.NoError(errors.Errorf("timeout to switch consensus state"))
		case sctx := <-sctxch:
			var csctx consensusSwitchContext
			t.True(errors.As(sctx, &csctx))
			base.EqualVoteproof(t.Assert(), ivp, csctx.ivp)
		}
	})
}

func (t *testSyncingHandler) TestFinishedButStuck() {
	t.Run("finished and expected last accept voteproof", func() {
		st, closef := t.newState(nil)
		defer closef()

		sctxch := make(chan switchContext, 1)
		st.switchStateFunc = func(sctx switchContext) error {
			sctxch <- sctx

			return nil
		}

		point := base.RawPoint(33, 2)
		deferred, err := st.enter(newSyncingSwitchContext(StateJoining, point.Height()))
		t.NoError(err)
		deferred()

		syncer := st.syncer.(*dummySyncer)

		afact := t.NewACCEPTBallotFact(point, nil, nil)
		avp, err := t.NewACCEPTVoteproof(afact, t.Local, []isaac.LocalNode{t.Local})
		t.NoError(err)

		st.setLastVoteproof(avp)

		st.waitStuck = time.Millisecond * 100

		syncer.finish(point.Height())

		select {
		case <-time.After(time.Second * 1):
			t.NoError(errors.Errorf("timeout to switch joining state"))
		case sctx := <-sctxch:
			var jsctx joiningSwitchContext
			t.True(errors.As(sctx, &jsctx))
			base.EqualVoteproof(t.Assert(), avp, jsctx.vp)
		}
	})

	t.Run("finished and expected last accept voteproof, but add new height", func() {
		st, _ := t.newState(nil)

		sctxch := make(chan switchContext, 1)
		st.switchStateFunc = func(sctx switchContext) error {
			sctxch <- sctx

			return nil
		}

		point := base.RawPoint(33, 2)
		deferred, err := st.enter(newSyncingSwitchContext(StateJoining, point.Height()))
		t.NoError(err)
		deferred()

		syncer := st.syncer.(*dummySyncer)

		afact := t.NewACCEPTBallotFact(point, nil, nil)
		avp, err := t.NewACCEPTVoteproof(afact, t.Local, []isaac.LocalNode{t.Local})
		t.NoError(err)

		st.setLastVoteproof(avp)

		st.waitStuck = time.Second

		syncer.finish(point.Height())
		syncer.Add(point.NextHeight().Height())

		select {
		case <-time.After(time.Second * 2):
		case <-sctxch:
			t.NoError(errors.Errorf("unexpected; switched joining state"))
		}
	})

	t.Run("finished and expected last accept voteproof, with new voteproof", func() {
		st, _ := t.newState(nil)

		sctxch := make(chan switchContext, 1)
		st.switchStateFunc = func(sctx switchContext) error {
			sctxch <- sctx

			return nil
		}

		point := base.RawPoint(33, 2)
		deferred, err := st.enter(newSyncingSwitchContext(StateJoining, point.Height()))
		t.NoError(err)
		deferred()

		syncer := st.syncer.(*dummySyncer)

		afact := t.NewACCEPTBallotFact(point, nil, nil)
		avp, err := t.NewACCEPTVoteproof(afact, t.Local, []isaac.LocalNode{t.Local})
		t.NoError(err)

		st.setLastVoteproof(avp)

		st.waitStuck = time.Second

		syncer.finish(point.Height())
		syncer.Add(point.NextHeight().Height())

		newavp, err := t.NewACCEPTVoteproof(t.NewACCEPTBallotFact(point.NextHeight(), nil, nil), t.Local, []isaac.LocalNode{t.Local})
		t.NoError(err)

		st.waitStuck = time.Millisecond * 100

		st.setLastVoteproof(newavp)
		syncer.finish(newavp.Point().Height())

		select {
		case <-time.After(time.Second * 2):
			t.NoError(errors.Errorf("timeout to switch joining state"))
		case sctx := <-sctxch:
			var jsctx joiningSwitchContext
			t.True(errors.As(sctx, &jsctx))
			base.EqualVoteproof(t.Assert(), newavp, jsctx.vp)
		}
	})
}

func (t *testSyncingHandler) TestSyncerErr() {
	st, closef := t.newState(nil)
	defer closef()

	sctxch := make(chan switchContext, 1)
	st.switchStateFunc = func(sctx switchContext) error {
		sctxch <- sctx

		return nil
	}

	point := base.RawPoint(33, 2)
	deferred, err := st.enter(newSyncingSwitchContext(StateJoining, point.Height()))
	t.NoError(err)
	deferred()

	syncer := st.syncer.(*dummySyncer)
	syncer.done(errors.Errorf("kekeke"))

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to switch joining state"))
	case sctx := <-sctxch:
		var bsctx baseErrorSwitchContext
		t.True(errors.As(sctx, &bsctx))
		t.ErrorContains(bsctx.err, "kekeke")
	}
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

func newDummySyncer(ch chan base.Height, donech chan struct{}) *dummySyncer {
	if ch == nil {
		ch = make(chan base.Height)
	}

	if donech == nil {
		donech = make(chan struct{})
	}

	return &dummySyncer{
		ch:     ch,
		donech: donech,
	}
}

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
