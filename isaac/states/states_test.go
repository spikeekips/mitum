package isaacstates

import (
	"context"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/stretchr/testify/suite"
)

type dummyNewHandler struct {
	ne func() (handler, error)
	st func(*States)
}

func (h dummyNewHandler) new() (handler, error) {
	return h.ne()
}

func (h dummyNewHandler) setStates(st *States) {
	h.st(st)
}

func (st *States) newVoteproof(vp base.Voteproof) error {
	current := st.current()
	if current == nil {
		st.Log().Debug().Msg("voteproof ignored; nil current")

		return nil
	}

	if !st.lvps.IsNew(vp) {
		return nil
	}

	go func() {
		st.vpch <- vp
	}()

	return nil
}

func (st *States) setHandler(h handler) *States {
	st.newHandlers[h.state()] = dummyNewHandler{
		ne: func() (handler, error) {
			return h, nil
		},
		st: func(st *States) {
			if i, ok := (interface{})(h).(interface{ setStates(*States) }); ok {
				i.setStates(st)
			}
		},
	}

	return st
}

type testStates struct {
	suite.Suite
}

func (t *testStates) TestWait() {
	st := NewStates(nil, nil, func(base.Ballot) error { return nil })
	_ = st.SetLogging(logging.TestNilLogging)

	_ = st.setHandler(newDummyStateHandler(StateStopped))
	booting := newDummyStateHandler(StateBooting)

	enterch := make(chan bool, 1)
	_ = booting.setEnter(func(StateType, switchContext) error {
		enterch <- true

		return nil
	}, nil)
	_ = st.setHandler(booting)

	defer st.Stop()

	errch := st.Wait(context.Background())

	select {
	case <-time.After(time.Second * 2):
	case err := <-errch:
		t.NoError(err)
	case <-enterch:
		t.Equal(StateBooting, st.current().state())
	}
}

func (t *testStates) TestExit() {
	st, _ := t.booted()
	defer st.Stop()

	_ = st.setHandler(newDummyStateHandler(StateJoining))

	handler, _ := st.newHandlers[StateBooting].new()
	booting := handler.(*dummyStateHandler)

	exitch := make(chan bool, 1)
	_ = booting.setExit(func(switchContext) error {
		exitch <- true

		return nil
	}, nil)

	sctx := newDummySwitchContext(st.current().state(), StateJoining, nil)
	t.NoError(st.MoveState(sctx))

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("failed to exit from booting"))
	case <-exitch:
	}
}

func (t *testStates) TestBootingAtStarting() {
	st := NewStates(nil, nil, func(base.Ballot) error { return nil })
	_ = st.SetLogging(logging.TestNilLogging)

	_ = st.setHandler(newDummyStateHandler(StateStopped))

	booting := newDummyStateHandler(StateBooting)

	enterch := make(chan bool, 1)
	_ = booting.setEnter(func(StateType, switchContext) error {
		enterch <- true

		return nil
	}, nil)
	_ = st.setHandler(booting)

	t.NoError(st.Start())
	defer st.Stop()

	select {
	case <-time.After(time.Second):
		t.NoError(errors.Errorf("failed to enter booting"))
	case <-enterch:
		t.Equal(StateBooting, st.current().state())
	}
}

func (t *testStates) TestFailedToEnterIntoBootingAtStarting() {
	st := NewStates(nil, nil, func(base.Ballot) error { return nil })
	_ = st.SetLogging(logging.TestNilLogging)

	_ = st.setHandler(newDummyStateHandler(StateStopped))

	booting := newDummyStateHandler(StateBooting)

	bootingenterch := make(chan bool, 1)
	_ = booting.setEnter(func(StateType, switchContext) error {
		bootingenterch <- true

		return errors.Errorf("something wrong in booting")
	}, nil)
	_ = st.setHandler(booting)

	broken := newDummyStateHandler(StateBroken)
	brokenenterch := make(chan bool, 1)
	_ = broken.setEnter(nil, func() {
		brokenenterch <- true
	})
	_ = st.setHandler(broken)

	t.NoError(st.Start())
	defer st.Stop()

	select {
	case <-time.After(time.Second):
		t.NoError(errors.Errorf("failed to enter booting"))
	case <-bootingenterch:
	}

	select {
	case <-time.After(time.Second):
		t.NoError(errors.Errorf("failed to enter broken"))
	case <-brokenenterch:
		t.Equal(StateBroken, st.current().state())
	}
}

func (t *testStates) booted() (*States, <-chan error) {
	st := NewStates(nil, nil, func(base.Ballot) error { return nil })
	_ = st.SetLogging(logging.TestNilLogging)

	_ = st.setHandler(newDummyStateHandler(StateStopped))

	booting := newDummyStateHandler(StateBooting)

	enterch := make(chan bool, 1)
	_ = booting.setEnter(func(StateType, switchContext) error {
		enterch <- true

		return nil
	}, nil)
	_ = st.setHandler(booting)

	errch := st.Wait(context.Background())

	select {
	case <-time.After(time.Second):
		t.NoError(errors.Errorf("failed to enter booting"))
	case <-enterch:
		t.Equal(StateBooting, st.current().state())
	}

	return st, errch
}

func (t *testStates) TestFailedToEnterIntoBrokenAtStarting() {
	st := NewStates(nil, nil, func(base.Ballot) error { return nil })
	_ = st.SetLogging(logging.TestNilLogging)

	_ = st.setHandler(newDummyStateHandler(StateStopped))

	booting := newDummyStateHandler(StateBooting)

	bootingenterch := make(chan bool, 1)
	_ = booting.setEnter(func(StateType, switchContext) error {
		bootingenterch <- true

		return errors.Errorf("something wrong in booting")
	}, nil)
	_ = st.setHandler(booting)

	broken := newDummyStateHandler(StateBroken)
	brokenenterch := make(chan bool, 1)
	_ = broken.setEnter(func(StateType, switchContext) error {
		brokenenterch <- true

		return errors.Errorf("something wrong in broken")
	}, nil)
	_ = st.setHandler(broken)

	stopch := st.Wait(context.Background())
	defer st.Stop()

	select {
	case <-time.After(time.Second):
		t.NoError(errors.Errorf("failed to enter booting"))
	case <-bootingenterch:
	}

	select {
	case <-time.After(time.Second):
		t.NoError(errors.Errorf("failed to enter broken"))
	case <-brokenenterch:
	}

	select {
	case <-time.After(time.Second):
		t.NoError(errors.Errorf("states was not stopped"))
	case err := <-stopch:
		t.Equal(StateStopped, st.current().state())

		t.Error(err)
		t.ErrorContains(err, "something wrong in broken")
	}
}

func (t *testStates) TestNewStateWithWrongFrom() {
	st, _ := t.booted()
	defer st.Stop()

	handler, _ := st.newHandlers[StateStopped].new()
	stopped := handler.(*dummyStateHandler)

	enterch := make(chan bool, 1)
	_ = stopped.setEnter(func(StateType, switchContext) error {
		enterch <- true

		return errors.Errorf("something wrong in joining")
	}, nil)

	t.Equal(StateBooting, st.current().state())

	joining := newDummyStateHandler(StateJoining)
	_ = st.setHandler(joining)

	sctx := newDummySwitchContext(StateJoining, StateStopped, nil)
	err := st.MoveState(sctx)
	t.NoError(err)

	select {
	case <-time.After(time.Second):
	case <-enterch:
		t.NoError(errors.Errorf("stopped should not be entered"))
	}
}

func (t *testStates) TestNewStateWithWrongNext() {
	st, _ := t.booted()
	defer st.Stop()

	t.Equal(StateBooting, st.current().state())

	sctx := newDummySwitchContext(st.current().state(), StateType(util.UUID().String()), nil)
	err := st.MoveState(sctx)
	t.Error(err)
	t.ErrorContains(err, "unknown next state")
}

func (t *testStates) TestNewState() {
	st, _ := t.booted()
	defer st.Stop()

	joining := newDummyStateHandler(StateJoining)

	enterch := make(chan bool, 1)
	_ = joining.setEnter(func(StateType, switchContext) error {
		enterch <- true

		return nil
	}, nil)
	_ = st.setHandler(joining)

	t.Equal(StateBooting, st.current().state())

	sctx := newDummySwitchContext(st.current().state(), StateJoining, nil)
	err := st.MoveState(sctx)
	t.NoError(err)

	select {
	case <-time.After(time.Second * 3):
		t.NoError(errors.Errorf("failed to enter joining"))
	case <-enterch:
		t.Equal(StateJoining, st.current().state())
	}
}

func (t *testStates) TestExitCurrentWhenStopped() {
	st, _ := t.booted()

	joining := newDummyStateHandler(StateJoining)

	enterch := make(chan bool, 1)
	exitch := make(chan bool, 1)
	_ = joining.setEnter(func(StateType, switchContext) error {
		enterch <- true

		return nil
	}, nil).setExit(func(switchContext) error {
		exitch <- true

		return nil
	}, nil)
	_ = st.setHandler(joining)

	t.Equal(StateBooting, st.current().state())

	sctx := newDummySwitchContext(st.current().state(), StateJoining, nil)
	err := st.MoveState(sctx)
	t.NoError(err)

	select {
	case <-time.After(time.Second * 3):
		t.NoError(errors.Errorf("failed to enter joining"))
	case <-enterch:
		t.Equal(StateJoining, st.current().state())
	}

	t.NoError(st.Stop())

	select {
	case <-time.After(time.Second * 3):
		t.NoError(errors.Errorf("failed to exit from joining"))
	case <-exitch:
		t.NotNil(st.current())
		t.Equal(StateStopped, st.current().state())
	}
}

func (t *testStates) TestEnterWithVoteproof() {
	st, _ := t.booted()
	defer st.Stop()

	joining := newDummyStateHandler(StateJoining)

	enterch := make(chan base.Voteproof, 1)
	_ = joining.setEnter(func(_ StateType, sctx switchContext) error {
		i, ok := sctx.(dummySwitchContext)
		t.True(ok)

		enterch <- i.vp

		return nil
	}, nil)
	_ = st.setHandler(joining)

	vp := isaac.INITVoteproof{}
	vp.SetID(util.UUID().String())

	sctx := newDummySwitchContext(st.current().state(), StateJoining, vp)
	t.NoError(st.MoveState(sctx))

	select {
	case <-time.After(time.Second * 2):
	case rvp := <-enterch:
		t.Equal(vp.ID(), rvp.ID())
	}
}

func (t *testStates) TestSameCurrentWithNext() {
	st, _ := t.booted()
	defer st.Stop()

	handler, _ := st.newHandlers[StateBooting].new()
	booting := handler.(*dummyStateHandler)

	reenterch := make(chan bool, 1)
	_ = booting.setEnter(func(StateType, switchContext) error {
		reenterch <- true

		return nil
	}, nil)

	vp := isaac.INITVoteproof{}
	vp.SetID(util.UUID().String())

	sctx := newDummySwitchContext(st.current().state(), StateBooting, vp)
	err := st.MoveState(sctx)
	t.NoError(err)

	select {
	case <-time.After(time.Second * 2):
	case <-reenterch:
		t.NoError(errors.Errorf("failed to prevent to enter again to booting"))
	}
}

func (t *testStates) TestSameCurrentWithNextWithoutVoteproof() {
	st, _ := t.booted()
	defer st.Stop()

	handler, _ := st.newHandlers[StateBooting].new()
	booting := handler.(*dummyStateHandler)

	reenterch := make(chan bool, 1)
	_ = booting.setEnter(func(StateType, switchContext) error {
		reenterch <- true

		return nil
	}, nil)

	sctx := newDummySwitchContext(st.current().state(), StateBooting, nil)
	t.NoError(st.MoveState(sctx))

	select {
	case <-time.After(time.Second * 2):
	case <-reenterch:
		t.NoError(errors.Errorf("failed to prevent to enter again to booting"))
	}
}

func (t *testStates) TestNewVoteproof() {
	st, _ := t.booted()
	defer st.Stop()

	handler, _ := st.newHandlers[StateBooting].new()
	booting := handler.(*dummyStateHandler)

	voteproofch := make(chan base.Voteproof, 1)
	_ = booting.setNewVoteproof(func(vp base.Voteproof) error {
		voteproofch <- vp

		return nil
	})

	vp := isaac.INITVoteproof{}
	vp.SetID(util.UUID().String())

	t.NoError(st.newVoteproof(vp))

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("failed to newVoteproof"))
	case rvp := <-voteproofch:
		t.Equal(vp.ID(), rvp.ID())
	}
}

func (t *testStates) TestNewVoteproofSwitchState() {
	st, _ := t.booted()
	defer st.Stop()

	joiningch := make(chan base.Voteproof, 1)
	joining := newDummyStateHandler(StateJoining).setEnter(func(_ StateType, sctx switchContext) error {
		i, ok := sctx.(dummySwitchContext)
		t.True(ok)

		joiningch <- i.vp

		return nil
	}, nil)
	_ = st.setHandler(joining)

	handler, _ := st.newHandlers[StateBooting].new()
	booting := handler.(*dummyStateHandler)

	voteproofch := make(chan base.Voteproof, 1)
	_ = booting.setNewVoteproof(func(vp base.Voteproof) error {
		voteproofch <- vp

		return newDummySwitchContext(st.current().state(), StateJoining, vp)
	})

	vp := isaac.INITVoteproof{}
	vp.SetID(util.UUID().String())

	t.NoError(st.newVoteproof(vp))

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("failed to newVoteproof"))
	case rvp := <-voteproofch:
		t.Equal(vp.ID(), rvp.ID())
	}

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("failed to enter joining"))
	case rvp := <-joiningch:
		t.Equal(vp.ID(), rvp.ID())
		t.Equal(StateJoining, st.current().state())
	}
}

func (t *testStates) TestCurrentIgnoresSwitchingState() {
	st, _ := t.booted()
	defer st.Stop()

	exitch := make(chan struct{}, 1)

	handler, _ := st.newHandlers[StateBooting].new()
	booting := handler.(*dummyStateHandler)
	_ = booting.setExit(func(switchContext) error {
		exitch <- struct{}{}

		return ErrIgnoreSwithingState.Call()
	}, nil)

	joining := newDummyStateHandler(StateJoining)

	enterch := make(chan bool, 1)
	_ = joining.setEnter(func(StateType, switchContext) error {
		enterch <- true

		return nil
	}, nil)
	_ = st.setHandler(joining)

	t.Equal(StateBooting, st.current().state())

	sctx := newDummySwitchContext(st.current().state(), StateJoining, nil)
	err := st.MoveState(sctx)
	t.NoError(err)

	select {
	case <-time.After(time.Second * 3):
		t.NoError(errors.Errorf("failed to call exiting from booting"))
	case <-exitch:
	}

	select {
	case <-time.After(time.Second * 3):
	case <-enterch:
		t.NoError(errors.Errorf("failed to prevent to enter to joining"))
	}

	t.NotNil(st.current())
	t.Equal(st.current().state(), StateBooting)
}

func (t *testStates) TestStoppedByStateStopped() {
	st, errch := t.booted()
	defer st.Stop()

	exitch := make(chan struct{}, 1)

	handler, _ := st.newHandlers[StateBooting].new()
	booting := handler.(*dummyStateHandler)
	_ = booting.setExit(func(switchContext) error {
		exitch <- struct{}{}

		return nil
	}, nil)

	_ = st.setHandler(newDummyStateHandler(StateStopped))

	t.Equal(StateBooting, st.current().state())

	sctx := newBaseErrorSwitchContext(StateStopped, errors.Errorf("something wrong"), switchContextOKFuncCheckFrom(st.current().state()))
	err := st.MoveState(sctx)
	t.NoError(err)

	select {
	case <-time.After(time.Second * 3):
		t.NoError(errors.Errorf("failed to call exiting from booting"))
	case <-exitch:
	}

	<-time.After(time.Second)
	err = <-errch
	t.Error(err)
	t.ErrorContains(err, "something wrong")

	t.Equal(StateStopped, st.current().state())
	t.False(st.IsStarted())
}

func TestStates(t *testing.T) {
	suite.Run(t, new(testStates))
}

type dummyStateHandler struct {
	s             StateType
	enterf        func(StateType, switchContext) error
	enterdefer    func()
	exitf         func(switchContext) error
	exitdefer     func()
	newVoteprooff func(base.Voteproof) error
}

func newDummyStateHandler(state StateType) *dummyStateHandler {
	return &dummyStateHandler{
		s: state,
	}
}

func (st *dummyStateHandler) state() StateType {
	return st.s
}

func (st *dummyStateHandler) enter(from StateType, sctx switchContext) (func(), error) {
	if st.enterf == nil {
		return st.enterdefer, nil
	}

	if err := st.enterf(from, sctx); err != nil {
		return nil, err
	}

	return st.enterdefer, nil
}

func (st *dummyStateHandler) exit(sctx switchContext) (func(), error) {
	if st.exitf == nil {
		return st.exitdefer, nil
	}

	if err := st.exitf(sctx); err != nil {
		return nil, err
	}

	return st.exitdefer, nil
}

func (st *dummyStateHandler) newVoteproof(vp base.Voteproof) error {
	if st.newVoteprooff == nil {
		return nil
	}

	return st.newVoteprooff(vp)
}

func (st *dummyStateHandler) onEmptyMembers() {}

func (st *dummyStateHandler) setEnter(f func(StateType, switchContext) error, d func()) *dummyStateHandler {
	st.enterf = f
	st.enterdefer = d

	return st
}

func (st *dummyStateHandler) setExit(f func(switchContext) error, d func()) *dummyStateHandler {
	st.exitf = f
	st.exitdefer = d

	return st
}

func (st *dummyStateHandler) setNewVoteproof(f func(base.Voteproof) error) *dummyStateHandler {
	st.newVoteprooff = f

	return st
}

type dummySwitchContext struct {
	baseSwitchContext
	vp base.Voteproof
}

func newDummySwitchContext(from, next StateType, vp base.Voteproof) dummySwitchContext {
	return dummySwitchContext{
		baseSwitchContext: newBaseSwitchContext(next, switchContextOKFuncCheckFrom(from)),
		vp:                vp,
	}
}

func (s dummySwitchContext) MarshalZerologObject(e *zerolog.Event) {
	s.baseSwitchContext.MarshalZerologObject(e)

	if s.vp != nil {
		e.Interface("voteproof", s.vp)
	}
}
