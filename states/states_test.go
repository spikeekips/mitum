package states

import (
	"context"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/stretchr/testify/suite"
)

func (st *States) setHandler(h stateHandler) *States {
	st.handlers[h.state()] = h

	return st
}

type testStates struct {
	suite.Suite
}

func (t *testStates) TestWait() {
	st := NewStates()
	_ = st.SetLogging(logging.TestNilLogging)

	_ = st.setHandler(newDummyStateHandler(StateStopped))
	booting := newDummyStateHandler(StateBooting)

	enterch := make(chan bool, 1)
	_ = booting.setEnter(func(base.Voteproof) error {
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

	booting := st.handlers[StateBooting].(*dummyStateHandler)

	exitch := make(chan bool, 1)
	_ = booting.setExit(func() error {
		exitch <- true

		return nil
	}, nil)

	sctx := newStateSwitchContext(st.current().state(), StateJoining, nil)
	t.NoError(st.newState(sctx))

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("failed to exit from booting"))
	case <-exitch:
	}
}

func (t *testStates) TestBootingAtStarting() {
	st := NewStates()
	_ = st.SetLogging(logging.TestNilLogging)

	_ = st.setHandler(newDummyStateHandler(StateStopped))

	booting := newDummyStateHandler(StateBooting)

	enterch := make(chan bool, 1)
	_ = booting.setEnter(func(base.Voteproof) error {
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
	st := NewStates()
	_ = st.SetLogging(logging.TestNilLogging)

	_ = st.setHandler(newDummyStateHandler(StateStopped))

	booting := newDummyStateHandler(StateBooting)

	bootingenterch := make(chan bool, 1)
	_ = booting.setEnter(func(base.Voteproof) error {
		bootingenterch <- true

		return errors.Errorf("something wrong in booting")
	}, nil)
	_ = st.setHandler(booting)

	broken := newDummyStateHandler(StateBroken)
	brokenenterch := make(chan bool, 1)
	_ = broken.setEnter(nil, func() error {
		brokenenterch <- true

		return nil
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
	st := NewStates()
	_ = st.SetLogging(logging.TestNilLogging)
	_ = st.SetLogging(logging.TestLogging)

	_ = st.setHandler(newDummyStateHandler(StateStopped))

	booting := newDummyStateHandler(StateBooting)

	enterch := make(chan bool, 1)
	_ = booting.setEnter(func(base.Voteproof) error {
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
	st := NewStates()
	_ = st.SetLogging(logging.TestNilLogging)

	_ = st.setHandler(newDummyStateHandler(StateStopped))

	booting := newDummyStateHandler(StateBooting)

	bootingenterch := make(chan bool, 1)
	_ = booting.setEnter(func(base.Voteproof) error {
		bootingenterch <- true

		return errors.Errorf("something wrong in booting")
	}, nil)
	_ = st.setHandler(booting)

	broken := newDummyStateHandler(StateBroken)
	brokenenterch := make(chan bool, 1)
	_ = broken.setEnter(func(base.Voteproof) error {
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
		t.Contains(err.Error(), "something wrong in broken")
	}
}

func (t *testStates) TestNewStateWithWrongFrom() {
	st, _ := t.booted()
	defer st.Stop()

	joining := newDummyStateHandler(StateJoining)

	enterch := make(chan bool, 1)
	_ = joining.setEnter(func(base.Voteproof) error {
		enterch <- true

		return errors.Errorf("something wrong in joining")
	}, nil)
	_ = st.setHandler(joining)

	t.Equal(StateBooting, st.current().state())

	sctx := newStateSwitchContext(StateJoining, StateStopped, nil)
	err := st.newState(sctx)
	t.Error(err)
	t.True(errors.Is(err, IgnoreSwithingStateError))
	t.Contains(err.Error(), "from not matched")
}

func (t *testStates) TestNewStateWithWrongNext() {
	st, _ := t.booted()
	defer st.Stop()

	t.Equal(StateBooting, st.current().state())

	sctx := newStateSwitchContext(st.current().state(), StateType(util.UUID().String()), nil)
	err := st.newState(sctx)
	t.Error(err)
	t.True(errors.Is(err, IgnoreSwithingStateError))
	t.Contains(err.Error(), "unknown next state")
}

func (t *testStates) TestNewState() {
	st, _ := t.booted()
	defer st.Stop()

	joining := newDummyStateHandler(StateJoining)

	enterch := make(chan bool, 1)
	_ = joining.setEnter(func(base.Voteproof) error {
		enterch <- true

		return nil
	}, nil)
	_ = st.setHandler(joining)

	t.Equal(StateBooting, st.current().state())

	sctx := newStateSwitchContext(st.current().state(), StateJoining, nil)
	err := st.newState(sctx)
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
	_ = joining.setEnter(func(base.Voteproof) error {
		enterch <- true

		return nil
	}, nil).setExit(func() error {
		exitch <- true

		return nil
	}, nil)
	_ = st.setHandler(joining)

	t.Equal(StateBooting, st.current().state())

	sctx := newStateSwitchContext(st.current().state(), StateJoining, nil)
	err := st.newState(sctx)
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
		t.Nil(st.current())
	}
}

func (t *testStates) TestEnterWithVoteproof() {
	st, _ := t.booted()
	defer st.Stop()

	joining := newDummyStateHandler(StateJoining)

	enterch := make(chan base.Voteproof, 1)
	_ = joining.setEnter(func(vp base.Voteproof) error {
		enterch <- vp

		return nil
	}, nil)
	_ = st.setHandler(joining)

	vp := INITVoteproof{}
	vp.id = util.UUID().String()

	sctx := newStateSwitchContext(st.current().state(), StateJoining, vp)
	t.NoError(st.newState(sctx))

	select {
	case <-time.After(time.Second * 2):
	case rvp := <-enterch:
		t.Equal(vp.ID(), rvp.ID())
	}
}

func (t *testStates) TestSameCurrentWithNext() {
	st, _ := t.booted()
	defer st.Stop()

	booting := st.handlers[StateBooting].(*dummyStateHandler)

	reenterch := make(chan bool, 1)
	voteproofch := make(chan base.Voteproof, 1)
	_ = booting.setEnter(func(base.Voteproof) error {
		reenterch <- true

		return nil
	}, nil).setNewVoteproof(func(vp base.Voteproof) error {
		voteproofch <- vp

		return nil
	})

	vp := INITVoteproof{}
	vp.id = util.UUID().String()

	sctx := newStateSwitchContext(st.current().state(), StateBooting, vp)
	err := st.newState(sctx)
	t.Error(err)
	t.True(errors.Is(err, IgnoreSwithingStateError))
	t.Contains(err.Error(), "same next state with voteproof")

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("failed to send voteproof to booting"))
	case rvp := <-voteproofch:
		t.Equal(vp.ID(), rvp.ID())
	}

	select {
	case <-time.After(time.Second * 2):
	case <-reenterch:
		t.NoError(errors.Errorf("failed to prevent to enter again to booting"))
	}
}

func (t *testStates) TestSameCurrentWithNextWithoutVoteproof() {
	st, _ := t.booted()
	defer st.Stop()

	booting := st.handlers[StateBooting].(*dummyStateHandler)

	reenterch := make(chan bool, 1)
	voteproofch := make(chan base.Voteproof, 1)
	_ = booting.setEnter(func(base.Voteproof) error {
		reenterch <- true

		return nil
	}, nil).setNewVoteproof(func(vp base.Voteproof) error {
		voteproofch <- vp

		return nil
	})

	sctx := newStateSwitchContext(st.current().state(), StateBooting, nil)
	err := st.newState(sctx)
	t.Error(err)
	t.True(errors.Is(err, IgnoreSwithingStateError))
	t.Contains(err.Error(), "same next state, but empty voteproof")

	select {
	case <-time.After(time.Second * 2):
	case <-reenterch:
		t.NoError(errors.Errorf("failed to prevent to enter again to booting"))
	}
}

func (t *testStates) TestNewVoteproof() {
	st, _ := t.booted()
	defer st.Stop()

	booting := st.handlers[StateBooting].(*dummyStateHandler)

	voteproofch := make(chan base.Voteproof, 1)
	_ = booting.setNewVoteproof(func(vp base.Voteproof) error {
		voteproofch <- vp

		return nil
	})

	vp := INITVoteproof{}
	vp.id = util.UUID().String()

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
	joining := newDummyStateHandler(StateJoining).setEnter(func(vp base.Voteproof) error {
		joiningch <- vp

		return nil
	}, nil)
	_ = st.setHandler(joining)

	booting := st.handlers[StateBooting].(*dummyStateHandler)

	voteproofch := make(chan base.Voteproof, 1)
	_ = booting.setNewVoteproof(func(vp base.Voteproof) error {
		voteproofch <- vp

		return newStateSwitchContext(st.current().state(), StateJoining, vp)
	})

	vp := INITVoteproof{}
	vp.id = util.UUID().String()

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
	booting := st.handlers[StateBooting].(*dummyStateHandler)
	_ = booting.setExit(func() error {
		exitch <- struct{}{}

		return IgnoreSwithingStateError.Call()
	}, nil)

	joining := newDummyStateHandler(StateJoining)

	enterch := make(chan bool, 1)
	_ = joining.setEnter(func(base.Voteproof) error {
		enterch <- true

		return nil
	}, nil)
	_ = st.setHandler(joining)

	t.Equal(StateBooting, st.current().state())

	sctx := newStateSwitchContext(st.current().state(), StateJoining, nil)
	err := st.newState(sctx)
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
	booting := st.handlers[StateBooting].(*dummyStateHandler)
	_ = booting.setExit(func() error {
		exitch <- struct{}{}

		return nil
	}, nil)

	_ = st.setHandler(newDummyStateHandler(StateStopped))

	t.Equal(StateBooting, st.current().state())

	sctx := newStateSwitchContext(st.current().state(), StateStopped, nil)
	sctx = sctx.SetError(errors.Errorf("something wrong"))
	err := st.newState(sctx)
	t.NoError(err)

	select {
	case <-time.After(time.Second * 3):
		t.NoError(errors.Errorf("failed to call exiting from booting"))
	case <-exitch:
	}

	<-time.After(time.Second)
	err = <-errch
	t.Error(err)
	t.Contains(err.Error(), "something wrong")

	t.Nil(st.current())
	t.False(st.IsStarted())
}

func TestStates(t *testing.T) {
	suite.Run(t, new(testStates))
}