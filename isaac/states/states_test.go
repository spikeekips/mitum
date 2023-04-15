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
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
)

func (st *States) newVoteproof(vp base.Voteproof) error {
	current := st.current()
	if current == nil {
		st.Log().Debug().Msg("voteproof ignored; nil current")

		return nil
	}

	if !st.args.LastVoteproofsHandler.IsNew(vp) {
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

type dummyStateHandler struct {
	s                  StateType
	enterf             func(StateType, switchContext) error
	enterdefer         func()
	exitf              func(switchContext) error
	exitdefer          func()
	newVoteprooff      func(base.Voteproof) error
	setAllowConsensusf func(bool)
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

func (st *dummyStateHandler) setAllowConsensus(allow bool) {
	if st.setAllowConsensusf == nil {
		return
	}

	st.setAllowConsensusf(allow)
}

func (st *dummyStateHandler) allowedConsensus() bool {
	return false
}

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

func (st *dummyStateHandler) setSetAllowConsensusf(f func(bool)) *dummyStateHandler {
	st.setAllowConsensusf = f

	return st
}

type dummySwitchContext struct {
	baseSwitchContext
	vp base.Voteproof
}

func newDummySwitchContext(from, next StateType, vp base.Voteproof) dummySwitchContext {
	return dummySwitchContext{
		baseSwitchContext: newBaseSwitchContext(from, next),
		vp:                vp,
	}
}

func (s dummySwitchContext) MarshalZerologObject(e *zerolog.Event) {
	s.baseSwitchContext.MarshalZerologObject(e)

	if s.vp != nil {
		e.Interface("voteproof", s.vp)
	}
}

type testStates struct {
	suite.Suite
	local  base.LocalNode
	params *isaac.LocalParams
}

func (t *testStates) SetupSuite() {
	t.local = base.RandomLocalNode()
	t.params = isaac.DefaultLocalParams(base.RandomNetworkID())
}

func (t *testStates) TestWait() {
	args := NewStatesArgs()
	st, err := NewStates(t.local, t.params, args)
	t.NoError(err)
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
	t.NoError(st.AskMoveState(sctx))

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("failed to exit from booting"))
	case <-exitch:
	}
}

func (t *testStates) TestBootingAtStarting() {
	args := NewStatesArgs()
	st, err := NewStates(t.local, t.params, args)
	t.NoError(err)
	_ = st.SetLogging(logging.TestNilLogging)

	_ = st.setHandler(newDummyStateHandler(StateStopped))

	booting := newDummyStateHandler(StateBooting)

	enterch := make(chan bool, 1)
	_ = booting.setEnter(func(StateType, switchContext) error {
		enterch <- true

		return nil
	}, nil)
	_ = st.setHandler(booting)

	t.NoError(st.Start(context.Background()))
	defer st.Stop()

	select {
	case <-time.After(time.Second):
		t.NoError(errors.Errorf("failed to enter booting"))
	case <-enterch:
		t.Equal(StateBooting, st.current().state())
	}
}

func (t *testStates) TestFailedToEnterIntoBootingAtStarting() {
	args := NewStatesArgs()
	st, err := NewStates(t.local, t.params, args)
	t.NoError(err)
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

	t.NoError(st.Start(context.Background()))
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
	args := NewStatesArgs()
	st, err := NewStates(t.local, t.params, args)
	t.NoError(err)
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
	args := NewStatesArgs()
	st, err := NewStates(t.local, t.params, args)
	t.NoError(err)
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
	t.NoError(st.AskMoveState(sctx))

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
	err := st.AskMoveState(sctx)
	t.Error(err)
	t.ErrorContains(err, "unknown next state")
}

func (t *testStates) TestNewState() {
	st, _ := t.booted()
	defer st.Stop()
	st.SetAllowConsensus(true)

	joining := newDummyStateHandler(StateJoining)

	enterch := make(chan bool, 1)
	_ = joining.setEnter(func(StateType, switchContext) error {
		enterch <- true

		return nil
	}, nil)
	_ = st.setHandler(joining)

	t.Equal(StateBooting, st.current().state())

	sctx := newDummySwitchContext(st.current().state(), StateJoining, nil)
	err := st.AskMoveState(sctx)
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
	st.SetAllowConsensus(true)

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
	t.NoError(st.AskMoveState(sctx))

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
	t.NoError(st.AskMoveState(sctx))

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
	t.NoError(st.AskMoveState(sctx))

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
	t.NoError(st.AskMoveState(sctx))

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
	st.SetAllowConsensus(true)

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

		return ErrIgnoreSwitchingState.Call()
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
	t.NoError(st.AskMoveState(sctx))

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

	sctx := newBaseErrorSwitchContext(StateBooting, StateStopped, errors.Errorf("something wrong"))
	t.NoError(st.AskMoveState(sctx))

	select {
	case <-time.After(time.Second * 3):
		t.NoError(errors.Errorf("failed to call exiting from booting"))
	case <-exitch:
	}

	<-time.After(time.Second)
	err := <-errch
	t.Error(err)
	t.ErrorContains(err, "something wrong")

	t.Equal(StateStopped, st.current().state())
	t.False(st.IsStarted())
}

func (t *testStates) TestMimicBallot() {
	local := base.RandomLocalNode()
	params := isaac.DefaultLocalParams(base.RandomNetworkID())
	remote := base.RandomLocalNode()

	point := base.RawPoint(32, 44)

	newINITBallot := func(local base.LocalNode, expels []base.SuffrageExpelOperation) base.Ballot {
		afact := isaac.NewACCEPTBallotFact(point, valuehash.RandomSHA256(), valuehash.RandomSHA256(), nil)
		afs := isaac.NewACCEPTBallotSignFact(afact)
		t.NoError(afs.NodeSign(local.Privatekey(), params.NetworkID(), local.Address()))

		avp := isaac.NewACCEPTVoteproof(afact.Point().Point)
		avp.
			SetMajority(afact).
			SetSignFacts([]base.BallotSignFact{afs}).
			SetThreshold(100).
			Finish()

		expelfacts := make([]util.Hash, len(expels))
		for i := range expels {
			expelfacts[i] = expels[i].Fact().Hash()
		}

		ifact := isaac.NewINITBallotFact(base.RawPoint(33, 44), valuehash.RandomSHA256(), valuehash.RandomSHA256(), expelfacts)
		ifs := isaac.NewINITBallotSignFact(ifact)
		t.NoError(ifs.NodeSign(local.Privatekey(), params.NetworkID(), local.Address()))

		return isaac.NewINITBallot(avp, ifs, expels)
	}

	newstates := func() (*States, <-chan error) {
		st, errch := t.booted()

		st.local = local
		st.params = params

		_ = st.setHandler(newDummyStateHandler(StateStopped))
		_ = st.setHandler(newDummyStateHandler(StateSyncing))

		return st, errch
	}

	newstatesinsyncing := func() (*States, <-chan error) {
		st, errch := newstates()
		syncinghandler := newDummyStateHandler(StateSyncing)
		_ = st.setHandler(syncinghandler)

		enterch := make(chan bool, 1)
		_ = syncinghandler.setEnter(func(StateType, switchContext) error {
			enterch <- true

			return nil
		}, nil)

		t.NoError(st.AskMoveState(newDummySwitchContext(st.current().state(), StateSyncing, nil)))
		<-enterch

		return st, errch
	}

	t.Run("ok", func() {
		st, errch := newstatesinsyncing()
		defer st.Stop()

		st.SetAllowConsensus(true)

		st.args.IsInSyncSourcePoolFunc = func(base.Address) bool { return true }

		blch := make(chan base.Ballot, 1)
		st.args.BallotBroadcaster = NewDummyBallotBroadcaster(st.local.Address(), func(bl base.Ballot) error {
			blch <- bl

			return nil
		})

		bl := newINITBallot(remote, nil)

		f, cancel := st.mimicBallotFunc()
		f(bl)
		defer cancel()

		select {
		case <-time.After(time.Second * 2):
			t.NoError(errors.Errorf("wait broadcasted ballot, but failed"))
		case err := <-errch:
			t.NoError(err)
		case newbl := <-blch:
			t.Equal(bl.Point(), newbl.Point())
		}
	})

	t.Run("not in valid states", func() {
		st, errch := t.booted()
		defer st.Stop()

		st.SetAllowConsensus(true)

		st.local = local
		st.params = params

		st.args.IsInSyncSourcePoolFunc = func(base.Address) bool { return true }

		blch := make(chan base.Ballot, 1)
		st.args.BallotBroadcaster = NewDummyBallotBroadcaster(st.local.Address(), func(bl base.Ballot) error {
			blch <- bl
			return nil
		})

		bl := newINITBallot(remote, nil)

		f, cancel := st.mimicBallotFunc()
		f(bl)
		defer cancel()

		select {
		case <-time.After(time.Second * 2):
		case err := <-errch:
			t.NoError(err)
		case <-blch:
			t.NoError(errors.Errorf("should be no broadcasted ballot, but broadcasted"))
		}
	})

	t.Run("not in sync sources", func() {
		st, errch := newstatesinsyncing()
		defer st.Stop()

		st.SetAllowConsensus(true)

		st.args.IsInSyncSourcePoolFunc = func(base.Address) bool { return false }

		blch := make(chan base.Ballot, 1)
		st.args.BallotBroadcaster = NewDummyBallotBroadcaster(st.local.Address(), func(bl base.Ballot) error {
			blch <- bl
			return nil
		})

		bl := newINITBallot(remote, nil)

		f, cancel := st.mimicBallotFunc()
		f(bl)
		defer cancel()

		select {
		case <-time.After(time.Second * 2):
		case err := <-errch:
			t.NoError(err)
		case <-blch:
			t.NoError(errors.Errorf("should be no broadcasted ballot, but broadcasted"))
		}
	})

	t.Run("different ballot signer", func() {
		st, errch := newstatesinsyncing()
		defer st.Stop()

		st.SetAllowConsensus(true)

		st.args.IsInSyncSourcePoolFunc = func(base.Address) bool { return true }

		blch := make(chan base.Ballot, 1)
		st.args.BallotBroadcaster = NewDummyBallotBroadcaster(st.local.Address(), func(bl base.Ballot) error {
			blch <- bl
			return nil
		})

		mimicBallotFunc, cancel := st.mimicBallotFunc()
		defer cancel()

		bl := newINITBallot(remote, nil)

		t.T().Log("initial ballot")
		mimicBallotFunc(bl)

		select {
		case <-time.After(time.Second * 2):
		case err := <-errch:
			t.NoError(err)
		case newbl := <-blch:
			t.Equal(bl.Point(), newbl.Point())
		}

		t.T().Log("second ballot by another node")

		anotherbl := newINITBallot(base.RandomLocalNode(), nil)
		mimicBallotFunc(anotherbl)

		select {
		case <-time.After(time.Second * 2):
		case err := <-errch:
			t.NoError(err)
		case <-blch:
			t.NoError(errors.Errorf("should be no broadcasted ballot, but broadcasted"))
		}
	})

	t.Run("local signed ballot", func() {
		st, errch := newstatesinsyncing()
		defer st.Stop()

		st.SetAllowConsensus(true)

		st.args.IsInSyncSourcePoolFunc = func(base.Address) bool { return true }

		blch := make(chan base.Ballot, 1)
		st.args.BallotBroadcaster = NewDummyBallotBroadcaster(st.local.Address(), func(bl base.Ballot) error {
			blch <- bl
			return nil
		})

		bl := newINITBallot(local, nil)

		f, cancel := st.mimicBallotFunc()
		f(bl)
		defer cancel()

		select {
		case <-time.After(time.Second * 2):
		case err := <-errch:
			t.NoError(err)
		case <-blch:
			t.NoError(errors.Errorf("should be no broadcasted ballot, but broadcasted"))
		}
	})

	t.Run("local is in expels", func() {
		st, errch := newstatesinsyncing()
		defer st.Stop()

		st.SetAllowConsensus(true)

		st.args.IsInSyncSourcePoolFunc = func(base.Address) bool { return true }

		blch := make(chan base.Ballot, 1)
		st.args.BallotBroadcaster = NewDummyBallotBroadcaster(st.local.Address(), func(bl base.Ballot) error {
			blch <- bl
			return nil
		})

		fact := isaac.NewSuffrageExpelFact(local.Address(), point.Height()-1, point.Height()+1, util.UUID().String())
		expel := isaac.NewSuffrageExpelOperation(fact)
		t.NoError(expel.NodeSign(remote.Privatekey(), params.NetworkID(), remote.Address()))

		bl := newINITBallot(remote, []base.SuffrageExpelOperation{expel})

		f, cancel := st.mimicBallotFunc()
		f(bl)
		defer cancel()

		select {
		case <-time.After(time.Second * 2):
		case err := <-errch:
			t.NoError(err)
		case <-blch:
			t.NoError(errors.Errorf("should be no broadcasted ballot, but broadcasted"))
		}
	})

	t.Run("not allow consensus", func() {
		st, errch := newstatesinsyncing()
		defer st.Stop()

		st.SetAllowConsensus(false)

		st.args.IsInSyncSourcePoolFunc = func(base.Address) bool { return true }

		blch := make(chan base.Ballot, 1)
		st.args.BallotBroadcaster = NewDummyBallotBroadcaster(st.local.Address(), func(bl base.Ballot) error {
			blch <- bl
			return nil
		})

		bl := newINITBallot(remote, nil)

		f, cancel := st.mimicBallotFunc()
		f(bl)
		defer cancel()

		select {
		case <-time.After(time.Second * 2):
		case err := <-errch:
			t.NoError(err)
		case <-blch:
			t.NoError(errors.Errorf("should be no broadcasted ballot, but broadcasted"))
		}
	})
}

func (t *testStates) TestNotAllowConsensusForConsensus() {
	st, _ := t.booted()
	defer st.Stop()

	consensushandler := newDummyStateHandler(StateConsensus)

	consensusenterch := make(chan bool, 1)
	_ = consensushandler.setEnter(func(StateType, switchContext) error {
		consensusenterch <- true

		return nil
	}, nil)
	consensusallowconsensusch := make(chan bool, 1)
	_ = consensushandler.setSetAllowConsensusf(func(allow bool) {
		consensusallowconsensusch <- allow

		if !allow {
			sctx := newDummySwitchContext(StateConsensus, StateSyncing, nil)
			t.NoError(st.AskMoveState(sctx))
		}
	})
	_ = st.setHandler(consensushandler)

	syncinghandler := newDummyStateHandler(StateSyncing)

	syncingenterch := make(chan bool, 1)
	_ = syncinghandler.setEnter(func(StateType, switchContext) error {
		syncingenterch <- true

		return nil
	}, nil)
	_ = st.setHandler(syncinghandler)

	t.Equal(StateBooting, st.current().state())

	t.Run("not allowed", func() {
		t.T().Log("current", st.current().state())

		sctx := newDummySwitchContext(st.current().state(), StateConsensus, nil)
		t.NoError(st.AskMoveState(sctx))

		select {
		case <-time.After(time.Second * 3):
			t.NoError(errors.Errorf("failed to wait"))
		case <-consensusenterch:
			t.NoError(errors.Errorf("consensus handler entered"))
		case <-syncingenterch:
			t.Equal(StateSyncing, st.current().state())
		}

		t.T().Log("current", st.current().state())
	})

	t.Run("in syncing", func() {
		t.T().Log("current", st.current().state())

		sctx := newDummySwitchContext(st.current().state(), StateConsensus, nil)
		t.NoError(st.AskMoveState(sctx))

		select {
		case <-time.After(time.Second * 3):
		case <-consensusenterch:
			t.NoError(errors.Errorf("consensus handler entered"))
		case <-syncingenterch:
			t.NoError(errors.Errorf("syncing handler entered"))
		}

		t.Equal(StateSyncing, st.current().state())

		t.T().Log("current", st.current().state())
	})

	t.Run("allowed", func() {
		t.T().Log("current", st.current().state())

		t.True(st.SetAllowConsensus(true))

		sctx := newDummySwitchContext(st.current().state(), StateConsensus, nil)
		t.NoError(st.AskMoveState(sctx))

		select {
		case <-time.After(time.Second * 3):
			t.NoError(errors.Errorf("failed to wait"))
		case <-consensusenterch:
			t.Equal(StateConsensus, st.current().state())
		}

		t.T().Log("current", st.current().state())
	})

	t.Run("set not allowed", func() {
		t.T().Log("current", st.current().state())

		t.True(st.SetAllowConsensus(false))

		select {
		case <-time.After(time.Second * 3):
			t.NoError(errors.Errorf("failed to wait"))
		case allow := <-consensusallowconsensusch:
			t.False(allow)
		}

		select {
		case <-time.After(time.Second * 3):
			t.NoError(errors.Errorf("failed to wait"))
		case <-syncingenterch:
			t.Equal(StateSyncing, st.current().state())
		}

		t.T().Log("current", st.current().state())
	})
}

func (t *testStates) TestNotAllowConsensusForJoining() {
	st, _ := t.booted()
	defer st.Stop()

	joininghandler := newDummyStateHandler(StateJoining)

	joiningenterch := make(chan bool, 1)
	_ = joininghandler.setEnter(func(StateType, switchContext) error {
		joiningenterch <- true

		return nil
	}, nil)
	joiningallowconsensusch := make(chan bool, 1)
	_ = joininghandler.setSetAllowConsensusf(func(allow bool) {
		joiningallowconsensusch <- allow

		if !allow {
			sctx := newDummySwitchContext(StateJoining, StateSyncing, nil)
			t.NoError(st.AskMoveState(sctx))
		}
	})

	_ = st.setHandler(joininghandler)

	syncinghandler := newDummyStateHandler(StateSyncing)

	syncingenterch := make(chan bool, 1)
	_ = syncinghandler.setEnter(func(StateType, switchContext) error {
		syncingenterch <- true

		return nil
	}, nil)
	_ = st.setHandler(syncinghandler)

	t.Equal(StateBooting, st.current().state())

	t.Run("not allowed", func() {
		t.T().Log("current", st.current().state())
		t.False(st.AllowedConsensus())

		sctx := newDummySwitchContext(st.current().state(), StateJoining, nil)
		t.NoError(st.AskMoveState(sctx))

		select {
		case <-time.After(time.Second * 3):
			t.NoError(errors.Errorf("failed to wait"))
		case <-joiningenterch:
			t.NoError(errors.Errorf("joining handler entered"))
		case <-syncingenterch:
			t.Equal(StateSyncing, st.current().state())
		}

		t.T().Log("current", st.current().state())
	})

	t.Run("in syncing", func() {
		t.T().Log("current", st.current().state())

		t.False(st.AllowedConsensus())

		sctx := newDummySwitchContext(st.current().state(), StateJoining, nil)
		t.NoError(st.AskMoveState(sctx))

		select {
		case <-time.After(time.Second * 3):
		case <-joiningenterch:
			t.NoError(errors.Errorf("joining handler entered"))
		case <-syncingenterch:
			t.NoError(errors.Errorf("syncing handler entered"))
		}

		t.T().Log("current", st.current().state())

		t.Equal(StateSyncing, st.current().state())
	})

	t.Run("allowed", func() {
		t.T().Log("current", st.current().state())

		t.True(st.SetAllowConsensus(true))

		sctx := newDummySwitchContext(st.current().state(), StateJoining, nil)
		t.NoError(st.AskMoveState(sctx))

		select {
		case <-time.After(time.Second * 3):
			t.NoError(errors.Errorf("failed to wait"))
		case <-joiningenterch:
			t.Equal(StateJoining, st.current().state())
		}

		t.T().Log("current", st.current().state())
	})

	t.Run("set not allowed", func() {
		t.T().Log("current", st.current().state())

		t.True(st.SetAllowConsensus(false))

		select {
		case <-time.After(time.Second * 3):
			t.NoError(errors.Errorf("failed to wait"))
		case allow := <-joiningallowconsensusch:
			t.False(allow)
		}

		select {
		case <-time.After(time.Second * 3):
			t.NoError(errors.Errorf("failed to wait"))
		case <-syncingenterch:
			t.Equal(StateSyncing, st.current().state())
		}

		t.T().Log("current", st.current().state())
	})
}

func (t *testStates) TestSwitchHandover() {
	st, _ := t.booted()
	defer st.Stop()

	handoverhandler := newDummyStateHandler(StateHandover)

	handoverenterch := make(chan switchContext, 1)
	_ = handoverhandler.setEnter(func(_ StateType, sctx switchContext) error {
		handoverenterch <- sctx

		return nil
	}, nil)

	_ = st.setHandler(handoverhandler)

	syncinghandler := newDummyStateHandler(StateSyncing)

	syncingenterch := make(chan bool, 1)
	_ = syncinghandler.setEnter(func(StateType, switchContext) error {
		syncingenterch <- true

		return nil
	}, nil)
	_ = st.setHandler(syncinghandler)

	consensushandler := newDummyStateHandler(StateConsensus)

	consensusenterch := make(chan bool, 1)
	_ = consensushandler.setEnter(func(StateType, switchContext) error {
		consensusenterch <- true

		return nil
	}, nil)
	_ = st.setHandler(consensushandler)

	t.Equal(StateBooting, st.current().state())

	entersyncing := func(m string) {
		t.T().Log(m)

		ssctx := newDummySwitchContext(st.current().state(), StateSyncing, nil)
		t.NoError(st.AskMoveState(ssctx))

		select {
		case <-time.After(time.Second * 3):
			t.NoError(errors.Errorf("failed to wait"))
		case <-syncingenterch:
			t.Equal(StateSyncing, st.current().state())
		}
	}

	t.Run("under alowed consensus", func() {
		t.T().Log("current", st.current().state())

		t.True(st.SetAllowConsensus(true))
		t.True(st.AllowedConsensus())

		sctx := newDummySwitchContext(st.current().state(), StateHandover, nil)
		t.NoError(st.AskMoveState(sctx))

		select {
		case <-time.After(time.Second * 2):
		case <-handoverenterch:
			t.NoError(errors.Errorf("entered handover; but allowed consensus"))
		}

		t.T().Log("current", st.current().state())
	})

	t.Run("not under handover", func() {
		t.T().Log("current", st.current().state())

		t.True(st.SetAllowConsensus(false))
		t.False(st.AllowedConsensus())

		entersyncing("enter syncing")

		nsctx := newDummySwitchContext(st.current().state(), StateHandover, nil)
		t.NoError(st.AskMoveState(nsctx))

		select {
		case <-time.After(time.Second * 3):
		case <-handoverenterch:
			t.NoError(errors.Errorf("entered handover; but under handover"))
		}

		t.T().Log("current", st.current().state())
	})

	t.Run("from syncing", func() {
		t.T().Log("current", st.current().state())

		_ = st.SetAllowConsensus(false)
		t.False(st.AllowedConsensus())

		t.T().Log("set under handover")
		t.NoError(st.StartHandoverY())
		t.True(st.UnderHandoverY())

		nsctx := newDummySwitchContext(st.current().state(), StateHandover, nil)
		t.NoError(st.AskMoveState(nsctx))

		select {
		case <-time.After(time.Second * 3):
			t.NoError(errors.Errorf("failed to wait"))
		case <-handoverenterch:
			t.Equal(StateHandover, st.current().state())
		}

		t.T().Log("current", st.current().state())
	})

	t.Run("under handover", func() {
		t.T().Log("current", st.current().state())

		_ = st.SetAllowConsensus(false)
		t.False(st.AllowedConsensus())

		_ = st.StartHandoverY()
		t.True(st.UnderHandoverY())

		entersyncing("enter syncing")

		t.T().Log("trying to enter consensus")
		ivp := isaac.NewINITVoteproof(base.RawPoint(32, 44))

		sctx, err := newConsensusSwitchContext(st.current().state(), ivp)
		t.NoError(err)
		t.NoError(st.AskMoveState(sctx))

		select {
		case <-time.After(time.Second * 3):
		case <-consensusenterch:
			t.NoError(errors.Errorf("entered consensus; but under handover"))
		}

		select {
		case <-time.After(time.Second * 3):
			t.NoError(errors.Errorf("failed to wait"))
		case sctx := <-handoverenterch:
			t.T().Log("entered handover")
			t.Equal(StateHandover, st.current().state())

			hsctx, ok := sctx.(handoverSwitchContext)
			t.True(ok)

			t.True(hsctx.vp.Point().Equal(ivp.Point()))
		}

		t.T().Log("current", st.current().state())
	})
}

func TestStates(t *testing.T) {
	suite.Run(t, new(testStates))
}
