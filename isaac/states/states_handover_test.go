package isaacstates

import (
	"context"
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
			t.Fail("failed to wait")
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
			t.Fail("entered handover; but allowed consensus")
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
			t.Fail("entered handover; but under handover")
		}

		t.T().Log("current", st.current().state())
	})

	st.args.NewHandoverYBroker = func(ctx context.Context, connInfo quicstream.ConnInfo) (*HandoverYBroker, error) {
		args := NewHandoverYBrokerArgs(t.params.NetworkID())
		args.AskRequestFunc = func(context.Context, quicstream.ConnInfo) (string, bool, error) {
			return util.UUID().String(), false, nil
		}
		args.SyncDataFunc = func(_ context.Context, _ quicstream.ConnInfo, readych chan<- struct{}) error {
			readych <- struct{}{}

			return nil
		}

		return NewHandoverYBroker(ctx, args, connInfo), nil
	}

	t.Run("from syncing, but not yet asked", func() {
		t.T().Log("current", st.current().state())

		_ = st.SetAllowConsensus(false)
		t.False(st.AllowedConsensus())

		t.T().Log("set under handover")
		t.NoError(st.NewHandoverYBroker(quicstream.ConnInfo{}))

		broker := st.HandoverYBroker()
		t.NotNil(broker)
		t.False(broker.IsAsked())

		nsctx := newDummySwitchContext(st.current().state(), StateHandover, nil)
		t.NoError(st.AskMoveState(nsctx))

		select {
		case <-time.After(time.Second * 3):
		case <-handoverenterch:
			t.Fail("unexpected swithching")
		}

		t.T().Log("current", st.current().state())
		t.Equal(StateSyncing, st.current().state())
	})

	t.Run("from syncing", func() {
		t.T().Log("current", st.current().state())

		_ = st.SetAllowConsensus(false)
		t.False(st.AllowedConsensus())

		t.T().Log("set under handover")

		broker := st.HandoverYBroker()
		t.NotNil(broker)

		t.False(t.ensureHandoverYBrokerAsk(broker))

		nsctx := newDummySwitchContext(st.current().state(), StateHandover, nil)
		t.NoError(st.AskMoveState(nsctx))

		select {
		case <-time.After(time.Second * 3):
			t.Fail("failed to wait")
		case <-handoverenterch:
			t.Equal(StateHandover, st.current().state())
		}

		t.T().Log("current", st.current().state())
	})

	t.Run("under handover", func() {
		t.T().Log("current", st.current().state())

		_ = st.SetAllowConsensus(false)
		t.False(st.AllowedConsensus())

		_ = st.NewHandoverYBroker(quicstream.ConnInfo{})
		t.NotNil(st.HandoverYBroker())

		entersyncing("enter syncing")

		t.T().Log("trying to enter consensus")
		ivp := isaac.NewINITVoteproof(base.RawPoint(32, 44))

		sctx, err := newConsensusSwitchContext(st.current().state(), ivp)
		t.NoError(err)
		t.NoError(st.AskMoveState(sctx))

		select {
		case <-time.After(time.Second * 3):
		case <-consensusenterch:
			t.Fail("entered consensus; but under handover")
		}

		select {
		case <-time.After(time.Second * 3):
			t.Fail("failed to wait")
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

func (t *testStates) newHandoverXBrokerFunc(
	st *States,
	local base.Node,
	networkID base.NetworkID,
) func(context.Context, quicstream.ConnInfo) (*HandoverXBroker, error) {
	return func(ctx context.Context, connInfo quicstream.ConnInfo) (*HandoverXBroker, error) {
		args := NewHandoverXBrokerArgs(local, networkID)
		args.SendMessageFunc = func(context.Context, quicstream.ConnInfo, HandoverMessage) error { return nil }

		return NewHandoverXBroker(ctx, args, connInfo), nil
	}
}

func (t *testStates) newHandoverYBrokerFunc(
	st *States,
	networkID base.NetworkID,
) func(context.Context, quicstream.ConnInfo) (*HandoverYBroker, error) {
	return func(ctx context.Context, connInfo quicstream.ConnInfo) (*HandoverYBroker, error) {
		args := NewHandoverYBrokerArgs(networkID)
		args.SendMessageFunc = func(context.Context, quicstream.ConnInfo, HandoverMessage) error { return nil }
		args.SyncDataFunc = func(_ context.Context, _ quicstream.ConnInfo, readych chan<- struct{}) error {
			readych <- struct{}{}

			return nil
		}
		args.AskRequestFunc = func(context.Context, quicstream.ConnInfo) (string, bool, error) {
			return util.UUID().String(), false, nil
		}

		return NewHandoverYBroker(ctx, args, connInfo), nil
	}
}

func (t *testStates) ensureHandoverYBrokerAsk(broker *HandoverYBroker) bool {
	ticker := time.NewTicker(time.Millisecond * 33)
	defer ticker.Stop()

	for range ticker.C {
		if synced, _ := broker.isReadyToAsk.Value(); synced {
			break
		}
	}

	canMoveConsensus, isAsked, err := broker.Ask()
	t.NoError(err)
	t.True(isAsked)
	t.True(broker.IsAsked())

	return canMoveConsensus
}

func (t *testStates) TestNewHandoverXBroker() {
	t.Run("start handover x broker; not allowed consensus", func() {
		st, _ := t.booted()
		defer st.Stop()

		_, err := st.NewHandoverXBroker(quicstream.ConnInfo{})
		t.Error(err)
		t.ErrorContains(err, "not allowed consensus")
	})

	t.Run("start handover x broker", func() {
		st, _ := t.booted()
		defer st.Stop()

		_ = st.SetAllowConsensus(true)
		st.args.NewHandoverXBroker = t.newHandoverXBrokerFunc(st, t.local, t.params.NetworkID())

		handoverid, err := st.NewHandoverXBroker(quicstream.ConnInfo{})
		t.NoError(err)
		t.NotEmpty(handoverid)
		t.NotNil(st.HandoverXBroker())

		t.Run("start again", func() {
			_, err := st.NewHandoverXBroker(quicstream.ConnInfo{})
			t.Error(err)
			t.ErrorContains(err, "already under handover x")
		})
	})

	t.Run("cancel handover x broker", func() {
		st, _ := t.booted()
		defer st.Stop()

		_ = st.SetAllowConsensus(true)
		st.args.NewHandoverXBroker = t.newHandoverXBrokerFunc(st, t.local, t.params.NetworkID())

		_, err := st.NewHandoverXBroker(quicstream.ConnInfo{})
		t.NoError(err)

		broker := st.HandoverXBroker()
		t.NotNil(broker)

		broker.cancel(nil)

		t.Nil(st.HandoverXBroker())
	})

	t.Run("start handover x broker, but y broker exists", func() {
		st, _ := t.booted()
		defer st.Stop()

		_ = st.SetAllowConsensus(false)
		st.args.NewHandoverXBroker = t.newHandoverXBrokerFunc(st, t.local, t.params.NetworkID())
		st.args.NewHandoverYBroker = t.newHandoverYBrokerFunc(st, t.params.NetworkID())

		t.NoError(st.NewHandoverYBroker(quicstream.ConnInfo{}))
		t.NotNil(st.HandoverYBroker())

		st.allowedConsensus.SetValue(true)

		_, err := st.NewHandoverXBroker(quicstream.ConnInfo{})
		t.Error(err)
		t.ErrorContains(err, "under handover y")
	})

	t.Run("finish", func() {
		st, _ := t.booted()
		defer st.Stop()

		syncinghandler := newDummyStateHandler(StateSyncing)

		syncingenterch := make(chan bool, 1)
		_ = syncinghandler.setEnter(func(StateType, switchContext) error {
			syncingenterch <- true

			return nil
		}, nil)
		_ = st.setHandler(syncinghandler)

		_ = st.SetAllowConsensus(true)
		st.args.NewHandoverXBroker = t.newHandoverXBrokerFunc(st, t.local, t.params.NetworkID())

		_, err := st.NewHandoverXBroker(quicstream.ConnInfo{})
		t.NoError(err)

		broker := st.HandoverXBroker()
		t.NotNil(broker)
		broker.args.CleanAfter = 0

		ivp := isaac.NewINITVoteproof(base.RawPoint(32, 44))
		t.NoError(broker.finish(ivp, nil))

		t.False(st.AllowedConsensus())

		<-time.After(time.Millisecond * 33)
		t.Nil(st.HandoverXBroker())

		t.T().Log("switching to syncing state")

		select {
		case <-time.After(time.Second * 3):
			t.Fail("failed to wait syncing state")
		case <-syncingenterch:
			t.Equal(StateSyncing, st.current().state())
		}
	})

	t.Run("under handover x, but out of consensus state", func() {
		st, _ := t.booted()
		defer st.Stop()
		_ = st.SetLogging(logging.TestNilLogging)

		t.T().Log("current", st.current().state())

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

		_ = st.SetAllowConsensus(true)

		t.Run("enter consensus state", func() {
			ivp := isaac.NewINITVoteproof(base.RawPoint(32, 44))

			sctx, err := newConsensusSwitchContext(st.current().state(), ivp)
			t.NoError(err)
			t.NoError(st.AskMoveState(sctx))

			select {
			case <-time.After(time.Second * 3):
				t.Fail("failed to enter consensus")
			case <-consensusenterch:
			}

			t.T().Log("current", st.current().state())
		})

		t.Run("under handover x", func() {
			st.args.NewHandoverXBroker = t.newHandoverXBrokerFunc(st, t.local, t.params.NetworkID())

			_, err := st.NewHandoverXBroker(quicstream.ConnInfo{})
			t.NoError(err)

			broker := st.HandoverXBroker()
			t.NotNil(broker)

			_ = broker.SetLogging(logging.TestNilLogging)
		})

		t.Run("moves to syncing state", func() {
			broker := st.HandoverXBroker()
			t.NotNil(broker)

			fch := make(chan struct{}, 1)
			broker.args.WhenFinished = func(string, base.INITVoteproof, base.Address, quicstream.ConnInfo) error {
				fch <- struct{}{}

				return nil
			}

			t.NoError(st.AskMoveState(newSyncingSwitchContext(st.current().state(), 0)))

			select {
			case <-time.After(time.Second * 3):
				t.Fail("failed to wait syncing state")
			case <-syncingenterch:
				t.Equal(StateSyncing, st.current().state())
			}

			t.T().Log("broker x finished")

			select {
			case <-time.After(time.Second * 2):
				t.Fail("failed to finish")
			case <-fch:
				isfinished, _ := broker.isFinishedLocked.Value()
				t.True(isfinished)
			}
		})
	})
}

func (t *testStates) TestYBrokerAskCanMoveConsensus() {
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

	t.T().Log("enter syncing")

	ssctx := newDummySwitchContext(st.current().state(), StateSyncing, nil)
	t.NoError(st.AskMoveState(ssctx))

	select {
	case <-time.After(time.Second * 3):
		t.Fail("failed to wait")
	case <-syncingenterch:
		t.T().Log("current", st.current().state())

		t.Equal(StateSyncing, st.current().state())
	}

	_ = st.SetAllowConsensus(false)
	t.False(st.AllowedConsensus())

	t.T().Log("set under handover")
	st.args.NewHandoverYBroker = func(ctx context.Context, connInfo quicstream.ConnInfo) (*HandoverYBroker, error) {
		args := NewHandoverYBrokerArgs(t.params.NetworkID())
		args.AskRequestFunc = func(context.Context, quicstream.ConnInfo) (string, bool, error) {
			return util.UUID().String(), true, nil
		}
		args.SyncDataFunc = func(_ context.Context, _ quicstream.ConnInfo, readych chan<- struct{}) error {
			readych <- struct{}{}

			return nil
		}

		broker := NewHandoverYBroker(ctx, args, connInfo)

		ticker := time.NewTicker(time.Millisecond * 33)
		defer ticker.Stop()

		for range ticker.C {
			if synced, _ := broker.isReadyToAsk.Value(); synced {
				break
			}
		}

		return broker, nil
	}

	t.NoError(st.NewHandoverYBroker(quicstream.ConnInfo{}))

	broker := st.HandoverYBroker()

	canMoveConsensus, isAsked, err := broker.Ask()
	t.NoError(err)
	t.True(isAsked)
	t.True(canMoveConsensus)
	t.True(broker.IsAsked())

	t.T().Log("current", st.current().state())
	t.Equal(StateSyncing, st.current().state())
}

func (t *testStates) TestNewHandoverYBroker() {
	t.Run("start handover y broker; not allowed consensus", func() {
		st, _ := t.booted()
		defer st.Stop()

		_ = st.SetAllowConsensus(true)

		err := st.NewHandoverYBroker(quicstream.ConnInfo{})
		t.Error(err)
		t.ErrorContains(err, "allowed consensus")
	})

	t.Run("start handover y broker", func() {
		st, _ := t.booted()
		defer st.Stop()

		st.args.NewHandoverYBroker = t.newHandoverYBrokerFunc(st, t.params.NetworkID())

		t.NoError(st.NewHandoverYBroker(quicstream.ConnInfo{}))
		t.NotNil(st.HandoverYBroker())

		t.Run("start again", func() {
			err := st.NewHandoverYBroker(quicstream.ConnInfo{})
			t.Error(err)
			t.ErrorContains(err, "already under handover y")
		})
	})

	t.Run("cancel handover y broker", func() {
		st, _ := t.booted()
		defer st.Stop()

		syncinghandler := newDummyStateHandler(StateSyncing)

		syncingenterch := make(chan bool, 1)
		_ = syncinghandler.setEnter(func(StateType, switchContext) error {
			syncingenterch <- true

			return nil
		}, nil)
		_ = st.setHandler(syncinghandler)

		st.args.NewHandoverYBroker = t.newHandoverYBrokerFunc(st, t.params.NetworkID())

		t.NoError(st.NewHandoverYBroker(quicstream.ConnInfo{}))

		broker := st.HandoverYBroker()
		t.NotNil(broker)

		broker.cancel(nil)

		t.Nil(st.HandoverYBroker())

		t.T().Log("switching to syncing state")

		select {
		case <-time.After(time.Second * 3):
			t.Fail("failed to wait syncing state")
		case <-syncingenterch:
			t.Equal(StateSyncing, st.current().state())
		}
	})

	t.Run("start handover y broker, but y broker exists", func() {
		st, _ := t.booted()
		defer st.Stop()

		st.args.NewHandoverXBroker = t.newHandoverXBrokerFunc(st, t.local, t.params.NetworkID())
		st.args.NewHandoverYBroker = t.newHandoverYBrokerFunc(st, t.params.NetworkID())

		_ = st.SetAllowConsensus(true)

		_, err := st.NewHandoverXBroker(quicstream.ConnInfo{})
		t.NoError(err)
		t.NotNil(st.HandoverXBroker())

		_ = st.allowedConsensus.SetValue(false)

		err = st.NewHandoverYBroker(quicstream.ConnInfo{})
		t.Error(err)
		t.ErrorContains(err, "under handover x")
	})

	t.Run("finished with empty voteproof", func() {
		st, _ := t.booted()
		defer st.Stop()

		syncinghandler := newDummyStateHandler(StateSyncing)

		syncingenterch := make(chan bool, 1)
		_ = syncinghandler.setEnter(func(StateType, switchContext) error {
			syncingenterch <- true

			return nil
		}, nil)
		_ = st.setHandler(syncinghandler)

		st.args.NewHandoverYBroker = t.newHandoverYBrokerFunc(st, t.params.NetworkID())

		t.NoError(st.NewHandoverYBroker(quicstream.ConnInfo{}))

		broker := st.HandoverYBroker()
		t.NotNil(broker)

		t.False(t.ensureHandoverYBrokerAsk(broker))

		fch := make(chan struct{}, 1)
		broker.args.WhenFinished = func(string, base.INITVoteproof, quicstream.ConnInfo) error {
			fch <- struct{}{}

			return nil
		}

		hc := newHandoverMessageFinish(broker.ID(), nil, nil)
		t.NoError(broker.receiveFinish(hc))

		select {
		case <-time.After(time.Second * 3):
			t.Fail("failed to finish")
		case <-fch:
		}

		t.NotNil(st.HandoverYBroker())

		t.T().Log("switching to syncing state")

		select {
		case <-time.After(time.Second * 3):
			t.Fail("failed to wait syncing state")
		case <-syncingenterch:
			t.Equal(StateSyncing, st.current().state())
		}

		t.False(st.AllowedConsensus())
		t.NotNil(st.HandoverYBroker())
	})

	t.Run("finished with voteproof", func() {
		st, _ := t.booted()
		defer st.Stop()

		st.args.NewHandoverYBroker = t.newHandoverYBrokerFunc(st, t.params.NetworkID())

		t.NoError(st.NewHandoverYBroker(quicstream.ConnInfo{}))

		broker := st.HandoverYBroker()
		t.NotNil(broker)

		ivp := isaac.NewINITVoteproof(base.RawPoint(32, 44))

		hc := newHandoverMessageFinish(broker.ID(), ivp, nil)
		t.NoError(broker.receiveFinish(hc))

		// NOTE 'not allowed consensus' and empty HandoverYBroker will be
		// changed by handover (or syncing) handler.
		t.False(st.AllowedConsensus())
		t.NotNil(st.HandoverYBroker())
	})
}

func (t *testStates) TestSetAllowConsensusCancelHandoverBrokers() {
	t.Run("handover x broker; not allowed consensus", func() {
		st, _ := t.booted()
		defer st.Stop()

		st.args.NewHandoverXBroker = t.newHandoverXBrokerFunc(st, t.local, t.params.NetworkID())

		_ = st.SetAllowConsensus(true)

		_, err := st.NewHandoverXBroker(quicstream.ConnInfo{})
		t.NoError(err)

		_ = st.SetAllowConsensus(false)

		t.Nil(st.HandoverXBroker())
	})

	t.Run("handover y broker; allowed consensus", func() {
		st, _ := t.booted()
		defer st.Stop()

		syncinghandler := newDummyStateHandler(StateSyncing)
		_ = st.setHandler(syncinghandler)

		st.args.NewHandoverYBroker = t.newHandoverYBrokerFunc(st, t.params.NetworkID())

		_ = st.SetAllowConsensus(false)

		t.NoError(st.NewHandoverYBroker(quicstream.ConnInfo{}))

		_ = st.SetAllowConsensus(true)

		t.Nil(st.HandoverYBroker())
	})
}

type testHandoverFuncs struct {
	suite.Suite
	local  base.LocalNode
	params *isaac.Params
	xci    quicstream.ConnInfo
	yci    quicstream.ConnInfo
}

func (t *testHandoverFuncs) SetupSuite() {
	t.local = base.RandomLocalNode()
	t.params = isaac.DefaultParams(base.RandomNetworkID())

	t.xci = quicstream.RandomConnInfo()
	t.yci = quicstream.RandomConnInfo()
}

func (t *testHandoverFuncs) TestStart() {
	var allowedConsensus, handoverStarted bool
	var checkXErr, addSyncSourceErr, startHandoverYErr error

	f := NewStartHandoverYFunc(
		t.local.Address(),
		t.yci,
		func() bool { return allowedConsensus },
		func() bool { return handoverStarted },
		func(context.Context, base.Address, quicstream.ConnInfo) error { return checkXErr },
		func(base.Address, quicstream.ConnInfo) error { return addSyncSourceErr },
		func(quicstream.ConnInfo) error { return startHandoverYErr },
	)

	ctx := context.Background()

	t.Run("address not matched", func() {
		err := f(ctx, base.RandomAddress(""), t.xci)
		t.Error(err)
		t.ErrorContains(err, "address not matched")
	})

	t.Run("same conn info", func() {
		err := f(ctx, t.local.Address(), t.yci)
		t.Error(err)
		t.ErrorContains(err, "same conn info")
	})

	t.Run("allowed consensus", func() {
		allowedConsensus = true
		defer func() {
			allowedConsensus = false
		}()

		err := f(ctx, t.local.Address(), t.xci)
		t.Error(err)
		t.ErrorContains(err, "allowed consensus")
	})

	t.Run("handover started", func() {
		handoverStarted = true
		defer func() {
			handoverStarted = false
		}()

		err := f(ctx, t.local.Address(), t.xci)
		t.Error(err)
		t.ErrorContains(err, "handover already started")
	})

	t.Run("check x", func() {
		checkXErr = errors.Errorf("hohoho")
		defer func() {
			checkXErr = nil
		}()

		err := f(ctx, t.local.Address(), t.xci)
		t.Error(err)
		t.ErrorContains(err, "hohoho")
	})

	t.Run("add sync source", func() {
		addSyncSourceErr = errors.Errorf("hehehe")
		defer func() {
			addSyncSourceErr = nil
		}()

		err := f(ctx, t.local.Address(), t.xci)
		t.Error(err)
		t.ErrorContains(err, "hehehe")
	})

	t.Run("start handover y", func() {
		startHandoverYErr = errors.Errorf("hihihi")
		defer func() {
			startHandoverYErr = nil
		}()

		err := f(ctx, t.local.Address(), t.xci)
		t.Error(err)
		t.ErrorContains(err, "hihihi")
	})

	t.Run("ok", func() {
		t.NoError(f(ctx, t.local.Address(), t.xci))
	})
}

func (t *testHandoverFuncs) TestCheck() {
	var joinedMemberlistErr error

	allowedConsensus := true
	handoverStarted := false
	isJoinedMemberlist := true
	currentState := StateConsensus

	f := NewCheckHandoverFunc(
		t.local.Address(),
		t.xci,
		func() bool { return allowedConsensus },
		func() bool { return handoverStarted },
		func() (bool, error) { return isJoinedMemberlist, joinedMemberlistErr },
		func() StateType { return currentState },
	)

	ctx := context.Background()

	t.Run("address not matched", func() {
		err := f(ctx, base.RandomAddress(""), t.yci)
		t.Error(err)
		t.ErrorContains(err, "address not matched")
	})

	t.Run("same conn info", func() {
		err := f(ctx, t.local.Address(), t.xci)
		t.Error(err)
		t.ErrorContains(err, "same conn info")
	})

	t.Run("allowed consensus", func() {
		allowedConsensus = false
		defer func() {
			allowedConsensus = true
		}()

		err := f(ctx, t.local.Address(), t.yci)
		t.Error(err)
		t.ErrorContains(err, "not allowed consensus")
	})

	t.Run("handover started", func() {
		handoverStarted = true
		defer func() {
			handoverStarted = false
		}()

		err := f(ctx, t.local.Address(), t.yci)
		t.Error(err)
		t.ErrorContains(err, "handover already started")
	})

	t.Run("not joined memberlist", func() {
		isJoinedMemberlist = false
		defer func() {
			isJoinedMemberlist = true
		}()

		err := f(ctx, t.local.Address(), t.yci)
		t.Error(err)
		t.ErrorContains(err, "not joined memberlist")
	})

	t.Run("error check joined memberlist", func() {
		joinedMemberlistErr = errors.Errorf("hehehe")
		defer func() {
			joinedMemberlistErr = nil
		}()

		err := f(ctx, t.local.Address(), t.yci)
		t.Error(err)
		t.ErrorContains(err, "hehehe")
	})

	t.Run("not valid state", func() {
		currentState = StateBroken
		defer func() {
			currentState = StateConsensus
		}()

		err := f(ctx, t.local.Address(), t.yci)
		t.Error(err)
		t.ErrorContains(err, "not valid state")
	})

	t.Run("ok", func() {
		t.NoError(f(ctx, t.local.Address(), t.yci))
	})
}

func (t *testHandoverFuncs) TestAsk() {
	var joinMemberlistErr, sendErr error
	var canMoveConsensus bool

	handoverid := util.UUID().String()

	f := NewAskHandoverFunc(
		t.local.Address(),
		func(context.Context, quicstream.ConnInfo) error { return joinMemberlistErr },
		func(context.Context, base.Address, quicstream.ConnInfo) (string, bool, error) {
			return handoverid, canMoveConsensus, sendErr
		},
	)

	ctx := context.Background()

	t.Run("ok", func() {
		rid, rcanMoveConsensus, err := f(ctx, t.xci)
		t.NoError(err)
		t.False(rcanMoveConsensus)
		t.Equal(handoverid, rid)
	})

	t.Run("ok; can move consensus", func() {
		canMoveConsensus = true
		defer func() {
			canMoveConsensus = false
		}()

		rid, rcanMoveConsensus, err := f(ctx, t.xci)
		t.NoError(err)
		t.True(rcanMoveConsensus)
		t.Equal(handoverid, rid)
	})
}

func (t *testHandoverFuncs) TestAskReceived() {
	var joinedMemberlistErr, startHandoverXErr error

	isJoinedMemberlist := true
	isHandoverStarted := false
	currentState := StateConsensus

	allowedConsensus := true

	setallowedch := make(chan struct{}, 1)
	f := NewAskHandoverReceivedFunc(
		t.local.Address(),
		t.xci,
		func() bool { return allowedConsensus },
		func() bool { return isHandoverStarted },
		func(quicstream.ConnInfo) (bool, error) { return isJoinedMemberlist, joinedMemberlistErr },
		func() StateType { return currentState },
		func() {
			setallowedch <- struct{}{}
		},
		func(quicstream.ConnInfo) (string, error) { return util.UUID().String(), startHandoverXErr },
	)

	ctx := context.Background()

	t.Run("address not matched", func() {
		_, _, err := f(ctx, base.RandomAddress(""), t.yci)
		t.Error(err)
		t.ErrorContains(err, "address not matched")
	})

	t.Run("same conn info", func() {
		_, _, err := f(ctx, t.local.Address(), t.xci)
		t.Error(err)
		t.ErrorContains(err, "same conn info")
	})

	t.Run("not allowed consensus", func() {
		allowedConsensus = false
		defer func() {
			allowedConsensus = true
		}()

		handoverid, canMoveConsensus, err := f(ctx, t.local.Address(), t.yci)
		t.NoError(err)
		t.Empty(handoverid)
		t.True(canMoveConsensus)
	})

	t.Run("handover started", func() {
		isHandoverStarted = true
		defer func() {
			isHandoverStarted = false
		}()

		_, _, err := f(ctx, t.local.Address(), t.yci)
		t.Error(err)
		t.ErrorContains(err, "handover already started")
	})

	t.Run("not join memberlist", func() {
		isJoinedMemberlist = false
		defer func() {
			isJoinedMemberlist = true
		}()

		_, _, err := f(ctx, t.local.Address(), t.yci)
		t.Error(err)
		t.ErrorContains(err, "not joined memberlist")

		t.Run("error", func() {
			joinedMemberlistErr = errors.Errorf("hihihi")
			defer func() {
				joinedMemberlistErr = nil
			}()

			_, _, err := f(ctx, t.local.Address(), t.yci)
			t.Error(err)
			t.ErrorContains(err, "hihihi")
		})
	})

	t.Run("not in some state", func() {
		currentState = StateSyncing
		defer func() {
			currentState = StateConsensus
		}()

		handoverid, canMoveConsensus, err := f(ctx, t.local.Address(), t.yci)
		t.NoError(err)
		t.Empty(handoverid)
		t.True(canMoveConsensus)

		select {
		case <-time.After(time.Second * 2):
			t.Fail("failed to wait set allow consensus")
		case <-setallowedch:
		}
	})

	t.Run("failed to start handover x", func() {
		startHandoverXErr = errors.Errorf("hohoho")
		defer func() {
			startHandoverXErr = nil
		}()

		_, _, err := f(ctx, t.local.Address(), t.yci)
		t.Error(err)
		t.ErrorContains(err, "hohoho")
	})

	t.Run("ok", func() {
		handoverid, canMoveConsensus, err := f(ctx, t.local.Address(), t.yci)
		t.NoError(err)
		t.NotEmpty(handoverid)
		t.False(canMoveConsensus)
	})
}

func (t *testHandoverFuncs) TestHandoverXFinished() {
	var leftMemberlistErr error

	f := NewHandoverXFinishedFunc(
		func() error { return leftMemberlistErr },
		func(base.Address, quicstream.ConnInfo) error { return nil },
	)

	t.Run("ok", func() {
		t.NoError(f(nil, nil, quicstream.ConnInfo{}))
	})

	t.Run("error", func() {
		leftMemberlistErr = errors.Errorf("hohoho")
		defer func() {
			leftMemberlistErr = nil
		}()

		err := f(nil, nil, quicstream.ConnInfo{})
		t.Error(err)
		t.ErrorContains(err, "hohoho")
	})
}

func (t *testHandoverFuncs) TestHandoverYFinished() {
	var removeSyncSourceErr error

	f := NewHandoverYFinishedFunc(
		func(quicstream.ConnInfo) error { return removeSyncSourceErr },
	)

	t.Run("ok", func() {
		t.NoError(f(nil, quicstream.ConnInfo{}))
	})

	t.Run("error remove sync source", func() {
		removeSyncSourceErr = errors.Errorf("hehehe")
		defer func() {
			removeSyncSourceErr = nil
		}()

		err := f(nil, quicstream.ConnInfo{})
		t.Error(err)
		t.ErrorContains(err, "hehehe")
	})
}

func TestHandoverFuncs(t *testing.T) {
	suite.Run(t, new(testHandoverFuncs))
}
