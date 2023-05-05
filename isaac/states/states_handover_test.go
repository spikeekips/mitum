package isaacstates

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util"
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

		st.args.NewHandoverYBroker = func(ctx context.Context, id string) (*HandoverYBroker, error) {
			args := NewHandoverYBrokerArgs(t.params.NetworkID())
			return NewHandoverYBroker(ctx, args, id), nil
		}

		_ = st.SetAllowConsensus(false)
		t.False(st.AllowedConsensus())

		t.T().Log("set under handover")
		t.NoError(st.NewHandoverYBroker(util.UUID().String()))
		t.NotNil(st.HandoverYBroker())

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

		_ = st.NewHandoverYBroker(util.UUID().String())
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

func (t *testStates) newHandoverXBrokerFunc(
	st *States,
	local base.Node,
	networkID base.NetworkID,
) func(context.Context) (*HandoverXBroker, error) {
	return func(ctx context.Context) (*HandoverXBroker, error) {
		args := NewHandoverXBrokerArgs(local, networkID)
		args.SendFunc = func(context.Context, interface{}) error { return nil }

		return NewHandoverXBroker(ctx, args), nil
	}
}

func (t *testStates) newHandoverYBrokerFunc(
	st *States,
	networkID base.NetworkID,
) func(context.Context, string) (*HandoverYBroker, error) {
	return func(ctx context.Context, id string) (*HandoverYBroker, error) {
		args := NewHandoverYBrokerArgs(networkID)
		args.SendFunc = func(context.Context, interface{}) error { return nil }

		return NewHandoverYBroker(ctx, args, id), nil
	}
}

func (t *testStates) TestNewHandoverXBroker() {
	t.Run("start handover x broker; not allowed consensus", func() {
		st, _ := t.booted()
		defer st.Stop()

		err := st.NewHandoverXBroker()
		t.Error(err)
		t.ErrorContains(err, "not allowed consensus")
	})

	t.Run("start handover x broker", func() {
		st, _ := t.booted()
		defer st.Stop()

		_ = st.SetAllowConsensus(true)
		st.args.NewHandoverXBroker = t.newHandoverXBrokerFunc(st, t.local, t.params.NetworkID())

		t.NoError(st.NewHandoverXBroker())
		t.NotNil(st.HandoverXBroker())

		t.Run("start again", func() {
			err := st.NewHandoverXBroker()
			t.Error(err)
			t.ErrorContains(err, "already under handover x")
		})
	})

	t.Run("cancel handover x broker", func() {
		st, _ := t.booted()
		defer st.Stop()

		_ = st.SetAllowConsensus(true)
		st.args.NewHandoverXBroker = t.newHandoverXBrokerFunc(st, t.local, t.params.NetworkID())

		t.NoError(st.NewHandoverXBroker())

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

		t.NoError(st.NewHandoverYBroker(util.UUID().String()))
		t.NotNil(st.HandoverYBroker())

		_ = st.SetAllowConsensus(true)

		err := st.NewHandoverXBroker()
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

		t.NoError(st.NewHandoverXBroker())

		broker := st.HandoverXBroker()
		t.NotNil(broker)

		ivp := isaac.NewINITVoteproof(base.RawPoint(32, 44))
		t.NoError(broker.finish(ivp, nil))

		t.False(st.AllowedConsensus())
		t.Nil(st.HandoverXBroker())

		t.T().Log("switching to syncing state")

		select {
		case <-time.After(time.Second * 3):
			t.NoError(errors.Errorf("failed to wait syncing state"))
		case <-syncingenterch:
			t.Equal(StateSyncing, st.current().state())
		}
	})
}

func (t *testStates) TestNewHandoverYBroker() {
	t.Run("start handover y broker; not allowed consensus", func() {
		st, _ := t.booted()
		defer st.Stop()

		_ = st.SetAllowConsensus(true)

		err := st.NewHandoverYBroker(util.UUID().String())
		t.Error(err)
		t.ErrorContains(err, "allowed consensus")
	})

	t.Run("start handover y broker", func() {
		st, _ := t.booted()
		defer st.Stop()

		st.args.NewHandoverYBroker = t.newHandoverYBrokerFunc(st, t.params.NetworkID())

		t.NoError(st.NewHandoverYBroker(util.UUID().String()))
		t.NotNil(st.HandoverYBroker())

		t.Run("start again", func() {
			err := st.NewHandoverYBroker(util.UUID().String())
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

		t.NoError(st.NewHandoverYBroker(util.UUID().String()))

		broker := st.HandoverYBroker()
		t.NotNil(broker)

		broker.cancel(nil)

		t.Nil(st.HandoverYBroker())

		t.T().Log("switching to syncing state")

		select {
		case <-time.After(time.Second * 3):
			t.NoError(errors.Errorf("failed to wait syncing state"))
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

		t.NoError(st.NewHandoverXBroker())
		t.NotNil(st.HandoverXBroker())

		_ = st.SetAllowConsensus(false)

		err := st.NewHandoverYBroker(util.UUID().String())
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

		t.NoError(st.NewHandoverYBroker(util.UUID().String()))

		broker := st.HandoverYBroker()
		t.NotNil(broker)

		hc := newHandoverMessageFinish(broker.ID(), nil, nil)
		t.NoError(broker.receiveFinish(hc))

		t.True(st.AllowedConsensus())
		t.Nil(st.HandoverYBroker())

		t.T().Log("switching to syncing state")

		select {
		case <-time.After(time.Second * 3):
			t.NoError(errors.Errorf("failed to wait syncing state"))
		case <-syncingenterch:
			t.Equal(StateSyncing, st.current().state())
		}
	})

	t.Run("finished with voteproof", func() {
		st, _ := t.booted()
		defer st.Stop()

		st.args.NewHandoverYBroker = t.newHandoverYBrokerFunc(st, t.params.NetworkID())

		t.NoError(st.NewHandoverYBroker(util.UUID().String()))

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
