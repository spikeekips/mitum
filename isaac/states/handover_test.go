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
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
)

type testHandoverHandler struct {
	isaac.BaseTestBallots
}

func (t *testHandoverHandler) newHandoverYBrokerFunc(
	networkID base.NetworkID,
) func(context.Context, string, quicstream.UDPConnInfo) (*HandoverYBroker, error) {
	return func(ctx context.Context, id string, connInfo quicstream.UDPConnInfo) (*HandoverYBroker, error) {
		args := NewHandoverYBrokerArgs(networkID)
		args.SendFunc = func(context.Context, interface{}) error { return nil }

		return NewHandoverYBroker(ctx, args, id, connInfo), nil
	}
}

func (t *testHandoverHandler) newargs(previous base.Manifest, suf base.Suffrage) *HandoverHandlerArgs {
	local := t.Local

	args := NewHandoverHandlerArgs()
	args.ProposalProcessors = isaac.NewProposalProcessors(nil, nil)
	args.GetManifestFunc = func(base.Height) (base.Manifest, error) { return previous, nil }
	args.NodeInConsensusNodesFunc = func(base.Node, base.Height) (base.Suffrage, bool, error) {
		if suf == nil {
			return nil, false, nil
		}

		return suf, suf.ExistsPublickey(local.Address(), local.Publickey()), nil
	}

	return args
}

func (t *testHandoverHandler) newState(args *HandoverHandlerArgs) (*HandoverHandler, func()) {
	newhandler := NewNewHandoverHandlerType(t.Local, t.LocalParams, args)
	_ = newhandler.SetLogging(logging.TestNilLogging)

	i, err := newhandler.new()
	t.NoError(err)

	st := i.(*HandoverHandler)
	st.setAllowConsensus(false)

	return st, func() {
		deferred, err := st.exit(nil)
		t.NoError(err)
		deferred()
	}
}

func (t *testHandoverHandler) newStateWithINITVoteproof(point base.Point, suf base.Suffrage) (
	*HandoverHandler,
	func(),
	*isaac.DummyProposalProcessor,
	base.INITVoteproof,
) {
	previous := base.NewDummyManifest(point.Height()-1, valuehash.RandomSHA256())

	prpool := t.PRPool
	fact := prpool.GetFact(point)

	args := t.newargs(previous, suf)

	pp := isaac.NewDummyProposalProcessor()
	pp.Processerr = func(context.Context, base.ProposalFact, base.INITVoteproof) (base.Manifest, error) {
		return nil, errors.Errorf("process error")
	}

	args.ProposalProcessors.SetMakeNew(pp.Make)
	args.ProposalProcessors.SetGetProposal(func(_ context.Context, _ base.Point, facthash util.Hash) (base.ProposalSignFact, error) {
		return prpool.ByHash(facthash)
	})

	args.ProposalSelectFunc = func(ctx context.Context, p base.Point, _ util.Hash, _ time.Duration) (base.ProposalSignFact, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			pr := prpool.ByPoint(p)
			if pr != nil {
				return pr, nil
			}
			return nil, util.ErrNotFound.Call()
		}
	}

	st, closef := t.newState(args)
	st.ballotBroadcaster = NewDummyBallotBroadcaster(t.Local.Address(), func(base.Ballot) error {
		return nil
	})
	st.switchStateFunc = func(switchContext) error {
		return nil
	}

	broker, err := t.newHandoverYBrokerFunc(t.LocalParams.NetworkID())(context.Background(), util.UUID().String(), quicstream.UDPConnInfo{})
	t.NoError(err)

	broker.args.SendFunc = func(_ context.Context, i interface{}) error {
		return nil
	}

	st.handoverYBrokerFunc = func() *HandoverYBroker { return broker }

	nodes := make([]base.LocalNode, suf.Len())
	sn := suf.Nodes()
	for i := range sn {
		nodes[i] = sn[i].(base.LocalNode)
	}

	avp, ivp := t.VoteproofsPair(point.PrevHeight(), point, nil, nil, fact.Hash(), nodes)
	t.True(st.setLastVoteproof(avp))
	t.True(st.setLastVoteproof(ivp))

	return st, closef, pp, ivp
}

func (t *testHandoverHandler) TestEnterButEmptyHandoverYBroker() {
	point := base.RawPoint(33, 44)
	suf, _ := isaac.NewTestSuffrage(2, t.Local)

	st, closefunc, _, ivp := t.newStateWithINITVoteproof(point, suf)
	defer closefunc()

	switchch := make(chan switchContext, 1)
	st.switchStateFunc = func(sctx switchContext) error {
		switchch <- sctx

		return nil
	}

	st.handoverYBrokerFunc = func() *HandoverYBroker { return nil } // NOTE set empty handover y broker

	sctx := newHandoverSwitchContext(StateSyncing, ivp)

	_, err := st.enter(StateSyncing, sctx)
	t.Error(err)

	var ssctx SyncingSwitchContext
	t.True(errors.As(err, &ssctx))

	t.Equal(ivp.Point().Height().SafePrev(), ssctx.height)
}

func (t *testHandoverHandler) TestEnterButAllowedConsensus() {
	point := base.RawPoint(33, 44)
	suf, _ := isaac.NewTestSuffrage(2, t.Local)

	st, closefunc, _, ivp := t.newStateWithINITVoteproof(point, suf)
	defer closefunc()

	st.setAllowConsensus(true)

	switchch := make(chan switchContext, 1)
	st.switchStateFunc = func(sctx switchContext) error {
		switchch <- sctx

		return nil
	}

	sctx := newHandoverSwitchContext(StateSyncing, ivp)

	t.T().Log("moves to syncing states")

	_, err := st.enter(StateSyncing, sctx)
	t.Error(err)

	var ssctx SyncingSwitchContext
	t.True(errors.As(err, &ssctx))

	t.Equal(ivp.Point().Height().SafePrev(), ssctx.height)
}

func (t *testHandoverHandler) TestEnterExpectedINITVoteproof() {
	point := base.RawPoint(33, 44)
	suf, _ := isaac.NewTestSuffrage(2, t.Local)

	st, closefunc, pp, ivp := t.newStateWithINITVoteproof(point, suf)
	defer closefunc()

	ballotch := make(chan base.Ballot, 1)
	st.ballotBroadcaster = NewDummyBallotBroadcaster(t.Local.Address(), func(bl base.Ballot) error {
		ballotch <- bl

		return nil
	})

	broker := st.handoverYBroker()

	brokersentch := make(chan interface{}, 3)
	broker.args.SendFunc = func(_ context.Context, i interface{}) error {
		brokersentch <- i

		return nil
	}

	st.handoverYBrokerFunc = func() *HandoverYBroker { return broker }

	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	prch := make(chan util.Hash, 1)
	pp.Processerr = func(context.Context, base.ProposalFact, base.INITVoteproof) (base.Manifest, error) {
		prch <- manifest.Hash()

		return manifest, nil
	}

	prpool := t.PRPool
	st.args.ProposalProcessors.SetGetProposal(func(_ context.Context, _ base.Point, facthash util.Hash) (base.ProposalSignFact, error) {
		return prpool.ByHash(facthash)
	})

	sctx := newHandoverSwitchContext(StateSyncing, ivp)

	deferred, err := st.enter(StateSyncing, sctx)
	t.NoError(err)
	deferred()

	t.T().Log("wait to send stagepoint")
	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait send stagepoint"))

		return
	case i := <-brokersentch:
		m, ok := i.(HandoverMessageChallengeStagePoint)
		t.True(ok)

		t.True(ivp.Point().Equal(m.Point()))
	}

	t.T().Log("wait process proposal")
	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait new processed proposal"))

		return
	case processed := <-prch:
		t.NotNil(processed)
		t.True(manifest.Hash().Equal(processed))
	}

	t.T().Log("ballot will not be broadcasted")
	select {
	case <-time.After(time.Second):
	case <-ballotch:
		t.NoError(errors.Errorf("unexpected ballot broadcasted"))
	}
}

func (t *testHandoverHandler) TestACCEPTVoteproof() {
	point := base.RawPoint(33, 44)
	suf, nodes := isaac.NewTestSuffrage(2, t.Local)

	st, closefunc, pp, ivp := t.newStateWithINITVoteproof(point, suf)
	defer closefunc()

	ballotch := make(chan base.Ballot, 1)
	st.ballotBroadcaster = NewDummyBallotBroadcaster(t.Local.Address(), func(bl base.Ballot) error {
		ballotch <- bl

		return nil
	})

	broker := st.handoverYBroker()

	brokersentch := make(chan interface{}, 3)
	broker.args.SendFunc = func(_ context.Context, i interface{}) error {
		switch i.(type) {
		case HandoverMessageChallengeStagePoint,
			HandoverMessageChallengeBlockMap:
			brokersentch <- i
		}

		return nil
	}

	st.handoverYBrokerFunc = func() *HandoverYBroker { return broker }

	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	prch := make(chan util.Hash, 1)
	pp.Processerr = func(context.Context, base.ProposalFact, base.INITVoteproof) (base.Manifest, error) {
		prch <- manifest.Hash()

		return manifest, nil
	}

	savedch := make(chan base.ACCEPTVoteproof, 1)
	pp.Saveerr = func(_ context.Context, avp base.ACCEPTVoteproof) (base.BlockMap, error) {
		savedch <- avp

		return base.NewDummyBlockMap(manifest), nil
	}

	prpool := t.PRPool
	st.args.ProposalProcessors.SetGetProposal(func(_ context.Context, _ base.Point, facthash util.Hash) (base.ProposalSignFact, error) {
		return prpool.ByHash(facthash)
	})

	sctx := newHandoverSwitchContext(StateSyncing, ivp)

	deferred, err := st.enter(StateSyncing, sctx)
	t.NoError(err)
	deferred()

	t.T().Log("wait to send stagepoint")
	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait send stagepoint"))

		return
	case i := <-brokersentch:
		m, ok := i.(HandoverMessageChallengeStagePoint)
		t.True(ok)

		t.True(ivp.Point().Equal(m.Point()))
	}

	t.T().Log("wait process proposal")
	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait new processed proposal"))

		return
	case processed := <-prch:
		t.NotNil(processed)
		t.True(manifest.Hash().Equal(processed))
	}

	t.T().Log("expected accept voteproof")
	avp, _ := t.VoteproofsPair(point, point.NextHeight(), manifest.Hash(), t.PRPool.Hash(point), nil, nodes)

	t.NoError(st.newVoteproof(avp))

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait save block"))

		return
	case ravp := <-savedch:
		base.EqualVoteproof(t.Assert(), avp, ravp)
	}

	t.T().Log("wait blockmap challenge")
	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait blockmap challenge"))

		return
	case i := <-brokersentch:
		m, ok := i.(HandoverMessageChallengeBlockMap)
		t.True(ok)

		t.Equal(manifest.Height(), m.BlockMap().Manifest().Height())
		t.True(manifest.Hash().Equal(m.BlockMap().Manifest().Hash()))
	}

	t.T().Log("ballot will not be broadcasted")
	select {
	case <-time.After(time.Second):
	case <-ballotch:
		t.NoError(errors.Errorf("unexpected ballot broadcasted"))
	}
}

func (t *testHandoverHandler) TestFinishedButHigherVoteproof() {
	point := base.RawPoint(33, 44)
	suf, nodes := isaac.NewTestSuffrage(2, t.Local)

	st, closefunc, pp, ivp := t.newStateWithINITVoteproof(point, suf)
	defer closefunc()

	broker := st.handoverYBroker()

	brokersentch := make(chan interface{}, 3)
	broker.args.SendFunc = func(_ context.Context, i interface{}) error {
		brokersentch <- i

		return nil
	}

	st.handoverYBrokerFunc = func() *HandoverYBroker { return broker }

	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	pp.Processerr = func(context.Context, base.ProposalFact, base.INITVoteproof) (base.Manifest, error) {
		return manifest, nil
	}

	sctx := newHandoverSwitchContext(StateSyncing, ivp)

	deferred, err := st.enter(StateSyncing, sctx)
	t.NoError(err)
	deferred()

	t.T().Log("wait to send stagepoint")
	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait send stagepoint"))

		return
	case i := <-brokersentch:
		m, ok := i.(HandoverMessageChallengeStagePoint)
		t.True(ok)

		t.True(ivp.Point().Equal(m.Point()))
	}

	t.T().Log("finished, moves to syncing state")
	_, nextivp := t.VoteproofsPair(point, point.NextHeight(), manifest.Hash(), t.PRPool.Hash(point), nil, nodes)

	err = st.newVoteproof(handoverFinishedVoteporof{nextivp})
	t.Error(err)

	var ssctx SyncingSwitchContext
	t.True(errors.As(err, &ssctx))

	t.Equal(nextivp.Point().Height().SafePrev(), ssctx.height)
}

func (t *testHandoverHandler) TestFinishedWithINITVoteproof() {
	point := base.RawPoint(33, 44)
	suf, nodes := isaac.NewTestSuffrage(2, t.Local)

	st, closefunc, pp, ivp := t.newStateWithINITVoteproof(point, suf)
	defer closefunc()

	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	prch := make(chan util.Hash, 1)
	pp.Processerr = func(context.Context, base.ProposalFact, base.INITVoteproof) (base.Manifest, error) {
		prch <- manifest.Hash()

		return manifest, nil
	}

	savedch := make(chan base.ACCEPTVoteproof, 1)
	pp.Saveerr = func(_ context.Context, avp base.ACCEPTVoteproof) (base.BlockMap, error) {
		savedch <- avp

		return base.NewDummyBlockMap(manifest), nil
	}

	prpool := t.PRPool
	st.args.ProposalProcessors.SetGetProposal(func(_ context.Context, _ base.Point, facthash util.Hash) (base.ProposalSignFact, error) {
		return prpool.ByHash(facthash)
	})

	sctx := newHandoverSwitchContext(StateSyncing, ivp)

	deferred, err := st.enter(StateSyncing, sctx)
	t.NoError(err)
	deferred()

	t.T().Log("wait process proposal")
	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait new processed proposal"))

		return
	case processed := <-prch:
		t.NotNil(processed)
		t.True(manifest.Hash().Equal(processed))
	}

	t.T().Log("expected accept voteproof")
	avp, nextivp := t.VoteproofsPair(point, point.NextHeight(), manifest.Hash(), t.PRPool.Hash(point), nil, nodes)

	t.NoError(st.newVoteproof(avp))

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("timeout to wait save block"))

		return
	case ravp := <-savedch:
		base.EqualVoteproof(t.Assert(), avp, ravp)
	}

	t.T().Log("finished, moves to consensus state")
	err = st.newVoteproof(handoverFinishedVoteporof{nextivp})
	t.Error(err)

	var csctx consensusSwitchContext
	t.True(errors.As(err, &csctx))

	fvp, ok := csctx.voteproof().(handoverFinishedVoteporof)
	t.True(ok)
	base.EqualVoteproof(t.Assert(), nextivp, fvp.INITVoteproof)

	t.Run("exit", func() {
		finished, _ := st.finishedWithVoteproof.Value()
		t.True(finished)

		_, err = st.exit(csctx)
		t.NoError(err)

		t.True(st.allowedConsensus())
	})
}

func (t *testHandoverHandler) TestExitWithoutINITVoteproof() {
	point := base.RawPoint(33, 44)
	suf, _ := isaac.NewTestSuffrage(2, t.Local)

	st, closefunc, _, _ := t.newStateWithINITVoteproof(point, suf)
	defer closefunc()

	t.Run("exit", func() {
		finished, _ := st.finishedWithVoteproof.Value()
		t.False(finished)

		ssctx := newSyncingSwitchContextWithVoteproof(StateHandover, nil)
		_, err := st.exit(ssctx)
		t.NoError(err)

		t.False(st.allowedConsensus())
	})
}

func TestHandoverHandler(t *testing.T) {
	suite.Run(t, new(testHandoverHandler))
}
