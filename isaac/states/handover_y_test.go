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
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
)

type testHandoverYBroker struct {
	baseTestHandoverBroker
}

func (t *testHandoverYBroker) TestNew() {
	args := t.yargs(util.UUID().String())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	broker := t.syncedHandoverYBroker(ctx, args, quicstream.ConnInfo{})

	t.Run("isCanceled", func() {
		t.NoError(broker.isCanceled())
	})

	t.Run("canceled by context; isCanceled", func() {
		cancel()

		err := broker.isCanceled()
		t.Error(err)
		t.ErrorIs(err, ErrHandoverCanceled)
	})

	t.Run("cancel(); isCanceled", func() {
		args := t.yargs(util.UUID().String())

		broker := t.syncedHandoverYBroker(context.Background(), args, quicstream.ConnInfo{})

		t.NoError(broker.isCanceled())

		broker.cancel(nil)

		err := broker.isCanceled()
		t.Error(err)
		t.ErrorIs(err, ErrHandoverCanceled)
	})
}

func (t *testHandoverYBroker) TestAsk() {
	t.Run("not ready synced data", func() {
		args := t.yargs(util.UUID().String())

		setready := make(chan struct{}, 1)
		args.SyncDataFunc = func(_ context.Context, _ quicstream.ConnInfo, readych chan<- struct{}) error {
			<-setready
			readych <- struct{}{}

			return nil
		}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		broker := NewHandoverYBroker(ctx, args, quicstream.ConnInfo{})
		canMoveConsensus, isAsked, err := broker.Ask()
		t.NoError(err)
		t.False(isAsked)
		t.False(canMoveConsensus)
		t.False(broker.IsAsked())

		t.T().Log("be ready")
		setready <- struct{}{}

		broker.checkSyncedDataDone()

		canMoveConsensus, isAsked, err = broker.Ask()
		t.NoError(err)
		t.True(isAsked)
		t.False(canMoveConsensus)
		t.True(broker.IsAsked())
	})

	t.Run("ok", func() {
		args := t.yargs(util.UUID().String())

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		broker := t.syncedHandoverYBroker(ctx, args, quicstream.ConnInfo{})
		canMoveConsensus, isAsked, err := broker.Ask()
		t.NoError(err)
		t.True(isAsked)
		t.False(canMoveConsensus)
		t.True(broker.IsAsked())

		t.T().Log("ask again")
		canMoveConsensus, isAsked, err = broker.Ask()
		t.NoError(err)
		t.True(isAsked)
		t.False(canMoveConsensus)
		t.True(broker.IsAsked())
	})

	t.Run("send stagepoint, but not yet data synced", func() {
		args := t.yargs(util.UUID().String())
		args.SyncDataFunc = func(_ context.Context, _ quicstream.ConnInfo, readych chan<- struct{}) error {
			readych <- struct{}{}

			<-time.After(time.Minute)

			return nil
		}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		broker := t.syncedHandoverYBroker(ctx, args, quicstream.ConnInfo{})
		canMoveConsensus, isAsked, err := broker.Ask()
		t.NoError(err)
		t.True(isAsked)
		t.False(canMoveConsensus)
		t.True(broker.IsAsked())

		sendch := make(chan HandoverMessage, 1)
		args.SendMessageFunc = func(_ context.Context, _ quicstream.ConnInfo, msg HandoverMessage) error {
			sendch <- msg

			return nil
		}

		t.T().Log("send stagepoint")
		t.NoError(broker.sendStagePoint(context.Background(), base.NewStagePoint(base.RawPoint(33, 44), base.StageINIT)))

		select {
		case <-time.After(time.Second * 2):
		case <-sendch:
			t.Fail("unexpected handover message")
		}
	})

	t.Run("ask func error", func() {
		args := t.yargs("")
		args.AskRequestFunc = func(context.Context, quicstream.ConnInfo) (string, bool, error) {
			return "", false, errors.Errorf("hehehe")
		}
		args.MaxEnsureAsk = 1

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		broker := t.syncedHandoverYBroker(ctx, args, quicstream.ConnInfo{})
		canMoveConsensus, isAsked, err := broker.Ask()
		t.Error(err)
		t.False(isAsked)
		t.False(canMoveConsensus)
		t.ErrorContains(err, "hehehe")

		t.False(broker.IsAsked())

		t.T().Log("broker will be cancled")
		err = broker.isCanceled()
		t.Error(err)
		t.ErrorIs(err, ErrHandoverCanceled)
	})

	t.Run("ask func error; ensure", func() {
		args := t.yargs("")
		args.AskRequestFunc = func(context.Context, quicstream.ConnInfo) (string, bool, error) {
			return "", false, errors.Errorf("hehehe")
		}
		args.MaxEnsureAsk = 2

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		t.T().Log("MaxEnsureAsk 2: ask error ignored")
		broker := t.syncedHandoverYBroker(ctx, args, quicstream.ConnInfo{})
		canMoveConsensus, isAsked, err := broker.Ask()
		t.NoError(err)
		t.False(isAsked)
		t.False(canMoveConsensus)

		t.T().Log("MaxEnsureAsk exhausted: ask error")
		canMoveConsensus, isAsked, err = broker.Ask()
		t.Error(err)
		t.False(isAsked)
		t.False(canMoveConsensus)
		t.ErrorContains(err, "hehehe")

		t.False(broker.IsAsked())

		t.T().Log("broker will be cancled")
		err = broker.isCanceled()
		t.Error(err)
		t.ErrorIs(err, ErrHandoverCanceled)
	})
}

func (t *testHandoverYBroker) TestReceiveVoteproof() {
	args := t.yargs(util.UUID().String())

	broker := t.syncedHandoverYBroker(context.Background(), args, quicstream.ConnInfo{})
	broker.Ask()

	vpch := make(chan base.Voteproof, 1)
	broker.newVoteprooff = func(vp base.Voteproof) error {
		vpch <- vp

		return nil
	}

	point := base.RawPoint(33, 44)
	_, ivp := t.VoteproofsPair(point.PrevRound(), point, nil, nil, nil, []base.LocalNode{base.RandomLocalNode()})

	hc := newHandoverMessageData(broker.ID(), HandoverMessageDataTypeVoteproof, ivp)
	t.NoError(broker.Receive(hc))

	rivp := <-vpch

	base.EqualVoteproof(t.Assert(), ivp, rivp)
}

func (t *testHandoverYBroker) TestReceiveMessageChallengeResponse() {
	point := base.RawPoint(33, 44)

	t.Run("wrong ID", func() {
		args := t.yargs(util.UUID().String())

		errch := make(chan error, 1)
		args.WhenCanceled = func(_ string, err error, _ quicstream.ConnInfo) {
			errch <- err
		}

		broker := t.syncedHandoverYBroker(context.Background(), args, quicstream.ConnInfo{})
		broker.Ask()

		hc := newHandoverMessageChallengeResponse(util.UUID().String(), base.NewStagePoint(point, base.StageINIT), true, nil)
		t.Error(broker.Receive(hc))

		err := broker.isCanceled()
		t.Error(err)
		t.ErrorIs(err, ErrHandoverCanceled)

		err = <-errch
		t.Error(err)
		t.ErrorContains(err, "id not matched")
	})

	args := t.yargs(util.UUID().String())
	args.SendMessageFunc = func(context.Context, quicstream.ConnInfo, HandoverMessage) error { return nil }

	broker := t.syncedHandoverYBroker(context.Background(), args, quicstream.ConnInfo{})
	broker.Ask()

	t.NoError(broker.sendStagePoint(context.Background(), base.NewStagePoint(point, base.StageINIT)))

	t.Run("ok", func() {
		hc := newHandoverMessageChallengeResponse(broker.ID(), base.NewStagePoint(point, base.StageINIT), true, nil)
		t.NoError(broker.Receive(hc))
	})

	t.Run("not ok", func() {
		hc := newHandoverMessageChallengeResponse(broker.ID(), base.NewStagePoint(point, base.StageINIT), false, nil)
		t.NoError(broker.Receive(hc))
	})

	t.Run("error", func() {
		errch := make(chan error, 1)
		args.WhenCanceled = func(_ string, err error, _ quicstream.ConnInfo) {
			errch <- err
		}

		hc := newHandoverMessageChallengeResponse(broker.ID(), base.NewStagePoint(point, base.StageINIT), false, errors.Errorf("hehehe"))
		err := broker.Receive(hc)
		t.Error(err)
		t.ErrorContains(err, "hehehe")

		err = broker.isCanceled()
		t.Error(err)
		t.ErrorIs(err, ErrHandoverCanceled)

		err = <-errch
		t.Error(err)
		t.ErrorContains(err, "hehehe")
	})

	t.Run("unknown point", func() {
		args := t.yargs(util.UUID().String())
		args.SendMessageFunc = func(context.Context, quicstream.ConnInfo, HandoverMessage) error { return nil }

		broker := t.syncedHandoverYBroker(context.Background(), args, quicstream.ConnInfo{})
		broker.Ask()

		hc := newHandoverMessageChallengeResponse(broker.ID(), base.NewStagePoint(point.NextHeight(), base.StageINIT), true, nil)
		err := broker.Receive(hc)
		t.NoError(err)

		err = broker.isCanceled()
		t.NoError(err)
	})

	t.Run("send failed", func() {
		args := t.yargs(util.UUID().String())
		args.SendMessageFunc = func(context.Context, quicstream.ConnInfo, HandoverMessage) error {
			return errors.Errorf("hihihi")
		}
		args.MaxEnsureSendFailure = 0

		broker := t.syncedHandoverYBroker(context.Background(), args, quicstream.ConnInfo{})
		broker.Ask()

		errch := make(chan error, 1)
		args.WhenCanceled = func(_ string, err error, _ quicstream.ConnInfo) {
			errch <- err
		}

		err := broker.sendStagePoint(context.Background(), base.NewStagePoint(point, base.StageINIT))
		t.Error(err)
		t.ErrorContains(err, "hihihi")

		select {
		case <-time.After(time.Second):
			t.Fail("failed to wait cancel")
		case err = <-errch:
			t.ErrorContains(err, "hihihi")
		}

		err = broker.isCanceled()
		t.Error(err)
		t.ErrorIs(err, ErrHandoverCanceled)
	})
}

func (t *testHandoverYBroker) TestReceiveMessageFinish() {
	t.Run("ok", func() {
		args := t.yargs(util.UUID().String())

		vpch := make(chan base.INITVoteproof, 1)
		args.WhenFinished = func(_ string, vp base.INITVoteproof, _ quicstream.ConnInfo) error {
			vpch <- vp

			return nil
		}

		datach := make(chan base.ProposalSignFact, 1)
		args.NewDataFunc = func(_ HandoverMessageDataType, i interface{}) error {
			if pr, ok := i.(base.ProposalSignFact); ok {
				datach <- pr
			}

			return nil
		}

		broker := t.syncedHandoverYBroker(context.Background(), args, quicstream.ConnInfo{})
		broker.Ask()

		point := base.RawPoint(33, 44)
		_, ivp := t.VoteproofsPair(point.PrevRound(), point, nil, nil, nil, []base.LocalNode{base.RandomLocalNode()})

		pr := isaac.NewProposalSignFact(isaac.NewProposalFact(point, t.Local.Address(), valuehash.RandomSHA256(), [][2]util.Hash{{valuehash.RandomSHA256(), valuehash.RandomSHA256()}}))
		_ = pr.Sign(t.Local.Privatekey(), t.LocalParams.NetworkID())

		hc := newHandoverMessageFinish(broker.ID(), ivp, pr)
		t.NoError(broker.Receive(hc))

		rivp := <-vpch

		base.EqualVoteproof(t.Assert(), ivp, rivp)

		base.EqualProposalSignFact(t.Assert(), pr, <-datach)
	})

	t.Run("error", func() {
		args := t.yargs(util.UUID().String())

		args.WhenFinished = func(_ string, vp base.INITVoteproof, _ quicstream.ConnInfo) error {
			return errors.Errorf("hihihi")
		}
		args.NewDataFunc = func(_ HandoverMessageDataType, i interface{}) error { return nil }

		broker := t.syncedHandoverYBroker(context.Background(), args, quicstream.ConnInfo{})
		broker.Ask()

		point := base.RawPoint(33, 44)
		_, ivp := t.VoteproofsPair(point.PrevRound(), point, nil, nil, nil, []base.LocalNode{base.RandomLocalNode()})

		hc := newHandoverMessageFinish(broker.ID(), ivp, nil)
		err := broker.Receive(hc)
		t.Error(err)
		t.ErrorIs(err, ErrHandoverCanceled)
		t.ErrorContains(err, "hihihi")

		err = broker.isCanceled()
		t.Error(err)
		t.ErrorIs(err, ErrHandoverCanceled)
	})
}

func TestHandoverYBroker(t *testing.T) {
	suite.Run(t, new(testHandoverYBroker))
}
