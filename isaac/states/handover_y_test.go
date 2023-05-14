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
	args := t.yargs()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	broker := NewHandoverYBroker(ctx, args, util.UUID().String(), quicstream.UDPConnInfo{})

	t.Run("isCanceled", func() {
		t.NoError(broker.isCanceled())
	})

	t.Run("canceled by context; isCanceled", func() {
		cancel()

		err := broker.isCanceled()
		t.Error(err)
		t.True(errors.Is(err, ErrHandoverCanceled))
	})

	t.Run("cancel(); isCanceled", func() {
		args := t.yargs()

		broker := NewHandoverYBroker(context.Background(), args, util.UUID().String(), quicstream.UDPConnInfo{})

		t.NoError(broker.isCanceled())

		broker.cancel(nil)

		err := broker.isCanceled()
		t.Error(err)
		t.True(errors.Is(err, ErrHandoverCanceled))
	})
}

func (t *testHandoverYBroker) TestReceiveVoteproof() {
	args := t.yargs()

	broker := NewHandoverYBroker(context.Background(), args, util.UUID().String(), quicstream.UDPConnInfo{})

	vpch := make(chan base.Voteproof, 1)
	broker.newVoteprooff = func(vp base.Voteproof) error {
		vpch <- vp

		return nil
	}

	point := base.RawPoint(33, 44)
	_, ivp := t.VoteproofsPair(point.PrevRound(), point, nil, nil, nil, []base.LocalNode{base.RandomLocalNode()})

	hc := newHandoverMessageData(broker.id, HandoverMessageDataTypeVoteproof, ivp)
	t.NoError(broker.receive(hc))

	rivp := <-vpch

	base.EqualVoteproof(t.Assert(), ivp, rivp)
}

func (t *testHandoverYBroker) TestReceiveMessageReadyResponse() {
	point := base.RawPoint(33, 44)

	t.Run("wrong ID", func() {
		args := t.yargs()

		errch := make(chan error, 1)
		args.WhenCanceled = func(err error) {
			errch <- err
		}

		broker := NewHandoverYBroker(context.Background(), args, util.UUID().String(), quicstream.UDPConnInfo{})

		hc := newHandoverMessageChallengeResponse(util.UUID().String(), base.NewStagePoint(point, base.StageINIT), true, nil)
		t.Error(broker.receive(hc))

		err := broker.isCanceled()
		t.Error(err)
		t.True(errors.Is(err, ErrHandoverCanceled))

		err = <-errch
		t.Error(err)
		t.ErrorContains(err, "id not matched")
	})

	args := t.yargs()
	args.SendFunc = func(context.Context, interface{}) error { return nil }

	broker := NewHandoverYBroker(context.Background(), args, util.UUID().String(), quicstream.UDPConnInfo{})

	t.NoError(broker.sendStagePoint(context.Background(), base.NewStagePoint(point, base.StageINIT)))

	t.Run("ok", func() {
		hc := newHandoverMessageChallengeResponse(broker.id, base.NewStagePoint(point, base.StageINIT), true, nil)
		t.NoError(broker.receive(hc))
	})

	t.Run("not ok", func() {
		hc := newHandoverMessageChallengeResponse(broker.id, base.NewStagePoint(point, base.StageINIT), false, nil)
		t.NoError(broker.receive(hc))
	})

	t.Run("error", func() {
		errch := make(chan error, 1)
		args.WhenCanceled = func(err error) {
			errch <- err
		}

		hc := newHandoverMessageChallengeResponse(broker.id, base.NewStagePoint(point, base.StageINIT), false, errors.Errorf("hehehe"))
		err := broker.receive(hc)
		t.Error(err)
		t.ErrorContains(err, "hehehe")

		err = broker.isCanceled()
		t.Error(err)
		t.True(errors.Is(err, ErrHandoverCanceled))

		err = <-errch
		t.Error(err)
		t.ErrorContains(err, "hehehe")
	})

	t.Run("unknown point", func() {
		args := t.yargs()
		args.SendFunc = func(context.Context, interface{}) error { return nil }

		broker := NewHandoverYBroker(context.Background(), args, util.UUID().String(), quicstream.UDPConnInfo{})

		errch := make(chan error, 1)
		args.WhenCanceled = func(err error) {
			errch <- err
		}

		hc := newHandoverMessageChallengeResponse(broker.id, base.NewStagePoint(point.NextHeight(), base.StageINIT), true, nil)
		err := broker.receive(hc)
		t.Error(err)
		t.True(errors.Is(err, ErrHandoverCanceled))
		t.ErrorContains(err, "unknown ready response message")

		err = broker.isCanceled()
		t.Error(err)
		t.True(errors.Is(err, ErrHandoverCanceled))

		err = <-errch
		t.Error(err)
		t.ErrorContains(err, "unknown ready response message")
	})

	t.Run("point mismatch", func() {
		args := t.yargs()
		args.SendFunc = func(context.Context, interface{}) error { return nil }

		broker := NewHandoverYBroker(context.Background(), args, util.UUID().String(), quicstream.UDPConnInfo{})

		t.NoError(broker.sendStagePoint(context.Background(), base.NewStagePoint(point, base.StageINIT)))

		errch := make(chan error, 1)
		args.WhenCanceled = func(err error) {
			errch <- err
		}

		hc := newHandoverMessageChallengeResponse(broker.id, base.NewStagePoint(point.NextHeight(), base.StageINIT), true, nil)
		err := broker.receive(hc)
		t.Error(err)
		t.True(errors.Is(err, ErrHandoverCanceled))
		t.ErrorContains(err, "ready response message point not matched")

		err = broker.isCanceled()
		t.Error(err)
		t.True(errors.Is(err, ErrHandoverCanceled))

		err = <-errch
		t.Error(err)
		t.ErrorContains(err, "ready response message point not matched")
	})

	t.Run("send failed", func() {
		args := t.yargs()
		args.SendFunc = func(context.Context, interface{}) error {
			return errors.Errorf("hihihi")
		}

		broker := NewHandoverYBroker(context.Background(), args, util.UUID().String(), quicstream.UDPConnInfo{})

		errch := make(chan error, 1)
		args.WhenCanceled = func(err error) {
			errch <- err
		}

		err := broker.sendStagePoint(context.Background(), base.NewStagePoint(point, base.StageINIT))
		t.Error(err)
		t.ErrorContains(err, "hihihi")

		select {
		case <-time.After(time.Second):
			t.NoError(errors.Errorf("failed to wait cancel"))
		case err = <-errch:
			t.ErrorContains(err, "hihihi")
		}

		err = broker.isCanceled()
		t.Error(err)
		t.True(errors.Is(err, ErrHandoverCanceled))
	})
}

func (t *testHandoverYBroker) TestReceiveMessageFinish() {
	t.Run("ok", func() {
		args := t.yargs()

		vpch := make(chan base.INITVoteproof, 1)
		args.WhenFinished = func(vp base.INITVoteproof) error {
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

		broker := NewHandoverYBroker(context.Background(), args, util.UUID().String(), quicstream.UDPConnInfo{})

		point := base.RawPoint(33, 44)
		_, ivp := t.VoteproofsPair(point.PrevRound(), point, nil, nil, nil, []base.LocalNode{base.RandomLocalNode()})

		pr := isaac.NewProposalSignFact(isaac.NewProposalFact(point, t.Local.Address(), valuehash.RandomSHA256(), []util.Hash{valuehash.RandomSHA256()}))
		_ = pr.Sign(t.Local.Privatekey(), t.LocalParams.NetworkID())

		hc := newHandoverMessageFinish(broker.id, ivp, pr)
		t.NoError(broker.receive(hc))

		rivp := <-vpch

		base.EqualVoteproof(t.Assert(), ivp, rivp)

		base.EqualProposalSignFact(t.Assert(), pr, <-datach)
	})

	t.Run("error", func() {
		args := t.yargs()

		args.WhenFinished = func(vp base.INITVoteproof) error {
			return errors.Errorf("hihihi")
		}
		args.NewDataFunc = func(_ HandoverMessageDataType, i interface{}) error { return nil }

		broker := NewHandoverYBroker(context.Background(), args, util.UUID().String(), quicstream.UDPConnInfo{})

		point := base.RawPoint(33, 44)
		_, ivp := t.VoteproofsPair(point.PrevRound(), point, nil, nil, nil, []base.LocalNode{base.RandomLocalNode()})

		hc := newHandoverMessageFinish(broker.id, ivp, nil)
		err := broker.receive(hc)
		t.Error(err)
		t.True(errors.Is(err, ErrHandoverCanceled))
		t.ErrorContains(err, "hihihi")

		err = broker.isCanceled()
		t.Error(err)
		t.True(errors.Is(err, ErrHandoverCanceled))
	})
}

func TestHandoverYBroker(t *testing.T) {
	suite.Run(t, new(testHandoverYBroker))
}
