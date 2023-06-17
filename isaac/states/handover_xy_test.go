package isaacstates

import (
	"context"
	"fmt"
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

type baseTestHandoverBroker struct {
	isaac.BaseTestBallots
}

func (t *baseTestHandoverBroker) xargs() *HandoverXBrokerArgs {
	args := NewHandoverXBrokerArgs(t.Local, t.LocalParams.NetworkID())
	args.SendMessageFunc = func(context.Context, quicstream.ConnInfo, HandoverMessage) error {
		return nil
	}

	args.CheckIsReady = func() (bool, error) {
		return true, nil
	}

	return args
}

func (t *baseTestHandoverBroker) yargs(id string) *HandoverYBrokerArgs {
	args := NewHandoverYBrokerArgs(t.LocalParams.NetworkID())
	args.WhenCanceled = func(error, quicstream.ConnInfo) {}
	args.NewDataFunc = func(HandoverMessageDataType, interface{}) error { return nil }
	args.AskRequestFunc = func(context.Context, quicstream.ConnInfo) (string, bool, error) {
		return id, false, nil
	}
	args.SyncDataFunc = func(_ context.Context, _ quicstream.ConnInfo, readych chan<- struct{}) error {
		readych <- struct{}{}

		return nil
	}

	return args
}

func (t *baseTestHandoverBroker) syncedHandoverYBroker(ctx context.Context, args *HandoverYBrokerArgs, ci quicstream.ConnInfo) *HandoverYBroker {
	broker := NewHandoverYBroker(ctx, args, ci)
	broker.checkSyncedDataReady()

	return broker
}

func (t *baseTestHandoverBroker) newBlockMap(
	height base.Height,
	local base.LocalNode,
	h util.Hash,
) base.DummyBlockMap {
	if h == nil {
		h = valuehash.RandomSHA256()
	}

	manifest := base.NewDummyManifest(height, h)

	return base.NewDummyBlockMapWithSign(manifest, local.Address(), local.Privatekey())
}

type testHandoverXYBroker struct {
	baseTestHandoverBroker
}

func (t *testHandoverXYBroker) TestFinished() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	xargs := t.xargs()
	xbroker := NewHandoverXBroker(ctx, xargs, quicstream.ConnInfo{})

	yargs := t.yargs(xbroker.id)
	ybroker := t.syncedHandoverYBroker(ctx, yargs, quicstream.ConnInfo{})

	canMoveConsensus, isAsked, err := ybroker.Ask()
	t.NoError(err)
	t.True(isAsked)
	t.False(canMoveConsensus)

	xargs.ReadyEnd = 0
	xargs.SendMessageFunc = func(_ context.Context, _ quicstream.ConnInfo, i HandoverMessage) error {
		go func() {
			if err := ybroker.Receive(i); err != nil {
				xbroker.Log().Error().Err(err).Msg("send error")
			}
		}()

		return nil
	}
	xargs.CheckIsReady = func() (bool, error) {
		return true, nil
	}
	xfinishch := make(chan base.INITVoteproof, 1)
	xargs.WhenFinished = func(vp base.INITVoteproof, _ base.Address, _ quicstream.ConnInfo) error {
		xfinishch <- vp

		return nil
	}

	yargs.SendMessageFunc = func(_ context.Context, _ quicstream.ConnInfo, i HandoverMessage) error {
		go func() {
			if err := xbroker.Receive(i); err != nil {
				ybroker.Log().Error().Err(err).Msg("send error")
			}
		}()

		return nil
	}
	ybroker.newVoteprooff = func(vp base.Voteproof) error {
		switch tvp := vp.(type) {
		case base.INITVoteproof:
			go func() {
				if err := ybroker.sendStagePoint(ctx, tvp.Point()); err != nil {
					ybroker.Log().Error().Err(err).Msg("new voteproof send stagepoint error")
				}
			}()
		case base.ACCEPTVoteproof:
			go func() {
				bm := t.newBlockMap(tvp.Point().Height(), t.Local, tvp.BallotMajority().NewBlock())
				if err := ybroker.sendBlockMap(ctx, tvp.Point(), bm); err != nil {
					ybroker.Log().Error().Err(err).Msg("new voteproof send blockmap error")
				}
			}()
		default:
			return errors.Errorf("unknown voteproof, %T", vp)
		}

		return nil
	}

	yfinishch := make(chan base.INITVoteproof, 1)
	yargs.WhenFinished = func(vp base.INITVoteproof, _ quicstream.ConnInfo) error {
		yfinishch <- vp

		return nil
	}

	point := base.RawPoint(33, 44)
	avp, ivp := t.VoteproofsPair(point.PrevRound(), point, nil, nil, nil, []base.LocalNode{base.RandomLocalNode()})

	t.Run(fmt.Sprintf("x, init voteproof, %q -> y", point), func() {
		isFinished, err := xbroker.sendVoteproof(ctx, ivp)
		t.NoError(err)
		t.False(isFinished)

		<-time.After(time.Second)

		t.NoError(xbroker.isCanceled())
		t.NoError(ybroker.isCanceled())
	})

	var lastvp base.INITVoteproof
	{
		point = point.NextHeight()
		avp, ivp = t.VoteproofsPair(ivp.Point().Point, point, avp.BallotMajority().NewBlock(), avp.BallotMajority().Proposal(), nil, []base.LocalNode{base.RandomLocalNode()})

		t.Run(fmt.Sprintf("x, accept voteproof, %q -> y", avp.Point()), func() {
			isFinished, err := xbroker.sendVoteproof(ctx, avp)
			t.NoError(err)
			t.False(isFinished)

			<-time.After(time.Second)
		})

		t.Run(fmt.Sprintf("x, init voteproof, %q -> y", ivp.Point()), func() {
			isFinished, err := xbroker.sendVoteproof(ctx, ivp)
			t.NoError(err)
			t.True(isFinished)
		})

		lastvp = ivp
	}

	t.T().Log("wait to finish")
	select {
	case <-time.After(time.Second * 3):
		t.NoError(errors.Errorf("failed to wait x finish"))
	case vp := <-xfinishch:
		base.EqualVoteproof(t.Assert(), lastvp, vp)
	}

	select {
	case <-time.After(time.Second * 3):
		t.NoError(errors.Errorf("failed to wait y finish"))
	case vp := <-yfinishch:
		base.EqualVoteproof(t.Assert(), lastvp, vp)
	}

	t.T().Log("check canceled")
	xfinished, _ := xbroker.isFinishedLocked.Value()
	t.True(xfinished)

	yisfinished, _ := ybroker.isFinishedLocked.Value()
	t.True(yisfinished)
}

func (t *testHandoverXYBroker) TestHandoverMessageCancel() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	xargs := t.xargs()
	xbroker := NewHandoverXBroker(ctx, xargs, quicstream.ConnInfo{})

	yargs := t.yargs(xbroker.id)
	ybroker := t.syncedHandoverYBroker(ctx, yargs, quicstream.ConnInfo{})
	canMoveConsensus, isAsked, err := ybroker.Ask()
	t.NoError(err)
	t.True(isAsked)
	t.False(canMoveConsensus)

	xargs.SendMessageFunc = func(_ context.Context, _ quicstream.ConnInfo, i HandoverMessage) error {
		if err := ybroker.Receive(i); err != nil {
			xbroker.Log().Error().Err(err).Msg("send error")
		}

		return nil
	}
	xargs.CheckIsReady = func() (bool, error) {
		return true, nil
	}

	yargs.SendMessageFunc = func(_ context.Context, _ quicstream.ConnInfo, i HandoverMessage) error {
		if err := xbroker.Receive(i); err != nil {
			ybroker.Log().Error().Err(err).Msg("send error")
		}

		return nil
	}
	ybroker.newVoteprooff = func(vp base.Voteproof) error {
		switch tvp := vp.(type) {
		case base.INITVoteproof:
			go func() {
				if err := ybroker.sendStagePoint(ctx, tvp.Point()); err != nil {
					ybroker.Log().Error().Err(err).Msg("new voteproof send stagepoint error")
				}
			}()
		case base.ACCEPTVoteproof:
			go func() {
				bm := t.newBlockMap(tvp.Point().Height(), t.Local, tvp.BallotMajority().NewBlock())
				if err := ybroker.sendBlockMap(ctx, tvp.Point(), bm); err != nil {
					ybroker.Log().Error().Err(err).Msg("new voteproof send blockmap error")
				}
			}()
		default:
			return errors.Errorf("unknown voteproof, %T", vp)
		}

		return nil
	}

	xcanceledch := make(chan error, 1)
	xargs.WhenCanceled = func(err error) {
		xcanceledch <- err
	}

	ycanceledch := make(chan error, 1)
	yargs.WhenCanceled = func(err error, _ quicstream.ConnInfo) {
		ycanceledch <- err
	}

	t.Run("send voteproof", func() {
		point := base.RawPoint(33, 44)
		_, ivp := t.VoteproofsPair(point.PrevRound(), point, nil, nil, nil, []base.LocalNode{base.RandomLocalNode()})

		isFinished, err := xbroker.sendVoteproof(ctx, ivp)
		t.NoError(err)
		t.False(isFinished)

		<-time.After(time.Second)
	})

	t.Run("y sends cancel message", func() {
		ybroker.cancel(nil)

		select {
		case <-time.After(time.Second * 2):
			t.NoError(errors.Errorf("failed to wait xbroker canceled"))
		case err := <-xcanceledch:
			t.Error(err)
			t.True(errors.Is(err, ErrHandoverCanceled))

			err = xbroker.isCanceled()
			t.Error(err)
			t.True(errors.Is(err, ErrHandoverCanceled))
		}

		select {
		case <-time.After(time.Second * 2):
			t.NoError(errors.Errorf("failed to wait ybroker canceled"))
		case err := <-ycanceledch:
			t.NoError(err)

			err = ybroker.isCanceled()
			t.Error(err)
			t.True(errors.Is(err, ErrHandoverCanceled))
		}
	})
}

func TestHandoverXYBroker(t *testing.T) {
	suite.Run(t, new(testHandoverXYBroker))
}
