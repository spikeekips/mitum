package isaacstates

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
)

type baseTestHandoverBroker struct {
	isaac.BaseTestBallots
}

func (t *baseTestHandoverBroker) xargs() *HandoverXBrokerArgs {
	args := NewHandoverXBrokerArgs(t.Local, t.LocalParams.NetworkID())
	args.CheckIsReady = func() (bool, error) {
		return true, nil
	}

	return args
}

func (t *baseTestHandoverBroker) yargs() *HandoverYBrokerArgs {
	args := NewHandoverYBrokerArgs(t.LocalParams.NetworkID())
	args.WhenCanceled = func(error) {}

	return args
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
	yargs := t.yargs()

	xbroker := NewHandoverXBroker(ctx, xargs)
	ybroker := NewHandoverYBroker(ctx, yargs, xbroker.id)

	xbroker.SetLogging(logging.TestNilLogging)
	ybroker.SetLogging(logging.TestNilLogging)

	xargs.ReadyEnd = 0
	xargs.SendFunc = func(_ context.Context, i interface{}) error {
		if err := ybroker.receive(i); err != nil {
			xbroker.Log().Error().Err(err).Msg("send error")
		}

		return nil
	}
	xargs.CheckIsReady = func() (bool, error) {
		return true, nil
	}

	yargs.SendFunc = func(_ context.Context, i interface{}) error {
		if err := xbroker.receive(i); err != nil {
			ybroker.Log().Error().Err(err).Msg("send error")
		}

		return nil
	}
	yargs.NewVoteproof = func(vp base.Voteproof) error {
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
			return errors.Errorf("unknown voteproof, %T", t)
		}

		return nil
	}

	finishch := make(chan base.INITVoteproof, 1)
	yargs.WhenFinished = func(vp base.INITVoteproof) error {
		finishch <- vp

		return nil
	}

	point := base.RawPoint(33, 44)
	avp, ivp := t.VoteproofsPair(point.PrevRound(), point, nil, nil, nil, []base.LocalNode{base.RandomLocalNode()})

	t.Run(fmt.Sprintf("x, init voteproof, %q -> y", point), func() {
		isFinished, err := xbroker.sendVoteproof(ctx, ivp)
		t.NoError(err)
		t.False(isFinished)

		<-time.After(time.Second)
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
		t.NoError(errors.Errorf("failed to wait to be finished"))
	case vp := <-finishch:
		base.EqualVoteproof(t.Assert(), lastvp, vp)
	}

	<-time.After(time.Second * 2)

	t.T().Log("check canceled")
	err := xbroker.isCanceled()
	t.Error(err)
	t.True(errors.Is(err, errHandoverCanceled))

	err = ybroker.isCanceled()
	t.Error(err)
	t.True(errors.Is(err, errHandoverCanceled))
}

func (t *testHandoverXYBroker) TestHandoverMessageCancel() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	xargs := t.xargs()
	yargs := t.yargs()

	xbroker := NewHandoverXBroker(ctx, xargs)
	ybroker := NewHandoverYBroker(ctx, yargs, xbroker.id)

	xbroker.SetLogging(logging.TestNilLogging)
	ybroker.SetLogging(logging.TestNilLogging)

	xargs.SendFunc = func(_ context.Context, i interface{}) error {
		if err := ybroker.receive(i); err != nil {
			xbroker.Log().Error().Err(err).Msg("send error")
		}

		return nil
	}
	xargs.CheckIsReady = func() (bool, error) {
		return true, nil
	}

	yargs.SendFunc = func(_ context.Context, i interface{}) error {
		if err := xbroker.receive(i); err != nil {
			ybroker.Log().Error().Err(err).Msg("send error")
		}

		return nil
	}
	yargs.NewVoteproof = func(vp base.Voteproof) error {
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
			return errors.Errorf("unknown voteproof, %T", t)
		}

		return nil
	}

	xcanceledch := make(chan error, 1)
	xargs.WhenCanceled = func(err error) {
		xcanceledch <- err
	}

	ycanceledch := make(chan error, 1)
	yargs.WhenCanceled = func(err error) {
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
			t.True(errors.Is(err, errHandoverCanceled))

			err = xbroker.isCanceled()
			t.Error(err)
			t.True(errors.Is(err, errHandoverCanceled))
		}

		select {
		case <-time.After(time.Second * 2):
			t.NoError(errors.Errorf("failed to wait ybroker canceled"))
		case err := <-ycanceledch:
			t.NoError(err)

			err = ybroker.isCanceled()
			t.Error(err)
			t.True(errors.Is(err, errHandoverCanceled))
		}
	})
}

func TestHandoverXYBroker(t *testing.T) {
	suite.Run(t, new(testHandoverXYBroker))
}
