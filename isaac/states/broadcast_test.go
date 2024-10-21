package isaacstates

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
	"golang.org/x/exp/slices"
)

type testBallotBroadcastTimers struct {
	isaac.BaseTestBallots
	priv      base.Privatekey
	networkID base.NetworkID
}

func (t *testBallotBroadcastTimers) SetupSuite() {
	t.priv = base.NewMPrivatekey()
	t.networkID = util.UUID().Bytes()
}

func (t *testBallotBroadcastTimers) newBallotBroadcaster(
	broadcast func(context.Context, base.Ballot) error,
) *ballotBroadcastTimers {
	timers, err := util.NewSimpleTimers(33, time.Millisecond*33)
	t.NoError(err)

	return newBallotBroadcastTimers(timers, broadcast, time.Second)
}

func (t *testBallotBroadcastTimers) newINITBallot(point base.Point) base.INITBallot {
	fact := t.NewINITBallotFact(point, valuehash.RandomSHA256(), valuehash.RandomSHA256())
	signfact := isaac.NewINITBallotSignFact(fact)
	t.NoError(signfact.NodeSign(t.priv, t.networkID, base.RandomAddress("")))

	return isaac.NewINITBallot(nil, signfact, nil)
}

func (t *testBallotBroadcastTimers) newACCEPTBallot(point base.Point) base.ACCEPTBallot {
	fact := t.NewACCEPTBallotFact(point, valuehash.RandomSHA256(), valuehash.RandomSHA256())
	signfact := isaac.NewACCEPTBallotSignFact(fact)
	t.NoError(signfact.NodeSign(t.priv, t.networkID, base.RandomAddress("")))

	return isaac.NewACCEPTBallot(nil, signfact, nil)
}

func (t *testBallotBroadcastTimers) isEqualTimerIDs(a, b []util.TimerID) {
	t.T().Log("a:", a)
	t.T().Log("b:", b)

	t.Equal(len(a), len(b))

	sort.Slice(a, func(i, j int) bool {
		return string(a[i]) < string(a[j])
	})
	sort.Slice(b, func(i, j int) bool {
		return string(b[i]) < string(b[j])
	})

	for i := range a {
		t.Equal(a[i], b[i])
	}
}

func (t *testBallotBroadcastTimers) TestINIT() {
	t.Run("no error", func() {
		bch := make(chan base.Ballot, 1)

		bbt := t.newBallotBroadcaster(func(_ context.Context, bl base.Ballot) error {
			bch <- bl

			return nil
		})
		defer bbt.timers.Stop()

		point := base.RawPoint(33, 0)
		t.T().Log("point:", point)

		bl := t.newINITBallot(point)

		var ts []util.TimerID
		ts = append(ts, bbt.timerID(bl))

		t.NoError(bbt.INIT(bl, 1))

		t.NoError(bbt.timers.Start(context.Background()))

		select {
		case <-time.After(time.Second):
			t.Fail("failed to wait ballot")
		case rbl := <-bch:
			base.EqualBallot(t.Assert(), bl, rbl)
		}

		t.T().Log("active timers:", bbt.timers.TimerIDs())

		t.isEqualTimerIDs(ts, bbt.timers.TimerIDs())
	})

	t.Run("already running", func() {
		bch := make(chan base.Ballot, 1)

		bbt := t.newBallotBroadcaster(func(_ context.Context, bl base.Ballot) error {
			bch <- bl

			return nil
		})
		defer bbt.timers.Stop()

		point := base.RawPoint(33, 0)
		t.T().Log("point:", point)

		var ts []util.TimerID
		bl := t.newINITBallot(point)
		ts = append(ts, bbt.timerID(bl))

		t.NoError(bbt.INIT(bl, 1))
		err := bbt.INIT(bl, 1)
		t.Error(err)
		t.ErrorContains(err, "already added")

		t.NoError(bbt.timers.Start(context.Background()))

		select {
		case <-time.After(time.Second):
			t.Fail("failed to wait ballot")
		case rbl := <-bch:
			base.EqualBallot(t.Assert(), bl, rbl)
		}

		t.T().Log("active timers:", bbt.timers.TimerIDs())

		t.isEqualTimerIDs(ts, bbt.timers.TimerIDs())
	})
}

func (t *testBallotBroadcastTimers) TestNotRemove() {
	threeballots := func(pa, pb base.Point, expected ...util.TimerID) {
		t.T().Log("point:", pb, "another:", pb)

		bch := make(chan base.Ballot, 1)
		bchi := make(chan base.Ballot, 1)
		bcha := make(chan base.Ballot, 1)

		bbt := t.newBallotBroadcaster(func(_ context.Context, bl base.Ballot) error {
			switch {
			case bl.Point().Point.Equal(pa):
				bch <- bl
			case bl.Point().Point.Equal(pb) && bl.Point().Stage() == base.StageINIT:
				bchi <- bl
			case bl.Point().Point.Equal(pb) && bl.Point().Stage() == base.StageACCEPT:
				bcha <- bl
			}

			return nil
		})
		defer bbt.timers.Stop()

		{
			ibl := t.newINITBallot(pb)
			t.NoError(bbt.INIT(ibl, 1))

			abl := t.newACCEPTBallot(pb)
			t.NoError(bbt.ACCEPT(abl, 1))
		}
		t.T().Log("active timers:", bbt.timers.TimerIDs())

		bl := t.newINITBallot(pa)
		t.NoError(bbt.INIT(bl, 1))

		t.NoError(bbt.timers.Start(context.Background()))

		if slices.Index(expected, ballotBroadcastTimerID(timerIDBroadcastINITBallot, pa)) >= 0 {
			select {
			case <-time.After(time.Second):
				t.Fail("failed to wait ballot")
			case rbl := <-bch:
				t.NotNil(rbl)
			}
		}

		if slices.Index(expected, ballotBroadcastTimerID(timerIDBroadcastINITBallot, pb)) >= 0 {
			select {
			case <-time.After(time.Second):
				t.Fail("failed to wait ballot")
			case rbl := <-bchi:
				t.NotNil(rbl)
			}
		}

		if slices.Index(expected, ballotBroadcastTimerID(timerIDBroadcastACCEPTBallot, pb)) >= 0 {
			select {
			case <-time.After(time.Second):
				t.Fail("failed to wait ballot")
			case rbl := <-bcha:
				t.NotNil(rbl)
			}
		}

		t.T().Log("active timers:", bbt.timers.TimerIDs())

		t.isEqualTimerIDs(expected, bbt.timers.TimerIDs())
	}

	t.Run("0 round; just before", func() {
		point := base.RawPoint(33, 0)
		p1 := point.PrevHeight().NextRound()

		threeballots(
			point, p1,
			ballotBroadcastTimerID(timerIDBroadcastACCEPTBallot, p1),
			ballotBroadcastTimerID(timerIDBroadcastINITBallot, point),
		)
	})

	t.Run("0 round; higher height", func() {
		point := base.RawPoint(33, 0)
		p1 := point.NextHeight()

		threeballots(
			point, p1,
			ballotBroadcastTimerID(timerIDBroadcastINITBallot, p1),
			ballotBroadcastTimerID(timerIDBroadcastACCEPTBallot, p1),
			ballotBroadcastTimerID(timerIDBroadcastINITBallot, point),
		)
	})

	t.Run("0 round; same height and higher stage", func() {
		point := base.RawPoint(33, 0)

		t.T().Log("point:", point)

		bch := make(chan base.Ballot, 1)
		bcha := make(chan base.Ballot, 1)

		bbt := t.newBallotBroadcaster(func(_ context.Context, bl base.Ballot) error {
			switch {
			case !bl.Point().Point.Equal(point):
			case bl.Point().Stage() == base.StageACCEPT:
				bcha <- bl
			default:
				bch <- bl
			}

			return nil
		})
		defer bbt.timers.Stop()

		var ts []util.TimerID

		{
			abl := t.newACCEPTBallot(point)
			ts = append(ts, bbt.timerID(abl))
			t.NoError(bbt.ACCEPT(abl, 1))
		}
		t.T().Log("active timers:", bbt.timers.TimerIDs())

		bl := t.newINITBallot(point)
		ts = append(ts, bbt.timerID(bl))
		t.NoError(bbt.INIT(bl, 1))

		t.NoError(bbt.timers.Start(context.Background()))

		select {
		case <-time.After(time.Second):
			t.Fail("failed to wait ballot")
		case rbl := <-bch:
			t.NotNil(rbl)
		}

		select {
		case <-time.After(time.Second):
			t.Fail("failed to wait ballot")
		case rbl := <-bcha:
			t.NotNil(rbl)
		}

		t.T().Log("active timers:", bbt.timers.TimerIDs())

		t.isEqualTimerIDs(ts, bbt.timers.TimerIDs())
	})

	t.Run("3 round; just before round", func() {
		point := base.RawPoint(33, 3)
		p1 := point.PrevRound()

		threeballots(
			point, p1,
			ballotBroadcastTimerID(timerIDBroadcastINITBallot, p1),
			ballotBroadcastTimerID(timerIDBroadcastACCEPTBallot, p1),
			ballotBroadcastTimerID(timerIDBroadcastINITBallot, point),
		)
	})

	t.Run("3 round; higher height", func() {
		point := base.RawPoint(33, 3)
		p1 := point.NextHeight().NextRound()

		threeballots(
			point, p1,
			ballotBroadcastTimerID(timerIDBroadcastINITBallot, p1),
			ballotBroadcastTimerID(timerIDBroadcastACCEPTBallot, p1),
			ballotBroadcastTimerID(timerIDBroadcastINITBallot, point),
		)
	})

	t.Run("3 round; higher round", func() {
		point := base.RawPoint(33, 3)
		p1 := point.NextRound()

		threeballots(
			point, p1,
			ballotBroadcastTimerID(timerIDBroadcastINITBallot, p1),
			ballotBroadcastTimerID(timerIDBroadcastACCEPTBallot, p1),
			ballotBroadcastTimerID(timerIDBroadcastINITBallot, point),
		)
	})
}

func (t *testBallotBroadcastTimers) TestRemove() {
	threeballots := func(
		pa, pb base.Point,
		anotherBallots func(bbt *ballotBroadcastTimers),
		expected ...util.TimerID,
	) {
		t.T().Log("point:", pa, "another:", pb)

		bch := make(chan base.Ballot, 1)
		bchi := make(chan base.Ballot, 1)
		bcha := make(chan base.Ballot, 1)

		bbt := t.newBallotBroadcaster(func(_ context.Context, bl base.Ballot) error {
			switch {
			case bl.Point().Point.Equal(pa):
				bch <- bl
			case bl.Point().Point.Equal(pb) && bl.Point().Stage() == base.StageINIT:
				bchi <- bl
			case bl.Point().Point.Equal(pb) && bl.Point().Stage() == base.StageACCEPT:
				bcha <- bl
			}

			return nil
		})
		defer bbt.timers.Stop()

		{
			ibl := t.newINITBallot(pb)
			t.NoError(bbt.INIT(ibl, 1))

			abl := t.newACCEPTBallot(pb)
			t.NoError(bbt.ACCEPT(abl, 1))
		}

		if anotherBallots != nil {
			anotherBallots(bbt)
		}

		t.T().Log("active timers:", bbt.timers.TimerIDs())

		bl := t.newINITBallot(pa)
		t.NoError(bbt.INIT(bl, 1))

		t.NoError(bbt.timers.Start(context.Background()))

		select {
		case <-time.After(time.Second):
			t.Fail("failed to wait ballot")
		case rbl := <-bch:
			t.NotNil(rbl)
		}

		select {
		case <-time.After(time.Second):
		case <-bchi:
			t.Fail("unexpected ballot")
		}

		select {
		case <-time.After(time.Second):
		case <-bcha:
			t.Fail("unexpected ballot")
		}

		t.T().Log("active timers:", bbt.timers.TimerIDs())

		t.isEqualTimerIDs(expected, bbt.timers.TimerIDs())
	}

	t.Run("0 round; previous and not just before height", func() {
		point := base.RawPoint(33, 0)

		threeballots(point,
			point.PrevHeight().PrevHeight().NextRound(),
			nil,
			ballotBroadcastTimerID(timerIDBroadcastINITBallot, point),
		)
	})

	t.Run("3 round; previous and not just before height", func() {
		point := base.RawPoint(33, 3)

		threeballots(
			point,
			point.PrevHeight().NextRound(),
			nil,
			ballotBroadcastTimerID(timerIDBroadcastINITBallot, point),
		)
	})

	t.Run("3 round; previous and not just before round", func() {
		point := base.RawPoint(33, 3)

		threeballots(
			point,
			point.PrevRound().PrevRound().PrevRound(),
			nil,
			ballotBroadcastTimerID(timerIDBroadcastINITBallot, point),
		)
	})

	t.Run("unavailable timer id", func() {
		point := base.RawPoint(33, 3)

		called := make(chan bool, 1)

		threeballots(
			point,
			point.PrevRound().PrevRound().PrevRound(),
			func(bbt *ballotBroadcastTimers) {
				_, err := bbt.timers.New(
					util.TimerID("showme"),
					func(uint64) time.Duration {
						return time.Nanosecond
					},
					func(context.Context, uint64) (bool, error) {
						called <- true

						return true, nil
					},
				)
				t.NoError(err)
			},
			ballotBroadcastTimerID(timerIDBroadcastINITBallot, point),
		)

		select {
		case <-time.After(time.Second):
		case <-called:
			t.Fail("unexpected ballot")
		}
	})
}

func (t *testBallotBroadcastTimers) TestSetBroadcaster() {
	bch := make(chan base.Ballot, 1)

	bbt := t.newBallotBroadcaster(func(_ context.Context, bl base.Ballot) error {
		bch <- bl

		return nil
	})
	defer bbt.timers.Stop()

	point := base.RawPoint(33, 0)
	t.T().Log("point:", point)

	bl := t.newINITBallot(point)

	var ts []util.TimerID
	ts = append(ts, bbt.timerID(bl))

	t.NoError(bbt.INIT(bl, 1))

	t.NoError(bbt.timers.Start(context.Background()))

	select {
	case <-time.After(time.Second):
		t.Fail("failed to wait ballot")
	case rbl := <-bch:
		base.EqualBallot(t.Assert(), bl, rbl)
	}

	t.T().Log("active timers:", bbt.timers.TimerIDs())

	t.isEqualTimerIDs(ts, bbt.timers.TimerIDs())

	nbch := make(chan base.Ballot, 1)
	nbb := bbt.Clone().SetBroadcasterFunc(func(_ context.Context, bl base.Ballot) error {
		nbch <- bl

		return nil
	})

	nbl := t.newACCEPTBallot(point)

	t.NoError(nbb.ACCEPT(nbl, 1))

	select {
	case <-time.After(time.Second):
		t.Fail("failed to wait ballot")
	case rbl := <-nbch:
		base.EqualBallot(t.Assert(), nbl, rbl)
	}

	t.T().Log("active timers:", bbt.timers.TimerIDs())
	t.T().Log("active timers:", nbb.timers.TimerIDs())
	t.Equal(bbt.timers.TimerIDs(), nbb.timers.TimerIDs())
}

func (t *testBallotBroadcastTimers) TestBroadcastError() {
	bbt := t.newBallotBroadcaster(func(context.Context, base.Ballot) error {
		return errors.Errorf("stop")
	})
	defer bbt.timers.Stop()

	point := base.RawPoint(33, 0)
	t.T().Log("point:", point)

	bl := t.newINITBallot(point)

	t.NoError(bbt.INIT(bl, 1))
	t.T().Log("active timers:", bbt.timers.TimerIDs())

	t.NoError(bbt.timers.Start(context.Background()))
	<-time.After(time.Second)
	t.T().Log("active timers:", bbt.timers.TimerIDs())
	t.Empty(bbt.timers.TimerIDs())
}

func TestBallotBroadcastTimers(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testBallotBroadcastTimers))
}
