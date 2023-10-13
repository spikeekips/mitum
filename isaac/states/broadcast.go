package isaacstates

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util"
)

var (
	timerIDBroadcastINITBallot            = util.TimerID("broadcast-init-ballot")
	timerIDBroadcastSuffrageConfirmBallot = util.TimerID("broadcast-suffrage-confirm-ballot")
	timerIDBroadcastACCEPTBallot          = util.TimerID("broadcast-accept-ballot")
)

type ballotBroadcastTimers struct {
	*sync.RWMutex
	timers    *util.SimpleTimers
	broadcast func(context.Context, base.Ballot) error
	points    map[util.TimerID]base.StagePoint
	interval  time.Duration
}

func newBallotBroadcastTimers(
	timers *util.SimpleTimers,
	broadcast func(context.Context, base.Ballot) error,
	interval time.Duration,
) *ballotBroadcastTimers {
	return &ballotBroadcastTimers{
		RWMutex:   &sync.RWMutex{},
		timers:    timers,
		broadcast: broadcast,
		points:    map[util.TimerID]base.StagePoint{},
		interval:  interval,
	}
}

func (bbt *ballotBroadcastTimers) Start(ctx context.Context) error {
	bbt.Lock()
	defer bbt.Unlock()

	return bbt.timers.Start(ctx)
}

func (bbt *ballotBroadcastTimers) Stop() error {
	bbt.Lock()
	defer bbt.Unlock()

	bbt.points = nil

	return bbt.timers.Stop()
}

func (bbt *ballotBroadcastTimers) StopTimers() error {
	bbt.Lock()
	defer bbt.Unlock()

	defer func() {
		for i := range bbt.points {
			delete(bbt.points, i)
		}
	}()

	return bbt.timers.StopAllTimers()
}

func (bbt *ballotBroadcastTimers) Clone() *ballotBroadcastTimers {
	bbt.RLock()
	defer bbt.RUnlock()

	return &ballotBroadcastTimers{
		RWMutex:   bbt.RWMutex,
		timers:    bbt.timers,
		broadcast: bbt.broadcast,
		points:    bbt.points,
		interval:  bbt.interval,
	}
}

func (bbt *ballotBroadcastTimers) SetBroadcasterFunc(
	f func(context.Context, base.Ballot) error,
) *ballotBroadcastTimers {
	bbt.Lock()
	defer bbt.Unlock()

	bbt.broadcast = f

	return bbt
}

func (bbt *ballotBroadcastTimers) SetInterval(i time.Duration) *ballotBroadcastTimers {
	bbt.Lock()
	defer bbt.Unlock()

	bbt.interval = i

	return bbt
}

func (bbt *ballotBroadcastTimers) Ballot(
	bl base.Ballot,
	intervalf func(uint64) time.Duration,
	timerIDs []util.TimerID,
) error {
	bbt.Lock()
	defer bbt.Unlock()

	if intervalf(0) < 1 {
		return nil
	}

	return bbt.addTimer(bl, intervalf, timerIDs)
}

func (bbt *ballotBroadcastTimers) INIT(bl base.INITBallot, initialWait time.Duration) error {
	bbt.Lock()
	defer bbt.Unlock()

	return bbt.addTimer(bl, bbt.defaultInterval(initialWait), []util.TimerID{
		timerIDBroadcastINITBallot,
		timerIDBroadcastSuffrageConfirmBallot,
		timerIDBroadcastACCEPTBallot,
	})
}

func (bbt *ballotBroadcastTimers) ACCEPT(bl base.ACCEPTBallot, initialWait time.Duration) error {
	bbt.Lock()
	defer bbt.Unlock()

	return bbt.addTimer(bl, bbt.defaultInterval(initialWait), []util.TimerID{
		timerIDBroadcastINITBallot,
		timerIDBroadcastSuffrageConfirmBallot,
		timerIDBroadcastACCEPTBallot,
	})
}

func (bbt *ballotBroadcastTimers) SuffrageConfirm(bl base.INITBallot, initialWait time.Duration) error {
	bbt.Lock()
	defer bbt.Unlock()

	return bbt.addTimer(bl, bbt.defaultInterval(initialWait), []util.TimerID{
		timerIDBroadcastINITBallot,
		timerIDBroadcastSuffrageConfirmBallot,
		timerIDBroadcastACCEPTBallot,
	})
}

func (bbt *ballotBroadcastTimers) addTimer(
	bl base.Ballot,
	intervalf func(uint64) time.Duration,
	timerIDs []util.TimerID,
) error {
	point := bl.Point()

	ti := bbt.timerID(bl)
	if len(ti) < 1 {
		return errors.Errorf("unknown timer id")
	}

	if _, found := bbt.points[ti]; found {
		return errors.Errorf("already added")
	}

	if _, err := bbt.timers.New(
		ti,
		intervalf,
		func(ctx context.Context, _ uint64) (bool, error) {
			return true, bbt.broadcast(ctx, bl)
		},
	); err != nil {
		return err
	}

	bbt.points[ti] = point

	return bbt.removeTimerIDs(point, timerIDs)
}

// removeTimerIDs removes old timers; it guarantees old, but same stage ballot
// can be broadcasted. Old, but same stage ballot may be stopped
// by new same stage ballot. This can be occurred at next round voting.
func (bbt *ballotBroadcastTimers) removeTimerIDs(point base.StagePoint, timerIDs []util.TimerID) error {
	filter := func(ti util.TimerID, p base.StagePoint) bool {
		if point.Round() == 0 {
			switch {
			case p.Height() > point.Height(),
				p.Height() == point.Height():
			case p.Height() == point.Height()-1:
				if point.Stage() == base.StageINIT && p.Stage().Compare(base.StageACCEPT) < 0 {
					return false
				}

				if point.Stage() == base.StageACCEPT {
					return false
				}
			default:
				return false
			}
		}

		if point.Round() > 0 {
			switch {
			case p.Height() > point.Height():
			case p.Height() == point.Height() && p.Round()+2 >= point.Round():
			default:
				return false
			}
		}

		for j := range timerIDs {
			if strings.HasPrefix(string(ti), string(timerIDs[j])) {
				return true
			}
		}

		return false
	}

	var actives []util.TimerID

	for ti := range bbt.points {
		switch {
		case filter(ti, bbt.points[ti]):
			actives = append(actives, ti)
		default:
			delete(bbt.points, ti)
		}
	}

	return bbt.timers.StopOthers(actives)
}

func (*ballotBroadcastTimers) timerID(bl base.Ballot) util.TimerID {
	var prefix util.TimerID

	switch st := bl.Point().Stage(); {
	case isaac.IsSuffrageConfirmBallotFact(bl.SignFact().Fact()):
		prefix = timerIDBroadcastSuffrageConfirmBallot
	case st == base.StageINIT:
		prefix = timerIDBroadcastINITBallot
	case st == base.StageACCEPT:
		prefix = timerIDBroadcastACCEPTBallot
	default:
		return ""
	}

	return ballotBroadcastTimerID(prefix, bl.Point().Point)
}

func (bbt *ballotBroadcastTimers) defaultInterval(initial time.Duration) func(uint64) time.Duration {
	ninitial := initial
	if ninitial < 1 {
		ninitial = time.Nanosecond
	}

	return func(i uint64) time.Duration {
		if i < 1 {
			return ninitial
		}

		return bbt.interval
	}
}

func ballotBroadcastTimerID(prefix util.TimerID, point base.Point) util.TimerID {
	return prefix + "/" + util.TimerID(point.String())
}
