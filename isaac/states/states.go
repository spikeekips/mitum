package isaacstates

import (
	"context"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
)

var ErrIgnoreSwithingState = util.NewMError("failed to switch state, but ignored")

var (
	timerIDBroadcastINITBallot            = util.TimerID("broadcast-init-ballot")
	timerIDBroadcastSuffrageConfirmBallot = util.TimerID("broadcast-suffrage-confirm-ballot")
	timerIDBroadcastACCEPTBallot          = util.TimerID("broadcast-accept-ballot")
)

type StatesArgs struct {
	Ballotbox              *Ballotbox
	BallotStuckResolver    BallotStuckResolver
	LastVoteproofsHandler  *LastVoteproofsHandler
	IsInSyncSourcePoolFunc func(base.Address) bool
	BallotBroadcaster      BallotBroadcaster
	WhenStateSwitchedFunc  func(StateType)
}

func NewStatesArgs() *StatesArgs {
	return &StatesArgs{
		LastVoteproofsHandler:  NewLastVoteproofsHandler(),
		IsInSyncSourcePoolFunc: func(base.Address) bool { return false },
		WhenStateSwitchedFunc:  func(StateType) {},
	}
}

type States struct {
	cs handler
	*logging.Logging
	local       base.LocalNode
	params      *isaac.LocalParams
	args        *StatesArgs
	statech     chan switchContext
	vpch        chan base.Voteproof
	newHandlers map[StateType]newHandler
	*util.ContextDaemon
	timers    *util.Timers
	stateLock sync.RWMutex
}

func NewStates(local base.LocalNode, params *isaac.LocalParams, args *StatesArgs) *States {
	st := &States{
		Logging: logging.NewLogging(func(lctx zerolog.Context) zerolog.Context {
			return lctx.Str("module", "states")
		}),
		local:       local,
		params:      params,
		args:        args,
		statech:     make(chan switchContext),
		vpch:        make(chan base.Voteproof),
		newHandlers: map[StateType]newHandler{},
		cs:          nil,
		timers: util.NewTimers([]util.TimerID{
			timerIDBroadcastINITBallot,
			timerIDBroadcastSuffrageConfirmBallot,
			timerIDBroadcastACCEPTBallot,
		}, false),
	}

	cancelf := func() {}

	if st.args.Ballotbox != nil {
		f, cancel := st.mimicBallotFunc()
		st.args.Ballotbox.SetNewBallotFunc(f)

		cancelf = cancel
	}

	st.ContextDaemon = util.NewContextDaemon(st.startFunc(cancelf))

	return st
}

func (st *States) SetHandler(state StateType, h newHandler) *States {
	if st.ContextDaemon.IsStarted() {
		panic("can not set state newHandler; already started")
	}

	if i, ok := (interface{})(h).(interface{ setStates(*States) }); ok {
		i.setStates(st)
	}

	st.newHandlers[state] = h

	if l, ok := (interface{})(h).(logging.SetLogging); ok {
		_ = l.SetLogging(st.Logging)
	}

	return st
}

func (st *States) SetLogging(l *logging.Logging) *logging.Logging {
	for i := range st.newHandlers {
		if j, ok := (interface{})(st.newHandlers[i]).(logging.SetLogging); ok {
			_ = j.SetLogging(l)
		}
	}

	return st.Logging.SetLogging(l)
}

func (st *States) SetWhenStateSwitched(f func(StateType)) {
	st.args.WhenStateSwitchedFunc = f
}

func (st *States) WhenEmptyMembers() {
	current := st.current()
	if current == nil {
		return
	}

	current.whenEmptyMembers()
}

func (st *States) Hold() error {
	current := st.current()
	if current == nil {
		return nil
	}

	st.Log().Debug().Msg("states holded")

	return st.switchState(newStoppedSwitchContext(current.state(), nil))
}

func (st *States) MoveState(sctx switchContext) error {
	l := st.stateSwitchContextLog(sctx, st.current())

	switch err := st.checkStateSwitchContext(sctx, st.current()); {
	case err == nil:
	case errors.Is(err, ErrIgnoreSwithingState):
		return nil
	default:
		l.Error().Err(err).Msg("failed to switch state")

		return errors.Wrap(err, "failed to switch state")
	}

	go func() {
		st.statech <- sctx
	}()

	return nil
}

func (st *States) Current() StateType {
	return st.current().state()
}

func (st *States) startFunc(cancel func()) func(context.Context) error {
	return func(ctx context.Context) error {
		defer cancel()
		defer st.Log().Debug().Msg("states stopped")

		// NOTE set stopped as current
		switch newHandler, found := st.newHandlers[StateStopped]; {
		case !found:
			return errors.Errorf("failed to find stopped handler")
		default:
			h, err := newHandler.new()
			if err != nil {
				return errors.WithMessage(err, "failed to create stopped new handler")
			}

			if _, err := h.enter(StateEmpty, nil); err != nil {
				return errors.Errorf("failed to enter stopped handler")
			}

			st.cs = h
		}

		// NOTE entering to booting at starting
		if err := st.ensureSwitchState(newBootingSwitchContext(StateStopped)); err != nil {
			return errors.Wrap(err, "failed to enter booting state")
		}

		serr := st.startStatesSwitch(ctx)

		// NOTE exit current
		switch current := st.current(); {
		case current == nil:
			return serr
		default:
			if err := st.switchState(newStoppedSwitchContext(current.state(), serr)); err != nil {
				st.Log().Error().Err(err).Msg("failed to switch to stopped; ignored")
			}
		}

		return serr
	}
}

func (st *States) startStatesSwitch(ctx context.Context) error {
	var resolvervpch <-chan base.Voteproof

	switch {
	case st.args.BallotStuckResolver == nil:
		resolvervpch = make(chan base.Voteproof)
	default:
		resolvervpch = st.args.BallotStuckResolver.Voteproof()
	}

	for {
		var sctx switchContext
		var vp base.Voteproof

		select {
		case <-ctx.Done():
			return errors.Wrap(ctx.Err(), "states stopped by context")
		case sctx = <-st.statech:
		case vp = <-st.args.Ballotbox.Voteproof():
		case vp = <-resolvervpch:
		case vp = <-st.vpch:
		}

		if vp != nil {
			if !st.args.LastVoteproofsHandler.IsNew(vp) {
				continue
			}

			switch err := st.voteproofToCurrent(vp, st.current()); {
			case err == nil:
				continue
			case !errors.As(err, &sctx):
				st.Log().Error().Err(err).
					Dict("voteproof", base.VoteproofLog(vp)).Msg("failed to handle voteproof")

				return err
			}
		}

		if sctx != nil {
			if err := st.ensureSwitchState(sctx); err != nil {
				return err
			}
		}
	}
}

func (st *States) current() handler {
	st.stateLock.RLock()
	defer st.stateLock.RUnlock()

	return st.cs
}

func (st *States) ensureSwitchState(sctx switchContext) error {
	var n int

	movetobroken := func(err error) switchContext {
		st.Log().Error().Err(err).Msg("failed to switch state; wil move to broken")

		n = 0

		return newBrokenSwitchContextFromEmpty(err)
	}

	nsctx := sctx
end:
	for {
		if n > 3 { //nolint:gomnd //...
			st.Log().Warn().Msg("suspicious infinite loop in switch states; > 3; will move to broken")

			nsctx = movetobroken(nsctx)

			continue
		}

		n++

		var rsctx switchContext

		switch err := st.switchState(nsctx); {
		case err == nil:
			if nsctx.next() == StateStopped {
				return errors.Wrap(nsctx, "states stopped")
			}

			return nil
		case errors.Is(err, ErrIgnoreSwithingState):
			return nil
		case !errors.As(err, &rsctx):
			if nsctx.next() == StateBroken {
				st.Log().Error().Err(err).Msg("failed to switch to broken; will stop switching")

				return errors.Wrap(err, "failed to switch to broken")
			}

			nsctx = movetobroken(err)

			continue end
		default:
			nsctx = rsctx
		}
	}
}

func (st *States) switchState(sctx switchContext) error {
	e := util.StringErrorFunc("failed to switch state")

	current := st.current()
	l := st.stateSwitchContextLog(sctx, current)

	if current != nil && current.state() == StateStopped {
		switch sctx.next() {
		case StateBooting, StateBroken:
		default:
			return ErrIgnoreSwithingState.Errorf("state stopped, next should be StateBooting or StateBroken")
		}
	}

	cdefer, ndefer, err := st.exitAndEnter(sctx, current)
	if err != nil {
		switch {
		case errors.Is(err, ErrIgnoreSwithingState):
			l.Debug().Msg("switching state ignored")

			return nil
		case isSwitchContextError(err):
			return err
		default:
			l.Error().Err(err).Msg("failed to switch(locked)")

			return e(err, "")
		}
	}

	st.callDeferStates(cdefer, ndefer)

	st.args.WhenStateSwitchedFunc(sctx.next())

	l.Debug().Msg("state switched")

	return nil
}

func (st *States) exitAndEnter(sctx switchContext, current handler) (func(), func(), error) {
	st.stateLock.Lock()
	defer st.stateLock.Unlock()

	e := util.StringErrorFunc("failed to switch state")
	l := st.stateSwitchContextLog(sctx, current)

	if err := st.checkStateSwitchContext(sctx, current); err != nil {
		return nil, nil, e(err, "")
	}

	var cdefer, ndefer func()

	// NOTE if switching to broken, error during exiting from current handler
	// will not be ignored
	if current != nil {
		switch i, err := current.exit(sctx); {
		case err == nil:
			cdefer = i
		case sctx.next() == StateBroken:
			l.Error().Err(err).Msg("failed to exit current state, but next is broken state; error will be ignored")
		default:
			if errors.Is(err, ErrIgnoreSwithingState) {
				l.Debug().Err(err).Msg("current state ignores switching state")

				return nil, nil, err
			}

			return nil, nil, e(err, "failed to exit current state")
		}
	}

	nextHandler, err := st.newHandlers[sctx.next()].new()
	if err != nil {
		return nil, nil, e(err, "failed to create new handler, %q", sctx.next())
	}

	ndefer, err = nextHandler.enter(current.state(), sctx)
	if err != nil {
		if isSwitchContextError(err) {
			st.cs = nextHandler

			return nil, nil, err
		}

		return nil, nil, e(err, "failed to enter next state")
	}

	st.cs = nextHandler

	return cdefer, ndefer, nil
}

func (st *States) voteproofToCurrent(vp base.Voteproof, current handler) error {
	e := util.StringErrorFunc("failed to send voteproof to current")

	st.Log().Debug().Interface("voteproof", vp).Msg("new voteproof")

	if err := current.newVoteproof(vp); err != nil {
		return e(err, "")
	}

	return nil
}

func (*States) callDeferStates(c, n func()) {
	go func() {
		if c != nil {
			c()
		}

		if n != nil {
			n()
		}
	}()
}

func (st *States) checkStateSwitchContext(sctx switchContext, current handler) error {
	if current == nil {
		return nil
	}

	if _, found := st.newHandlers[sctx.next()]; !found {
		return errors.Errorf("unknown next state, %q", sctx.next())
	}

	if !sctx.ok(current.state()) {
		return ErrIgnoreSwithingState.Errorf("not ok")
	}

	if sctx.next() == current.state() {
		return ErrIgnoreSwithingState.Errorf("same next state")
	}

	return nil
}

func (st *States) stateSwitchContextLog(sctx switchContext, current handler) zerolog.Logger {
	return st.Log().With().
		Stringer("current_state", handlerLog(current)).
		Dict("next_state", switchContextLog(sctx)).Logger()
}

func (st *States) voteproofs(point base.StagePoint) (LastVoteproofs, bool) {
	return st.args.LastVoteproofsHandler.Voteproofs(point)
}

func (st *States) lastVoteproof() LastVoteproofs {
	return st.args.LastVoteproofsHandler.Last()
}

func (st *States) setLastVoteproof(vp base.Voteproof) bool {
	return st.args.LastVoteproofsHandler.Set(vp)
}

// mimicBallotFunc mimics incoming ballot when node can not broadcast ballot; this will
// prevent to be gussed by the other nodes, local node is dead.
// - ballot signer should be in sync sources
func (st *States) mimicBallotFunc() (func(base.Ballot), func()) {
	mimicBallotf := st.mimicBallot()

	timers := util.NewTimers([]util.TimerID{
		timerIDBroadcastINITBallot,
		timerIDBroadcastACCEPTBallot,
	}, false)

	votef := func(bl base.Ballot, threshold base.Threshold) error {
		return nil
	}

	if st.args.Ballotbox != nil {
		votef = func(bl base.Ballot, threshold base.Threshold) error {
			_, err := st.args.Ballotbox.Vote(bl, threshold)

			return err
		}
	}

	return func(bl base.Ballot) {
			if bl.SignFact().Node().Equal(st.local.Address()) {
				return
			}

			switch s := st.current().state(); {
			case bl.SignFact().Node().Equal(st.local.Address()):
				return
			case s != StateSyncing && s != StateBroken:
				if err := timers.StopTimersAll(); err != nil {
					st.Log().Error().Err(err).Msg("failed to stop mimic timers; ignore")
				}

				return
			case !st.args.IsInSyncSourcePoolFunc(bl.SignFact().Node()):
				return
			case st.filterMimicBallot(bl):
				return
			}

			newbl := mimicBallotf(bl)
			if newbl == nil {
				return
			}

			l := st.Log().With().Interface("ballot", bl).Interface("new_ballot", newbl).Logger()

			go func() {
				if err := votef(newbl, st.params.Threshold()); err != nil {
					l.Error().Err(err).Msg("failed to vote mimic ballot")
				}
			}()

			var timerid util.TimerID

			switch newbl.Point().Stage() {
			case base.StageINIT:
				timerid = timerIDBroadcastINITBallot
			case base.StageACCEPT:
				timerid = timerIDBroadcastACCEPTBallot
			default:
				return
			}

			if err := broadcastBallot(
				newbl,
				timers,
				timerid,
				st.args.BallotBroadcaster.Broadcast,
				st.Logging,
				func(i int, _ time.Duration) time.Duration {
					if i < 1 {
						return time.Nanosecond
					}

					return st.params.IntervalBroadcastBallot()
				},
			); err != nil {
				l.Error().Err(err).Msg("failed to broadcast mimic ballot")

				return
			}

			if err := timers.StartTimers([]util.TimerID{timerid}, false); err != nil {
				l.Error().Err(err).Msg("failed to broadcast mimic ballot")
			}
		}, func() {
			_ = timers.Stop()
		}
}

func (st *States) mimicBallot() func(base.Ballot) base.Ballot {
	var lock sync.Mutex

	return func(bl base.Ballot) base.Ballot {
		lock.Lock()
		defer lock.Unlock()

		switch _, found, err := st.args.BallotBroadcaster.Ballot(
			bl.Point().Point,
			bl.Point().Stage(),
			isaac.IsSuffrageConfirmBallotFact(bl.SignFact().Fact()),
		); {
		case err != nil:
			return nil
		case found:
			return nil
		}

		l := st.Log().With().Interface("ballot", bl).Logger()

		switch i, err := st.signMimicBallot(bl); {
		case err != nil:
			l.Error().Err(err).Msg("failed to mimic")

			return nil
		default:
			return i
		}
	}
}

func (st *States) signMimicBallot(bl base.Ballot) (base.Ballot, error) {
	var withdraws []base.SuffrageWithdrawOperation

	if w, ok := bl.(base.HasWithdraws); ok {
		withdraws = w.Withdraws()
	}

	return mimicBallot(
		st.local,
		st.params,
		bl.SignFact().Fact().(base.BallotFact), //nolint:forcetypeassert //...
		withdraws,
		bl.Voteproof(),
	)
}

func (st *States) filterMimicBallot(bl base.Ballot) bool {
	l := st.Log().With().Interface("ballot", bl).Logger()

	// NOTE if local is in withdraws, ignore
	switch w, ok := bl.(base.HasWithdraws); {
	case !ok:
	default:
		if util.InSliceFunc(w.Withdraws(), func(i base.SuffrageWithdrawOperation) bool {
			return i.WithdrawFact().Node().Equal(st.local.Address())
		}) >= 0 {
			l.Debug().Msg("local in withdraws; ignore")

			return true
		}
	}

	if w, ok := bl.Voteproof().(base.HasWithdraws); ok {
		if util.InSliceFunc(w.Withdraws(), func(i base.SuffrageWithdrawOperation) bool {
			return i.WithdrawFact().Node().Equal(st.local.Address())
		}) >= 0 {
			l.Debug().Msg("local in withdraws voteproof; ignore")

			return true
		}
	}

	return false
}

func mimicBallot(
	local base.LocalNode,
	params *isaac.LocalParams,
	fact base.BallotFact,
	withdraws []base.SuffrageWithdrawOperation,
	voteproof base.Voteproof,
) (base.Ballot, error) {
	var newbl base.Ballot

	switch t := fact.(type) {
	case isaac.SuffrageConfirmBallotFact:
		sf := isaac.NewINITBallotSignFact(t)

		if err := sf.NodeSign(local.Privatekey(), params.NetworkID(), local.Address()); err != nil {
			return nil, err
		}

		newbl = isaac.NewINITBallot(voteproof, sf, nil)
	case base.INITBallotFact:
		sf := isaac.NewINITBallotSignFact(t)

		if err := sf.NodeSign(local.Privatekey(), params.NetworkID(), local.Address()); err != nil {
			return nil, err
		}

		newbl = isaac.NewINITBallot(voteproof, sf, withdraws)
	case isaac.ACCEPTBallotFact:
		sf := isaac.NewACCEPTBallotSignFact(t)

		if err := sf.NodeSign(local.Privatekey(), params.NetworkID(), local.Address()); err != nil {
			return nil, err
		}

		newbl = isaac.NewACCEPTBallot( //nolint:forcetypeassert //...
			voteproof.(base.INITVoteproof),
			sf,
			withdraws,
		)
	default:
		return nil, errors.Errorf("unknown ballot, %T", fact)
	}

	return newbl, nil
}
