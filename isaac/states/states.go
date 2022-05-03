package isaacstates

import (
	"context"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
)

var ignoreSwithingStateError = util.NewError("failed to switch state, but ignored")

var (
	timerIDBroadcastINITBallot   = util.TimerID("broadcast-init-ballot")
	timerIDBroadcastACCEPTBallot = util.TimerID("broadcast-accept-ballot")
)

type States struct {
	*logging.Logging
	*util.ContextDaemon
	box       *Ballotbox
	stateLock sync.RWMutex
	statech   chan switchContext
	vpch      chan base.Voteproof
	handlers  map[StateType]handler
	cs        handler
	timers    *util.Timers
	lvps      *LastVoteproofsHandler
}

func NewStates(box *Ballotbox) *States {
	st := &States{
		Logging: logging.NewLogging(func(lctx zerolog.Context) zerolog.Context {
			return lctx.Str("module", "states")
		}),
		box:      box,
		statech:  make(chan switchContext),
		vpch:     make(chan base.Voteproof),
		handlers: map[StateType]handler{},
		cs:       nil,
		timers: util.NewTimers([]util.TimerID{
			timerIDBroadcastINITBallot,
			timerIDBroadcastACCEPTBallot,
		}, false),
		lvps: NewLastVoteproofsHandler(),
	}

	st.ContextDaemon = util.NewContextDaemon("states", st.start)

	return st
}

func (st *States) SetHandler(h handler) *States {
	if st.ContextDaemon.IsStarted() {
		panic("can not set state handler; already started")
	}

	if i, ok := h.(interface{ setStates(*States) }); ok {
		i.setStates(st)
	}

	st.handlers[h.state()] = h

	if l, ok := h.(logging.SetLogging); ok {
		_ = l.SetLogging(st.Logging)
	}

	return st
}

func (st *States) SetLogging(l *logging.Logging) *logging.Logging {
	for i := range st.handlers {
		if j, ok := st.handlers[i].(logging.SetLogging); ok {
			_ = j.SetLogging(l)
		}
	}

	return st.Logging.SetLogging(l)
}

func (st *States) LastVoteproofsHandler() *LastVoteproofsHandler {
	return st.lvps
}

func (st *States) start(ctx context.Context) error {
	defer st.Log().Debug().Msg("states stopped")

	// NOTE set stopped as current
	switch h, found := st.handlers[StateStopped]; {
	case !found:
		return errors.Errorf("failed to find stopped handler")
	default:
		if _, err := h.enter(nil); err != nil {
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
		return errors.WithStack(serr)
	default:
		e := util.StringErrorFunc("failed to exit current state")
		deferred, err := current.exit(nil)
		if err != nil {
			return e(err, "failed to exit current")
		}

		st.callDeferStates(deferred, nil)

		st.setCurrent(nil)
	}

	return serr
}

func (st *States) startStatesSwitch(ctx context.Context) error {
	for {
		var sctx switchContext
		var vp base.Voteproof

		select {
		case <-ctx.Done():
			return errors.Wrap(ctx.Err(), "states stopped by context")
		case sctx = <-st.statech:
		case vp = <-st.box.Voteproof():
		case vp = <-st.vpch:
		}

		if vp != nil {
			if !st.lvps.IsNew(vp) {
				continue
			}

			switch err := st.voteproofToCurrent(vp, st.current()); {
			case err == nil:
				continue
			case !errors.As(err, &sctx):
				st.Log().Error().Err(err).
					Dict("voteproof", base.VoteproofLog(vp)).Msg("failed to handle voteproof")

				return errors.WithStack(err)
			}
		}

		if sctx != nil {
			if err := st.ensureSwitchState(sctx); err != nil {
				return errors.WithStack(err)
			}
		}
	}
}

func (st *States) Current() StateType {
	return st.current().state()
}

func (st *States) current() handler {
	st.stateLock.RLock()
	defer st.stateLock.RUnlock()

	return st.cs
}

func (st *States) setCurrent(cs handler) {
	st.stateLock.Lock()
	defer st.stateLock.Unlock()

	st.cs = cs
}

func (st *States) ensureSwitchState(sctx switchContext) error {
	var n int

	current := st.cs

	movetobroken := func(nsctx switchContext) switchContext {
		l := st.stateSwitchContextLog(nsctx, current)
		l.Error().Msg("failed to switch state; wil move to broken")

		n = 0

		from := StateEmpty
		if current != nil {
			from = current.state()
		}

		return newBrokenSwitchContext(from, nsctx)
	}

	nsctx := sctx
end:
	for {
		if n > 3 {
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
		case errors.Is(err, ignoreSwithingStateError):
			return nil
		case !errors.As(err, &rsctx):
			if nsctx.next() == StateBroken {
				st.Log().Error().Err(err).Msg("failed to switch to broken; will stop switching")

				return errors.Wrap(err, "failed to switch to broken")
			}

			if nsctx.from() == StateBroken {
				<-time.After(time.Second) // NOTE prevents too fast switching
			}

			nsctx = movetobroken(nsctx)

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

	cdefer, ndefer, err := st.exitAndEnter(sctx, current)
	if err != nil {
		switch {
		case errors.Is(err, ignoreSwithingStateError):
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
			if errors.Is(err, ignoreSwithingStateError) {
				l.Debug().Err(err).Msg("current state ignores switching state")

				return nil, nil, errors.WithStack(err)
			}

			st.cs = nil

			return nil, nil, e(err, "failed to exit current state")
		}
	}

	next := st.handlers[sctx.next()]

	ndefer, err := next.enter(sctx)
	if err != nil {
		if isSwitchContextError(err) {
			return nil, nil, err
		}

		return nil, nil, e(err, "failed to enter next state")
	}

	st.cs = next

	return cdefer, ndefer, nil
}

func (st *States) newState(sctx switchContext) error {
	l := st.stateSwitchContextLog(sctx, st.current())

	switch err := st.checkStateSwitchContext(sctx, st.current()); {
	case err == nil:
	case errors.Is(err, ignoreSwithingStateError):
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

func (*States) voteproofToCurrent(vp base.Voteproof, current handler) error {
	e := util.StringErrorFunc("failed to send voteproof to current")

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

	from := sctx.from()
	switch {
	case from == StateEmpty:
		from = current.state()
	default:
		if _, found := st.handlers[from]; !found {
			return errors.Errorf("unknown from state, %q", from)
		}
	}

	if _, found := st.handlers[sctx.next()]; !found {
		return errors.Errorf("unknown next state, %q", sctx.next())
	}

	switch {
	case from != current.state():
		return ignoreSwithingStateError.Errorf("from not matched")
	case sctx.next() == current.state():
		return ignoreSwithingStateError.Errorf("same next state")
	}

	return nil
}

func (st *States) stateSwitchContextLog(sctx switchContext, current handler) zerolog.Logger {
	return st.Log().With().
		Stringer("current_state", handlerLog(current)).
		Dict("next_state", switchContextLog(sctx)).Logger()
}

func (*States) broadcastBallot(base.Ballot) error {
	// BLOCK broadcast thru memberlist

	return nil
}

func (st *States) lastVoteproof() LastVoteproofs {
	return st.lvps.Last()
}

func (st *States) setLastVoteproof(vp base.Voteproof) bool {
	return st.lvps.Set(vp)
}
