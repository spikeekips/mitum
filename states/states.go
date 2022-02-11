package states

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

var IgnoreSwithingStateError = util.NewError("failed to switch state, but ignored")

type States struct {
	*logging.Logging
	*util.ContextDaemon
	stateLock   sync.RWMutex
	statech     chan stateSwitchContext
	voteproofch chan base.Voteproof
	handlers    map[StateType]stateHandler
	cs          stateHandler
	timers      *util.Timers
}

func NewStates() *States {
	st := &States{
		Logging: logging.NewLogging(func(lctx zerolog.Context) zerolog.Context {
			return lctx.Str("module", "states")
		}),
		statech:     make(chan stateSwitchContext),
		voteproofch: make(chan base.Voteproof),
		handlers:    map[StateType]stateHandler{},
		cs:          nil,
		timers: util.NewTimers([]util.TimerID{
			timerIDBroadcastINITBallot,
		}, false),
	}

	st.ContextDaemon = util.NewContextDaemon("states", st.start)

	return st
}

func (st *States) SetHandler(h stateHandler) *States {
	if st.ContextDaemon.IsStarted() {
		panic("can not set state handler; already started")
	}

	st.handlers[h.state()] = h

	return st
}

func (st *States) start(ctx context.Context) error {
	defer st.Log().Debug().Msg("states stopped")

	// NOTE set stopped as current
	switch h, found := st.handlers[StateStopped]; {
	case !found:
		return errors.Errorf("failed to find stopped handler")
	default:
		st.cs = h
	}

	// NOTE entering to booting at starting
	if err := st.ensureSwitchState(newBootingSwitchContext()); err != nil {
		return errors.Wrap(err, "failed to enter booting state")
	}

	err := st.startStatesSwitch(ctx)

	// NOTE exit current
	switch current := st.current(); {
	case current == nil:
		return errors.WithStack(err)
	default:
		e := util.StringErrorFunc("failed to exit current state")
		deferred, err := current.exit()
		if err != nil {
			return e(err, "failed to exit current")
		}

		if err := st.callDeferStates(deferred, nil); err != nil {
			return e(err, "")
		}

		st.setCurrent(nil)
	}

	return err
}

func (st *States) startStatesSwitch(ctx context.Context) error {
	for {
		var sctx stateSwitchContext
		select {
		case <-ctx.Done():
			return errors.Wrap(ctx.Err(), "states stopped by context")
		case sctx = <-st.statech:
		case vp := <-st.voteproofch:
			err := st.voteproofToCurrent(vp, st.current())
			if err == nil {
				return nil
			}

			if !errors.As(err, &sctx) {
				st.Log().Error().Err(err).
					Dict("voteproof", base.VoteproofLog(vp)).Msg("failed to handle voteproof")

				return errors.WithStack(err)
			}
		}

		if err := st.ensureSwitchState(sctx); err != nil {
			return errors.WithStack(err)
		}
	}
}

func (st *States) current() stateHandler {
	st.stateLock.RLock()
	defer st.stateLock.RUnlock()

	return st.cs
}

func (st *States) setCurrent(cs stateHandler) {
	st.stateLock.Lock()
	defer st.stateLock.Unlock()

	st.cs = cs
}

func (st *States) ensureSwitchState(sctx stateSwitchContext) error {
	var n int

	current := st.cs

	movetobroken := func(nsctx stateSwitchContext) stateSwitchContext {
		l := st.stateSwitchContextLog(nsctx, current)
		l.Error().Msg("failed to switch state; wil move to broken")

		n = 0

		from := StateEmpty
		if current != nil {
			from = current.state()
		}

		return newBrokenSwitchContext(from, nsctx)
	}

end:
	for {
		if n > 3 {
			st.Log().Warn().Msg("suspicious infinit loop in switch states; > 3; will move to broken")

			sctx = movetobroken(sctx)

			continue
		}

		n++

		var nsctx stateSwitchContext
		switch err := st.switchState(sctx); {
		case err == nil:
			if sctx.next() == StateStopped {
				return errors.Wrap(sctx, "states stopped")
			}

			return nil
		case errors.Is(err, IgnoreSwithingStateError):
			return nil
		case !errors.As(err, &nsctx):
			if sctx.next() == StateBroken {
				st.Log().Error().Err(err).Msg("failed to switch to broken; will stop switching")

				return errors.Wrap(err, "failed to switch to broken")
			}

			if sctx.from() == StateBroken {
				<-time.After(time.Second) // NOTE prevents too fast switching
			}

			sctx = movetobroken(sctx)

			continue end
		default:
			sctx = nsctx
		}
	}
}

func (st *States) switchState(sctx stateSwitchContext) error {
	e := util.StringErrorFunc("failed to switch state")

	current := st.current()
	l := st.stateSwitchContextLog(sctx, current)

	cdefer, ndefer, err := st.exitAndEnter(sctx, current)
	if err != nil {
		if errors.Is(err, IgnoreSwithingStateError) {
			l.Debug().Msg("switching state ignored")

			return nil
		}

		l.Error().Err(err).Msg("failed to switch(locked)")

		return e(err, "")
	}

	if err := st.callDeferStates(cdefer, ndefer); err != nil {
		l.Error().Err(err).Msg("failed deferred")

		return e(err, "failed to deferred")
	}

	l.Debug().Msg("state switched")

	return nil
}

func (st *States) exitAndEnter(sctx stateSwitchContext, current stateHandler) (func() error, func() error, error) {
	st.stateLock.Lock()
	defer st.stateLock.Unlock()

	e := util.StringErrorFunc("failed to switch state")
	l := st.stateSwitchContextLog(sctx, current)

	if err := st.checkStateSwitchContext(sctx, current); err != nil {
		return nil, nil, e(err, "")
	}

	var cdefer, ndefer func() error

	// NOTE if switching to broken, error during exiting from current handler
	// will not be ignored
	if current != nil {
		switch i, err := current.exit(); {
		case err == nil:
			cdefer = i
		case sctx.next() == StateBroken:
			l.Error().Err(err).Msg("failed to exit current state, but next is broken state; error will be ignored")
		default:
			if errors.Is(err, IgnoreSwithingStateError) {
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
		return nil, nil, e(err, "failed to enter next state")
	}

	st.cs = next

	return cdefer, ndefer, nil
}

func (st *States) newState(sctx stateSwitchContext) error {
	l := st.stateSwitchContextLog(sctx, st.current())

	if err := st.checkStateSwitchContext(sctx, st.current()); err != nil {
		l.Error().Err(err).Msg("failed to switch state")

		return errors.Wrap(err, "failed to switch state")
	}

	go func() {
		st.statech <- sctx
	}()

	return nil
}

func (st *States) newVoteproof(vp base.Voteproof) error {
	current := st.current()
	if current == nil {
		st.Log().Debug().Msg("voteproof ignored; nil current")

		return nil
	}

	// BLOCK compare last init and accept voteproof

	go func() {
		st.voteproofch <- vp
	}()

	return nil
}

func (st *States) voteproofToCurrent(vp base.Voteproof, current stateHandler) error {
	// BLOCK compare last init and accept voteproof

	e := util.StringErrorFunc("failed to send voteproof to current")

	if err := current.newVoteproof(vp); err != nil {
		return e(err, "")
	}

	return nil
}

func (st *States) callDeferStates(c, n func() error) error {
	st.stateLock.Lock()
	defer st.stateLock.Unlock()

	err := func() error {
		if c != nil {
			if err := c(); err != nil {
				return errors.Wrap(err, "failed deferred of current state")
			}
		}

		if n != nil {
			if err := n(); err != nil {
				return errors.Wrap(err, "failed deferred of next state")
			}
		}

		return nil
	}()

	if err != nil && !errors.Is(err, IgnoreSwithingStateError) {
		st.cs = nil
	}

	return nil
}

func (st *States) checkStateSwitchContext(sctx stateSwitchContext, current stateHandler) error {
	if current == nil {
		return nil
	}

	from := sctx.from()
	switch {
	case from == StateEmpty:
		from = current.state()
	default:
		if _, found := st.handlers[from]; !found {
			return IgnoreSwithingStateError.Errorf("unknown from state, %q", from)
		}
	}

	if _, found := st.handlers[sctx.next()]; !found {
		return IgnoreSwithingStateError.Errorf("unknown next state, %q", sctx.next())
	}

	switch {
	case from != current.state():
		return IgnoreSwithingStateError.Errorf("from not matched")
	case sctx.next() == current.state():
		// BLOCK remove voteproof handover from test
		return IgnoreSwithingStateError.Errorf("same next state")
	}

	return nil
}

func (st *States) stateSwitchContextLog(sctx stateSwitchContext, current stateHandler) zerolog.Logger {
	l := st.Log().With().
		Stringer("current_state", stateHandlerLog(current))

	o, ok := sctx.(zerolog.LogObjectMarshaler)
	switch {
	case ok:
		l = l.Object("next_state", o)
	default:
		l = l.Stringer("from", sctx.from()).Stringer("next", sctx.next())
	}

	return l.Logger()
}

func (st *States) broadcastBallot(bl base.Ballot, tolocal bool) error {
	// BLOCK implement

	return nil
}
