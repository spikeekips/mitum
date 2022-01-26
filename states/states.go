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
	currentHandlerLock sync.RWMutex
	statech            chan stateSwitchContext
	voteproofch        chan voteproofWithState
	handlers           map[StateType]handler
	cs                 handler
}

func NewStates() *States {
	st := &States{
		Logging: logging.NewLogging(func(lctx zerolog.Context) zerolog.Context {
			return lctx.Str("module", "states")
		}),
		statech:     make(chan stateSwitchContext),
		voteproofch: make(chan voteproofWithState),
		handlers:    map[StateType]handler{},
		cs:          nil,
	}

	st.ContextDaemon = util.NewContextDaemon("states", st.start)

	return st
}

func (st *States) SetHandler(h handler) *States {
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
	if err := st.switchStates(stateSwitchContext{next: StateBooting}); err != nil {
		return errors.Wrap(err, "failed to enter booting state")
	}

	var stopch <-chan error
	go func() {
		stopch = st.startVoteproofCH(ctx)
	}()

	err := st.startStatesCH(ctx)
	if err == nil {
		err = <-stopch
	}

	// NOTE exit current
	current := st.current()
	if current == nil {
		return errors.Wrap(err, "")
	}

	e := util.StringErrorFunc("failed to exit current state")
	deferred, err := current.exit()
	if err != nil {
		return e(err, "failed to exit current")
	}

	if err := st.callDeferStates(deferred, nil); err != nil {
		return e(err, "")
	}

	st.cs = nil

	return nil
}

func (st *States) startStatesCH(ctx context.Context) error {
	var err error
end:
	for {
		select {
		case <-ctx.Done():
			err = errors.Wrap(ctx.Err(), "states stopped by parent context")

			break end
		case sctx := <-st.statech:
			e := st.switchStates(sctx)
			if e == nil {
				continue
			}

			st.Log().Error().Err(e).Msg("failed to switch state; stop states")

			err = errors.Wrap(e, "failed to switch state")

			break end
		}
	}

	return err
}

func (st *States) startVoteproofCH(ctx context.Context) <-chan error {
	errch := make(chan error, 1)

end:
	for {
		select {
		case <-ctx.Done():
			errch <- errors.Wrap(ctx.Err(), "states stopped by parent context")

			break end
		case vp := <-st.voteproofch:
			if err := st.voteproofToCurrent(vp, st.current()); err != nil {
				st.Log().Error().Err(err).Stringer("voteproof", util.Stringer(func() string {
					if vp.Voteproof == nil {
						return ""
					}

					return vp.Voteproof.ID()
				})).Msg("failed to handle voteproof")
			}
		}
	}

	return errch
}

func (st *States) current() handler {
	st.currentHandlerLock.RLock()
	defer st.currentHandlerLock.RUnlock()

	return st.cs
}

func (st *States) switchStates(sctx stateSwitchContext) error {
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

		return stateSwitchContext{from: from, next: StateBroken}
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
			return nil
		case errors.Is(err, IgnoreSwithingStateError):
			return nil
		case !errors.As(err, &nsctx):
			if sctx.next == StateBroken {
				st.Log().Error().Err(err).Msg("failed to switch to broken; will stop switching")

				return errors.Wrap(err, "failed to switch to broken")
			}

			if sctx.from == StateBroken {
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

	cdefer, ndefer, err := st.switchcurrentHandlerLocked(sctx, current)
	if err != nil {
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

func (st *States) switchcurrentHandlerLocked(sctx stateSwitchContext, current handler) (func() error, func() error, error) {
	st.currentHandlerLock.Lock()
	defer st.currentHandlerLock.Unlock()

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
		case sctx.next == StateBroken:
			l.Error().Err(err).Msg("failed to exit current state, but next is broken state; error will be ignored")
		default:
			st.cs = nil

			return nil, nil, e(err, "failed to exit current state")
		}
	}

	next := st.handlers[sctx.next]

	ndefer, err := next.enter(sctx.voteproof())
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
	return st.newVoteproofWithCurrent(vp, st.current())
}

func (st *States) newVoteproofWithCurrent(vp base.Voteproof, current handler) error {
	if current == nil {
		return nil
	}

	if _, err := st.checkVoteproofWithCurrent(vp, current); err != nil {
		return errors.Wrap(err, "failed to newVoteproof")
	}

	go func() {
		st.voteproofch <- vp.(voteproofWithState)
	}()

	return nil
}

func (st *States) voteproofToCurrent(vp base.Voteproof, current handler) error {
	e := util.StringErrorFunc("failed to send voteproof to current")
	nvp, err := st.checkVoteproofWithCurrent(vp, current)
	if err != nil {
		return e(err, "")
	}

	if err := current.newVoteproof(nvp); err != nil {
		return e(err, "")
	}

	return nil
}

func (st *States) checkVoteproofWithCurrent(vp base.Voteproof, current handler) (base.Voteproof, error) {
	vps, ok := vp.(voteproofWithState)
	switch {
	case !ok:
		return nil, errors.Errorf("not voteproofWithState")
	case vps.state() != current.state():
		return nil, errors.Errorf("not for current state, %q", current.state())
	}

	return vps.Voteproof, nil
}

func (st *States) callDeferStates(c, n func() error) error {
	st.currentHandlerLock.Lock()
	defer st.currentHandlerLock.Unlock()

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

func (st *States) checkStateSwitchContext(sctx stateSwitchContext, current handler) error {
	if current == nil {
		return nil
	}

	switch {
	case sctx.from == StateEmpty:
		sctx.from = current.state()
	default:
		if _, found := st.handlers[sctx.from]; !found {
			return IgnoreSwithingStateError.Errorf("unknown from state, %q", sctx.from)
		}
	}

	if _, found := st.handlers[sctx.next]; !found {
		return IgnoreSwithingStateError.Errorf("unknown next state, %q", sctx.next)
	}

	switch {
	case sctx.from != current.state():
		return IgnoreSwithingStateError.Errorf("from not matched")
	case sctx.next == current.state():
		if sctx.voteproof() == nil {
			return IgnoreSwithingStateError.Errorf("same next state, but empty voteproof")
		}

		if err := st.voteproofToCurrent(sctx.voteproof(), current); err != nil {
			return errors.Wrap(err, "")
		}

		return IgnoreSwithingStateError.Errorf("same next state with voteproof")
	}

	return nil
}

func (st *States) stateSwitchContextLog(sctx stateSwitchContext, current handler) zerolog.Logger {
	return st.Log().With().
		Object("next_state", sctx).
		Stringer("current_state", util.Stringer(func() string {
			if current == nil {
				return ""
			}

			return current.state().String()
		})).Logger()
}
