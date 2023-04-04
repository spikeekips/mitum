package isaacstates

import (
	"fmt"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
)

type StateType string

const (
	StateEmpty = StateType("")
	// StateStopped indicates all processes is finished.
	StateStopped = StateType("STOPPED")
	// StateBooting indicates node checks it's state.
	StateBooting = StateType("BOOTING")
	// StateJoining indicates node is trying to join consensus.
	StateJoining = StateType("JOINING")
	// StateConsensus indicates node participates consensus with the other
	// nodes.
	StateConsensus = StateType("CONSENSUS")
	// StateSyncing indicates node is syncing block.
	StateSyncing = StateType("SYNCING")
	// StateHandover indicates node tries to replace the existing same node.
	StateHandover = StateType("HANDOVER")
	// StateBroken is used when something wrong in states.
	StateBroken = StateType("BROKEN")
)

func (s StateType) String() string {
	return string(s)
}

func (s StateType) IsValid([]byte) error {
	switch s {
	case StateEmpty,
		StateStopped,
		StateBooting,
		StateJoining,
		StateConsensus,
		StateSyncing,
		StateHandover,
		StateBroken:
		return nil
	default:
		return util.ErrInvalid.Errorf("unknown StateType, %q", s)
	}
}

type newHandler interface {
	new() (handler, error)
	setStates(*States)
}

type handler interface {
	state() StateType
	enter(from StateType, _ switchContext) (func(), error)
	exit(switchContext) (func(), error)
	newVoteproof(base.Voteproof) error
	whenEmptyMembers()
	allowConsensus() bool
	setAllowConsensus(bool)
}

func handlerLog(st handler) fmt.Stringer {
	return util.Stringer(func() string {
		if st == nil {
			return ""
		}

		return st.state().String()
	})
}

type switchContext interface {
	next() StateType
	Error() string
	ok(current StateType) bool
}

type baseSwitchContext struct { //nolint:errname //...
	okf func(StateType) bool
	n   StateType
}

func newBaseSwitchContext(next StateType, okf func(StateType) bool) baseSwitchContext {
	return baseSwitchContext{
		n:   next,
		okf: okf,
	}
}

func (s baseSwitchContext) next() StateType {
	return s.n
}

func (s baseSwitchContext) ok(current StateType) bool {
	return s.okf(current)
}

func (s baseSwitchContext) Error() string {
	return s.String()
}

func (s baseSwitchContext) String() string {
	return fmt.Sprintf("state switch next=%s", s.n)
}

func (s baseSwitchContext) MarshalZerologObject(e *zerolog.Event) {
	e.Stringer("next", s.n)
}

type baseErrorSwitchContext struct { //nolint:errname //...
	err error
	baseSwitchContext
}

func newBaseErrorSwitchContext(next StateType, err error, okf func(StateType) bool) baseErrorSwitchContext {
	return baseErrorSwitchContext{
		baseSwitchContext: newBaseSwitchContext(next, okf),
		err:               err,
	}
}

func (s baseErrorSwitchContext) Error() string {
	if s.err != nil {
		return s.err.Error()
	}

	return s.String()
}

func (s baseErrorSwitchContext) Unwrap() error {
	return s.err
}

func (s baseErrorSwitchContext) MarshalZerologObject(e *zerolog.Event) {
	s.baseSwitchContext.MarshalZerologObject(e)

	if s.err != nil {
		e.Err(s.err)
	}
}

func switchContextLog(sctx switchContext) *zerolog.Event {
	e := zerolog.Dict()

	o, ok := sctx.(zerolog.LogObjectMarshaler)

	switch {
	case ok:
		e = e.EmbedObject(o)
	default:
		e = e.Stringer("next", sctx.next())
	}

	return e
}

func isSwitchContextError(err error) bool {
	var sctx switchContext
	return errors.As(err, &sctx)
}

func switchContextOKFuncNil(StateType) bool {
	return true
}

func switchContextOKFuncCheckFrom(st StateType) func(StateType) bool {
	return func(current StateType) bool {
		return current == st
	}
}
