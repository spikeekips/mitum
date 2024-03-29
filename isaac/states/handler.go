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
	allowedConsensus() bool
	whenSetAllowConsensus(bool)
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
	from() StateType
	next() StateType
	Error() string
}

type voteproofSwitchContext interface {
	switchContext
	voteproof() base.Voteproof
}

type baseSwitchContext struct { //nolint:errname //...
	f StateType
	n StateType
}

func newBaseSwitchContext(from, next StateType) baseSwitchContext {
	return baseSwitchContext{
		f: from,
		n: next,
	}
}

func (s baseSwitchContext) from() StateType {
	return s.f
}

func (s baseSwitchContext) next() StateType {
	return s.n
}

func (s baseSwitchContext) Error() string {
	return s.String()
}

func (s baseSwitchContext) String() string {
	return fmt.Sprintf("state switch from=%q next=%q", s.f, s.n)
}

func (s baseSwitchContext) MarshalZerologObject(e *zerolog.Event) {
	e.
		Stringer("from", s.f).
		Stringer("next", s.n)
}

type baseErrorSwitchContext struct { //nolint:errname //...
	err error
	baseSwitchContext
}

func newBaseErrorSwitchContext(from, next StateType, err error) baseErrorSwitchContext {
	return baseErrorSwitchContext{
		baseSwitchContext: newBaseSwitchContext(from, next),
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
