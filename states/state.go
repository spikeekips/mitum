package states

import (
	"fmt"

	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
)

type StateType string

const (
	StateEmpty = StateType("")
	// StateStopped indicates node is in state, all processes is finished.
	StateStopped = StateType("STOPPED")
	// StateBooting indicates node is in state, node checks it's state.
	StateBooting = StateType("BOOTING")
	// StateJoining indicates node is in state, node is trying to join
	// consensus.
	StateJoining = StateType("JOINING")
	// StateConsensus indicates node is in state, node participates consensus
	// with the other nodes.
	StateConsensus = StateType("CONSENSUS")
	// StateSyncing indicates node is in state, node is syncing block.
	StateSyncing = StateType("SYNCING")
	// StateHandover indicates that node tries to replace the existing same
	// node.
	StateHandover = StateType("HANDOVER")
	// StateBroken is used whne something wrong in states.
	StateBroken = StateType("BROKEN")
)

func (s StateType) String() string {
	return string(s)
}

type stateSwitchContext struct {
	from StateType
	next StateType
	vp   base.Voteproof
}

func newStateSwitchContext(from, next StateType, vp base.Voteproof) stateSwitchContext {
	return stateSwitchContext{
		from: from,
		next: next,
		vp:   vp,
	}
}

func (sctx stateSwitchContext) voteproof() base.Voteproof {
	return sctx.vp
}

func (sctx stateSwitchContext) Error() string {
	return ""
}

func (sctx stateSwitchContext) MarshalZerologObject(e *zerolog.Event) {
	e.
		Stringer("from", sctx.from).
		Stringer("next", sctx.next)

	if sctx.vp != nil {
		e.Str("voteproof", sctx.vp.ID())
	}
}

type stateHandler interface {
	state() StateType
	enter(base.Voteproof) (func() error, error)
	exit() (func() error, error)
	newVoteproof(base.Voteproof) error
	newProposal(base.ProposalFact) error
}

func stateHandlerLog(st stateHandler) fmt.Stringer {
	return util.Stringer(func() string {
		if st == nil {
			return ""
		}

		return st.state().String()
	})
}
