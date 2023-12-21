package base

import (
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
)

type NetworkPolicy interface {
	util.IsValider
	util.HashByter
	MaxOperationsInProposal() uint64
	SuffrageCandidateLifespan() Height
	SuffrageCandidateLimiterRule() SuffrageCandidateLimiterRule
	MaxSuffrageSize() uint64
	SuffrageExpelLifespan() Height
	// EmptyProposalNoBlock indicates, if no valid operations in proposal, new
	// block will not be stored and moves to next round.
	EmptyProposalNoBlock() bool
}

type NetworkPolicyStateValue interface {
	StateValue
	Policy() NetworkPolicy
}

func IsNetworkPolicyState(st State) bool {
	if st.Value() == nil {
		return false
	}

	_, ok := st.Value().(NetworkPolicyStateValue)

	return ok
}

type SuffrageCandidateLimiterRule interface {
	hint.Hinter
	util.IsValider
	util.HashByter
}

func IsEqualNetworkPolicy(a, b NetworkPolicy) bool {
	return util.IsEqualHashByter(a, b)
}
