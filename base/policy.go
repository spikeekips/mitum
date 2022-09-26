package base

import (
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
)

type NetworkPolicy interface {
	util.IsValider
	util.HashByter
	MaxOperationsInProposal() uint64
	SuffrageCandidateLifeSpan() Height
	SuffrageCandidateLimiterRule() SuffrageCandidateLimiterRule
	MaxSuffrageSize() uint64
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
