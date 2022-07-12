package base

import (
	"github.com/spikeekips/mitum/util"
)

type NetworkPolicy interface {
	util.IsValider
	util.HashByter
	MaxOperationsInProposal() uint64
	SuffrageCandidateLifespan() Height
}

type NodePolicy interface {
	util.IsValider
	NetworkID() NetworkID
	Threshold() Threshold
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
