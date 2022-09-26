package isaacoperation

import (
	"context"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/localtime"
	"github.com/spikeekips/mitum/util/valuehash"
)

var (
	GenesisNetworkPolicyFactHint = hint.MustNewHint("genesis-network-policy-fact-v0.0.1")
	GenesisNetworkPolicyHint     = hint.MustNewHint("genesis-network-policy-v0.0.1")
)

type GenesisNetworkPolicyFact struct {
	policy base.NetworkPolicy
	base.BaseFact
}

func NewGenesisNetworkPolicyFact(policy base.NetworkPolicy) GenesisNetworkPolicyFact {
	fact := GenesisNetworkPolicyFact{
		BaseFact: base.NewBaseFact(GenesisNetworkPolicyFactHint, base.Token(localtime.New(localtime.UTCNow()).Bytes())),
		policy:   policy,
	}

	fact.SetHash(fact.hash())

	return fact
}

func (fact GenesisNetworkPolicyFact) IsValid([]byte) error {
	e := util.StringErrorFunc("invalid GenesisNetworkPolicyFact")

	if err := util.CheckIsValiders(nil, false, fact.BaseFact, fact.policy); err != nil {
		return e(err, "")
	}

	if !fact.Hash().Equal(fact.hash()) {
		return e(util.ErrInvalid.Errorf("hash does not match"), "")
	}

	return nil
}

func (fact GenesisNetworkPolicyFact) Policy() base.NetworkPolicy {
	return fact.policy
}

func (fact GenesisNetworkPolicyFact) hash() util.Hash {
	return valuehash.NewSHA256(util.ConcatByters(
		util.BytesToByter(fact.Token()),
		util.DummyByter(fact.policy.HashBytes),
	))
}

// GenesisNetworkPolicy is only for used for genesis block
type GenesisNetworkPolicy struct {
	base.BaseOperation
}

func NewGenesisNetworkPolicy(fact GenesisNetworkPolicyFact) GenesisNetworkPolicy {
	return GenesisNetworkPolicy{
		BaseOperation: base.NewBaseOperation(GenesisNetworkPolicyHint, fact),
	}
}

func (op GenesisNetworkPolicy) IsValid(networkID []byte) error {
	e := util.ErrInvalid.Errorf("invalid GenesisNetworkPolicy")

	if err := op.BaseOperation.IsValid(networkID); err != nil {
		return e.Wrap(err)
	}

	if len(op.Signs()) > 1 {
		return e.Errorf("multiple signs found")
	}

	if _, ok := op.Fact().(GenesisNetworkPolicyFact); !ok {
		return e.Errorf("not GenesisNetworkPolicyFact, %T", op.Fact())
	}

	return nil
}

func (GenesisNetworkPolicy) PreProcess(_ context.Context, getStateFunc base.GetStateFunc) (
	base.OperationProcessReasonError, error,
) {
	switch _, found, err := getStateFunc(isaac.NetworkPolicyStateKey); {
	case err != nil:
		return base.NewBaseOperationProcessReasonError("failed to check network policy state: %w", err), nil
	case found:
		return base.NewBaseOperationProcessReasonError("network policy state already exists"), nil
	default:
		return nil, nil
	}
}

func (op GenesisNetworkPolicy) Process(context.Context, base.GetStateFunc) (
	[]base.StateMergeValue, base.OperationProcessReasonError, error,
) {
	fact := op.Fact().(GenesisNetworkPolicyFact) //nolint:forcetypeassert //...

	return []base.StateMergeValue{
		base.NewBaseStateMergeValue(
			isaac.NetworkPolicyStateKey,
			isaac.NewNetworkPolicyStateValue(fact.Policy()),
			nil,
		),
	}, nil, nil
}
