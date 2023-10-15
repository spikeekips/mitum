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
	GenesisNetworkPolicyHint     = hint.MustNewHint("genesis-network-policy-operation-v0.0.1")
	NetworkPolicyFactHint        = hint.MustNewHint("network-policy-fact-v0.0.1")
	NetworkPolicyHint            = hint.MustNewHint("network-policy-operation-v0.0.1")
)

type GenesisNetworkPolicyFact struct {
	baseNetworkPolicyFact
}

func NewGenesisNetworkPolicyFact(policy base.NetworkPolicy) GenesisNetworkPolicyFact {
	fact := GenesisNetworkPolicyFact{
		baseNetworkPolicyFact: newBaseNetworkPolicyFact(
			GenesisNetworkPolicyFactHint,
			base.Token(localtime.New(localtime.Now().UTC()).Bytes()),
			policy,
		),
	}

	fact.SetHash(fact.hash())

	return fact
}

func (fact GenesisNetworkPolicyFact) IsValid([]byte) error {
	e := util.ErrInvalid.Errorf("invalid GenesisNetworkPolicyFact")

	if err := fact.baseNetworkPolicyFact.IsValid(nil); err != nil {
		return e.Wrap(err)
	}

	if !fact.Hash().Equal(fact.hash()) {
		return e.Errorf("hash does not match")
	}

	return nil
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

func (GenesisNetworkPolicy) PreProcess(ctx context.Context, getStateFunc base.GetStateFunc) (
	context.Context, base.OperationProcessReasonError, error,
) {
	switch _, found, err := getStateFunc(isaac.NetworkPolicyStateKey); {
	case err != nil:
		return ctx, nil, err
	case found:
		return ctx, base.NewBaseOperationProcessReasonError("network policy state already exists"), nil
	default:
		return ctx, nil, nil
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

type baseNetworkPolicyFact struct {
	policy base.NetworkPolicy
	base.BaseFact
}

func newBaseNetworkPolicyFact(ht hint.Hint, token base.Token, policy base.NetworkPolicy) baseNetworkPolicyFact {
	return baseNetworkPolicyFact{
		BaseFact: base.NewBaseFact(ht, token),
		policy:   policy,
	}
}

func (fact baseNetworkPolicyFact) IsValid([]byte) error {
	err := util.CheckIsValiders(nil, false, fact.BaseFact, fact.policy)

	return util.ErrInvalid.WithMessage(err, "invalid baseNetworkPolicyFact")
}

func (fact baseNetworkPolicyFact) Policy() base.NetworkPolicy {
	return fact.policy
}

func (fact baseNetworkPolicyFact) hash() util.Hash {
	return valuehash.NewSHA256(util.ConcatByters(
		util.BytesToByter(fact.Token()),
		util.DummyByter(fact.policy.HashBytes),
	))
}

type NetworkPolicyFact struct {
	baseNetworkPolicyFact
}

func NewNetworkPolicyFact(token base.Token, policy base.NetworkPolicy) NetworkPolicyFact {
	fact := NetworkPolicyFact{
		baseNetworkPolicyFact: newBaseNetworkPolicyFact(NetworkPolicyFactHint, token, policy),
	}

	fact.SetHash(fact.hash())

	return fact
}

func (fact NetworkPolicyFact) IsValid([]byte) error {
	e := util.ErrInvalid.Errorf("invalid NetworkPolicyFact")

	if err := fact.baseNetworkPolicyFact.IsValid(nil); err != nil {
		return e.Wrap(err)
	}

	if !fact.Hash().Equal(fact.hash()) {
		return e.Errorf("hash does not match")
	}

	return nil
}

type NetworkPolicy struct {
	base.BaseNodeOperation
}

func NewNetworkPolicy(fact NetworkPolicyFact) NetworkPolicy {
	return NetworkPolicy{
		BaseNodeOperation: base.NewBaseNodeOperation(NetworkPolicyHint, fact),
	}
}

func (op NetworkPolicy) IsValid(networkID []byte) error {
	e := util.ErrInvalid.Errorf("invalid NetworkPolicy")

	if err := op.BaseNodeOperation.IsValid(networkID); err != nil {
		return e.Wrap(err)
	}

	if _, ok := op.Fact().(NetworkPolicyFact); !ok {
		return e.Errorf("not NetworkPolicyFact, %T", op.Fact())
	}

	return nil
}

func (op *NetworkPolicy) SetToken(t base.Token) error {
	fact := op.Fact().(NetworkPolicyFact) //nolint:forcetypeassert //...

	if err := fact.SetToken(t); err != nil {
		return err
	}

	fact.SetHash(fact.hash())

	op.BaseNodeOperation.SetFact(fact)

	return nil
}
