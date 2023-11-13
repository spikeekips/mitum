package isaacoperation

import (
	"context"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util"
)

type NetworkPolicyProcessor struct {
	*base.BaseOperationProcessor
	stv       base.NetworkPolicyStateValue
	suffrage  base.Suffrage
	policy    base.NetworkPolicy
	newop     util.Hash
	threshold base.Threshold
}

func NewNetworkPolicyProcessor(
	height base.Height,
	threshold base.Threshold,
	getStateFunc base.GetStateFunc,
	newPreProcessConstraintFunc base.NewOperationProcessorProcessFunc,
	newProcessConstraintFunc base.NewOperationProcessorProcessFunc,
) (*NetworkPolicyProcessor, error) {
	e := util.StringError("create new NetworkPolicyProcessor")

	b, err := base.NewBaseOperationProcessor(
		height, getStateFunc, newPreProcessConstraintFunc, newProcessConstraintFunc)
	if err != nil {
		return nil, e.Wrap(err)
	}

	p := &NetworkPolicyProcessor{
		BaseOperationProcessor: b,
		threshold:              threshold,
	}

	switch i, found, err := getStateFunc(isaac.NetworkPolicyStateKey); {
	case err != nil:
		return nil, e.Wrap(err)
	case !found, i == nil:
		return nil, e.Errorf("empty state")
	default:
		p.stv = i.Value().(base.NetworkPolicyStateValue) //nolint:forcetypeassert //...
		p.policy = p.stv.Policy()
	}

	switch i, found, err := getStateFunc(isaac.SuffrageStateKey); {
	case err != nil:
		return nil, e.Wrap(err)
	case !found, i == nil:
		return nil, e.Errorf("empty state")
	default:
		sufstv := i.Value().(base.SuffrageNodesStateValue) //nolint:forcetypeassert //...

		suf, err := sufstv.Suffrage()
		if err != nil {
			return nil, e.Errorf("get suffrage from state")
		}

		p.suffrage = suf
	}

	return p, nil
}

func (p *NetworkPolicyProcessor) Close() error {
	if err := p.BaseOperationProcessor.Close(); err != nil {
		return err
	}

	p.suffrage = nil
	p.stv = nil
	p.policy = nil

	return nil
}

func (p *NetworkPolicyProcessor) PreProcess(ctx context.Context, op base.Operation, getStateFunc base.GetStateFunc) (
	context.Context, base.OperationProcessReasonError, error,
) {
	e := util.StringError("preprocess for network policy")

	if p.newop != nil {
		return ctx, base.NewBaseOperationProcessReasonError("only one network policy operation allowed"), nil
	}

	noop, ok := op.(base.NodeSignFact)
	if !ok {
		return ctx, nil, e.Errorf("not NodeSignFact, %T", op)
	}

	switch reasonerr, err := p.PreProcessConstraintFunc(ctx, op, getStateFunc); {
	case err != nil:
		return ctx, nil, e.Wrap(err)
	case reasonerr != nil:
		return ctx, reasonerr, nil
	}

	if err := base.CheckFactSignsBySuffrage(p.suffrage, p.threshold, noop.NodeSigns()); err != nil {
		return ctx, base.NewBaseOperationProcessReasonError("not enough signs"), nil
	}

	newpolicy := op.Fact().(NetworkPolicyFact).Policy() //nolint:forcetypeassert //...

	if base.IsEqualNetworkPolicy(p.policy, newpolicy) {
		return ctx, base.NewBaseOperationProcessReasonError("same with existing network policy"), nil
	}

	p.newop = op.Hash()

	return ctx, nil, nil
}

func (p *NetworkPolicyProcessor) Process(ctx context.Context, op base.Operation, getStateFunc base.GetStateFunc) (
	[]base.StateMergeValue, base.OperationProcessReasonError, error,
) {
	switch {
	case p.newop == nil:
		return nil, nil, errors.Errorf("not pre processed for NetworkPolicy")
	case !p.newop.Equal(op.Hash()):
		return nil, nil, errors.Errorf("op not passed in pre processed for NetworkPolicy")
	}

	switch reasonerr, err := p.ProcessConstraintFunc(ctx, op, getStateFunc); {
	case err != nil:
		return nil, nil, errors.WithMessage(err, "process for NetworkPolicy")
	case reasonerr != nil:
		return nil, reasonerr, nil
	}

	fact := op.Fact().(NetworkPolicyFact) //nolint:forcetypeassert //...

	return []base.StateMergeValue{
		base.NewBaseStateMergeValue(
			isaac.NetworkPolicyStateKey,
			isaac.NewNetworkPolicyStateValue(fact.Policy()),
			nil,
		),
	}, nil, nil
}
