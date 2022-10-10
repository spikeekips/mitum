package isaacoperation

import (
	"context"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util"
)

var WithdrawPreProcessedContextKey = util.ContextKey("withdraw-preprocessed")

type SuffrageWithdrawProcessor struct {
	*base.BaseOperationProcessor
	sufstv       base.SuffrageNodesStateValue
	suffrage     base.Suffrage
	preprocessed map[string]struct{} //revive:disable-line:nested-structs
}

func NewSuffrageWithdrawProcessor(
	height base.Height,
	getStateFunc base.GetStateFunc,
	newPreProcessConstraintFunc base.NewOperationProcessorProcessFunc,
	newProcessConstraintFunc base.NewOperationProcessorProcessFunc,
) (*SuffrageWithdrawProcessor, error) {
	e := util.StringErrorFunc("failed to create new SuffrageWithdrawProcessor")

	b, err := base.NewBaseOperationProcessor(
		height, getStateFunc, newPreProcessConstraintFunc, newProcessConstraintFunc)
	if err != nil {
		return nil, e(err, "")
	}

	p := &SuffrageWithdrawProcessor{
		BaseOperationProcessor: b,
		preprocessed:           map[string]struct{}{},
	}

	switch i, found, err := getStateFunc(isaac.SuffrageStateKey); {
	case err != nil:
		return nil, e(err, "")
	case !found, i == nil:
		return nil, e(isaac.ErrStopProcessingRetry.Errorf("empty state"), "")
	default:
		p.sufstv = i.Value().(base.SuffrageNodesStateValue) //nolint:forcetypeassert //...

		suf, err := p.sufstv.Suffrage()
		if err != nil {
			return nil, e(isaac.ErrStopProcessingRetry.Errorf("failed to get suffrage from state"), "")
		}

		p.suffrage = suf
	}

	return p, nil
}

func (p *SuffrageWithdrawProcessor) Close() error {
	if err := p.BaseOperationProcessor.Close(); err != nil {
		return err
	}

	p.sufstv = nil
	p.suffrage = nil
	p.preprocessed = nil

	return nil
}

func (p *SuffrageWithdrawProcessor) PreProcess(ctx context.Context, op base.Operation, getStateFunc base.GetStateFunc) (
	context.Context, base.OperationProcessReasonError, error,
) {
	e := util.StringErrorFunc("failed to preprocess for SuffrageWithdraw")

	fact := op.Fact().(base.SuffrageWithdrawFact) //nolint:forcetypeassert //...

	switch {
	case fact.WithdrawStart() > p.Height():
		return ctx, base.NewBaseOperationProcessReasonError("wrong start height"), nil
	case fact.WithdrawEnd() < p.Height():
		return ctx, base.NewBaseOperationProcessReasonError("expired"), nil
	}

	n := fact.Node()

	if _, found := p.preprocessed[n.String()]; found {
		return ctx, base.NewBaseOperationProcessReasonError("already preprocessed, %q", n), nil
	}

	if !p.suffrage.Exists(n) {
		return ctx, base.NewBaseOperationProcessReasonError("not in suffrage, %q", n), nil
	}

	switch reasonerr, err := p.PreProcessConstraintFunc(ctx, op, getStateFunc); {
	case err != nil:
		return ctx, nil, e(err, "")
	case reasonerr != nil:
		return ctx, reasonerr, nil
	}

	p.preprocessed[n.String()] = struct{}{}

	var preprocessed []base.Address

	_ = util.LoadFromContext(ctx, WithdrawPreProcessedContextKey, &preprocessed)
	preprocessed = append(preprocessed, n)

	ctx = context.WithValue(ctx, WithdrawPreProcessedContextKey, preprocessed) //revive:disable-line:modifies-parameter

	return ctx, nil, nil
}

func (p *SuffrageWithdrawProcessor) Process(ctx context.Context, op base.Operation, getStateFunc base.GetStateFunc) (
	[]base.StateMergeValue, base.OperationProcessReasonError, error,
) {
	e := util.StringErrorFunc("failed to process for SuffrageWithdraw")

	switch reasonerr, err := p.ProcessConstraintFunc(ctx, op, getStateFunc); {
	case err != nil:
		return nil, nil, e(err, "")
	case reasonerr != nil:
		return nil, reasonerr, nil
	}

	fact := op.Fact().(base.SuffrageWithdrawFact) //nolint:forcetypeassert //...

	return []base.StateMergeValue{
		base.NewBaseStateMergeValue(
			isaac.SuffrageStateKey,
			newSuffrageDisjoinNodeStateValue(fact.Node()),
			func(height base.Height, st base.State) base.StateValueMerger {
				return NewSuffrageJoinStateValueMerger(height, st)
			},
		),
	}, nil, nil
}
