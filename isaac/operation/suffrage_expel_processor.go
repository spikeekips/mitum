package isaacoperation

import (
	"context"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util"
)

var ExpelPreProcessedContextKey = util.ContextKey("expel-preprocessed")

type SuffrageExpelProcessor struct {
	*base.BaseOperationProcessor
	sufstv       base.SuffrageNodesStateValue
	suffrage     base.Suffrage
	preprocessed map[string]struct{} //revive:disable-line:nested-structs
}

func NewSuffrageExpelProcessor(
	height base.Height,
	getStateFunc base.GetStateFunc,
	newPreProcessConstraintFunc base.NewOperationProcessorProcessFunc,
	newProcessConstraintFunc base.NewOperationProcessorProcessFunc,
) (*SuffrageExpelProcessor, error) {
	e := util.StringError("create new SuffrageExpelProcessor")

	b, err := base.NewBaseOperationProcessor(
		height, getStateFunc, newPreProcessConstraintFunc, newProcessConstraintFunc)
	if err != nil {
		return nil, e.Wrap(err)
	}

	p := &SuffrageExpelProcessor{
		BaseOperationProcessor: b,
		preprocessed:           map[string]struct{}{},
	}

	switch i, found, err := getStateFunc(isaac.SuffrageStateKey); {
	case err != nil:
		return nil, e.Wrap(err)
	case !found, i == nil:
		return nil, e.Errorf("empty state")
	default:
		p.sufstv = i.Value().(base.SuffrageNodesStateValue) //nolint:forcetypeassert //...

		suf, err := p.sufstv.Suffrage()
		if err != nil {
			return nil, e.Errorf("get suffrage from state")
		}

		p.suffrage = suf
	}

	return p, nil
}

func (p *SuffrageExpelProcessor) Close() error {
	if err := p.BaseOperationProcessor.Close(); err != nil {
		return err
	}

	p.sufstv = nil
	p.suffrage = nil
	clear(p.preprocessed)

	return nil
}

func (p *SuffrageExpelProcessor) PreProcess(ctx context.Context, op base.Operation, getStateFunc base.GetStateFunc) (
	context.Context, base.OperationProcessReasonError, error,
) {
	e := util.StringError("preprocess for SuffrageExpel")

	fact := op.Fact().(base.SuffrageExpelFact) //nolint:forcetypeassert //...

	switch {
	case fact.ExpelStart() > p.Height():
		return ctx, base.NewBaseOperationProcessReason("wrong start height"), nil
	case fact.ExpelEnd() < p.Height():
		return ctx, base.NewBaseOperationProcessReason("expired"), nil
	}

	n := fact.Node()

	if _, found := p.preprocessed[n.String()]; found {
		return ctx, base.NewBaseOperationProcessReasonf("already preprocessed, %q", n), nil
	}

	if !p.suffrage.Exists(n) {
		return ctx, base.NewBaseOperationProcessReasonf("not in suffrage, %q", n), nil
	}

	switch reasonerr, err := p.PreProcessConstraintFunc(ctx, op, getStateFunc); {
	case err != nil:
		return ctx, nil, e.Wrap(err)
	case reasonerr != nil:
		return ctx, reasonerr, nil
	}

	p.preprocessed[n.String()] = struct{}{}

	var preprocessed []base.Address

	_ = util.LoadFromContext(ctx, ExpelPreProcessedContextKey, &preprocessed)
	preprocessed = append(preprocessed, n)

	return context.WithValue(ctx, ExpelPreProcessedContextKey, preprocessed), nil, nil
}

func (p *SuffrageExpelProcessor) Process(ctx context.Context, op base.Operation, getStateFunc base.GetStateFunc) (
	[]base.StateMergeValue, base.OperationProcessReasonError, error,
) {
	e := util.StringError("process for SuffrageExpel")

	switch reasonerr, err := p.ProcessConstraintFunc(ctx, op, getStateFunc); {
	case err != nil:
		return nil, nil, e.Wrap(err)
	case reasonerr != nil:
		return nil, reasonerr, nil
	}

	fact := op.Fact().(base.SuffrageExpelFact) //nolint:forcetypeassert //...

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
