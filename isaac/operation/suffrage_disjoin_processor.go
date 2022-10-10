package isaacoperation

import (
	"context"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util"
)

type SuffrageDisjoinProcessor struct {
	*base.BaseOperationProcessor
	suffrage     map[string]base.SuffrageNodeStateValue
	preprocessed map[string]struct{} //revive:disable-line:nested-structs
}

func NewSuffrageDisjoinProcessor(
	height base.Height,
	getStateFunc base.GetStateFunc,
	newPreProcessConstraintFunc base.NewOperationProcessorProcessFunc,
	newProcessConstraintFunc base.NewOperationProcessorProcessFunc,
) (*SuffrageDisjoinProcessor, error) {
	e := util.StringErrorFunc("failed to create new SuffrageDisjoinProcessor")

	b, err := base.NewBaseOperationProcessor(
		height, getStateFunc, newPreProcessConstraintFunc, newProcessConstraintFunc)
	if err != nil {
		return nil, e(err, "")
	}

	p := &SuffrageDisjoinProcessor{
		BaseOperationProcessor: b,
		preprocessed:           map[string]struct{}{},
	}

	switch i, found, err := getStateFunc(isaac.SuffrageStateKey); {
	case err != nil:
		return nil, e(err, "")
	case !found, i == nil:
		return nil, e(isaac.ErrStopProcessingRetry.Errorf("empty state"), "")
	default:
		sufstv := i.Value().(base.SuffrageNodesStateValue) //nolint:forcetypeassert //...
		p.suffrage = map[string]base.SuffrageNodeStateValue{}

		snodes := sufstv.Nodes()

		for i := range snodes {
			node := snodes[i]

			p.suffrage[node.Address().String()] = node
		}
	}

	return p, nil
}

func (p *SuffrageDisjoinProcessor) Close() error {
	if err := p.BaseOperationProcessor.Close(); err != nil {
		return err
	}

	p.suffrage = nil
	p.preprocessed = nil

	return nil
}

func (p *SuffrageDisjoinProcessor) PreProcess(ctx context.Context, op base.Operation, getStateFunc base.GetStateFunc) (
	context.Context, base.OperationProcessReasonError, error,
) {
	e := util.StringErrorFunc("failed to preprocess for SuffrageDisjoin")

	var signer base.Publickey

	switch sf, ok := op.(base.NodeSignFact); {
	case !ok:
		return ctx, nil, e(nil, "not NodeSignFact, %T", op)
	default:
		signer = sf.NodeSigns()[0].Signer()
	}

	fact := op.Fact().(SuffrageDisjoinFact) //nolint:forcetypeassert //...
	n := fact.Node()

	if _, found := p.preprocessed[n.String()]; found {
		return ctx, base.NewBaseOperationProcessReasonError("already preprocessed, %q", n), nil
	}

	var withdrawpreprocessed []base.Address

	_ = util.LoadFromContext(ctx, WithdrawPreProcessedContextKey, &withdrawpreprocessed)

	if util.InSliceFunc(withdrawpreprocessed, func(_ interface{}, i int) bool {
		return withdrawpreprocessed[i].Equal(n)
	}) >= 0 {
		return ctx, base.NewBaseOperationProcessReasonError("already withdrew, %q", n), nil
	}

	switch stv, found := p.suffrage[n.String()]; {
	case !found:
		return ctx, base.NewBaseOperationProcessReasonError("not in suffrage, %q", n), nil
	case fact.Start() != stv.Start():
		return ctx, base.NewBaseOperationProcessReasonError("start does not match"), nil
	case !signer.Equal(stv.Publickey()):
		return ctx, base.NewBaseOperationProcessReasonError("not signed by node key"), nil
	}

	switch reasonerr, err := p.PreProcessConstraintFunc(ctx, op, getStateFunc); {
	case err != nil:
		return ctx, nil, e(err, "")
	case reasonerr != nil:
		return ctx, reasonerr, nil
	}

	p.preprocessed[n.String()] = struct{}{}

	return ctx, nil, nil
}

func (p *SuffrageDisjoinProcessor) Process(ctx context.Context, op base.Operation, getStateFunc base.GetStateFunc) (
	[]base.StateMergeValue, base.OperationProcessReasonError, error,
) {
	e := util.StringErrorFunc("failed to process for SuffrageDisjoin")

	switch reasonerr, err := p.ProcessConstraintFunc(ctx, op, getStateFunc); {
	case err != nil:
		return nil, nil, e(err, "")
	case reasonerr != nil:
		return nil, reasonerr, nil
	}

	fact := op.Fact().(SuffrageDisjoinFact) //nolint:forcetypeassert //...

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

type suffrageDisjoinNodeStateValue struct {
	node base.Address
}

func newSuffrageDisjoinNodeStateValue(node base.Address) suffrageDisjoinNodeStateValue {
	return suffrageDisjoinNodeStateValue{
		node: node,
	}
}

func (s suffrageDisjoinNodeStateValue) IsValid([]byte) error {
	if err := util.CheckIsValiders(nil, false, s.node); err != nil {
		return util.ErrInvalid.Errorf("invalie suffrageDisjoinNodeStateValue")
	}

	return nil
}

func (s suffrageDisjoinNodeStateValue) HashBytes() []byte {
	return util.ConcatByters(s.node)
}
