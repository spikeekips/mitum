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
	base.OperationProcessReasonError, error,
) {
	e := util.StringErrorFunc("failed to preprocess for SuffrageDisjoin")

	var signer base.Publickey

	switch sf, ok := op.(base.NodeSignedFact); {
	case !ok:
		return nil, e(nil, "not NodeSignedFact, %T", op)
	default:
		signer = sf.NodeSigned()[0].Signer()
	}

	fact := op.Fact().(SuffrageDisjoinFact) //nolint:forcetypeassert //...
	n := fact.Node()

	if _, found := p.preprocessed[n.String()]; found {
		return base.NewBaseOperationProcessReasonError("already preprocessed, %q", n), nil
	}

	switch stv, found := p.suffrage[n.String()]; {
	case !found:
		return base.NewBaseOperationProcessReasonError("not in suffrage, %q", n), nil
	case fact.Start() != stv.Start():
		return base.NewBaseOperationProcessReasonError("start does not match"), nil
	case !signer.Equal(stv.Publickey()):
		return base.NewBaseOperationProcessReasonError("not signed by node key"), nil
	}

	switch reasonerr, err := p.PreProcessConstraintFunc(ctx, op, getStateFunc); {
	case err != nil:
		return nil, e(err, "")
	case reasonerr != nil:
		return reasonerr, nil
	}

	p.preprocessed[n.String()] = struct{}{}

	return nil, nil
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
			newSuffrageDisjoinNodeStateValue([]base.Address{fact.Node()}),
			func(height base.Height, st base.State) base.StateValueMerger {
				return NewSuffrageJoinStateValueMerger(height, st)
			},
		),
	}, nil, nil
}

type suffrageDisjoinNodeStateValue struct {
	nodes []base.Address
}

func newSuffrageDisjoinNodeStateValue(nodes []base.Address) suffrageDisjoinNodeStateValue {
	return suffrageDisjoinNodeStateValue{
		nodes: nodes,
	}
}

func (s suffrageDisjoinNodeStateValue) IsValid([]byte) error {
	vs := make([]util.IsValider, len(s.nodes))

	for i := range vs {
		vs[i] = s.nodes[i]
	}

	if err := util.CheckIsValid(nil, false, vs...); err != nil {
		return util.ErrInvalid.Errorf("invalie suffrageDisjoinNodeStateValue")
	}

	return nil
}

func (s suffrageDisjoinNodeStateValue) HashBytes() []byte {
	bs := make([]util.Byter, len(s.nodes))

	for i := range s.nodes {
		bs[i] = util.DummyByter(s.nodes[i].Bytes)
	}

	return util.ConcatByters(bs...)
}
