package isaacoperation

import (
	"context"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util"
)

type SuffrageJoinProcessor struct {
	sufcst                   base.State
	sufstv                   base.SuffrageStateValue
	sufcstv                  base.SuffrageCandidateStateValue
	sufst                    base.State
	candidates               map[string]base.SuffrageCandidate
	processConstraintFunc    base.OperationProcessorProcessFunc
	suffrage                 base.Suffrage
	preProcessConstraintFunc base.OperationProcessorProcessFunc
	height                   base.Height
	threshold                base.Threshold
}

func NewSuffrageJoinProcessor(
	height base.Height,
	threshold base.Threshold,
	getStateFunc base.GetStateFunc,
	preProcessConstraintFunc base.OperationProcessorProcessFunc,
	processConstraintFunc base.OperationProcessorProcessFunc,
) (*SuffrageJoinProcessor, error) {
	e := util.StringErrorFunc("failed to create new SuffrageJoinProcessor")

	p := &SuffrageJoinProcessor{
		height:     height,
		threshold:  threshold,
		candidates: map[string]base.SuffrageCandidate{},
	}

	switch i, found, err := getStateFunc(isaac.SuffrageStateKey); {
	case err != nil:
		return nil, e(err, "")
	case !found, i == nil:
		return nil, e(isaac.StopProcessingRetryError.Errorf("empty state returned"), "")
	default:
		p.sufst = i
		p.sufstv = i.Value().(base.SuffrageStateValue) //nolint:forcetypeassert //...

		suf, err := isaac.NewSuffrageFromState(i)
		if err != nil {
			return nil, e(isaac.StopProcessingRetryError.Errorf("failed to get suffrage from state"), "")
		}

		p.suffrage = suf
	}

	switch i, found, err := getStateFunc(isaac.SuffrageCandidateStateKey); {
	case err != nil:
		return nil, e(err, "")
	case !found:
	case i == nil:
		return nil, e(isaac.StopProcessingRetryError.Errorf("empty state returned"), "")
	default:
		p.sufcst = i
		p.sufcstv = i.Value().(base.SuffrageCandidateStateValue) //nolint:forcetypeassert //...

		sufcnodes := p.sufcstv.Nodes()

		for i := range sufcnodes {
			n := sufcnodes[i]
			p.candidates[n.Address().String()] = n
		}
	}

	// revive:disable:modifies-parameter
	if preProcessConstraintFunc == nil {
		preProcessConstraintFunc = base.EmptyOperationProcessorProcessFunc
	}

	p.preProcessConstraintFunc = preProcessConstraintFunc

	if processConstraintFunc == nil {
		processConstraintFunc = base.EmptyOperationProcessorProcessFunc
	}

	p.processConstraintFunc = processConstraintFunc
	// revive:enable:modifies-parameter

	return p, nil
}

func (p *SuffrageJoinProcessor) PreProcess(ctx context.Context, op base.Operation, getStateFunc base.GetStateFunc) (
	base.OperationProcessReasonError, error,
) {
	e := util.StringErrorFunc("failed to preprocess for SuffrageJoin")

	if p.sufcst == nil {
		return nil, e(nil, "no existing candidates")
	}

	noop, ok := op.(base.BaseNodeOperation)
	if !ok {
		return nil, e(nil, "not BaseNodeOperation, %T", op)
	}

	fact := op.Fact().(SuffrageJoinFact) //nolint:forcetypeassert //...
	n := fact.Candidate()

	var info base.SuffrageCandidate

	switch i, found := p.candidates[n.String()]; {
	case !found:
		return base.NewBaseOperationProcessReasonError("candidate not in candidates, %q", n), nil
	case i.Deadline() < p.height:
		return base.NewBaseOperationProcessReasonError("candidate expired, %q", n), nil
	default:
		info = i
	}

	switch node, err := p.findCandidateFromSigned(op); {
	case err != nil:
		return base.NewBaseOperationProcessReasonError(err.Error()), nil
	case !node.Publickey().Equal(info.Publickey()):
		return base.NewBaseOperationProcessReasonError("not signed by candidate key"), nil
	}

	if p.suffrage.Exists(n) {
		return base.NewBaseOperationProcessReasonError("candidate already in suffrage, %q", n), nil
	}

	if err := base.CheckFactSignsBySuffrage(p.suffrage, p.threshold, noop.NodeSigned()); err != nil {
		return base.NewBaseOperationProcessReasonError("not enough signs"), nil
	}

	switch reasonerr, err := p.preProcessConstraintFunc(ctx, op, getStateFunc); {
	case err != nil:
		return nil, e(err, "")
	case reasonerr != nil:
		return reasonerr, nil
	}

	return nil, nil
}

func (p *SuffrageJoinProcessor) Process(ctx context.Context, op base.Operation, getStateFunc base.GetStateFunc) (
	[]base.StateMergeValue, base.OperationProcessReasonError, error,
) {
	e := util.StringErrorFunc("failed to process for SuffrageJoin")

	fact := op.Fact().(SuffrageJoinFact) //nolint:forcetypeassert //...

	info := p.candidates[fact.Candidate().String()]

	if p.sufst == nil {
		return []base.StateMergeValue{
			base.NewBaseStateMergeValue(
				isaac.SuffrageStateKey,
				isaac.NewSuffrageStateValue(base.GenesisHeight, []base.Node{info}),
				nil,
			),
		}, nil, nil
	}

	switch reasonerr, err := p.processConstraintFunc(ctx, op, getStateFunc); {
	case err != nil:
		return nil, nil, e(err, "")
	case reasonerr != nil:
		return nil, reasonerr, nil
	}

	// FIXME remove canidadte from candidate state value

	members := p.sufstv.Nodes()
	newmembers := make([]base.Node, len(members)+1)
	newmembers[len(members)] = isaac.NewNode(info.Publickey(), info.Address())

	return []base.StateMergeValue{
		base.NewBaseStateMergeValue(
			isaac.SuffrageStateKey,
			isaac.NewSuffrageStateValue(p.sufstv.Height()+1, newmembers),
			nil,
		),
	}, nil, nil
}

func (*SuffrageJoinProcessor) findCandidateFromSigned(op base.Operation) (base.Node, error) {
	fact, ok := op.Fact().(SuffrageJoinFact)
	if !ok {
		return nil, errors.Errorf("not SuffrageJoinFact, %T", op.Fact())
	}

	sfs := op.Signed()

	for i := range sfs {
		ns := sfs[i].(base.NodeSigned) //nolint:forcetypeassert //...

		if !ns.Node().Equal(fact.Candidate()) {
			continue
		}

		return isaac.NewNode(ns.Signer(), ns.Node()), nil
	}

	return nil, errors.Errorf("not signed by Join")
}
