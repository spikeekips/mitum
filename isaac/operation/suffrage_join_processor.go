package isaacoperation

import (
	"context"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util"
)

type SuffrageJoinProcessor struct {
	sufcst                   base.State
	sufstv                   base.SuffrageStateValue
	sufcstv                  base.SuffrageCandidateStateValue
	sufst                    base.State
	candidateInfo            base.SuffrageCandidate
	candidates               map[string]base.SuffrageCandidate
	processConstraintFunc    base.OperationProcessorProcessFunc
	suffrage                 map[string]base.Node
	preProcessConstraintFunc base.OperationProcessorProcessFunc
	pubs                     []base.Publickey
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
		suffrage:   map[string]base.Node{},
		candidates: map[string]base.SuffrageCandidate{},
	}

	switch i, found, err := getStateFunc(isaac.SuffrageStateKey); {
	case err != nil:
		return nil, e(err, "")
	case !found:
	case i == nil:
		return nil, e(isaac.StopProcessingRetryError.Errorf("empty state returned"), "")
	default:
		p.sufst = i
		p.sufstv = i.Value().(base.SuffrageStateValue) //nolint:forcetypeassert //...

		sufnodes := p.sufstv.Nodes()
		p.pubs = make([]base.Publickey, len(sufnodes))

		for i := range sufnodes {
			n := sufnodes[i]
			p.suffrage[n.Address().String()] = n
			p.pubs[i] = n.Publickey()
		}
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

	if preProcessConstraintFunc == nil {
		p.preProcessConstraintFunc = func(context.Context, base.Operation, base.GetStateFunc) (
			base.OperationProcessReasonError, error,
		) {
			return nil, nil
		}
	}

	if processConstraintFunc == nil {
		p.processConstraintFunc = func(context.Context, base.Operation, base.GetStateFunc) (
			base.OperationProcessReasonError, error,
		) {
			return nil, nil
		}
	}

	return p, nil
}

func (p *SuffrageJoinProcessor) PreProcess(ctx context.Context, op base.Operation, getStateFunc base.GetStateFunc) (
	base.OperationProcessReasonError, error,
) {
	e := util.StringErrorFunc("failed to preprocess for SuffrageJoin")

	if err := base.CheckFactSignsByPubs(p.pubs, p.threshold, op.Signed()); err != nil {
		return base.NewBaseOperationProcessReasonError("not enough signs"), nil
	}

	fact := op.Fact().(SuffrageJoinPermissionFact) //nolint:forcetypeassert //...

	n := fact.Candidate().String()
	if _, found := p.suffrage[n]; found {
		return base.NewBaseOperationProcessReasonError("candidate, %q already in suffrage", n), nil
	}

	switch i, found := p.candidates[n]; {
	case !found:
		return base.NewBaseOperationProcessReasonError("candidate, %q not in candidates", n), nil
	case i.Deadline() < p.height:
		return base.NewBaseOperationProcessReasonError("candidate, %q expired", n), nil
	default:
		p.candidateInfo = i
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

	if p.sufst == nil {
		return []base.StateMergeValue{
			base.NewBaseStateMergeValue(
				isaac.SuffrageStateKey,
				isaac.NewSuffrageStateValue(base.GenesisHeight, []base.Node{p.candidateInfo}),
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

	members := p.sufstv.Nodes()
	newmembers := make([]base.Node, len(members)+1)
	newmembers[len(members)] = isaac.NewNode(p.candidateInfo.Publickey(), p.candidateInfo.Address())

	return []base.StateMergeValue{
		base.NewBaseStateMergeValue(
			isaac.SuffrageStateKey,
			isaac.NewSuffrageStateValue(p.sufstv.Height()+1, newmembers),
			nil,
		),
	}, nil, nil
}
