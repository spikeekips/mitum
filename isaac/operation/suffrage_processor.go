package isaacoperation

import (
	"context"
	"sync"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util"
)

type SuffrageUpdateProcessor struct {
	sync.Mutex
	threshold                base.Threshold
	suffrage                 map[string]base.Node
	candidates               map[string]struct{}
	pubs                     []base.Publickey
	foundValid               bool
	st                       base.State
	stv                      base.SuffrageStateValue
	preProcessConstraintFunc base.OperationProcessorProcessFunc
	processConstraintFunc    base.OperationProcessorProcessFunc
}

func NewSuffrageUpdateProcessor(
	height base.Height,
	threshold base.Threshold,
	getNodes func() (suffrages []base.Node, candidates []base.Address, err error), // BLOCK use GetStateFunc
	preProcessConstraintFunc base.OperationProcessorProcessFunc,
	processConstraintFunc base.OperationProcessorProcessFunc,
) (*SuffrageUpdateProcessor, error) {
	suffrage := map[string]base.Node{}
	candidates := map[string]struct{}{}

	e := util.StringErrorFunc("failed to create new SuffrageUpdateProcessor")
	sn, cn, err := getNodes()
	switch {
	case err != nil:
		return nil, e(err, "")
	case height != base.GenesisHeight && len(sn) < 1:
		return nil, e(isaac.StopProcessingRetryError.Errorf("empty nodes"), "")
	}

	pubs := make([]base.Publickey, len(sn))
	for i := range sn {
		n := sn[i]
		suffrage[n.Address().String()] = n
		pubs[i] = n.Publickey()
	}
	for i := range cn {
		n := cn[i]
		candidates[n.String()] = struct{}{}
	}

	if preProcessConstraintFunc == nil {
		preProcessConstraintFunc = func(context.Context, base.Operation, base.GetStateFunc) (base.OperationProcessReasonError, error) {
			return nil, nil
		}
	}
	if processConstraintFunc == nil {
		processConstraintFunc = func(context.Context, base.Operation, base.GetStateFunc) (base.OperationProcessReasonError, error) {
			return nil, nil
		}
	}

	return &SuffrageUpdateProcessor{
		threshold:                threshold,
		suffrage:                 suffrage,
		candidates:               candidates,
		pubs:                     pubs,
		preProcessConstraintFunc: preProcessConstraintFunc,
		processConstraintFunc:    processConstraintFunc,
	}, nil
}

func (p *SuffrageUpdateProcessor) PreProcess(ctx context.Context, op base.Operation, getStateFunc base.GetStateFunc) (base.OperationProcessReasonError, error) {
	e := util.StringErrorFunc("failed to preprocess for SuffrageUpdate")

	if p.foundValid {
		return base.NewBaseOperationProcessReasonError("another valid SuffrageUpdate processed"), nil
	}

	if err := base.CheckFactSignsByPubs(p.pubs, p.threshold, op.Signed()); err != nil {
		return base.NewBaseOperationProcessReasonError("not enough signs"), nil
	}

	fact := op.Fact().(SuffrageUpdateFact)

	news := fact.NewMembers()
	for i := range news {
		n := news[i].Address().String()

		if _, found := p.suffrage[n]; found {
			return base.NewBaseOperationProcessReasonError("new member, %q already in suffrage", n), nil
		}
		if _, found := p.candidates[n]; found {
			return base.NewBaseOperationProcessReasonError("new member, %q already in candidates", n), nil
		}
	}

	outs := fact.OutMembers()
	for i := range outs {
		n := outs[i].String()

		if _, found := p.suffrage[n]; !found {
			return base.NewBaseOperationProcessReasonError("out member, %q not in suffrage", n), nil
		}
		if _, found := p.candidates[n]; !found {
			return base.NewBaseOperationProcessReasonError("out member, %q not in candidates", n), nil
		}
	}

	switch i, found, err := getStateFunc(isaac.SuffrageStateKey); {
	case err != nil:
		return nil, errors.Wrap(err, "")
	case !found:
		if len(fact.OutMembers()) > 0 {
			return nil, e(isaac.StopProcessingRetryError.Errorf("empty state, but operation has out members"), "")
		}
	case i == nil:
		return nil, e(isaac.StopProcessingRetryError.Errorf("empty state returned"), "")
	default:
		p.foundValid = true

		p.st = i
		p.stv = i.Value().(base.SuffrageStateValue)
	}

	switch reasonerr, err := p.preProcessConstraintFunc(ctx, op, getStateFunc); {
	case err != nil:
		return nil, e(err, "")
	case reasonerr != nil:
		return reasonerr, nil
	}

	return nil, nil
}

func (p *SuffrageUpdateProcessor) Process(ctx context.Context, op base.Operation, getStateFunc base.GetStateFunc) (
	[]base.StateMergeValue, base.OperationProcessReasonError, error,
) {
	e := util.StringErrorFunc("failed to process for SuffrageUpdate")

	fact := op.Fact().(SuffrageUpdateFact)

	if p.st == nil {
		return []base.StateMergeValue{
			base.NewBaseStateMergeValue(
				isaac.SuffrageStateKey,
				isaac.NewSuffrageStateValue(base.Height(0), nil, fact.NewMembers()),
				nil,
			),
		}, nil, nil
	}

	members := make([]base.Node, len(p.stv.Nodes()))
	{
		n := util.FilterSlice(p.stv.Nodes(), fact.OutMembers(), func(a, b interface{}) bool {
			return a.(base.Node).Address().Equal(b.(base.Address))
		})

		for i := range n {
			members[i] = n[i].(base.Node)
		}
	}

	var newmembers []base.Node
	news := fact.NewMembers()
	if len(news) < 1 {
		if len(members) < 1 {
			return nil, base.NewBaseOperationProcessReasonError("empty left members"), nil
		}

		newmembers = members
	}

	switch reasonerr, err := p.processConstraintFunc(ctx, op, getStateFunc); {
	case err != nil:
		return nil, nil, e(err, "")
	case reasonerr != nil:
		return nil, reasonerr, nil
	}

	if len(news) > 0 {
		newmembers = make([]base.Node, len(members)+len(news))
		copy(newmembers[:len(members)], members)
		copy(newmembers[len(members):], news)
	}

	return []base.StateMergeValue{
		base.NewBaseStateMergeValue(
			isaac.SuffrageStateKey,
			isaac.NewSuffrageStateValue(p.stv.Height()+1, p.st.Hash(), newmembers),
			nil,
		),
	}, nil, nil
}
