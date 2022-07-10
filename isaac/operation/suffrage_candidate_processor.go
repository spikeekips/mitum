package isaacoperation

import (
	"context"
	"sort"
	"strings"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util"
)

type SuffrageCandidateProcessor struct {
	height                   base.Height
	preProcessConstraintFunc base.OperationProcessorProcessFunc
	processConstraintFunc    base.OperationProcessorProcessFunc
	suffrages                map[string]base.Node
	existings                map[string]base.SuffrageCandidate
	preprocessed             map[string]struct{}
	merger                   *SuffrageCandiateStateValueMerger
	startheight              base.Height
	deadlineheight           base.Height
}

func NewSuffrageCandidateProcessor(
	height base.Height,
	getStateFunc base.GetStateFunc,
	preProcessConstraintFunc base.OperationProcessorProcessFunc,
	processConstraintFunc base.OperationProcessorProcessFunc,
) (*SuffrageCandidateProcessor, error) {
	e := util.StringErrorFunc("failed to create new SuffrageCandidateProcessor")

	p := &SuffrageCandidateProcessor{
		height:         height,
		existings:      map[string]base.SuffrageCandidate{},
		preprocessed:   map[string]struct{}{},
		startheight:    height + 1,
		deadlineheight: height + 1 + 50, // FIXME from network policy
	}

	switch i, found, err := getStateFunc(isaac.SuffrageStateKey); {
	case err != nil:
		return nil, e(err, "")
	case !found:
	case i == nil:
		return nil, e(isaac.StopProcessingRetryError.Errorf("empty state returned"), "")
	default:
		sufstv := i.Value().(base.SuffrageStateValue) //nolint:forcetypeassert //...

		nodes := sufstv.Nodes()

		for i := range nodes {
			n := nodes[i]
			p.suffrages[n.Address().String()] = n
		}
	}

	var st base.State

	switch i, found, err := getStateFunc(isaac.SuffrageCandidateStateKey); {
	case err != nil:
		return nil, e(err, "")
	case !found:
	case i != nil:
		st = i
		sufcstv := i.Value().(base.SuffrageCandidateStateValue) //nolint:forcetypeassert //...

		nodes := sufcstv.Nodes()

		for i := range nodes {
			n := nodes[i]
			p.existings[n.Address().String()] = n
		}

	}

	p.merger = NewSuffrageCandiateStateValueMerger(height, st)

	if preProcessConstraintFunc == nil {
		p.preProcessConstraintFunc = base.EmptyOperationProcessorProcessFunc
	}

	if processConstraintFunc == nil {
		p.processConstraintFunc = base.EmptyOperationProcessorProcessFunc
	}

	return p, nil
}

func (p *SuffrageCandidateProcessor) PreProcess(ctx context.Context, op base.Operation, getStateFunc base.GetStateFunc) (
	base.OperationProcessReasonError, error,
) {
	e := util.StringErrorFunc("failed to preprocess for SuffrageCandidate")

	fact := op.Fact().(SuffrageCandidateFact) //nolint:forcetypeassert //...

	if _, found := p.preprocessed[fact.Address().String()]; found {
		return base.NewBaseOperationProcessReasonError("candidate already pre-processed, %q", fact.Address()), nil
	}

	if _, found := p.suffrages[fact.Address().String()]; found {
		return base.NewBaseOperationProcessReasonError("candidate already in suffrage, %q", fact.Address()), nil
	}

	switch record, found := p.existings[fact.Address().String()]; {
	case !found:
	case p.height <= record.Deadline():
		p.preprocessed[fact.Address().String()] = struct{}{}

		return base.NewBaseOperationProcessReasonError("already candidate up to, %d", record.Deadline()), nil
	default:
		p.preprocessed[fact.Address().String()] = struct{}{}
	}

	switch reasonerr, err := p.preProcessConstraintFunc(ctx, op, getStateFunc); {
	case err != nil:
		return nil, e(err, "")
	case reasonerr != nil:
		return reasonerr, nil
	}

	return nil, nil
}

func (p *SuffrageCandidateProcessor) Process(ctx context.Context, op base.Operation, getStateFunc base.GetStateFunc) (
	[]base.StateMergeValue, base.OperationProcessReasonError, error,
) {
	fact := op.Fact().(SuffrageCandidateFact) //nolint:forcetypeassert //...

	node := isaac.NewSuffrageCandidate(
		isaac.NewNode(fact.Publickey(), fact.Address()),
		p.startheight,
		p.deadlineheight,
	)

	return []base.StateMergeValue{
		base.NewBaseStateMergeValue(
			isaac.SuffrageCandidateStateKey,
			isaac.NewSuffrageCandidateStateValue([]base.SuffrageCandidate{node}),
			func(base.Height, base.State) base.StateValueMerger {
				return p.merger
			},
		),
	}, nil, nil
}

type SuffrageCandiateStateValueMerger struct {
	*base.BaseStateValueMerger
	olds  []base.SuffrageCandidate
	nodes []base.SuffrageCandidate
}

func NewSuffrageCandiateStateValueMerger(height base.Height, st base.State) *SuffrageCandiateStateValueMerger {
	s := &SuffrageCandiateStateValueMerger{
		BaseStateValueMerger: base.NewBaseStateValueMerger(height, isaac.SuffrageCandidateStateKey, st),
	}

	if st != nil {
		if v := st.Value(); v != nil {
			s.olds = v.(base.SuffrageCandidateStateValue).Nodes()
		}
	}

	return s
}

func (s *SuffrageCandiateStateValueMerger) Merge(value base.StateValue, ops []util.Hash) error {
	mergevalue, ok := value.(base.StateMergeValue)
	if !ok {
		return errors.Errorf("not StateMergeValue, %T", value)
	}

	v, ok := mergevalue.Value().(base.SuffrageCandidateStateValue)
	if !ok {
		return errors.Errorf("not SuffrageCandidateStateValue, %T", value)
	}

	s.Lock()
	defer s.Unlock()

	s.nodes = append(s.nodes, v.Nodes()...)

	s.AddOperations(ops)

	return nil
}

func (s *SuffrageCandiateStateValueMerger) Close() error {
	newvalue, err := s.close()
	if err != nil {
		return errors.WithMessage(err, "failed to close SuffrageCandiateStateValueMerger")
	}

	s.BaseStateValueMerger.SetValue(newvalue)

	return s.BaseStateValueMerger.Close()
}

func (s *SuffrageCandiateStateValueMerger) close() (base.StateValue, error) {
	s.Lock()
	defer s.Unlock()

	if len(s.nodes) < 1 {
		return nil, errors.Errorf("empty new candiates")
	}

	olds := s.olds

	if len(olds) > 0 {
		// NOTE filter new nodes
		filtered := util.Filter2Slices(s.olds, s.nodes, func(_, _ interface{}, i, j int) bool {
			return s.olds[i].Address().Equal(s.nodes[j].Address())
		})

		switch {
		case len(filtered) < 0:
			olds = nil
		default:
			olds := make([]base.SuffrageCandidate, len(filtered))
			for i := range filtered {
				olds[i] = filtered[i].(base.SuffrageCandidate)
			}
		}
	}

	sort.Slice(s.nodes, func(i, j int) bool { // NOTE sort by address
		return strings.Compare(s.nodes[i].Address().String(), s.nodes[j].Address().String()) < 0
	})

	newnodes := make([]base.SuffrageCandidate, len(olds)+len(s.nodes))
	copy(newnodes, olds)
	copy(newnodes[len(olds):], s.nodes)

	return isaac.NewSuffrageCandidateStateValue(newnodes), nil
}
