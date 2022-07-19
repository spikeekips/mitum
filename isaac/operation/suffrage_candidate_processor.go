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
	*base.BaseOperationProcessor
	suffrages      map[string]base.Node
	existings      map[string]base.SuffrageCandidate
	preprocessed   map[string]struct{} // revive:disable-line:nested-structs
	startheight    base.Height
	deadlineheight base.Height
}

func NewSuffrageCandidateProcessor(
	height base.Height,
	getStateFunc base.GetStateFunc,
	newPreProcessConstraintFunc base.NewOperationProcessorProcessFunc,
	newProcessConstraintFunc base.NewOperationProcessorProcessFunc,
	lifespan base.Height,
) (*SuffrageCandidateProcessor, error) {
	e := util.StringErrorFunc("failed to create new SuffrageCandidateProcessor")

	b, err := base.NewBaseOperationProcessor(
		height, getStateFunc, newPreProcessConstraintFunc, newProcessConstraintFunc)
	if err != nil {
		return nil, e(err, "")
	}

	p := &SuffrageCandidateProcessor{
		BaseOperationProcessor: b,
		existings:              map[string]base.SuffrageCandidate{},
		suffrages:              map[string]base.Node{},
		preprocessed:           map[string]struct{}{},
		startheight:            height + 1,
		deadlineheight:         height + 1 + lifespan,
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

	switch candidates, err := isaac.LastCandidatesFromState(height, getStateFunc); {
	case err != nil:
		return nil, e(err, "")
	case candidates == nil:
	default:
		for i := range candidates {
			n := candidates[i]

			p.existings[n.Address().String()] = n
		}
	}

	return p, nil
}

func (p *SuffrageCandidateProcessor) PreProcess(
	ctx context.Context, op base.Operation, getStateFunc base.GetStateFunc) (
	base.OperationProcessReasonError, error,
) {
	e := util.StringErrorFunc("failed to preprocess for SuffrageCandidate")

	fact := op.Fact().(SuffrageCandidateFact) //nolint:forcetypeassert //...

	if _, found := p.preprocessed[fact.Address().String()]; found {
		return base.NewBaseOperationProcessReasonError("candidate already preprocessed, %q", fact.Address()), nil
	}

	if _, found := p.suffrages[fact.Address().String()]; found {
		return base.NewBaseOperationProcessReasonError("candidate already in suffrage, %q", fact.Address()), nil
	}

	switch record, found := p.existings[fact.Address().String()]; {
	case !found:
		p.preprocessed[fact.Address().String()] = struct{}{}
	case p.Height() <= record.Deadline():
		p.preprocessed[fact.Address().String()] = struct{}{}

		return base.NewBaseOperationProcessReasonError("already candidate up to, %d", record.Deadline()), nil
	}

	switch reasonerr, err := p.PreProcessConstraintFunc(ctx, op, getStateFunc); {
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
	e := util.StringErrorFunc("failed to process for SuffrageCandidate")

	switch reasonerr, err := p.ProcessConstraintFunc(ctx, op, getStateFunc); {
	case err != nil:
		return nil, nil, e(err, "")
	case reasonerr != nil:
		return nil, reasonerr, nil
	}

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
			func(height base.Height, st base.State) base.StateValueMerger {
				return NewSuffrageCandidateStateValueMerger(height, st)
			},
		),
	}, nil, nil
}

type SuffrageCandidateStateValueMerger struct {
	*base.BaseStateValueMerger
	existings []base.SuffrageCandidate
	added     []base.SuffrageCandidate
	removes   []base.Address
}

func NewSuffrageCandidateStateValueMerger(height base.Height, st base.State) *SuffrageCandidateStateValueMerger {
	s := &SuffrageCandidateStateValueMerger{
		BaseStateValueMerger: base.NewBaseStateValueMerger(height, isaac.SuffrageCandidateStateKey, st),
	}

	if st != nil {
		if v := st.Value(); v != nil {
			s.existings = v.(base.SuffrageCandidateStateValue).Nodes() //nolint:forcetypeassert //...
		}
	}

	return s
}

func (s *SuffrageCandidateStateValueMerger) Merge(value base.StateValue, ops []util.Hash) error {
	s.Lock()
	defer s.Unlock()

	switch t := value.(type) {
	case isaac.SuffrageCandidateStateValue:
		s.added = append(s.added, t.Nodes()...)
	case isaac.SuffrageRemoveCandidateStateValue:
		s.removes = append(s.removes, t.Nodes()...)
	default:
		return errors.Errorf("unknown SuffrageCandidateStateValue, %T", value)
	}

	s.AddOperations(ops)

	return nil
}

func (s *SuffrageCandidateStateValueMerger) Close() error {
	newvalue, err := s.close()
	if err != nil {
		return errors.WithMessage(err, "failed to close SuffrageCandidateStateValueMerger")
	}

	s.BaseStateValueMerger.SetValue(newvalue)

	return s.BaseStateValueMerger.Close()
}

func (s *SuffrageCandidateStateValueMerger) close() (base.StateValue, error) {
	s.Lock()
	defer s.Unlock()

	if len(s.removes) < 1 && len(s.added) < 1 {
		return nil, isaac.ErrIgnoreStateValue.Errorf("empty newly added or removes nodes")
	}

	convert := func(j []interface{}) []base.SuffrageCandidate {
		if len(j) < 1 {
			return nil
		}

		n := make([]base.SuffrageCandidate, len(j))
		for i := range j {
			n[i] = j[i].(base.SuffrageCandidate) //nolint:forcetypeassert //...
		}

		return n
	}

	existings := s.existings
	if len(s.removes) > 0 {
		filtered := util.Filter2Slices(existings, s.removes, func(_, _ interface{}, i, j int) bool {
			return existings[i].Address().Equal(s.removes[j])
		})

		existings = convert(filtered)
	}

	if len(s.added) > 0 {
		// NOTE filter new nodes
		filtered := util.Filter2Slices(existings, s.added, func(_, _ interface{}, i, j int) bool {
			return s.existings[i].Address().Equal(s.added[j].Address())
		})

		existings = convert(filtered)
	}

	sort.Slice(s.added, func(i, j int) bool { // NOTE sort by address
		return strings.Compare(s.added[i].Address().String(), s.added[j].Address().String()) < 0
	})

	newnodes := make([]base.SuffrageCandidate, len(existings)+len(s.added))
	copy(newnodes, existings)
	copy(newnodes[len(existings):], s.added)

	return isaac.NewSuffrageCandidateStateValue(newnodes), nil
}
