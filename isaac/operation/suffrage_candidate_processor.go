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
	existings      map[string]base.SuffrageCandidateStateValue
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
	e := util.StringError("create new SuffrageCandidateProcessor")

	b, err := base.NewBaseOperationProcessor(
		height, getStateFunc, newPreProcessConstraintFunc, newProcessConstraintFunc)
	if err != nil {
		return nil, e.Wrap(err)
	}

	p := &SuffrageCandidateProcessor{
		BaseOperationProcessor: b,
		existings:              map[string]base.SuffrageCandidateStateValue{},
		suffrages:              map[string]base.Node{},
		preprocessed:           map[string]struct{}{},
		startheight:            height + 1,
		deadlineheight:         height + 1 + lifespan,
	}

	switch i, found, err := getStateFunc(isaac.SuffrageStateKey); {
	case err != nil:
		return nil, e.Wrap(err)
	case !found:
	case i == nil:
		return nil, e.Errorf("empty state returned")
	default:
		sufstv := i.Value().(base.SuffrageNodesStateValue) //nolint:forcetypeassert //...

		nodes := sufstv.Nodes()

		for i := range nodes {
			n := nodes[i]
			p.suffrages[n.Address().String()] = n
		}
	}

	switch _, candidates, err := isaac.LastCandidatesFromState(height, getStateFunc); {
	case err != nil:
		return nil, e.Wrap(err)
	case candidates == nil:
	default:
		for i := range candidates {
			n := candidates[i]

			p.existings[n.Address().String()] = n
		}
	}

	return p, nil
}

func (p *SuffrageCandidateProcessor) Close() error {
	if err := p.BaseOperationProcessor.Close(); err != nil {
		return err
	}

	p.suffrages = nil
	p.existings = nil
	p.preprocessed = nil
	p.startheight = base.NilHeight
	p.deadlineheight = base.NilHeight

	return nil
}

func (p *SuffrageCandidateProcessor) PreProcess(
	ctx context.Context, op base.Operation, getStateFunc base.GetStateFunc) (
	context.Context, base.OperationProcessReasonError, error,
) {
	e := util.StringError("preprocess for SuffrageCandidateStateValue")

	fact := op.Fact().(SuffrageCandidateFact) //nolint:forcetypeassert //...

	if _, found := p.preprocessed[fact.Address().String()]; found {
		return ctx, base.NewBaseOperationProcessReasonError("candidate already preprocessed, %q", fact.Address()), nil
	}

	if _, found := p.suffrages[fact.Address().String()]; found {
		return ctx, base.NewBaseOperationProcessReasonError("candidate already in suffrage, %q", fact.Address()), nil
	}

	switch record, found := p.existings[fact.Address().String()]; {
	case !found:
		p.preprocessed[fact.Address().String()] = struct{}{}
	case p.Height() <= record.Deadline():
		p.preprocessed[fact.Address().String()] = struct{}{}

		return ctx, base.NewBaseOperationProcessReasonError("already candidate up to, %d", record.Deadline()), nil
	}

	switch reasonerr, err := p.PreProcessConstraintFunc(ctx, op, getStateFunc); {
	case err != nil:
		return ctx, nil, e.Wrap(err)
	case reasonerr != nil:
		return ctx, reasonerr, nil
	}

	return ctx, nil, nil
}

func (p *SuffrageCandidateProcessor) Process(ctx context.Context, op base.Operation, getStateFunc base.GetStateFunc) (
	[]base.StateMergeValue, base.OperationProcessReasonError, error,
) {
	e := util.StringError("process for SuffrageCandidateStateValue")

	switch reasonerr, err := p.ProcessConstraintFunc(ctx, op, getStateFunc); {
	case err != nil:
		return nil, nil, e.Wrap(err)
	case reasonerr != nil:
		return nil, reasonerr, nil
	}

	fact := op.Fact().(SuffrageCandidateFact) //nolint:forcetypeassert //...

	node := isaac.NewSuffrageCandidateStateValue(
		isaac.NewNode(fact.Publickey(), fact.Address()),
		p.startheight,
		p.deadlineheight,
	)

	return []base.StateMergeValue{
		base.NewBaseStateMergeValue(
			isaac.SuffrageCandidateStateKey,
			isaac.NewSuffrageCandidatesStateValue([]base.SuffrageCandidateStateValue{node}),
			func(height base.Height, st base.State) base.StateValueMerger {
				return NewSuffrageCandidatesStateValueMerger(height, st)
			},
		),
	}, nil, nil
}

type SuffrageCandidatesStateValueMerger struct {
	*base.BaseStateValueMerger
	existings []base.SuffrageCandidateStateValue
	added     []base.SuffrageCandidateStateValue
	removes   []base.Address
}

func NewSuffrageCandidatesStateValueMerger(height base.Height, st base.State) *SuffrageCandidatesStateValueMerger {
	s := &SuffrageCandidatesStateValueMerger{
		BaseStateValueMerger: base.NewBaseStateValueMerger(height, isaac.SuffrageCandidateStateKey, st),
	}

	if st != nil {
		if v := st.Value(); v != nil {
			s.existings = v.(base.SuffrageCandidatesStateValue).Nodes() //nolint:forcetypeassert //...
		}
	}

	return s
}

func (s *SuffrageCandidatesStateValueMerger) Merge(value base.StateValue, op util.Hash) error {
	s.Lock()
	defer s.Unlock()

	switch t := value.(type) {
	case isaac.SuffrageCandidatesStateValue:
		s.added = append(s.added, t.Nodes()...)
	case suffrageRemoveCandidateStateValue:
		s.removes = append(s.removes, t.Nodes()...)
	default:
		return errors.Errorf("unknown SuffrageCandidatesStateValue, %T", value)
	}

	s.AddOperation(op)

	return nil
}

func (s *SuffrageCandidatesStateValueMerger) Close() error {
	newvalue, err := s.close()
	if err != nil {
		return errors.WithMessage(err, "close SuffrageCandidatesStateValueMerger")
	}

	s.BaseStateValueMerger.SetValue(newvalue)

	return s.BaseStateValueMerger.Close()
}

func (s *SuffrageCandidatesStateValueMerger) close() (base.StateValue, error) {
	s.Lock()
	defer s.Unlock()

	if len(s.removes) < 1 && len(s.added) < 1 {
		return nil, base.ErrIgnoreStateValue.Errorf("empty newly added or removes nodes")
	}

	existings := s.existings
	if len(s.removes) > 0 {
		existings = util.Filter2Slices(
			existings,
			s.removes,
			func(x base.SuffrageCandidateStateValue, y base.Address) bool {
				return x.Address().Equal(y)
			},
		)
	}

	if len(s.added) > 0 {
		// NOTE filter new nodes
		existings = util.Filter2Slices(existings, s.added, func(x, y base.SuffrageCandidateStateValue) bool {
			return x.Address().Equal(y.Address())
		})
	}

	sort.Slice(s.added, func(i, j int) bool { // NOTE sort by address
		return strings.Compare(s.added[i].Address().String(), s.added[j].Address().String()) < 0
	})

	newnodes := make([]base.SuffrageCandidateStateValue, len(existings)+len(s.added))
	copy(newnodes, existings)
	copy(newnodes[len(existings):], s.added)

	return isaac.NewSuffrageCandidatesStateValue(newnodes), nil
}

type suffrageRemoveCandidateStateValue struct {
	nodes []base.Address
}

func newSuffrageRemoveCandidateStateValue(nodes []base.Address) suffrageRemoveCandidateStateValue {
	return suffrageRemoveCandidateStateValue{
		nodes: nodes,
	}
}

func (suffrageRemoveCandidateStateValue) HashBytes() []byte {
	return nil
}

func (s suffrageRemoveCandidateStateValue) IsValid([]byte) error {
	if err := util.CheckIsValiderSlice(nil, false, s.nodes); err != nil {
		return util.ErrInvalid.WithMessage(err, "invalid suffrageRemoveCandidateStateValue")
	}

	return nil
}

func (s suffrageRemoveCandidateStateValue) Nodes() []base.Address {
	return s.nodes
}
