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

type SuffrageJoinProcessor struct {
	*base.BaseOperationProcessor
	sufstv       base.SuffrageNodesStateValue
	suffrage     base.Suffrage
	candidates   map[string]base.SuffrageCandidateStateValue
	preprocessed map[string]struct{} //revive:disable-line:nested-structs
	threshold    base.Threshold
}

func NewSuffrageJoinProcessor(
	height base.Height,
	threshold base.Threshold,
	getStateFunc base.GetStateFunc,
	newPreProcessConstraintFunc base.NewOperationProcessorProcessFunc,
	newProcessConstraintFunc base.NewOperationProcessorProcessFunc,
) (*SuffrageJoinProcessor, error) {
	e := util.StringError("create new SuffrageJoinProcessor")

	b, err := base.NewBaseOperationProcessor(
		height, getStateFunc, newPreProcessConstraintFunc, newProcessConstraintFunc)
	if err != nil {
		return nil, e.Wrap(err)
	}

	p := &SuffrageJoinProcessor{
		BaseOperationProcessor: b,
		threshold:              threshold,
		candidates:             map[string]base.SuffrageCandidateStateValue{},
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

	switch _, candidates, err := isaac.LastCandidatesFromState(height, getStateFunc); {
	case err != nil:
		return nil, e.Wrap(err)
	case candidates == nil:
	default:
		for i := range candidates {
			n := candidates[i]

			p.candidates[n.Address().String()] = n
		}
	}

	return p, nil
}

func (p *SuffrageJoinProcessor) Close() error {
	if err := p.BaseOperationProcessor.Close(); err != nil {
		return err
	}

	p.sufstv = nil
	p.suffrage = nil
	p.candidates = nil
	p.preprocessed = nil
	p.threshold = 0

	return nil
}

func (p *SuffrageJoinProcessor) PreProcess(ctx context.Context, op base.Operation, getStateFunc base.GetStateFunc) (
	context.Context, base.OperationProcessReasonError, error,
) {
	if len(p.candidates) < 1 {
		return ctx, base.NewBaseOperationProcessReasonError("not candidate"), nil
	}

	e := util.StringError("preprocess for SuffrageJoin")

	noop, ok := op.(base.NodeSignFact)
	if !ok {
		return ctx, nil, e.Errorf("not NodeSignFact, %T", op)
	}

	fact := op.Fact().(SuffrageJoinFact) //nolint:forcetypeassert //...
	n := fact.Candidate()

	if _, found := p.preprocessed[n.String()]; found {
		return ctx, base.NewBaseOperationProcessReasonError("already preprocessed, %q", n), nil
	}

	if p.suffrage.Exists(n) {
		return ctx, base.NewBaseOperationProcessReasonError("candidate already in suffrage, %q", n), nil
	}

	var info base.SuffrageCandidateStateValue

	switch i, found := p.candidates[n.String()]; {
	case !found:
		return ctx, base.NewBaseOperationProcessReasonError("candidate not in candidates, %q", n), nil
	case fact.Start() != i.Start():
		return ctx, base.NewBaseOperationProcessReasonError("start does not match"), nil
	case i.Deadline() < p.Height():
		return ctx, base.NewBaseOperationProcessReasonError("candidate expired, %q", n), nil
	default:
		info = i
	}

	switch node, err := p.findCandidateFromSigns(op); {
	case err != nil:
		return ctx, base.NewBaseOperationProcessReasonError(err.Error()), nil
	case !node.Publickey().Equal(info.Publickey()):
		return ctx, base.NewBaseOperationProcessReasonError("not signed by candidate key"), nil
	}

	switch reasonerr, err := p.PreProcessConstraintFunc(ctx, op, getStateFunc); {
	case err != nil:
		return ctx, nil, e.Wrap(err)
	case reasonerr != nil:
		return ctx, reasonerr, nil
	}

	if err := base.CheckFactSignsBySuffrage(p.suffrage, p.threshold, noop.NodeSigns()); err != nil {
		return ctx, base.NewBaseOperationProcessReasonError("not enough signs"), nil
	}

	p.preprocessed[info.Address().String()] = struct{}{}

	return ctx, nil, nil
}

func (p *SuffrageJoinProcessor) Process(ctx context.Context, op base.Operation, getStateFunc base.GetStateFunc) (
	[]base.StateMergeValue, base.OperationProcessReasonError, error,
) {
	e := util.StringError("process for SuffrageJoin")

	switch reasonerr, err := p.ProcessConstraintFunc(ctx, op, getStateFunc); {
	case err != nil:
		return nil, nil, e.Wrap(err)
	case reasonerr != nil:
		return nil, reasonerr, nil
	}

	fact := op.Fact().(SuffrageJoinFact) //nolint:forcetypeassert //...

	member := p.candidates[fact.Candidate().String()]

	return []base.StateMergeValue{
		base.NewBaseStateMergeValue(
			isaac.SuffrageCandidateStateKey,
			newSuffrageRemoveCandidateStateValue([]base.Address{member.Address()}),
			func(height base.Height, st base.State) base.StateValueMerger {
				return NewSuffrageCandidatesStateValueMerger(height, st)
			},
		),
		base.NewBaseStateMergeValue(
			isaac.SuffrageStateKey,
			newSuffrageJoinNodeStateValue([]base.Node{member}),
			func(height base.Height, st base.State) base.StateValueMerger {
				return NewSuffrageJoinStateValueMerger(height, st)
			},
		),
	}, nil, nil
}

func (*SuffrageJoinProcessor) findCandidateFromSigns(op base.Operation) (base.Node, error) {
	fact, ok := op.Fact().(SuffrageJoinFact)
	if !ok {
		return nil, errors.Errorf("not SuffrageJoinFact, %T", op.Fact())
	}

	sfs := op.Signs()

	for i := range sfs {
		ns := sfs[i].(base.NodeSign) //nolint:forcetypeassert //...

		if !ns.Node().Equal(fact.Candidate()) {
			continue
		}

		return isaac.NewNode(ns.Signer(), ns.Node()), nil
	}

	return nil, errors.Errorf("not signed by Join")
}

type SuffrageJoinStateValueMerger struct {
	*base.BaseStateValueMerger
	existing  base.SuffrageNodesStateValue
	joined    []base.Node
	disjoined []base.Address
}

func NewSuffrageJoinStateValueMerger(height base.Height, st base.State) *SuffrageJoinStateValueMerger {
	s := &SuffrageJoinStateValueMerger{
		BaseStateValueMerger: base.NewBaseStateValueMerger(height, isaac.SuffrageStateKey, st),
	}

	s.existing = st.Value().(base.SuffrageNodesStateValue) //nolint:forcetypeassert //...

	return s
}

func (s *SuffrageJoinStateValueMerger) Merge(value base.StateValue, op util.Hash) error {
	s.Lock()
	defer s.Unlock()

	switch t := value.(type) {
	case suffrageJoinNodeStateValue:
		s.joined = append(s.joined, t.nodes...)
	case suffrageDisjoinNodeStateValue:
		s.disjoined = append(s.disjoined, t.node)
	default:
		return errors.Errorf("unsupported suffrage state value, %T", value)
	}

	s.AddOperation(op)

	return nil
}

func (s *SuffrageJoinStateValueMerger) Close() error {
	newvalue, err := s.close()
	if err != nil {
		return errors.WithMessage(err, "close SuffrageJoinStateValueMerger")
	}

	s.BaseStateValueMerger.SetValue(newvalue)

	return s.BaseStateValueMerger.Close()
}

func (s *SuffrageJoinStateValueMerger) close() (base.StateValue, error) {
	s.Lock()
	defer s.Unlock()

	if len(s.disjoined) < 1 && len(s.joined) < 1 {
		return nil, base.ErrIgnoreStateValue.Errorf("no nodes changes")
	}

	existingnodes := s.existing.Nodes()

	if len(s.disjoined) > 0 {
		existingnodes = util.Filter2Slices(
			existingnodes,
			s.disjoined,
			func(x base.SuffrageNodeStateValue, y base.Address) bool {
				return x.Address().Equal(y)
			},
		)
	}

	if len(s.joined) > 0 {
		sort.Slice(s.joined, func(i, j int) bool { // NOTE sort by address
			return strings.Compare(s.joined[i].Address().String(), s.joined[j].Address().String()) < 0
		})
	}

	newnodes := make([]base.SuffrageNodeStateValue, len(existingnodes)+len(s.joined))
	copy(newnodes, existingnodes)

	for i := range s.joined {
		newnodes[len(existingnodes)+i] = isaac.NewSuffrageNodeStateValue(s.joined[i], s.Height()+1)
	}

	return isaac.NewSuffrageNodesStateValue(
		s.existing.Height()+1,
		newnodes,
	), nil
}

type suffrageJoinNodeStateValue struct {
	nodes []base.Node
}

func newSuffrageJoinNodeStateValue(nodes []base.Node) suffrageJoinNodeStateValue {
	return suffrageJoinNodeStateValue{
		nodes: nodes,
	}
}

func (s suffrageJoinNodeStateValue) IsValid([]byte) error {
	if err := util.CheckIsValiderSlice(nil, false, s.nodes); err != nil {
		return util.ErrInvalid.Errorf("invalie suffrageJoinNodeStateValue")
	}

	return nil
}

func (s suffrageJoinNodeStateValue) HashBytes() []byte {
	bs := make([]util.Byter, len(s.nodes))

	for i := range s.nodes {
		bs[i] = util.DummyByter(s.nodes[i].HashBytes)
	}

	return util.ConcatByters(bs...)
}
