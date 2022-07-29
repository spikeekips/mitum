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
	e := util.StringErrorFunc("failed to create new SuffrageJoinProcessor")

	b, err := base.NewBaseOperationProcessor(
		height, getStateFunc, newPreProcessConstraintFunc, newProcessConstraintFunc)
	if err != nil {
		return nil, e(err, "")
	}

	p := &SuffrageJoinProcessor{
		BaseOperationProcessor: b,
		threshold:              threshold,
		candidates:             map[string]base.SuffrageCandidateStateValue{},
		preprocessed:           map[string]struct{}{},
	}

	switch i, found, err := getStateFunc(isaac.SuffrageStateKey); {
	case err != nil:
		return nil, e(err, "")
	case !found, i == nil:
		return nil, e(isaac.StopProcessingRetryError.Errorf("empty state returned"), "")
	default:
		p.sufstv = i.Value().(base.SuffrageNodesStateValue) //nolint:forcetypeassert //...

		suf, err := p.sufstv.Suffrage()
		if err != nil {
			return nil, e(isaac.StopProcessingRetryError.Errorf("failed to get suffrage from state"), "")
		}

		p.suffrage = suf
	}

	switch _, candidates, err := isaac.LastCandidatesFromState(height, getStateFunc); {
	case err != nil:
		return nil, e(err, "")
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
	base.OperationProcessReasonError, error,
) {
	if len(p.candidates) < 1 {
		return base.NewBaseOperationProcessReasonError("not candidate"), nil
	}

	e := util.StringErrorFunc("failed to preprocess for SuffrageJoin")

	noop, ok := op.(base.NodeSignedFact)
	if !ok {
		return nil, e(nil, "not NodeSignedFact, %T", op)
	}

	fact := op.Fact().(SuffrageJoinFact) //nolint:forcetypeassert //...
	n := fact.Candidate()

	if _, found := p.preprocessed[n.String()]; found {
		return base.NewBaseOperationProcessReasonError("already preprocessed, %q", n), nil
	}

	if p.suffrage.Exists(n) {
		return base.NewBaseOperationProcessReasonError("candidate already in suffrage, %q", n), nil
	}

	var info base.SuffrageCandidateStateValue

	switch i, found := p.candidates[n.String()]; {
	case !found:
		return base.NewBaseOperationProcessReasonError("candidate not in candidates, %q", n), nil
	case fact.StartHeight() != i.Start():
		return base.NewBaseOperationProcessReasonError("start height does not match"), nil
	case i.Deadline() < p.Height():
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

	switch reasonerr, err := p.PreProcessConstraintFunc(ctx, op, getStateFunc); {
	case err != nil:
		return nil, e(err, "")
	case reasonerr != nil:
		return reasonerr, nil
	}

	if err := base.CheckFactSignsBySuffrage(p.suffrage, p.threshold, noop.NodeSigned()); err != nil {
		return base.NewBaseOperationProcessReasonError("not enough signs"), nil
	}

	p.preprocessed[info.Address().String()] = struct{}{}

	return nil, nil
}

func (p *SuffrageJoinProcessor) Process(ctx context.Context, op base.Operation, getStateFunc base.GetStateFunc) (
	[]base.StateMergeValue, base.OperationProcessReasonError, error,
) {
	e := util.StringErrorFunc("failed to process for SuffrageJoin")

	switch reasonerr, err := p.ProcessConstraintFunc(ctx, op, getStateFunc); {
	case err != nil:
		return nil, nil, e(err, "")
	case reasonerr != nil:
		return nil, reasonerr, nil
	}

	fact := op.Fact().(SuffrageJoinFact) //nolint:forcetypeassert //...

	member := p.candidates[fact.Candidate().String()]

	return []base.StateMergeValue{
		base.NewBaseStateMergeValue(
			isaac.SuffrageCandidateStateKey,
			isaac.NewSuffrageRemoveCandidateStateValue([]base.Address{member.Address()}),
			func(height base.Height, st base.State) base.StateValueMerger {
				return NewSuffrageCandidatesStateValueMerger(height, st)
			},
		),
		base.NewBaseStateMergeValue(
			isaac.SuffrageStateKey,
			newSuffrageAddNodeStateValue(p.sufstv.Height()+1, []base.Node{member}),
			func(height base.Height, st base.State) base.StateValueMerger {
				return NewSuffrageJoinStateValueMerger(height, st)
			},
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

type SuffrageJoinStateValueMerger struct {
	*base.BaseStateValueMerger
	existings []base.SuffrageNodeStateValue
	added     []base.Node
}

func NewSuffrageJoinStateValueMerger(height base.Height, st base.State) *SuffrageJoinStateValueMerger {
	s := &SuffrageJoinStateValueMerger{
		BaseStateValueMerger: base.NewBaseStateValueMerger(height, isaac.SuffrageStateKey, st),
	}

	s.existings = st.Value().(base.SuffrageNodesStateValue).Nodes() //nolint:forcetypeassert //...

	return s
}

func (s *SuffrageJoinStateValueMerger) Merge(value base.StateValue, ops []util.Hash) error {
	v, ok := value.(suffrageAddNodeStateValue)
	if !ok {
		return errors.Errorf("not suffrageAddNodeStateValue, %T", value)
	}

	s.Lock()
	defer s.Unlock()

	s.added = append(s.added, v.nodes...)

	s.AddOperations(ops)

	return nil
}

func (s *SuffrageJoinStateValueMerger) Close() error {
	newvalue, err := s.close()
	if err != nil {
		return errors.WithMessage(err, "failed to close SuffrageJoinStateValueMerger")
	}

	s.BaseStateValueMerger.SetValue(newvalue)

	return s.BaseStateValueMerger.Close()
}

func (s *SuffrageJoinStateValueMerger) close() (base.StateValue, error) {
	s.Lock()
	defer s.Unlock()

	if len(s.added) < 1 {
		return nil, isaac.ErrIgnoreStateValue.Errorf("empty newly added nodes")
	}

	sort.Slice(s.added, func(i, j int) bool { // NOTE sort by address
		return strings.Compare(s.added[i].Address().String(), s.added[j].Address().String()) < 0
	})

	newnodes := make([]base.SuffrageNodeStateValue, len(s.existings)+len(s.added))
	copy(newnodes, s.existings)

	for i := range s.added {
		newnodes[len(s.existings)+i] = isaac.NewSuffrageNodeStateValue(s.added[i], s.Height()+1)
	}

	return isaac.NewSuffrageNodesStateValue(s.Height(), newnodes), nil
}

type suffrageAddNodeStateValue struct {
	nodes          []base.Node
	suffrageheight base.Height
}

func newSuffrageAddNodeStateValue(suffrageheight base.Height, nodes []base.Node) suffrageAddNodeStateValue {
	return suffrageAddNodeStateValue{
		nodes:          nodes,
		suffrageheight: suffrageheight,
	}
}

func (s suffrageAddNodeStateValue) IsValid([]byte) error {
	vs := make([]util.IsValider, len(s.nodes)+1)
	vs[0] = s.suffrageheight

	for i := range vs {
		vs[i+1] = s.nodes[i]
	}

	if err := util.CheckIsValid(nil, false, vs...); err != nil {
		return util.ErrInvalid.Errorf("invalie suffrageAddNodeStateValue")
	}

	return nil
}

func (s suffrageAddNodeStateValue) HashBytes() []byte {
	bs := make([]util.Byter, len(s.nodes)+1)
	bs[0] = util.DummyByter(s.suffrageheight.Bytes)

	for i := range s.nodes {
		bs[i+1] = util.DummyByter(s.nodes[i].HashBytes)
	}

	return util.ConcatByters(bs...)
}
