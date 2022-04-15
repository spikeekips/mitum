package isaac

import (
	"context"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/tree"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type DummyBlockDataWriter struct {
	sync.RWMutex
	height      base.Height
	proposal    base.ProposalSignedFact
	manifest    base.Manifest
	manifesterr error
	opstree     *tree.FixedTreeGenerator
	ops         []base.Operation
	sts         *util.LockedMap
	setstatesf  func(context.Context, int, []base.State, base.Operation) error
	savef       func(context.Context) error
}

func NewDummyBlockDataWriter(height base.Height) *DummyBlockDataWriter {
	return &DummyBlockDataWriter{
		height: height,
		sts:    util.NewLockedMap(),
	}
}

func (w *DummyBlockDataWriter) SetProposal(_ context.Context, proposal base.ProposalSignedFact) error {
	w.Lock()
	defer w.Unlock()

	w.proposal = proposal

	return nil
}

func (w *DummyBlockDataWriter) SetOperationsSize(n uint64) {
	w.ops = nil
	w.opstree = tree.NewFixedTreeGenerator(n)
}

func (w *DummyBlockDataWriter) SetProcessResult(ctx context.Context, index int, facthash util.Hash, instate bool, errorreason base.OperationProcessReasonError) error {
	var msg string
	if errorreason != nil {
		msg = errorreason.Msg()
	}

	node := base.NewOperationFixedTreeNode(
		uint64(index),
		facthash,
		instate,
		msg,
	)
	if err := w.opstree.Add(node); err != nil {
		return errors.Wrap(err, "failed to set operation")
	}

	return nil
}

func (w *DummyBlockDataWriter) SetStates(ctx context.Context, index int, states []base.State, operation base.Operation) error {
	if w.setstatesf != nil {
		return w.setstatesf(ctx, index, states, operation)
	}

	return w.setStates(ctx, index, states, operation)
}

func (w *DummyBlockDataWriter) setStates(ctx context.Context, index int, states []base.State, op base.Operation) error {
	w.Lock()
	defer w.Unlock()

	e := util.StringErrorFunc("failed to set states")

	for i := range states {
		st := states[i]

		j, _, _ := w.sts.Get(st.Key(), func() (interface{}, error) {
			return st.Merger(w.height), nil
		})

		merger := j.(base.StateValueMerger)

		if err := merger.Merge(st.Value(), []util.Hash{op.Fact().Hash()}); err != nil {
			return e(err, "failed to merge")
		}
	}

	w.ops = append(w.ops, op)

	return nil
}

func (w *DummyBlockDataWriter) SetINITVoteproof(ctx context.Context, vp base.INITVoteproof) error {
	return nil
}

func (w *DummyBlockDataWriter) SetACCEPTVoteproof(ctx context.Context, vp base.ACCEPTVoteproof) error {
	return nil
}

func (w *DummyBlockDataWriter) Manifest(context.Context, base.Manifest) (base.Manifest, error) {
	return w.manifest, w.manifesterr
}

func (w *DummyBlockDataWriter) Save(ctx context.Context) error {
	if w.savef == nil {
		return nil
	}

	return w.savef(ctx)
}

func (w *DummyBlockDataWriter) Cancel() error {
	return nil
}

type DummyOperationProcessor struct {
	preprocess func(context.Context, base.Operation, base.StatePool) (base.OperationProcessReasonError, error)
	process    func(context.Context, base.Operation, base.StatePool) ([]base.State, base.OperationProcessReasonError, error)
}

func (p *DummyOperationProcessor) PreProcess(ctx context.Context, op base.Operation, sp base.StatePool) (base.OperationProcessReasonError, error) {
	if p.preprocess == nil {
		return base.NewBaseOperationProcessReasonError("nil preprocess"), nil
	}

	return p.preprocess(ctx, op, sp)
}

func (p *DummyOperationProcessor) Process(ctx context.Context, op base.Operation, sp base.StatePool) ([]base.State, base.OperationProcessReasonError, error) {
	if p.process == nil {
		return nil, nil, nil
	}

	return p.process(ctx, op, sp)
}

type testDefaultProposalProcessor struct {
	BaseTestBallots
}

func (t *testDefaultProposalProcessor) newproposal(fact ProposalFact) base.ProposalSignedFact {
	fs := NewProposalSignedFact(fact)
	_ = fs.Sign(t.Local.Privatekey(), t.Policy.NetworkID())

	return fs
}

func (t *testDefaultProposalProcessor) newStates(height base.Height, keys ...string) []base.State {
	stts := make([]base.State, len(keys))
	for i := range keys {
		v := base.NewDummyStateValue(util.UUID().String())
		stts[i] = base.NewBaseState(
			height,
			keys[i],
			v,
			valuehash.RandomSHA256(),
			nil,
		)
	}

	return stts
}

func (t *testDefaultProposalProcessor) prepareOperations(
	height base.Height,
	n int,
	newoperation func(DummyOperationFact) base.Operation,
) (
	[]util.Hash,
	map[string]base.Operation,
	map[string]base.State,
) {
	if newoperation == nil {
		newoperation = func(fact DummyOperationFact) base.Operation {
			op, _ := NewDummyOperationProcessable(fact, t.Local.Privatekey(), t.Policy.NetworkID())
			return op
		}
	}

	ophs := make([]util.Hash, n)
	ops := map[string]base.Operation{}
	sts := map[string]base.State{}

	for i := range ophs {
		fact := NewDummyOperationFact(util.UUID().Bytes(), valuehash.RandomSHA256())
		op := newoperation(fact)

		ophs[i] = fact.Hash()
		st := t.newStates(height, fact.Hash().String())[0]

		if p, ok := op.(DummyOperationProcessable); ok {
			p.preprocess = func(context.Context, base.StatePool) (base.OperationProcessReasonError, error) {
				return nil, nil
			}
			p.process = func(context.Context, base.StatePool) ([]base.State, base.OperationProcessReasonError, error) {
				return []base.State{st}, nil, nil
			}

			op = p
		}

		ops[fact.Hash().String()] = op
		sts[fact.Hash().String()] = st
	}

	return ophs, ops, sts
}

func (t *testDefaultProposalProcessor) TestNew() {
	point := base.RawPoint(33, 44)

	pr := t.newproposal(NewProposalFact(point, t.Local.Address(), []util.Hash{valuehash.RandomSHA256()}))

	previous := base.NewDummyManifest(point.Height()-1, valuehash.RandomSHA256())
	opp := NewDefaultProposalProcessor(pr, previous, nil, nil, nil, nil)
	_ = (interface{})(opp).(proposalProcessor)

	base.EqualProposalSignedFact(t.Assert(), pr, opp.Proposal())
}

func (t *testDefaultProposalProcessor) TestCollectOperations() {
	point := base.RawPoint(33, 44)

	ophs, ops, _ := t.prepareOperations(point.Height()-1, 4, nil)

	pr := t.newproposal(NewProposalFact(point, t.Local.Address(), ophs))

	previous := base.NewDummyManifest(point.Height()-1, valuehash.RandomSHA256())
	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	writer := NewDummyBlockDataWriter(point.Height())
	writer.manifest = manifest

	opp := NewDefaultProposalProcessor(pr, previous, writer, nil, func(_ context.Context, facthash util.Hash) (base.Operation, error) {
		op, found := ops[facthash.String()]
		if !found {
			return nil, OperationNotFoundInProcessorError.Errorf("operation not found")
		}

		return op, nil
	},
		func(hint.Hint) (base.OperationProcessor, bool) { return nil, false },
	)

	m, err := opp.Process(context.Background(), nil)
	t.NoError(err)
	t.NotNil(m)

	for i := range ophs {
		a := ops[ophs[i].String()]
		b := opp.ops[i]

		t.NotNil(a)
		t.NotNil(b)

		base.EqualSignedFact(t.Assert(), a, b)
	}
}

func (t *testDefaultProposalProcessor) TestCollectOperationsFailed() {
	point := base.RawPoint(33, 44)

	ophs, ops, _ := t.prepareOperations(point.Height()-1, 4, nil)

	pr := t.newproposal(NewProposalFact(point, t.Local.Address(), ophs))

	previous := base.NewDummyManifest(point.Height()-1, valuehash.RandomSHA256())
	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	writer := NewDummyBlockDataWriter(point.Height())
	writer.manifest = manifest

	opp := NewDefaultProposalProcessor(pr, previous, writer, nil, func(_ context.Context, facthash util.Hash) (base.Operation, error) {
		op, found := ops[facthash.String()]
		if !found {
			return nil, OperationNotFoundInProcessorError.Errorf("operation not found")
		}

		switch {
		case facthash.Equal(ophs[1]), facthash.Equal(ophs[3]):
		default:
			return nil, util.WrongTypeError.Errorf("operation not found")
		}

		return op, nil
	}, nil)
	opp.retrylimit = 1
	opp.retryinterval = 1

	m, err := opp.Process(context.Background(), nil)
	t.Error(err)
	t.Nil(m)

	t.True(errors.Is(err, util.WrongTypeError))
	t.Contains(err.Error(), "failed to collect operations")
}

func (t *testDefaultProposalProcessor) TestCollectOperationsFailedButIgnored() {
	point := base.RawPoint(33, 44)

	ophs, ops, _ := t.prepareOperations(point.Height()-1, 4, nil)

	pr := t.newproposal(NewProposalFact(point, t.Local.Address(), ophs))

	previous := base.NewDummyManifest(point.Height()-1, valuehash.RandomSHA256())
	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	writer := NewDummyBlockDataWriter(point.Height())
	writer.manifest = manifest

	opp := NewDefaultProposalProcessor(pr, previous, writer, nil, func(_ context.Context, facthash util.Hash) (base.Operation, error) {
		op, found := ops[facthash.String()]
		if !found {
			return nil, nil
		}

		switch {
		case facthash.Equal(ophs[1]):
			return nil, InvalidOperationInProcessorError.Call()
		case facthash.Equal(ophs[3]):
			return nil, OperationNotFoundInProcessorError.Call()
		}

		return op, nil
	},
		func(hint.Hint) (base.OperationProcessor, bool) { return nil, false },
	)

	m, err := opp.Process(context.Background(), nil)
	t.NoError(err)
	t.NotNil(m)

	for i := range ophs {
		b := opp.ops[i]
		if i == 1 {
			j, ok := b.(ReasonProcessedOperation)
			t.True(ok)

			t.Contains("invalid operation", j.Reason().Msg())

			continue
		}

		if i == 3 {
			t.Nil(b)

			continue
		}

		a := ops[ophs[i].String()]
		t.NotNil(a)
		t.NotNil(b)

		base.EqualSignedFact(t.Assert(), a, b)
	}
}

func (t *testDefaultProposalProcessor) TestCollectOperationsInvalidError() {
	point := base.RawPoint(33, 44)

	ophs, ops, _ := t.prepareOperations(point.Height()-1, 4, nil)

	pr := t.newproposal(NewProposalFact(point, t.Local.Address(), ophs))

	previous := base.NewDummyManifest(point.Height()-1, valuehash.RandomSHA256())
	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	writer := NewDummyBlockDataWriter(point.Height())
	writer.manifest = manifest

	opp := NewDefaultProposalProcessor(pr, previous, writer, nil, func(_ context.Context, facthash util.Hash) (base.Operation, error) {
		op, found := ops[facthash.String()]
		if !found {
			return nil, nil
		}

		switch {
		case facthash.Equal(ophs[1]):
			return nil, OperationAlreadyProcessedInProcessorError.Errorf("already processed in previous")
		case facthash.Equal(ophs[3]):
			return nil, InvalidOperationInProcessorError.Errorf("hehehe")
		}

		return op, nil
	},
		func(hint.Hint) (base.OperationProcessor, bool) { return nil, false },
	)

	m, err := opp.Process(context.Background(), nil)
	t.NoError(err)
	t.NotNil(m)

	t.Equal(4, len(opp.ops))

	for i := range ophs {
		b := opp.ops[i]
		if i == 1 {
			t.Nil(b)

			continue
		}

		if i == 3 {
			j, ok := b.(ReasonProcessedOperation)
			t.True(ok)

			t.Contains("invalid operation", j.Reason().Msg())

			continue
		}

		a := ops[ophs[i].String()]
		t.NotNil(a)
		t.NotNil(b)

		base.EqualSignedFact(t.Assert(), a, b)
	}
}

func (t *testDefaultProposalProcessor) TestPreProcessWithOperationProcessor() {
	point := base.RawPoint(33, 44)

	ophs, ops, sts := t.prepareOperations(point.Height()-1, 4, func(fact DummyOperationFact) base.Operation {
		op, _ := NewDummyOperation(fact, t.Local.Privatekey(), t.Policy.NetworkID()) // NOTE not processable

		return op
	})

	pr := t.newproposal(NewProposalFact(point, t.Local.Address(), ophs))

	previous := base.NewDummyManifest(point.Height()-1, valuehash.RandomSHA256())
	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	writer := NewDummyBlockDataWriter(point.Height())
	writer.manifest = manifest

	opp := NewDefaultProposalProcessor(pr, previous, writer, nil, func(_ context.Context, facthash util.Hash) (base.Operation, error) {
		op, found := ops[facthash.String()]
		if !found {
			return nil, OperationNotFoundInProcessorError.Call()
		}

		return op, nil
	},
		func(ht hint.Hint) (base.OperationProcessor, bool) {
			if !ht.IsCompatible(DummyOperationHint) {
				return nil, false
			}

			return &DummyOperationProcessor{
				preprocess: func(_ context.Context, op base.Operation, _ base.StatePool) (base.OperationProcessReasonError, error) {
					switch h := op.Fact().Hash(); {
					case h.Equal(ophs[1]),
						h.Equal(ophs[3]): // NOTE only will process, index 1 and 3 operation
						return nil, nil
					default:
						return base.NewBaseOperationProcessReasonError("bad"), nil
					}
				},
				process: func(_ context.Context, op base.Operation, _ base.StatePool) ([]base.State, base.OperationProcessReasonError, error) {
					return []base.State{sts[op.Fact().Hash().String()]}, nil, nil
				},
			}, true
		},
	)

	m, err := opp.Process(context.Background(), nil)
	t.NoError(err)
	t.NotNil(m)

	t.Equal(2, writer.sts.Len())

	for i := range ophs {
		h := ophs[i]
		st := sts[h.String()]

		var bst base.State
		writer.sts.Traverse(func(_, v interface{}) bool {
			k := v.(base.State)
			if k.Key() == st.Key() {
				bst = k

				return false
			}

			return true
		})

		if i != 1 && i != 3 {
			t.Nil(bst)

			continue
		}

		t.NotNil(bst)
		_ = bst.(io.Closer).Close()

		bops := bst.Operations()
		t.Equal(1, len(bops))
		t.True(h.Equal(bops[0]))
	}
}

func (t *testDefaultProposalProcessor) TestPreProcess() {
	point := base.RawPoint(33, 44)

	ophs, ops, sts := t.prepareOperations(point.Height()-1, 4, nil)
	for i := range ops {
		op := ops[i].(DummyOperationProcessable)
		op.preprocess = func(context.Context, base.StatePool) (base.OperationProcessReasonError, error) {
			switch {
			case op.Fact().Hash().Equal(ophs[1]),
				op.Fact().Hash().Equal(ophs[3]): // NOTE only will process, index 1 and 3 operation
				return nil, nil
			default:
				return base.NewBaseOperationProcessReasonError("bad"), nil
			}
		}

		ops[i] = op
	}

	pr := t.newproposal(NewProposalFact(point, t.Local.Address(), ophs))

	previous := base.NewDummyManifest(point.Height()-1, valuehash.RandomSHA256())
	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	writer := NewDummyBlockDataWriter(point.Height())
	writer.manifest = manifest

	opp := NewDefaultProposalProcessor(pr, previous, writer, nil, func(_ context.Context, facthash util.Hash) (base.Operation, error) {
		op, found := ops[facthash.String()]
		if !found {
			return nil, OperationNotFoundInProcessorError.Errorf("operation not found")
		}

		if facthash.Equal(ophs[3]) {
			return nil, InvalidOperationInProcessorError.Call()
		}

		return op, nil
	},
		func(hint.Hint) (base.OperationProcessor, bool) { return nil, false },
	)

	m, err := opp.Process(context.Background(), nil)
	t.NoError(err)
	t.NotNil(m)

	t.Equal(1, writer.sts.Len())

	for i := range ophs {
		h := ophs[i]
		st := sts[h.String()]

		var bst base.State
		writer.sts.Traverse(func(_, v interface{}) bool {
			k := v.(base.State)
			if k.Key() == st.Key() {
				bst = k

				return false
			}

			return true
		})

		if i != 1 {
			t.Nil(bst)

			continue
		}

		t.NotNil(bst)

		_ = bst.(io.Closer).Close()

		bops := bst.Operations()
		t.Equal(1, len(bops))
		t.True(h.Equal(bops[0]))
	}
}

func (t *testDefaultProposalProcessor) TestPreProcessButError() {
	point := base.RawPoint(33, 44)

	ophs, ops, _ := t.prepareOperations(point.Height()-1, 4, nil)

	for i := range ops {
		i := i
		op := ops[i].(DummyOperationProcessable)
		if op.Fact().Hash().Equal(ophs[1]) {
			op.preprocess = func(ctx context.Context, _ base.StatePool) (base.OperationProcessReasonError, error) {
				return nil, errors.Errorf("findme: %q", op.Fact().Hash())
			}
		}

		ops[i] = op
	}

	pr := t.newproposal(NewProposalFact(point, t.Local.Address(), ophs))

	previous := base.NewDummyManifest(point.Height()-1, valuehash.RandomSHA256())
	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	writer := NewDummyBlockDataWriter(point.Height())
	writer.manifest = manifest

	opp := NewDefaultProposalProcessor(pr, previous, writer, nil, func(_ context.Context, facthash util.Hash) (base.Operation, error) {
		op, found := ops[facthash.String()]
		if !found {
			return nil, OperationNotFoundInProcessorError.Call()
		}

		return op, nil
	},
		func(hint.Hint) (base.OperationProcessor, bool) { return nil, false },
	)
	opp.retrylimit = 1
	opp.retryinterval = 1

	m, err := opp.Process(context.Background(), nil)
	t.Error(err)
	t.Nil(m)

	t.Contains(err.Error(), fmt.Sprintf("findme: %q", ophs[1]))
}

func (t *testDefaultProposalProcessor) TestPreProcessButWithOperationReasonError() {
	point := base.RawPoint(33, 44)

	ophs, ops, _ := t.prepareOperations(point.Height()-1, 4, nil)

	for i := range ops {
		i := i
		op := ops[i].(DummyOperationProcessable)
		op.preprocess = func(ctx context.Context, _ base.StatePool) (base.OperationProcessReasonError, error) {
			switch {
			case op.Fact().Hash().Equal(ophs[1]),
				op.Fact().Hash().Equal(ophs[3]):
				return base.NewBaseOperationProcessReasonError("showme, %q", op.Fact().Hash()), nil
			default:
				return nil, nil
			}
		}

		ops[i] = op
	}

	pr := t.newproposal(NewProposalFact(point, t.Local.Address(), ophs))

	previous := base.NewDummyManifest(point.Height()-1, valuehash.RandomSHA256())
	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	writer := NewDummyBlockDataWriter(point.Height())
	writer.manifest = manifest

	opp := NewDefaultProposalProcessor(pr, previous, writer, nil, func(_ context.Context, facthash util.Hash) (base.Operation, error) {
		op, found := ops[facthash.String()]
		if !found {
			return nil, OperationNotFoundInProcessorError.Call()
		}

		return op, nil
	},
		func(hint.Hint) (base.OperationProcessor, bool) { return nil, false },
	)
	opp.retrylimit = 1
	opp.retryinterval = 1

	m, err := opp.Process(context.Background(), nil)
	t.NoError(err)
	t.NotNil(m)

	opstree, err := writer.opstree.Tree()
	t.NoError(err)

	opstree.Traverse(func(n tree.FixedTreeNode) (bool, error) {
		node := n.(base.OperationFixedTreeNode)

		i := node.Index()
		switch {
		case i == 1 || i == 3:
			t.False(node.InState())
			t.NotNil(node.Reason())
			t.Contains(node.Reason().Msg(), "showme")
		default:
			t.True(node.InState())
			t.Nil(node.Reason())
		}

		return true, nil
	})
}

func (t *testDefaultProposalProcessor) TestPreProcessButErrorRetry() {
	point := base.RawPoint(33, 44)

	ophs, ops, _ := t.prepareOperations(point.Height()-1, 4, nil)

	var called int
	for i := range ops {
		op := ops[i].(DummyOperationProcessable)
		if op.Fact().Hash().Equal(ophs[1]) {
			op.preprocess = func(ctx context.Context, _ base.StatePool) (base.OperationProcessReasonError, error) {
				called++
				return nil, errors.Errorf("findme: %q", op.Fact().Hash())
			}
		}

		ops[i] = op
	}

	pr := t.newproposal(NewProposalFact(point, t.Local.Address(), ophs))

	previous := base.NewDummyManifest(point.Height()-1, valuehash.RandomSHA256())
	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	writer := NewDummyBlockDataWriter(point.Height())
	writer.manifest = manifest

	opp := NewDefaultProposalProcessor(pr, previous, writer, nil, func(_ context.Context, facthash util.Hash) (base.Operation, error) {
		op, found := ops[facthash.String()]
		if !found {
			return nil, OperationNotFoundInProcessorError.Call()
		}

		return op, nil
	},
		func(hint.Hint) (base.OperationProcessor, bool) { return nil, false },
	)
	opp.retrylimit = 3
	opp.retryinterval = 1

	m, err := opp.Process(context.Background(), nil)
	t.Error(err)
	t.Nil(m)

	t.Contains(err.Error(), fmt.Sprintf("findme: %q", ophs[1]))
	t.Equal(opp.retrylimit, called)
}

func (t *testDefaultProposalProcessor) TestPreProcessContextCancel() {
	point := base.RawPoint(33, 44)

	ophs, ops, _ := t.prepareOperations(point.Height()-1, 4, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	startprocessedch := make(chan struct{}, 1)
	var startprocessedonece sync.Once

	for i := range ops {
		op := ops[i].(DummyOperationProcessable)
		if op.Fact().Hash().Equal(ophs[1]) {
			op.preprocess = func(ctx context.Context, _ base.StatePool) (base.OperationProcessReasonError, error) {
				select {
				case <-time.After(time.Minute):
				case <-ctx.Done():
					return nil, ctx.Err()
				}

				return nil, nil
			}
		} else {
			op.preprocess = func(ctx context.Context, _ base.StatePool) (base.OperationProcessReasonError, error) {
				defer startprocessedonece.Do(func() {
					startprocessedch <- struct{}{}
				})

				return nil, nil
			}
		}

		ops[i] = op
	}

	pr := t.newproposal(NewProposalFact(point, t.Local.Address(), ophs))

	previous := base.NewDummyManifest(point.Height()-1, valuehash.RandomSHA256())
	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	writer := NewDummyBlockDataWriter(point.Height())
	writer.manifest = manifest

	opp := NewDefaultProposalProcessor(pr, previous, writer, nil, func(_ context.Context, facthash util.Hash) (base.Operation, error) {
		op, found := ops[facthash.String()]
		if !found {
			return nil, OperationNotFoundInProcessorError.Errorf("operation not found")
		}

		return op, nil
	},
		func(hint.Hint) (base.OperationProcessor, bool) { return nil, false },
	)
	opp.retrylimit = 1
	opp.retryinterval = 1

	donech := make(chan error)
	go func() {
		m, err := opp.Process(ctx, nil)
		t.Error(err)
		t.Nil(m)

		donech <- err
	}()

	<-startprocessedch
	<-time.After(time.Millisecond * 100)
	cancel()

	err := <-donech
	t.Error(err)

	t.True(errors.Is(err, context.Canceled))
	t.Contains(err.Error(), "failed to pre process operation")
}

func (t *testDefaultProposalProcessor) TestPreProcessIgnoreNotProcessableOperation() {
	point := base.RawPoint(33, 44)

	ophs, ops, sts := t.prepareOperations(point.Height()-1, 4, nil)
	for i := range ops {
		op := ops[i]
		switch {
		case op.Fact().Hash().Equal(ophs[1]), op.Fact().Hash().Equal(ophs[3]):
			op, _ = NewDummyOperation(op.Fact().(DummyOperationFact), t.Local.Privatekey(), t.Policy.NetworkID())
		}

		ops[i] = op
	}

	pr := t.newproposal(NewProposalFact(point, t.Local.Address(), ophs))

	previous := base.NewDummyManifest(point.Height()-1, valuehash.RandomSHA256())
	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	writer := NewDummyBlockDataWriter(point.Height())
	writer.manifest = manifest

	opp := NewDefaultProposalProcessor(pr, previous, writer, nil, func(_ context.Context, facthash util.Hash) (base.Operation, error) {
		op, found := ops[facthash.String()]
		if !found {
			return nil, OperationNotFoundInProcessorError.Call()
		}

		return op, nil
	},
		func(hint.Hint) (base.OperationProcessor, bool) { return nil, false },
	)

	m, err := opp.Process(context.Background(), nil)
	t.NoError(err)
	t.NotNil(m)

	t.Equal(2, writer.sts.Len())

	for i := range ophs {
		h := ophs[i]
		st := sts[h.String()]

		var bst base.State
		writer.sts.Traverse(func(_, v interface{}) bool {
			k := v.(base.State)
			if k.Key() == st.Key() {
				bst = k

				return false
			}

			return true
		})

		if i == 1 || i == 3 {
			t.Nil(bst)

			continue
		}
		t.NotNil(bst)
		_ = bst.(io.Closer).Close()

		bops := bst.Operations()
		t.Equal(1, len(bops))
		t.True(h.Equal(bops[0]))
	}
}

func (t *testDefaultProposalProcessor) TestProcess() {
	point := base.RawPoint(33, 44)

	ophs, ops, sts := t.prepareOperations(point.Height()-1, 4, nil)
	for i := range ops {
		op := ops[i].(DummyOperationProcessable)
		if !op.Fact().Hash().Equal(ophs[1]) && !op.Fact().Hash().Equal(ophs[3]) {
			// NOTE only will process, index 1 and 3 operation
			op.process = func(ctx context.Context, sp base.StatePool) ([]base.State, base.OperationProcessReasonError, error) {
				return nil, base.NotChangedOperationProcessReasonError, nil
			}
		}

		ops[i] = op
	}

	pr := t.newproposal(NewProposalFact(point, t.Local.Address(), ophs))

	previous := base.NewDummyManifest(point.Height()-1, valuehash.RandomSHA256())
	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	writer := NewDummyBlockDataWriter(point.Height())
	writer.manifest = manifest

	opp := NewDefaultProposalProcessor(pr, previous, writer, nil, func(_ context.Context, facthash util.Hash) (base.Operation, error) {
		op, found := ops[facthash.String()]
		if !found {
			return nil, OperationNotFoundInProcessorError.Call()
		}

		return op, nil
	},
		func(hint.Hint) (base.OperationProcessor, bool) { return nil, false },
	)

	m, err := opp.Process(context.Background(), nil)
	t.NoError(err)
	t.NotNil(m)

	t.Equal(2, writer.sts.Len())

	for i := range ophs {
		h := ophs[i]
		st := sts[h.String()]

		var bst base.State
		writer.sts.Traverse(func(_, v interface{}) bool {
			k := v.(base.State)
			if k.Key() == st.Key() {
				bst = k

				return false
			}

			return true
		})

		if i != 1 && i != 3 {
			t.Nil(bst)

			continue
		}

		t.NotNil(bst)
		_ = bst.(io.Closer).Close()

		bops := bst.Operations()
		t.Equal(1, len(bops))
		t.True(h.Equal(bops[0]))
	}

	opstree, err := writer.opstree.Tree()
	t.NoError(err)
	t.Equal(4, opstree.Len())

	opstree.Traverse(func(n tree.FixedTreeNode) (bool, error) {
		node := n.(base.OperationFixedTreeNode)
		switch {
		case node.Index() == 1, node.Index() == 3:
			t.True(node.InState())
		default:
			t.Contains(base.NotChangedOperationProcessReasonError.Msg(), node.Reason().Msg())
		}

		return true, nil
	})
}

func (t *testDefaultProposalProcessor) TestProcessWithOperationProcessor() {
	point := base.RawPoint(33, 44)

	ophs, ops, sts := t.prepareOperations(point.Height()-1, 4, func(fact DummyOperationFact) base.Operation {
		op, _ := NewDummyOperation(fact, t.Local.Privatekey(), t.Policy.NetworkID()) // NOTE not processable

		return op
	})

	pr := t.newproposal(NewProposalFact(point, t.Local.Address(), ophs))

	previous := base.NewDummyManifest(point.Height()-1, valuehash.RandomSHA256())
	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	writer := NewDummyBlockDataWriter(point.Height())
	writer.manifest = manifest

	opp := NewDefaultProposalProcessor(pr, previous, writer, nil, func(_ context.Context, facthash util.Hash) (base.Operation, error) {
		op, found := ops[facthash.String()]
		if !found {
			return nil, OperationNotFoundInProcessorError.Call()
		}

		return op, nil
	},
		func(ht hint.Hint) (base.OperationProcessor, bool) {
			if !ht.IsCompatible(DummyOperationHint) {
				return nil, false
			}

			return &DummyOperationProcessor{
				preprocess: func(_ context.Context, op base.Operation, _ base.StatePool) (base.OperationProcessReasonError, error) {
					return nil, nil
				},
				process: func(_ context.Context, op base.Operation, _ base.StatePool) ([]base.State, base.OperationProcessReasonError, error) {
					switch h := op.Fact().Hash(); {
					case h.Equal(ophs[1]),
						h.Equal(ophs[3]): // NOTE only will process, index 1 and 3 operation
						return []base.State{sts[op.Fact().Hash().String()]}, nil, nil
					default:
						return nil, base.NotChangedOperationProcessReasonError, nil
					}
				},
			}, true
		},
	)

	m, err := opp.Process(context.Background(), nil)
	t.NoError(err)
	t.NotNil(m)

	t.Equal(2, writer.sts.Len())

	for i := range ophs {
		h := ophs[i]
		st := sts[h.String()]

		var bst base.State
		writer.sts.Traverse(func(_, v interface{}) bool {
			k := v.(base.State)
			if k.Key() == st.Key() {
				bst = k

				return false
			}

			return true
		})

		if i != 1 && i != 3 {
			t.Nil(bst)

			continue
		}

		t.NotNil(bst)
		_ = bst.(io.Closer).Close()

		bops := bst.Operations()
		t.Equal(1, len(bops))
		t.True(h.Equal(bops[0]))
	}
}

func (t *testDefaultProposalProcessor) TestProcessButError() {
	point := base.RawPoint(33, 44)

	ophs, ops, _ := t.prepareOperations(point.Height()-1, 4, nil)
	for i := range ops {
		op := ops[i].(DummyOperationProcessable)

		if op.Fact().Hash().Equal(ophs[1]) {
			op.process = func(ctx context.Context, sp base.StatePool) ([]base.State, base.OperationProcessReasonError, error) {
				return nil, nil, errors.Errorf("findme: %q", op.Fact().Hash())
			}
		}

		ops[i] = op
	}

	pr := t.newproposal(NewProposalFact(point, t.Local.Address(), ophs))

	previous := base.NewDummyManifest(point.Height()-1, valuehash.RandomSHA256())
	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	writer := NewDummyBlockDataWriter(point.Height())
	writer.manifest = manifest

	opp := NewDefaultProposalProcessor(pr, previous, writer, nil, func(_ context.Context, facthash util.Hash) (base.Operation, error) {
		op, found := ops[facthash.String()]
		if !found {
			return nil, nil
		}

		return op, nil
	},
		func(hint.Hint) (base.OperationProcessor, bool) { return nil, false },
	)
	opp.retrylimit = 1
	opp.retryinterval = 1

	m, err := opp.Process(context.Background(), nil)
	t.Error(err)
	t.Nil(m)

	t.Contains(err.Error(), fmt.Sprintf("findme: %q", ophs[1]))
}

func (t *testDefaultProposalProcessor) TestProcessButErrorRetry() {
	point := base.RawPoint(33, 44)

	var called int
	ophs, ops, _ := t.prepareOperations(point.Height()-1, 4, nil)
	for i := range ops {
		op := ops[i].(DummyOperationProcessable)

		if op.Fact().Hash().Equal(ophs[1]) {
			op.process = func(ctx context.Context, sp base.StatePool) ([]base.State, base.OperationProcessReasonError, error) {
				called++
				return nil, nil, errors.Errorf("findme: %q", op.Fact().Hash())
			}
		}

		ops[i] = op
	}

	pr := t.newproposal(NewProposalFact(point, t.Local.Address(), ophs))

	previous := base.NewDummyManifest(point.Height()-1, valuehash.RandomSHA256())
	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	writer := NewDummyBlockDataWriter(point.Height())
	writer.manifest = manifest

	opp := NewDefaultProposalProcessor(pr, previous, writer, nil, func(_ context.Context, facthash util.Hash) (base.Operation, error) {
		op, found := ops[facthash.String()]
		if !found {
			return nil, OperationNotFoundInProcessorError.Call()
		}

		return op, nil
	},
		func(hint.Hint) (base.OperationProcessor, bool) { return nil, false },
	)
	opp.retrylimit = 3
	opp.retryinterval = 1

	m, err := opp.Process(context.Background(), nil)
	t.Error(err)
	t.Nil(m)

	t.Contains(err.Error(), "failed to process operation")
	t.Equal(opp.retrylimit, called)
}

func (t *testDefaultProposalProcessor) TestProcessButSetStatesErrorRetry() {
	point := base.RawPoint(33, 44)

	ophs, ops, _ := t.prepareOperations(point.Height()-1, 4, nil)

	pr := t.newproposal(NewProposalFact(point, t.Local.Address(), ophs))

	previous := base.NewDummyManifest(point.Height()-1, valuehash.RandomSHA256())
	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	writer := NewDummyBlockDataWriter(point.Height())
	writer.manifest = manifest

	var called int
	writer.setstatesf = func(ctx context.Context, index int, sts []base.State, op base.Operation) error {
		if index == 2 {
			called++
			return errors.Errorf("findme: %q", op.Fact().Hash())
		}

		return writer.setStates(ctx, index, sts, op)
	}

	opp := NewDefaultProposalProcessor(pr, previous, writer, nil, func(_ context.Context, facthash util.Hash) (base.Operation, error) {
		op, found := ops[facthash.String()]
		if !found {
			return nil, OperationNotFoundInProcessorError.Call()
		}

		return op, nil
	},
		func(hint.Hint) (base.OperationProcessor, bool) { return nil, false },
	)
	opp.retrylimit = 2
	opp.retryinterval = 1

	m, err := opp.Process(context.Background(), nil)
	t.Error(err)
	t.Nil(m)

	t.Contains(err.Error(), "failed to process operation")
	t.Equal(opp.retrylimit, called)
}

func (t *testDefaultProposalProcessor) TestProcessContextCancel() {
	point := base.RawPoint(33, 44)

	ophs, ops, _ := t.prepareOperations(point.Height()-1, 4, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	startprocessedch := make(chan struct{}, 1)
	var startprocessedonece sync.Once
	for i := range ops {
		op := ops[i].(DummyOperationProcessable)
		orig := op.process
		if op.Fact().Hash().Equal(ophs[1]) { // NOTE only will process, index 1 and 3 operation
			op.process = func(ctx context.Context, sp base.StatePool) ([]base.State, base.OperationProcessReasonError, error) {
				select {
				case <-time.After(time.Minute):
					return orig(ctx, sp)
				case <-ctx.Done():
					return nil, nil, ctx.Err()
				}
			}
		} else {
			op.process = func(ctx context.Context, sp base.StatePool) ([]base.State, base.OperationProcessReasonError, error) {
				defer startprocessedonece.Do(func() {
					startprocessedch <- struct{}{}
				})

				return orig(ctx, sp)
			}
		}

		ops[i] = op
	}

	pr := t.newproposal(NewProposalFact(point, t.Local.Address(), ophs))

	previous := base.NewDummyManifest(point.Height()-1, valuehash.RandomSHA256())
	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	writer := NewDummyBlockDataWriter(point.Height())
	writer.manifest = manifest

	opp := NewDefaultProposalProcessor(pr, previous, writer, nil, func(_ context.Context, facthash util.Hash) (base.Operation, error) {
		op, found := ops[facthash.String()]
		if !found {
			return nil, OperationNotFoundInProcessorError.Call()
		}

		return op, nil
	},
		func(hint.Hint) (base.OperationProcessor, bool) { return nil, false },
	)
	opp.retrylimit = 1
	opp.retryinterval = 1

	donech := make(chan error)
	go func() {
		m, err := opp.Process(ctx, nil)
		t.Error(err)
		t.Nil(m)

		donech <- err
	}()

	<-startprocessedch
	<-time.After(time.Millisecond * 100)
	cancel()

	err := <-donech
	t.Error(err)

	t.True(errors.Is(err, context.Canceled))
	t.Contains(err.Error(), "failed to process operation")
}

func (t *testDefaultProposalProcessor) TestProcessCancel() {
	point := base.RawPoint(33, 44)

	ophs, ops, _ := t.prepareOperations(point.Height()-1, 4, nil)

	startprocessedch := make(chan struct{}, 1)
	var startprocessedonece sync.Once

	for i := range ops {
		op := ops[i].(DummyOperationProcessable)
		orig := op.process
		if op.Fact().Hash().Equal(ophs[1]) { // NOTE only will process, index 1 and 3 operation
			op.process = func(ctx context.Context, sp base.StatePool) ([]base.State, base.OperationProcessReasonError, error) {
				select {
				case <-time.After(time.Minute):
					return orig(ctx, sp)
				case <-ctx.Done():
					return nil, nil, ctx.Err()
				}
			}
		} else {
			op.process = func(ctx context.Context, sp base.StatePool) ([]base.State, base.OperationProcessReasonError, error) {
				defer startprocessedonece.Do(func() {
					startprocessedch <- struct{}{}
				})

				return orig(ctx, sp)
			}
		}

		ops[i] = op
	}

	pr := t.newproposal(NewProposalFact(point, t.Local.Address(), ophs))

	previous := base.NewDummyManifest(point.Height()-1, valuehash.RandomSHA256())
	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	writer := NewDummyBlockDataWriter(point.Height())
	writer.manifest = manifest

	opp := NewDefaultProposalProcessor(pr, previous, writer, nil, func(_ context.Context, facthash util.Hash) (base.Operation, error) {
		op, found := ops[facthash.String()]
		if !found {
			return nil, OperationNotFoundInProcessorError.Call()
		}

		return op, nil
	},
		func(hint.Hint) (base.OperationProcessor, bool) { return nil, false },
	)
	opp.retrylimit = 1
	opp.retryinterval = 1

	donech := make(chan error)
	go func() {
		m, err := opp.Process(context.Background(), nil)
		t.Error(err)
		t.Nil(m)

		donech <- err
	}()

	<-startprocessedch
	<-time.After(time.Millisecond * 100)
	opp.Cancel()

	err := <-donech
	t.Error(err)

	t.True(errors.Is(err, context.Canceled))
	t.Contains(err.Error(), "failed to process operation")
}

func (t *testDefaultProposalProcessor) TestSave() {
	point := base.RawPoint(33, 44)

	ophs, ops, _ := t.prepareOperations(point.Height()-1, 4, nil)

	pr := t.newproposal(NewProposalFact(point, t.Local.Address(), ophs))

	previous := base.NewDummyManifest(point.Height()-1, valuehash.RandomSHA256())
	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	writer := NewDummyBlockDataWriter(point.Height())
	writer.manifest = manifest

	savech := make(chan struct{}, 1)
	writer.savef = func(_ context.Context) error {
		savech <- struct{}{}

		return nil
	}

	opp := NewDefaultProposalProcessor(pr, previous, writer, nil, func(_ context.Context, facthash util.Hash) (base.Operation, error) {
		return ops[facthash.String()], nil
	},
		func(hint.Hint) (base.OperationProcessor, bool) { return nil, false },
	)

	m, err := opp.Process(context.Background(), nil)
	t.NoError(err)
	t.NotNil(m)

	t.Equal(4, writer.sts.Len())

	afact := t.NewACCEPTBallotFact(point.Next(), nil, nil)
	avp, err := t.NewACCEPTVoteproof(afact, t.Local, []LocalNode{t.Local})
	t.NoError(err)

	t.NoError(opp.Save(context.Background(), avp))

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("failed to wait to save"))
	case <-savech:
	}
}

func (t *testDefaultProposalProcessor) TestSaveFailed() {
	point := base.RawPoint(33, 44)

	ophs, ops, _ := t.prepareOperations(point.Height()-1, 4, nil)

	pr := t.newproposal(NewProposalFact(point, t.Local.Address(), ophs))

	previous := base.NewDummyManifest(point.Height()-1, valuehash.RandomSHA256())
	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	writer := NewDummyBlockDataWriter(point.Height())
	writer.manifest = manifest

	writer.savef = func(_ context.Context) error {
		return errors.Errorf("killme")
	}

	opp := NewDefaultProposalProcessor(pr, previous, writer, nil, func(_ context.Context, facthash util.Hash) (base.Operation, error) {
		return ops[facthash.String()], nil
	},
		func(hint.Hint) (base.OperationProcessor, bool) { return nil, false },
	)

	m, err := opp.Process(context.Background(), nil)
	t.NoError(err)
	t.NotNil(m)

	t.Equal(4, writer.sts.Len())

	afact := t.NewACCEPTBallotFact(point.Next(), nil, nil)
	avp, err := t.NewACCEPTVoteproof(afact, t.Local, []LocalNode{t.Local})
	t.NoError(err)

	err = opp.Save(context.Background(), avp)
	t.Error(err)
	t.Contains(err.Error(), "killme")
}

func TestDefaultProposalProcessor(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testDefaultProposalProcessor))
}
