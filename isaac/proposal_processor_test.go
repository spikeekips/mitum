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
	"github.com/spikeekips/mitum/util/fixedtree"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type DummyBlockWriter struct {
	sync.RWMutex
	getStateFunc base.GetStateFunc
	proposal     base.ProposalSignFact
	manifest     base.Manifest
	manifesterr  error
	opstreeg     *fixedtree.Writer
	ops          []base.Operation
	sts          *util.LockedMap
	setstatesf   func(context.Context, uint64, []base.StateMergeValue, base.Operation) error
	savef        func(context.Context) (base.BlockMap, error)
}

func NewDummyBlockWriter(proposal base.ProposalSignFact, getStateFunc base.GetStateFunc) *DummyBlockWriter {
	return &DummyBlockWriter{
		proposal:     proposal,
		getStateFunc: getStateFunc,
		sts:          util.NewLockedMap(),
	}
}

func (w *DummyBlockWriter) SetOperationsSize(n uint64) {
	w.ops = nil
	w.opstreeg, _ = fixedtree.NewWriter(base.OperationFixedtreeHint, n)
}

func (w *DummyBlockWriter) SetProcessResult(ctx context.Context, index uint64, _, facthash util.Hash, instate bool, errorreason base.OperationProcessReasonError) error {
	var msg string
	if errorreason != nil {
		msg = errorreason.Msg()
	}

	var node base.OperationFixedtreeNode
	if instate {
		node = base.NewInStateOperationFixedtreeNode(facthash, msg)
	} else {
		node = base.NewNotInStateOperationFixedtreeNode(facthash, msg)
	}

	if err := w.opstreeg.Add(index, node); err != nil {
		return errors.Wrap(err, "failed to set operation")
	}

	return nil
}

func (w *DummyBlockWriter) SetStates(ctx context.Context, index uint64, states []base.StateMergeValue, operation base.Operation) error {
	if w.setstatesf != nil {
		return w.setstatesf(ctx, index, states, operation)
	}

	return w.setStates(ctx, index, states, operation)
}

func (w *DummyBlockWriter) setStates(ctx context.Context, index uint64, states []base.StateMergeValue, op base.Operation) error {
	w.Lock()
	defer w.Unlock()

	e := util.StringErrorFunc("failed to set states")

	for i := range states {
		stv := states[i]

		j, _, _ := w.sts.Get(stv.Key(), func() (interface{}, error) {
			var st base.State
			switch j, found, err := w.getStateFunc(stv.Key()); {
			case err != nil:
				return nil, err
			case found:
				st = j
			}

			return stv.Merger(w.proposal.Point().Height(), st), nil
		})

		if err := j.(base.StateValueMerger).Merge(stv, []util.Hash{op.Fact().Hash()}); err != nil {
			return e(err, "failed to merge")
		}
	}

	w.ops = append(w.ops, op)

	return nil
}

func (w *DummyBlockWriter) SetINITVoteproof(ctx context.Context, vp base.INITVoteproof) error {
	return nil
}

func (w *DummyBlockWriter) SetACCEPTVoteproof(ctx context.Context, vp base.ACCEPTVoteproof) error {
	return nil
}

func (w *DummyBlockWriter) Manifest(context.Context, base.Manifest) (base.Manifest, error) {
	return w.manifest, w.manifesterr
}

func (w *DummyBlockWriter) Save(ctx context.Context) (base.BlockMap, error) {
	if w.savef == nil {
		return nil, nil
	}

	return w.savef(ctx)
}

func (w *DummyBlockWriter) Cancel() error {
	return nil
}

type DummyOperationProcessor struct {
	preprocess func(context.Context, base.Operation, base.GetStateFunc) (context.Context, base.OperationProcessReasonError, error)
	process    func(context.Context, base.Operation, base.GetStateFunc) ([]base.StateMergeValue, base.OperationProcessReasonError, error)
}

func (*DummyOperationProcessor) Close() error {
	return nil
}

func (p *DummyOperationProcessor) PreProcess(ctx context.Context, op base.Operation, getStateFunc base.GetStateFunc) (context.Context, base.OperationProcessReasonError, error) {
	if p.preprocess == nil {
		return ctx, base.NewBaseOperationProcessReasonError("nil preprocess"), nil
	}

	return p.preprocess(ctx, op, getStateFunc)
}

func (p *DummyOperationProcessor) Process(ctx context.Context, op base.Operation, getStateFunc base.GetStateFunc) ([]base.StateMergeValue, base.OperationProcessReasonError, error) {
	if p.process == nil {
		return nil, nil, nil
	}

	return p.process(ctx, op, getStateFunc)
}

type testDefaultProposalProcessor struct {
	BaseTestBallots
}

func (t *testDefaultProposalProcessor) newproposal(fact ProposalFact) base.ProposalSignFact {
	fs := NewProposalSignFact(fact)
	_ = fs.Sign(t.Local.Privatekey(), t.LocalParams.NetworkID())

	return fs
}

func (t *testDefaultProposalProcessor) newStateMergeValue(key string) base.StateMergeValue {
	v := base.NewDummyStateValue(util.UUID().String())

	return base.NewBaseStateMergeValue(key, v, nil)
}

func (t *testDefaultProposalProcessor) prepareOperations(height base.Height, n int) (
	[]util.Hash,
	map[string]base.Operation,
	map[string]base.StateMergeValue,
) {
	ophs := make([]util.Hash, n)
	ops := map[string]base.Operation{}
	sts := map[string]base.StateMergeValue{}

	for i := range ophs {
		fact := NewDummyOperationFact(util.UUID().Bytes(), valuehash.RandomSHA256())
		op, _ := NewDummyOperation(fact, t.Local.Privatekey(), t.LocalParams.NetworkID())

		ophs[i] = fact.Hash()
		st := t.newStateMergeValue(fact.Hash().String())

		op.preprocess = func(ctx context.Context, _ base.GetStateFunc) (context.Context, base.OperationProcessReasonError, error) {
			return ctx, nil, nil
		}
		op.process = func(context.Context, base.GetStateFunc) ([]base.StateMergeValue, base.OperationProcessReasonError, error) {
			return []base.StateMergeValue{st}, nil, nil
		}

		ops[fact.Hash().String()] = op
		sts[fact.Hash().String()] = st
	}

	return ophs, ops, sts
}

func (t *testDefaultProposalProcessor) newBlockWriter() (
	*DummyBlockWriter,
	NewBlockWriterFunc,
) {
	writer := NewDummyBlockWriter(nil, base.NilGetState)

	return writer, func(proposal base.ProposalSignFact, getStateFunc base.GetStateFunc) (BlockWriter, error) {
		writer.proposal = proposal

		if getStateFunc != nil {
			writer.getStateFunc = getStateFunc
		}

		return writer, nil
	}
}

func (t *testDefaultProposalProcessor) TestNew() {
	point := base.RawPoint(33, 44)

	pr := t.newproposal(NewProposalFact(point, t.Local.Address(), []util.Hash{valuehash.RandomSHA256()}))

	previous := base.NewDummyManifest(point.Height()-1, valuehash.RandomSHA256())
	_, newwriterf := t.newBlockWriter()
	opp, _ := NewDefaultProposalProcessor(pr, previous, newwriterf, nil, nil, nil)
	_ = (interface{})(opp).(ProposalProcessor)

	base.EqualProposalSignFact(t.Assert(), pr, opp.Proposal())
}

func (t *testDefaultProposalProcessor) TestCollectOperations() {
	point := base.RawPoint(33, 44)

	ophs, ops, _ := t.prepareOperations(point.Height()-1, 4)

	pr := t.newproposal(NewProposalFact(point, t.Local.Address(), ophs))

	previous := base.NewDummyManifest(point.Height()-1, valuehash.RandomSHA256())
	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	writer, newwriterf := t.newBlockWriter()
	writer.manifest = manifest

	var copslock sync.Mutex
	var cops []base.Operation

	opp, _ := NewDefaultProposalProcessor(pr, previous, newwriterf, nil, func(_ context.Context, facthash util.Hash) (base.Operation, error) {
		op, found := ops[facthash.String()]
		if !found {
			return nil, ErrOperationNotFoundInProcessor.Errorf("operation not found")
		}

		copslock.Lock()
		cops = append(cops, op)
		copslock.Unlock()

		return op, nil
	},
		func(base.Height, hint.Hint) (base.OperationProcessor, error) { return nil, nil },
	)

	m, err := opp.Process(context.Background(), nil)
	t.NoError(err)
	t.NotNil(m)

	t.Equal(len(ophs), len(cops))

	for i := range ophs {
		a := ops[ophs[i].String()]

		ci := util.InSliceFunc(cops, func(_ interface{}, j int) bool {
			return cops[j].Fact().Hash().Equal(ophs[i])
		})

		b := cops[ci]

		t.NotNil(a)
		t.NotNil(b)

		base.EqualSignFact(t.Assert(), a, b)
	}
}

func (t *testDefaultProposalProcessor) TestCollectOperationsFailed() {
	point := base.RawPoint(33, 44)

	ophs, ops, _ := t.prepareOperations(point.Height()-1, 4)

	pr := t.newproposal(NewProposalFact(point, t.Local.Address(), ophs))

	previous := base.NewDummyManifest(point.Height()-1, valuehash.RandomSHA256())
	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	writer, newwriterf := t.newBlockWriter()
	writer.manifest = manifest

	opp, _ := NewDefaultProposalProcessor(pr, previous, newwriterf, nil, func(_ context.Context, facthash util.Hash) (base.Operation, error) {
		op, found := ops[facthash.String()]
		if !found {
			return nil, ErrOperationNotFoundInProcessor.Errorf("operation not found")
		}

		switch {
		case facthash.Equal(ophs[1]), facthash.Equal(ophs[3]):
		default:
			return nil, util.ErrWrongType.Errorf("operation not found")
		}

		return op, nil
	},
		nil,
	)
	opp.retrylimit = 1
	opp.retryinterval = 1

	m, err := opp.Process(context.Background(), nil)
	t.Error(err)
	t.Nil(m)

	switch {
	case errors.Is(err, context.Canceled):
	case errors.Is(err, util.ErrWrongType):
		t.ErrorContains(err, "failed to collect operations")
	default:
		t.Error(errors.Errorf("unexpected error, %T", err))
	}
}

func (t *testDefaultProposalProcessor) TestCollectOperationsFailedButIgnored() {
	point := base.RawPoint(33, 44)

	ophs, ops, _ := t.prepareOperations(point.Height()-1, 4)

	pr := t.newproposal(NewProposalFact(point, t.Local.Address(), ophs))

	previous := base.NewDummyManifest(point.Height()-1, valuehash.RandomSHA256())
	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	writer, newwriterf := t.newBlockWriter()
	writer.manifest = manifest

	var copslock sync.Mutex
	var cops []base.Operation

	opp, _ := NewDefaultProposalProcessor(pr, previous, newwriterf, nil, func(_ context.Context, facthash util.Hash) (base.Operation, error) {
		op, found := ops[facthash.String()]
		if !found {
			return nil, nil
		}

		switch {
		case facthash.Equal(ophs[1]):
			return nil, ErrInvalidOperationInProcessor.Call()
		case facthash.Equal(ophs[2]):
			return nil, util.ErrInvalid.Call()
		case facthash.Equal(ophs[3]):
			return nil, ErrOperationNotFoundInProcessor.Call()
		}

		copslock.Lock()
		cops = append(cops, op)
		copslock.Unlock()

		return op, nil
	},
		func(base.Height, hint.Hint) (base.OperationProcessor, error) { return nil, nil },
	)

	m, err := opp.Process(context.Background(), nil)
	t.NoError(err)
	t.NotNil(m)

	for i := range ophs {
		if i == 1 || i == 2 {
			continue
		}

		ci := util.InSliceFunc(cops, func(_ interface{}, j int) bool {
			return cops[j].Fact().Hash().Equal(ophs[i])
		})

		if i == 3 {
			t.Equal(-1, ci)

			continue
		}

		b := cops[ci]

		a := ops[ophs[i].String()]
		t.NotNil(a)
		t.NotNil(b)

		base.EqualSignFact(t.Assert(), a, b)
	}
}

func (t *testDefaultProposalProcessor) TestCollectOperationsInvalidError() {
	point := base.RawPoint(33, 44)

	ophs, ops, _ := t.prepareOperations(point.Height()-1, 4)

	pr := t.newproposal(NewProposalFact(point, t.Local.Address(), ophs))

	previous := base.NewDummyManifest(point.Height()-1, valuehash.RandomSHA256())
	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	writer, newwriterf := t.newBlockWriter()
	writer.manifest = manifest

	var copslock sync.Mutex
	var cops []base.Operation

	opp, _ := NewDefaultProposalProcessor(pr, previous, newwriterf, nil, func(_ context.Context, facthash util.Hash) (base.Operation, error) {
		op, found := ops[facthash.String()]
		if !found {
			return nil, nil
		}

		switch {
		case facthash.Equal(ophs[1]):
			return nil, ErrOperationAlreadyProcessedInProcessor.Errorf("already processed in previous")
		case facthash.Equal(ophs[3]):
			return nil, ErrInvalidOperationInProcessor.Errorf("hehehe")
		}

		copslock.Lock()
		cops = append(cops, op)
		copslock.Unlock()

		return op, nil
	},
		func(base.Height, hint.Hint) (base.OperationProcessor, error) { return nil, nil },
	)

	m, err := opp.Process(context.Background(), nil)
	t.NoError(err)
	t.NotNil(m)

	for i := range ophs {
		ci := util.InSliceFunc(cops, func(_ interface{}, j int) bool {
			return cops[j].Fact().Hash().Equal(ophs[i])
		})

		if i == 1 {
			t.Equal(-1, ci)

			continue
		}

		if i == 3 {
			continue
		}

		b := cops[ci]

		a := ops[ophs[i].String()]
		t.NotNil(a)
		t.NotNil(b)

		base.EqualSignFact(t.Assert(), a, b)
	}
}

func (t *testDefaultProposalProcessor) TestPreProcessButFailedToGetOperationProcessor() {
	point := base.RawPoint(33, 44)

	ophs, ops, _ := t.prepareOperations(point.Height()-1, 4)

	pr := t.newproposal(NewProposalFact(point, t.Local.Address(), ophs))
	previous := base.NewDummyManifest(point.Height()-1, valuehash.RandomSHA256())
	_, newwriterf := t.newBlockWriter()

	opp, _ := NewDefaultProposalProcessor(pr, previous, newwriterf, nil, func(_ context.Context, facthash util.Hash) (base.Operation, error) {
		op, found := ops[facthash.String()]
		if !found {
			return nil, ErrOperationNotFoundInProcessor.Call()
		}

		return op, nil
	},
		func(_ base.Height, ht hint.Hint) (base.OperationProcessor, error) {
			if !ht.IsCompatible(DummyOperationHint) {
				return nil, nil
			}

			return nil, errors.Errorf("hehehe")
		},
	)
	opp.retrylimit = 3
	opp.retryinterval = 1

	_, err := opp.Process(context.Background(), nil)
	t.Error(err)
	t.ErrorContains(err, "hehehe")
}

func (t *testDefaultProposalProcessor) TestPreProcessButErrSuspendOperation() {
	point := base.RawPoint(33, 44)

	ophs, ops, sts := t.prepareOperations(point.Height()-1, 4)

	pr := t.newproposal(NewProposalFact(point, t.Local.Address(), ophs))

	previous := base.NewDummyManifest(point.Height()-1, valuehash.RandomSHA256())
	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	writer, newwriterf := t.newBlockWriter()
	writer.manifest = manifest

	suspended := ophs[1]

	opp, _ := NewDefaultProposalProcessor(pr, previous, newwriterf, nil, func(_ context.Context, facthash util.Hash) (base.Operation, error) {
		op, found := ops[facthash.String()]
		if !found {
			return nil, ErrOperationNotFoundInProcessor.Call()
		}

		return op, nil
	},
		func(_ base.Height, ht hint.Hint) (base.OperationProcessor, error) {
			if !ht.IsCompatible(DummyOperationHint) {
				return nil, nil
			}

			return &DummyOperationProcessor{
				preprocess: func(ctx context.Context, op base.Operation, _ base.GetStateFunc) (context.Context, base.OperationProcessReasonError, error) {
					if op.Fact().Hash().Equal(suspended) {
						return ctx, nil, ErrSuspendOperation.Errorf("hohoho")
					}

					return ctx, nil, nil
				},
				process: func(_ context.Context, op base.Operation, _ base.GetStateFunc) ([]base.StateMergeValue, base.OperationProcessReasonError, error) {
					return []base.StateMergeValue{sts[op.Fact().Hash().String()]}, nil, nil
				},
			}, nil
		},
	)

	m, err := opp.Process(context.Background(), nil)
	t.NoError(err)
	t.NotNil(m)

	t.Equal(len(ophs), writer.opstreeg.Len())
	t.Equal(3, len(writer.ops))
	t.Equal(3, writer.sts.Len())

	t.NoError(writer.opstreeg.Write(func(i uint64, node fixedtree.Node) error {
		b, err := util.MarshalJSON(node)
		if err != nil {
			return err
		}

		t.T().Log("operation tree node:", i, string(b))

		return nil
	}))
	t.Equal(3, writer.opstreeg.Len())
}

func (t *testDefaultProposalProcessor) TestPreProcessWithOperationProcessor() {
	point := base.RawPoint(33, 44)

	ophs, ops, sts := t.prepareOperations(point.Height()-1, 4)

	pr := t.newproposal(NewProposalFact(point, t.Local.Address(), ophs))

	previous := base.NewDummyManifest(point.Height()-1, valuehash.RandomSHA256())
	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	writer, newwriterf := t.newBlockWriter()
	writer.manifest = manifest

	opp, _ := NewDefaultProposalProcessor(pr, previous, newwriterf, nil, func(_ context.Context, facthash util.Hash) (base.Operation, error) {
		op, found := ops[facthash.String()]
		if !found {
			return nil, ErrOperationNotFoundInProcessor.Call()
		}

		return op, nil
	},
		func(_ base.Height, ht hint.Hint) (base.OperationProcessor, error) {
			if !ht.IsCompatible(DummyOperationHint) {
				return nil, nil
			}

			return &DummyOperationProcessor{
				preprocess: func(ctx context.Context, op base.Operation, _ base.GetStateFunc) (context.Context, base.OperationProcessReasonError, error) {
					switch h := op.Fact().Hash(); {
					case h.Equal(ophs[1]),
						h.Equal(ophs[3]): // NOTE only will process, index 1 and 3 operation
						return ctx, nil, nil
					default:
						return ctx, base.NewBaseOperationProcessReasonError("bad"), nil
					}
				},
				process: func(_ context.Context, op base.Operation, _ base.GetStateFunc) ([]base.StateMergeValue, base.OperationProcessReasonError, error) {
					return []base.StateMergeValue{sts[op.Fact().Hash().String()]}, nil, nil
				},
			}, nil
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

	ophs, ops, sts := t.prepareOperations(point.Height()-1, 4)
	for i := range ops {
		op := ops[i].(DummyOperation)
		op.preprocess = func(ctx context.Context, _ base.GetStateFunc) (context.Context, base.OperationProcessReasonError, error) {
			switch {
			case op.Fact().Hash().Equal(ophs[1]),
				op.Fact().Hash().Equal(ophs[3]): // NOTE only will process, index 1 and 3 operation
				return ctx, nil, nil
			default:
				return ctx, base.NewBaseOperationProcessReasonError("bad"), nil
			}
		}

		ops[i] = op
	}

	pr := t.newproposal(NewProposalFact(point, t.Local.Address(), ophs))

	previous := base.NewDummyManifest(point.Height()-1, valuehash.RandomSHA256())
	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	writer, newwriterf := t.newBlockWriter()
	writer.manifest = manifest

	opp, _ := NewDefaultProposalProcessor(pr, previous, newwriterf, nil, func(_ context.Context, facthash util.Hash) (base.Operation, error) {
		op, found := ops[facthash.String()]
		if !found {
			return nil, ErrOperationNotFoundInProcessor.Errorf("operation not found")
		}

		if facthash.Equal(ophs[3]) {
			return nil, ErrInvalidOperationInProcessor.Call()
		}

		return op, nil
	},
		func(base.Height, hint.Hint) (base.OperationProcessor, error) { return nil, nil },
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

	ophs, ops, _ := t.prepareOperations(point.Height()-1, 4)

	for i := range ops {
		i := i
		op := ops[i].(DummyOperation)
		if op.Fact().Hash().Equal(ophs[1]) {
			op.preprocess = func(ctx context.Context, _ base.GetStateFunc) (context.Context, base.OperationProcessReasonError, error) {
				return ctx, nil, errors.Errorf("findme: %q", op.Fact().Hash())
			}
		}

		ops[i] = op
	}

	pr := t.newproposal(NewProposalFact(point, t.Local.Address(), ophs))

	previous := base.NewDummyManifest(point.Height()-1, valuehash.RandomSHA256())
	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	writer, newwriterf := t.newBlockWriter()
	writer.manifest = manifest

	opp, _ := NewDefaultProposalProcessor(pr, previous, newwriterf, nil, func(_ context.Context, facthash util.Hash) (base.Operation, error) {
		op, found := ops[facthash.String()]
		if !found {
			return nil, ErrOperationNotFoundInProcessor.Call()
		}

		return op, nil
	},
		func(base.Height, hint.Hint) (base.OperationProcessor, error) { return nil, nil },
	)
	opp.retrylimit = 1
	opp.retryinterval = 1

	m, err := opp.Process(context.Background(), nil)
	t.Error(err)
	t.Nil(m)

	t.ErrorContains(err, fmt.Sprintf("findme: %q", ophs[1]))
}

func (t *testDefaultProposalProcessor) TestPreProcessButWithOperationReasonError() {
	point := base.RawPoint(33, 44)

	ophs, ops, _ := t.prepareOperations(point.Height()-1, 4)

	for i := range ops {
		i := i
		op := ops[i].(DummyOperation)
		op.preprocess = func(ctx context.Context, _ base.GetStateFunc) (context.Context, base.OperationProcessReasonError, error) {
			switch {
			case op.Fact().Hash().Equal(ophs[1]),
				op.Fact().Hash().Equal(ophs[3]):
				return ctx, base.NewBaseOperationProcessReasonError("showme, %q", op.Fact().Hash()), nil
			default:
				return ctx, nil, nil
			}
		}

		ops[i] = op
	}

	pr := t.newproposal(NewProposalFact(point, t.Local.Address(), ophs))

	previous := base.NewDummyManifest(point.Height()-1, valuehash.RandomSHA256())
	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	writer, newwriterf := t.newBlockWriter()
	writer.manifest = manifest

	opp, _ := NewDefaultProposalProcessor(pr, previous, newwriterf, nil, func(_ context.Context, facthash util.Hash) (base.Operation, error) {
		op, found := ops[facthash.String()]
		if !found {
			return nil, ErrOperationNotFoundInProcessor.Call()
		}

		return op, nil
	},
		func(base.Height, hint.Hint) (base.OperationProcessor, error) { return nil, nil },
	)
	opp.retrylimit = 1
	opp.retryinterval = 1

	m, err := opp.Process(context.Background(), nil)
	t.NoError(err)
	t.NotNil(m)

	writer.opstreeg.Traverse(func(index uint64, n fixedtree.Node) (bool, error) {
		node := n.(base.OperationFixedtreeNode)

		switch {
		case index == 1 || index == 3:
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

	ophs, ops, _ := t.prepareOperations(point.Height()-1, 4)

	var called int
	for i := range ops {
		op := ops[i].(DummyOperation)
		if op.Fact().Hash().Equal(ophs[1]) {
			op.preprocess = func(ctx context.Context, _ base.GetStateFunc) (context.Context, base.OperationProcessReasonError, error) {
				called++
				return ctx, nil, errors.Errorf("findme: %q", op.Fact().Hash())
			}
		}

		ops[i] = op
	}

	pr := t.newproposal(NewProposalFact(point, t.Local.Address(), ophs))

	previous := base.NewDummyManifest(point.Height()-1, valuehash.RandomSHA256())
	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	writer, newwriterf := t.newBlockWriter()
	writer.manifest = manifest

	opp, _ := NewDefaultProposalProcessor(pr, previous, newwriterf, nil, func(_ context.Context, facthash util.Hash) (base.Operation, error) {
		op, found := ops[facthash.String()]
		if !found {
			return nil, ErrOperationNotFoundInProcessor.Call()
		}

		return op, nil
	},
		func(base.Height, hint.Hint) (base.OperationProcessor, error) { return nil, nil },
	)
	opp.retrylimit = 3
	opp.retryinterval = 1

	m, err := opp.Process(context.Background(), nil)
	t.Error(err)
	t.Nil(m)

	t.ErrorContains(err, fmt.Sprintf("findme: %q", ophs[1]))
	t.Equal(opp.retrylimit, called)
}

func (t *testDefaultProposalProcessor) TestPreProcessContextCancel() {
	point := base.RawPoint(33, 44)

	ophs, ops, _ := t.prepareOperations(point.Height()-1, 4)

	startprocessedch := make(chan struct{}, 1)
	var startprocessedonece sync.Once

	for i := range ops {
		op := ops[i].(DummyOperation)
		if op.Fact().Hash().Equal(ophs[1]) {
			op.preprocess = func(ctx context.Context, _ base.GetStateFunc) (context.Context, base.OperationProcessReasonError, error) {
				select {
				case <-time.After(time.Minute):
				case <-ctx.Done():
					return ctx, nil, ctx.Err()
				}

				return ctx, nil, nil
			}
		} else {
			op.preprocess = func(ctx context.Context, _ base.GetStateFunc) (context.Context, base.OperationProcessReasonError, error) {
				defer startprocessedonece.Do(func() {
					startprocessedch <- struct{}{}
				})

				return ctx, nil, nil
			}
		}

		ops[i] = op
	}

	pr := t.newproposal(NewProposalFact(point, t.Local.Address(), ophs))

	previous := base.NewDummyManifest(point.Height()-1, valuehash.RandomSHA256())
	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	writer, newwriterf := t.newBlockWriter()
	writer.manifest = manifest

	opp, _ := NewDefaultProposalProcessor(pr, previous, newwriterf, nil, func(_ context.Context, facthash util.Hash) (base.Operation, error) {
		op, found := ops[facthash.String()]
		if !found {
			return nil, ErrOperationNotFoundInProcessor.Errorf("operation not found")
		}

		return op, nil
	},
		func(base.Height, hint.Hint) (base.OperationProcessor, error) { return nil, nil },
	)
	opp.retrylimit = 1
	opp.retryinterval = 1

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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
	t.ErrorContains(err, "failed to pre process operation")
}

func (t *testDefaultProposalProcessor) TestPreProcessWithContext() {
	point := base.RawPoint(33, 44)

	collectedstringkey := util.ContextKey("index")
	var collected int

	ophs, ops, _ := t.prepareOperations(point.Height()-1, 4)
	for i := range ops {
		i := i

		op := ops[i].(DummyOperation)
		op.preprocess = func(ctx context.Context, _ base.GetStateFunc) (context.Context, base.OperationProcessReasonError, error) {
			var index int

			s := ctx.Value(collectedstringkey)
			if s != nil {
				index = s.(int)
			}

			if i == ophs[len(ophs)-1].String() {
				collected = index
			}

			ctx = context.WithValue(ctx, collectedstringkey, index+1)

			return ctx, nil, nil
		}

		ops[i] = op
	}

	pr := t.newproposal(NewProposalFact(point, t.Local.Address(), ophs))

	previous := base.NewDummyManifest(point.Height()-1, valuehash.RandomSHA256())
	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	writer, newwriterf := t.newBlockWriter()
	writer.manifest = manifest

	opp, _ := NewDefaultProposalProcessor(pr, previous, newwriterf, nil, func(_ context.Context, facthash util.Hash) (base.Operation, error) {
		op, found := ops[facthash.String()]
		if !found {
			return nil, ErrOperationNotFoundInProcessor.Errorf("operation not found")
		}

		return op, nil
	},
		func(base.Height, hint.Hint) (base.OperationProcessor, error) { return nil, nil },
	)

	m, err := opp.Process(context.Background(), nil)
	t.NoError(err)
	t.NotNil(m)

	t.Equal(len(ops), writer.sts.Len())

	t.Equal(len(ops)-1, collected)
}

func (t *testDefaultProposalProcessor) TestProcess() {
	point := base.RawPoint(33, 44)

	ophs, ops, sts := t.prepareOperations(point.Height()-1, 4)
	for i := range ops {
		op := ops[i].(DummyOperation)
		if !op.Fact().Hash().Equal(ophs[1]) && !op.Fact().Hash().Equal(ophs[3]) {
			// NOTE only will process, index 1 and 3 operation
			op.process = func(ctx context.Context, getStateFunc base.GetStateFunc) ([]base.StateMergeValue, base.OperationProcessReasonError, error) {
				return nil, base.ErrNotChangedOperationProcessReason, nil
			}
		}

		ops[i] = op
	}

	prevsts := make([]base.State, len(sts))
	prevsts[1] = base.NewBaseState(point.Height()-1, sts[ophs[1].String()].Key(), nil, valuehash.RandomSHA256(), nil)

	pr := t.newproposal(NewProposalFact(point, t.Local.Address(), ophs))

	previous := base.NewDummyManifest(point.Height()-1, valuehash.RandomSHA256())
	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	writer, newwriterf := t.newBlockWriter()
	writer.manifest = manifest
	writer.getStateFunc = func(key string) (base.State, bool, error) {
		switch key {
		case sts[ophs[1].String()].Key():
			return prevsts[1], true, nil
		default:
			return nil, false, nil
		}
	}

	opp, _ := NewDefaultProposalProcessor(pr, previous, newwriterf, nil, func(_ context.Context, facthash util.Hash) (base.Operation, error) {
		op, found := ops[facthash.String()]
		if !found {
			return nil, ErrOperationNotFoundInProcessor.Call()
		}

		return op, nil
	},
		func(base.Height, hint.Hint) (base.OperationProcessor, error) { return nil, nil },
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

		t.Equal(point.Height(), bst.Height())
		t.True(base.IsEqualStateValue(st, bst.Value()))

		bops := bst.Operations()
		t.Equal(1, len(bops))
		t.True(h.Equal(bops[0]))

		if i == 1 {
			t.True(prevsts[1].Hash().Equal(bst.Previous()))
		}
	}

	t.Equal(4, writer.opstreeg.Len())

	writer.opstreeg.Traverse(func(index uint64, n fixedtree.Node) (bool, error) {
		node := n.(base.OperationFixedtreeNode)
		switch {
		case index == 1, index == 3:
			t.True(node.InState())
		default:
			t.Contains(base.ErrNotChangedOperationProcessReason.Msg(), node.Reason().Msg())
		}

		return true, nil
	})
}

func (t *testDefaultProposalProcessor) TestProcessWithOperationProcessor() {
	point := base.RawPoint(33, 44)

	ophs, ops, sts := t.prepareOperations(point.Height()-1, 4)

	pr := t.newproposal(NewProposalFact(point, t.Local.Address(), ophs))

	previous := base.NewDummyManifest(point.Height()-1, valuehash.RandomSHA256())
	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	writer, newwriterf := t.newBlockWriter()
	writer.manifest = manifest

	opp, _ := NewDefaultProposalProcessor(pr, previous, newwriterf, nil, func(_ context.Context, facthash util.Hash) (base.Operation, error) {
		op, found := ops[facthash.String()]
		if !found {
			return nil, ErrOperationNotFoundInProcessor.Call()
		}

		return op, nil
	},
		func(_ base.Height, ht hint.Hint) (base.OperationProcessor, error) {
			if !ht.IsCompatible(DummyOperationHint) {
				return nil, nil
			}

			return &DummyOperationProcessor{
				preprocess: func(ctx context.Context, op base.Operation, _ base.GetStateFunc) (context.Context, base.OperationProcessReasonError, error) {
					return ctx, nil, nil
				},
				process: func(_ context.Context, op base.Operation, _ base.GetStateFunc) ([]base.StateMergeValue, base.OperationProcessReasonError, error) {
					switch h := op.Fact().Hash(); {
					case h.Equal(ophs[1]),
						h.Equal(ophs[3]): // NOTE only will process, index 1 and 3 operation
						return []base.StateMergeValue{sts[op.Fact().Hash().String()]}, nil, nil
					default:
						return nil, base.ErrNotChangedOperationProcessReason, nil
					}
				},
			}, nil
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

	ophs, ops, _ := t.prepareOperations(point.Height()-1, 4)
	for i := range ops {
		op := ops[i].(DummyOperation)

		if op.Fact().Hash().Equal(ophs[1]) {
			op.process = func(ctx context.Context, getStateFunc base.GetStateFunc) ([]base.StateMergeValue, base.OperationProcessReasonError, error) {
				return nil, nil, errors.Errorf("findme: %q", op.Fact().Hash())
			}
		}

		ops[i] = op
	}

	pr := t.newproposal(NewProposalFact(point, t.Local.Address(), ophs))

	previous := base.NewDummyManifest(point.Height()-1, valuehash.RandomSHA256())
	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	writer, newwriterf := t.newBlockWriter()
	writer.manifest = manifest

	opp, _ := NewDefaultProposalProcessor(pr, previous, newwriterf, nil, func(_ context.Context, facthash util.Hash) (base.Operation, error) {
		op, found := ops[facthash.String()]
		if !found {
			return nil, nil
		}

		return op, nil
	},
		func(base.Height, hint.Hint) (base.OperationProcessor, error) { return nil, nil },
	)
	opp.retrylimit = 1
	opp.retryinterval = 1

	m, err := opp.Process(context.Background(), nil)
	t.Error(err)
	t.Nil(m)
}

func (t *testDefaultProposalProcessor) TestProcessButErrorRetry() {
	point := base.RawPoint(33, 44)

	var called int
	ophs, ops, _ := t.prepareOperations(point.Height()-1, 4)
	for i := range ops {
		op := ops[i].(DummyOperation)

		if op.Fact().Hash().Equal(ophs[1]) {
			op.process = func(ctx context.Context, getStateFunc base.GetStateFunc) ([]base.StateMergeValue, base.OperationProcessReasonError, error) {
				called++
				return nil, nil, errors.Errorf("findme: %q", op.Fact().Hash())
			}
		}

		ops[i] = op
	}

	pr := t.newproposal(NewProposalFact(point, t.Local.Address(), ophs))

	previous := base.NewDummyManifest(point.Height()-1, valuehash.RandomSHA256())
	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	writer, newwriterf := t.newBlockWriter()
	writer.manifest = manifest

	opp, _ := NewDefaultProposalProcessor(pr, previous, newwriterf, nil, func(_ context.Context, facthash util.Hash) (base.Operation, error) {
		op, found := ops[facthash.String()]
		if !found {
			return nil, ErrOperationNotFoundInProcessor.Call()
		}

		return op, nil
	},
		func(base.Height, hint.Hint) (base.OperationProcessor, error) { return nil, nil },
	)
	opp.retrylimit = 3
	opp.retryinterval = 1

	m, err := opp.Process(context.Background(), nil)
	t.Error(err)
	t.Nil(m)

	t.ErrorContains(err, "failed to process operation")
	t.Equal(opp.retrylimit, called)
}

func (t *testDefaultProposalProcessor) TestProcessButSetStatesErrorRetry() {
	point := base.RawPoint(33, 44)

	ophs, ops, _ := t.prepareOperations(point.Height()-1, 4)

	pr := t.newproposal(NewProposalFact(point, t.Local.Address(), ophs))

	previous := base.NewDummyManifest(point.Height()-1, valuehash.RandomSHA256())
	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	writer, newwriterf := t.newBlockWriter()
	writer.manifest = manifest

	var called int
	writer.setstatesf = func(ctx context.Context, index uint64, sts []base.StateMergeValue, op base.Operation) error {
		if index == 2 {
			called++
			return errors.Errorf("findme: %q", op.Fact().Hash())
		}

		return writer.setStates(ctx, index, sts, op)
	}

	opp, _ := NewDefaultProposalProcessor(pr, previous, newwriterf, nil, func(_ context.Context, facthash util.Hash) (base.Operation, error) {
		op, found := ops[facthash.String()]
		if !found {
			return nil, ErrOperationNotFoundInProcessor.Call()
		}

		return op, nil
	},
		func(base.Height, hint.Hint) (base.OperationProcessor, error) { return nil, nil },
	)
	opp.retrylimit = 2
	opp.retryinterval = 1

	m, err := opp.Process(context.Background(), nil)
	t.Error(err)
	t.Nil(m)

	t.ErrorContains(err, "failed to process operation")
	t.Equal(opp.retrylimit, called)
}

func (t *testDefaultProposalProcessor) TestProcessContextCancel() {
	point := base.RawPoint(33, 44)

	ophs, ops, _ := t.prepareOperations(point.Height()-1, 4)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	startprocessedch := make(chan struct{}, 1)
	var startprocessedonece sync.Once
	for i := range ops {
		op := ops[i].(DummyOperation)
		orig := op.process
		if op.Fact().Hash().Equal(ophs[1]) { // NOTE only will process, index 1 and 3 operation
			op.process = func(ctx context.Context, getStateFunc base.GetStateFunc) ([]base.StateMergeValue, base.OperationProcessReasonError, error) {
				select {
				case <-time.After(time.Minute):
					return orig(ctx, getStateFunc)
				case <-ctx.Done():
					return nil, nil, ctx.Err()
				}
			}
		} else {
			op.process = func(ctx context.Context, getStateFunc base.GetStateFunc) ([]base.StateMergeValue, base.OperationProcessReasonError, error) {
				defer startprocessedonece.Do(func() {
					startprocessedch <- struct{}{}
				})

				return orig(ctx, getStateFunc)
			}
		}

		ops[i] = op
	}

	pr := t.newproposal(NewProposalFact(point, t.Local.Address(), ophs))

	previous := base.NewDummyManifest(point.Height()-1, valuehash.RandomSHA256())
	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	writer, newwriterf := t.newBlockWriter()
	writer.manifest = manifest

	opp, _ := NewDefaultProposalProcessor(pr, previous, newwriterf, nil, func(_ context.Context, facthash util.Hash) (base.Operation, error) {
		op, found := ops[facthash.String()]
		if !found {
			return nil, ErrOperationNotFoundInProcessor.Call()
		}

		return op, nil
	},
		func(base.Height, hint.Hint) (base.OperationProcessor, error) { return nil, nil },
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
	t.ErrorContains(err, "failed to process operation")
}

func (t *testDefaultProposalProcessor) TestProcessCancel() {
	point := base.RawPoint(33, 44)

	ophs, ops, _ := t.prepareOperations(point.Height()-1, 4)

	startprocessedch := make(chan struct{}, 1)
	var startprocessedonece sync.Once

	for i := range ops {
		op := ops[i].(DummyOperation)
		orig := op.process
		if op.Fact().Hash().Equal(ophs[1]) { // NOTE only will process, index 1 and 3 operation
			op.process = func(ctx context.Context, getStateFunc base.GetStateFunc) ([]base.StateMergeValue, base.OperationProcessReasonError, error) {
				select {
				case <-time.After(time.Minute):
					return orig(ctx, getStateFunc)
				case <-ctx.Done():
					return nil, nil, ctx.Err()
				}
			}
		} else {
			op.process = func(ctx context.Context, getStateFunc base.GetStateFunc) ([]base.StateMergeValue, base.OperationProcessReasonError, error) {
				defer startprocessedonece.Do(func() {
					startprocessedch <- struct{}{}
				})

				return orig(ctx, getStateFunc)
			}
		}

		ops[i] = op
	}

	pr := t.newproposal(NewProposalFact(point, t.Local.Address(), ophs))

	previous := base.NewDummyManifest(point.Height()-1, valuehash.RandomSHA256())
	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	writer, newwriterf := t.newBlockWriter()
	writer.manifest = manifest

	opp, _ := NewDefaultProposalProcessor(pr, previous, newwriterf, nil, func(_ context.Context, facthash util.Hash) (base.Operation, error) {
		op, found := ops[facthash.String()]
		if !found {
			return nil, ErrOperationNotFoundInProcessor.Call()
		}

		return op, nil
	},
		func(base.Height, hint.Hint) (base.OperationProcessor, error) { return nil, nil },
	)
	opp.retrylimit = 1
	opp.retryinterval = 1

	donech := make(chan error)
	go func() {
		m, err := opp.Process(context.Background(), nil)
		if m != nil {
			panic("not nil")
		}

		donech <- err
	}()

	<-startprocessedch
	<-time.After(time.Millisecond * 100)
	opp.Cancel()

	err := <-donech
	t.Error(err)

	t.True(errors.Is(err, context.Canceled))
	t.ErrorContains(err, "failed to process operation")
}

func (t *testDefaultProposalProcessor) TestSave() {
	point := base.RawPoint(33, 44)

	ophs, ops, _ := t.prepareOperations(point.Height()-1, 4)

	pr := t.newproposal(NewProposalFact(point, t.Local.Address(), ophs))

	previous := base.NewDummyManifest(point.Height()-1, valuehash.RandomSHA256())
	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	writer, newwriterf := t.newBlockWriter()
	writer.manifest = manifest

	savech := make(chan struct{}, 1)
	writer.savef = func(_ context.Context) (base.BlockMap, error) {
		savech <- struct{}{}

		return nil, nil
	}

	opp, _ := NewDefaultProposalProcessor(pr, previous, newwriterf, nil, func(_ context.Context, facthash util.Hash) (base.Operation, error) {
		return ops[facthash.String()], nil
	},
		func(base.Height, hint.Hint) (base.OperationProcessor, error) { return nil, nil },
	)

	ifact := t.NewINITBallotFact(point.NextHeight(), previous.Hash(), pr.Fact().Hash())
	ivp, err := t.NewINITVoteproof(ifact, t.Local, []LocalNode{t.Local})
	t.NoError(err)

	m, err := opp.Process(context.Background(), ivp)
	t.NoError(err)
	t.NotNil(m)

	t.Equal(4, writer.sts.Len())

	afact := t.NewACCEPTBallotFact(point.NextHeight(), nil, nil)
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

	ophs, ops, _ := t.prepareOperations(point.Height()-1, 4)

	pr := t.newproposal(NewProposalFact(point, t.Local.Address(), ophs))

	previous := base.NewDummyManifest(point.Height()-1, valuehash.RandomSHA256())
	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	writer, newwriterf := t.newBlockWriter()
	writer.manifest = manifest

	writer.savef = func(_ context.Context) (base.BlockMap, error) {
		return nil, errors.Errorf("killme")
	}

	opp, _ := NewDefaultProposalProcessor(pr, previous, newwriterf, nil, func(_ context.Context, facthash util.Hash) (base.Operation, error) {
		return ops[facthash.String()], nil
	},
		func(base.Height, hint.Hint) (base.OperationProcessor, error) { return nil, nil },
	)
	opp.retrylimit = 2
	opp.retryinterval = 1

	m, err := opp.Process(context.Background(), nil)
	t.NoError(err)
	t.NotNil(m)

	t.Equal(4, writer.sts.Len())

	afact := t.NewACCEPTBallotFact(point.NextHeight(), nil, nil)
	avp, err := t.NewACCEPTVoteproof(afact, t.Local, []LocalNode{t.Local})
	t.NoError(err)

	err = opp.Save(context.Background(), avp)
	t.Error(err)
	t.ErrorContains(err, "killme")
}

func (t *testDefaultProposalProcessor) TestSaveAgain() {
	point := base.RawPoint(33, 44)

	ophs, ops, _ := t.prepareOperations(point.Height()-1, 4)

	pr := t.newproposal(NewProposalFact(point, t.Local.Address(), ophs))

	previous := base.NewDummyManifest(point.Height()-1, valuehash.RandomSHA256())
	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	writer, newwriterf := t.newBlockWriter()
	writer.manifest = manifest

	savech := make(chan struct{}, 1)
	writer.savef = func(_ context.Context) (base.BlockMap, error) {
		savech <- struct{}{}

		return nil, nil
	}

	opp, _ := NewDefaultProposalProcessor(pr, previous, newwriterf, nil, func(_ context.Context, facthash util.Hash) (base.Operation, error) {
		return ops[facthash.String()], nil
	},
		func(base.Height, hint.Hint) (base.OperationProcessor, error) { return nil, nil },
	)

	ifact := t.NewINITBallotFact(point.NextHeight(), previous.Hash(), pr.Fact().Hash())
	ivp, err := t.NewINITVoteproof(ifact, t.Local, []LocalNode{t.Local})
	t.NoError(err)

	m, err := opp.Process(context.Background(), ivp)
	t.NoError(err)
	t.NotNil(m)

	t.Equal(4, writer.sts.Len())

	afact := t.NewACCEPTBallotFact(point.NextHeight(), nil, nil)
	avp, err := t.NewACCEPTVoteproof(afact, t.Local, []LocalNode{t.Local})
	t.NoError(err)

	t.NoError(opp.Save(context.Background(), avp))

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("failed to wait to save"))
	case <-savech:
	}

	err = opp.Save(context.Background(), avp)
	t.Error(err)
	t.True(errors.Is(err, ErrProcessorAlreadySaved))
}

func TestDefaultProposalProcessor(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testDefaultProposalProcessor))
}
