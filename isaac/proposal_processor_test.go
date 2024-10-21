package isaac

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/fixedtree"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
	"golang.org/x/exp/slices"
)

type DummyBlockWriter struct {
	sync.RWMutex
	getStateFunc base.GetStateFunc
	proposal     base.ProposalSignFact
	manifest     base.Manifest
	manifesterr  error
	opstreeg     *fixedtree.Writer
	ops          []base.Operation
	sts          *util.SingleLockedMap[string, base.StateValueMerger]
	setstatesf   func(context.Context, uint64, []base.StateMergeValue, base.Operation) error
	savef        func(context.Context) (base.BlockMap, error)
}

func NewDummyBlockWriter(proposal base.ProposalSignFact, getStateFunc base.GetStateFunc) *DummyBlockWriter {
	return &DummyBlockWriter{
		proposal:     proposal,
		getStateFunc: getStateFunc,
		sts:          util.NewSingleLockedMap[string, base.StateValueMerger](),
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

	e := util.StringError("failed to set states")

	for i := range states {
		stv := states[i]

		_ = w.sts.GetOrCreate(
			stv.Key(),
			func(j base.StateValueMerger, _ bool) error {
				if err := j.Merge(stv, op.Fact().Hash()); err != nil {
					return e.WithMessage(err, "failed to merge")
				}

				return nil
			},
			func() (base.StateValueMerger, error) {
				var st base.State

				switch j, found, err := w.getStateFunc(stv.Key()); {
				case err != nil:
					return nil, err
				case found:
					st = j
				}

				return stv.Merger(w.proposal.Point().Height(), st), nil
			},
		)
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
		return ctx, base.NewBaseOperationProcessReason("nil preprocess"), nil
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
	Encs *encoder.Encoders
	Enc  encoder.Encoder
}

func (t *testDefaultProposalProcessor) SetupSuite() {
	t.Enc = jsonenc.NewEncoder()
	t.Encs = encoder.NewEncoders(t.Enc, t.Enc)

	t.NoError(t.Encs.AddHinter(base.DummyManifest{}))
	t.NoError(t.Encs.AddHinter(base.DummyBlockMap{}))
	t.NoError(t.Encs.AddDetail(encoder.DecodeDetail{Hint: base.MPublickeyHint, Instance: &base.MPublickey{}}))
	t.NoError(t.Encs.AddDetail(encoder.DecodeDetail{Hint: base.StringAddressHint, Instance: base.StringAddress{}}))
	t.NoError(t.Encs.AddDetail(encoder.DecodeDetail{Hint: base.DummyNodeHint, Instance: base.BaseNode{}}))
	t.NoError(t.Encs.AddDetail(encoder.DecodeDetail{Hint: DummyOperationFactHint, Instance: DummyOperationFact{}}))
	t.NoError(t.Encs.AddDetail(encoder.DecodeDetail{Hint: DummyOperationHint, Instance: DummyOperation{}}))
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

func (t *testDefaultProposalProcessor) newOperation(height base.Height) DummyOperation {
	fact := NewDummyOperationFact(util.UUID().Bytes(), valuehash.RandomSHA256())
	op, _ := NewDummyOperation(fact, t.Local.Privatekey(), t.LocalParams.NetworkID())

	return op
}

func (t *testDefaultProposalProcessor) newOperationExtended(height base.Height, k string, v interface{}) DummyOperation {
	op := t.newOperation(height)

	b0, err := util.MarshalJSON(op)
	t.NoError(err)

	var m map[string]interface{}

	t.NoError(util.UnmarshalJSON(b0, &m))
	m[k] = v

	b1, err := util.MarshalJSON(m)
	t.NoError(err)

	op.SetMarshaledJSON(b1)

	return op
}

func (t *testDefaultProposalProcessor) prepareOperations(height base.Height, n int) (
	[][2]util.Hash,
	map[string]base.Operation,
	map[string]base.StateMergeValue,
) {
	ophs := make([][2]util.Hash, n)
	ops := map[string]base.Operation{}
	sts := map[string]base.StateMergeValue{}

	for i := range ophs {
		op := t.newOperation(height)

		fact := op.Fact().Hash()

		ophs[i] = [2]util.Hash{op.Hash(), fact}
		st := t.newStateMergeValue(fact.String())

		op.preprocess = func(ctx context.Context, _ base.GetStateFunc) (context.Context, base.OperationProcessReasonError, error) {
			return ctx, nil, nil
		}
		op.process = func(context.Context, base.GetStateFunc) ([]base.StateMergeValue, base.OperationProcessReasonError, error) {
			return []base.StateMergeValue{st}, nil, nil
		}

		ops[op.Hash().String()] = op
		sts[fact.String()] = st
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

func (t *testDefaultProposalProcessor) newargs(
	newWriterFunc NewBlockWriterFunc,
) *DefaultProposalProcessorArgs {
	args := NewDefaultProposalProcessorArgs()
	args.NewWriterFunc = newWriterFunc
	args.GetStateFunc = base.NilGetState

	return args
}

func (t *testDefaultProposalProcessor) TestNew() {
	point := base.RawPoint(33, 44)

	pr := t.newproposal(NewProposalFact(point, t.Local.Address(), valuehash.RandomSHA256(), [][2]util.Hash{{valuehash.RandomSHA256(), valuehash.RandomSHA256()}}))

	previous := base.NewDummyManifest(point.Height()-1, valuehash.RandomSHA256())
	_, newwriterf := t.newBlockWriter()
	args := t.newargs(newwriterf)

	opp, _ := NewDefaultProposalProcessor(pr, previous, args)
	_ = (interface{})(opp).(ProposalProcessor)

	base.EqualProposalSignFact(t.Assert(), pr, opp.Proposal())
}

func (t *testDefaultProposalProcessor) TestCollectOperations() {
	point := base.RawPoint(33, 44)

	ophs, ops, _ := t.prepareOperations(point.Height()-1, 4)

	pr := t.newproposal(NewProposalFact(point, t.Local.Address(), valuehash.RandomSHA256(), ophs))

	previous := base.NewDummyManifest(point.Height()-1, valuehash.RandomSHA256())
	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	writer, newwriterf := t.newBlockWriter()
	writer.manifest = manifest

	args := t.newargs(newwriterf)

	var copslock sync.Mutex
	var cops []base.Operation

	args.GetOperationFunc = func(_ context.Context, oph, fact util.Hash) (base.Operation, error) {
		op, found := ops[oph.String()]
		if !found {
			return nil, ErrOperationNotFoundInProcessor.Errorf("operation not found")
		}

		copslock.Lock()
		cops = append(cops, op)
		copslock.Unlock()

		return op, nil
	}

	opp, _ := NewDefaultProposalProcessor(pr, previous, args)

	m, err := opp.Process(context.Background(), nil)
	t.NoError(err)
	t.NotNil(m)

	t.Equal(len(ophs), len(cops))

	for i := range ophs {
		a := ops[ophs[i][0].String()]

		ci := slices.IndexFunc(cops, func(op base.Operation) bool {
			return op.Fact().Hash().Equal(ophs[i][1])
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

	pr := t.newproposal(NewProposalFact(point, t.Local.Address(), valuehash.RandomSHA256(), ophs))

	previous := base.NewDummyManifest(point.Height()-1, valuehash.RandomSHA256())
	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	writer, newwriterf := t.newBlockWriter()
	writer.manifest = manifest

	args := t.newargs(newwriterf)

	args.GetOperationFunc = func(_ context.Context, oph, fact util.Hash) (base.Operation, error) {
		op, found := ops[oph.String()]
		if !found {
			return nil, ErrOperationNotFoundInProcessor.Errorf("operation not found")
		}

		switch {
		case oph.Equal(ophs[1][0]), oph.Equal(ophs[3][0]):
		default:
			return nil, errors.Errorf("operation not found; wrong type")
		}

		return op, nil
	}

	opp, _ := NewDefaultProposalProcessor(pr, previous, args)

	m, err := opp.Process(context.Background(), nil)
	t.Error(err)
	t.Nil(m)

	switch {
	case errors.Is(err, context.Canceled):
	case err != nil && strings.Contains(err.Error(), "operation not found; wrong type"):
		t.ErrorContains(err, "collect operations")
	default:
		t.Error(errors.Errorf("unexpected error, %T", err))
	}
}

func (t *testDefaultProposalProcessor) TestCollectOperationsFailedButIgnored() {
	point := base.RawPoint(33, 44)

	ophs, ops, _ := t.prepareOperations(point.Height()-1, 4)

	pr := t.newproposal(NewProposalFact(point, t.Local.Address(), valuehash.RandomSHA256(), ophs))

	previous := base.NewDummyManifest(point.Height()-1, valuehash.RandomSHA256())
	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	writer, newwriterf := t.newBlockWriter()
	writer.manifest = manifest

	args := t.newargs(newwriterf)

	var copslock sync.Mutex
	var cops []base.Operation

	args.GetOperationFunc = func(_ context.Context, oph, fact util.Hash) (base.Operation, error) {
		op, found := ops[oph.String()]
		if !found {
			return nil, nil
		}

		switch {
		case oph.Equal(ophs[1][0]):
			return nil, ErrInvalidOperationInProcessor.WithStack()
		case oph.Equal(ophs[2][0]):
			return nil, util.ErrInvalid.WithStack()
		case oph.Equal(ophs[3][0]):
			return nil, ErrOperationNotFoundInProcessor.WithStack()
		}

		copslock.Lock()
		cops = append(cops, op)
		copslock.Unlock()

		return op, nil
	}

	opp, _ := NewDefaultProposalProcessor(pr, previous, args)

	m, err := opp.Process(context.Background(), nil)
	t.NoError(err)
	t.NotNil(m)

	for i := range ophs {
		if i == 1 || i == 2 {
			continue
		}

		ci := slices.IndexFunc(cops, func(op base.Operation) bool {
			return op.Fact().Hash().Equal(ophs[i][1])
		})

		if i == 3 {
			t.Equal(-1, ci)

			continue
		}

		b := cops[ci]

		a := ops[ophs[i][0].String()]
		t.NotNil(a)
		t.NotNil(b)

		base.EqualSignFact(t.Assert(), a, b)
	}
}

func (t *testDefaultProposalProcessor) TestCollectOperationsInvalidError() {
	point := base.RawPoint(33, 44)

	ophs, ops, _ := t.prepareOperations(point.Height()-1, 4)

	pr := t.newproposal(NewProposalFact(point, t.Local.Address(), valuehash.RandomSHA256(), ophs))

	previous := base.NewDummyManifest(point.Height()-1, valuehash.RandomSHA256())
	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	writer, newwriterf := t.newBlockWriter()
	writer.manifest = manifest

	args := t.newargs(newwriterf)

	var copslock sync.Mutex
	var cops []base.Operation

	args.GetOperationFunc = func(_ context.Context, oph, fact util.Hash) (base.Operation, error) {
		op, found := ops[oph.String()]
		if !found {
			return nil, nil
		}

		switch {
		case oph.Equal(ophs[1][0]):
			return nil, ErrOperationAlreadyProcessedInProcessor.Errorf("already processed in previous")
		case oph.Equal(ophs[3][0]):
			return nil, ErrInvalidOperationInProcessor.Errorf("hehehe")
		}

		copslock.Lock()
		cops = append(cops, op)
		copslock.Unlock()

		return op, nil
	}

	opp, _ := NewDefaultProposalProcessor(pr, previous, args)

	m, err := opp.Process(context.Background(), nil)
	t.NoError(err)
	t.NotNil(m)

	for i := range ophs {
		ci := slices.IndexFunc(cops, func(op base.Operation) bool {
			return op.Fact().Hash().Equal(ophs[i][1])
		})

		if i == 1 {
			t.Equal(-1, ci)

			continue
		}

		if i == 3 {
			continue
		}

		b := cops[ci]

		a := ops[ophs[i][0].String()]
		t.NotNil(a)
		t.NotNil(b)

		base.EqualSignFact(t.Assert(), a, b)
	}
}

func (t *testDefaultProposalProcessor) TestCollectOperationsWrongFactInProposal() {
	point := base.RawPoint(33, 44)

	ophs, ops, _ := t.prepareOperations(point.Height()-1, 4)

	fabophs := make([][2]util.Hash, len(ophs))
	copy(fabophs, ophs)
	fabophs[3] = [2]util.Hash{ophs[3][0], valuehash.RandomSHA256()}

	pr := t.newproposal(NewProposalFact(point, t.Local.Address(), valuehash.RandomSHA256(), fabophs))

	previous := base.NewDummyManifest(point.Height()-1, valuehash.RandomSHA256())
	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	writer, newwriterf := t.newBlockWriter()
	writer.manifest = manifest

	args := t.newargs(newwriterf)
	args.GetOperationFunc = func(_ context.Context, oph, fact util.Hash) (base.Operation, error) {
		op, found := ops[oph.String()]
		if !found {
			return nil, ErrOperationNotFoundInProcessor.Errorf("operation not found")
		}

		return op, nil
	}

	opp, _ := NewDefaultProposalProcessor(pr, previous, args)

	m, err := opp.Process(context.Background(), nil)
	t.Error(err)
	t.Nil(m)
	t.ErrorContains(err, "proposal processor not processed")
}

func (t *testDefaultProposalProcessor) TestPreProcessButFailedToGetOperationProcessor() {
	point := base.RawPoint(33, 44)

	ophs, ops, _ := t.prepareOperations(point.Height()-1, 4)

	pr := t.newproposal(NewProposalFact(point, t.Local.Address(), valuehash.RandomSHA256(), ophs))
	previous := base.NewDummyManifest(point.Height()-1, valuehash.RandomSHA256())
	_, newwriterf := t.newBlockWriter()

	args := t.newargs(newwriterf)

	args.GetOperationFunc = func(_ context.Context, oph, fact util.Hash) (base.Operation, error) {
		op, found := ops[oph.String()]
		if !found {
			return nil, ErrOperationNotFoundInProcessor.WithStack()
		}

		return op, nil
	}
	args.NewOperationProcessorFunc = func(_ base.Height, ht hint.Hint, getStatef base.GetStateFunc) (base.OperationProcessor, error) {
		if !ht.IsCompatible(DummyOperationHint) {
			return nil, nil
		}

		return nil, errors.Errorf("hehehe")
	}

	opp, _ := NewDefaultProposalProcessor(pr, previous, args)

	_, err := opp.Process(context.Background(), nil)
	t.Error(err)
	t.ErrorContains(err, "hehehe")
}

func (t *testDefaultProposalProcessor) TestPreProcessButErrSuspendOperation() {
	point := base.RawPoint(33, 44)

	ophs, ops, sts := t.prepareOperations(point.Height()-1, 4)

	pr := t.newproposal(NewProposalFact(point, t.Local.Address(), valuehash.RandomSHA256(), ophs))

	previous := base.NewDummyManifest(point.Height()-1, valuehash.RandomSHA256())
	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	writer, newwriterf := t.newBlockWriter()
	writer.manifest = manifest

	suspended := ophs[1]

	args := t.newargs(newwriterf)
	args.GetOperationFunc = func(_ context.Context, oph, fact util.Hash) (base.Operation, error) {
		op, found := ops[oph.String()]
		if !found {
			return nil, ErrOperationNotFoundInProcessor.WithStack()
		}

		return op, nil
	}

	args.NewOperationProcessorFunc = func(_ base.Height, ht hint.Hint, getStatef base.GetStateFunc) (base.OperationProcessor, error) {
		if !ht.IsCompatible(DummyOperationHint) {
			return nil, nil
		}

		return &DummyOperationProcessor{
			preprocess: func(ctx context.Context, op base.Operation, _ base.GetStateFunc) (context.Context, base.OperationProcessReasonError, error) {
				if op.Fact().Hash().Equal(suspended[1]) {
					return ctx, nil, ErrSuspendOperation.Errorf("hohoho")
				}

				return ctx, nil, nil
			},
			process: func(_ context.Context, op base.Operation, _ base.GetStateFunc) ([]base.StateMergeValue, base.OperationProcessReasonError, error) {
				return []base.StateMergeValue{sts[op.Fact().Hash().String()]}, nil, nil
			},
		}, nil
	}

	opp, _ := NewDefaultProposalProcessor(pr, previous, args)

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

	pr := t.newproposal(NewProposalFact(point, t.Local.Address(), valuehash.RandomSHA256(), ophs))

	previous := base.NewDummyManifest(point.Height()-1, valuehash.RandomSHA256())
	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	writer, newwriterf := t.newBlockWriter()
	writer.manifest = manifest

	args := t.newargs(newwriterf)
	args.GetOperationFunc = func(_ context.Context, oph, fact util.Hash) (base.Operation, error) {
		op, found := ops[oph.String()]
		if !found {
			return nil, ErrOperationNotFoundInProcessor.WithStack()
		}

		return op, nil
	}
	args.NewOperationProcessorFunc = func(_ base.Height, ht hint.Hint, getStatef base.GetStateFunc) (base.OperationProcessor, error) {
		if !ht.IsCompatible(DummyOperationHint) {
			return nil, nil
		}

		return &DummyOperationProcessor{
			preprocess: func(ctx context.Context, op base.Operation, _ base.GetStateFunc) (context.Context, base.OperationProcessReasonError, error) {
				switch h := op.Fact().Hash(); {
				case h.Equal(ophs[1][1]),
					h.Equal(ophs[3][1]): // NOTE only will process, index 1 and 3 operation
					return ctx, nil, nil
				default:
					return ctx, base.NewBaseOperationProcessReason("bad"), nil
				}
			},
			process: func(_ context.Context, op base.Operation, _ base.GetStateFunc) ([]base.StateMergeValue, base.OperationProcessReasonError, error) {
				return []base.StateMergeValue{sts[op.Fact().Hash().String()]}, nil, nil
			},
		}, nil
	}

	opp, _ := NewDefaultProposalProcessor(pr, previous, args)

	m, err := opp.Process(context.Background(), nil)
	t.NoError(err)
	t.NotNil(m)

	t.Equal(2, writer.sts.Len())

	for i := range ophs {
		h := ophs[i][1]
		st := sts[h.String()]

		var bst base.StateValueMerger

		writer.sts.Traverse(func(_ string, v base.StateValueMerger) bool {
			if v.Key() == st.Key() {
				bst = v

				return false
			}

			return true
		})

		if i != 1 && i != 3 {
			t.Nil(bst)

			continue
		}

		t.NotNil(bst)
		nst, err := bst.CloseValue()
		t.NoError(err)

		bops := nst.Operations()
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
			case op.Fact().Hash().Equal(ophs[1][1]),
				op.Fact().Hash().Equal(ophs[3][1]): // NOTE only will process, index 1 and 3 operation
				return ctx, nil, nil
			default:
				return ctx, base.NewBaseOperationProcessReason("bad"), nil
			}
		}

		ops[i] = op
	}

	pr := t.newproposal(NewProposalFact(point, t.Local.Address(), valuehash.RandomSHA256(), ophs))

	previous := base.NewDummyManifest(point.Height()-1, valuehash.RandomSHA256())
	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	writer, newwriterf := t.newBlockWriter()
	writer.manifest = manifest

	args := t.newargs(newwriterf)
	args.GetOperationFunc = func(_ context.Context, oph, fact util.Hash) (base.Operation, error) {
		op, found := ops[oph.String()]
		if !found {
			return nil, ErrOperationNotFoundInProcessor.Errorf("operation not found")
		}

		if oph.Equal(ophs[3][0]) {
			return nil, ErrInvalidOperationInProcessor.WithStack()
		}

		return op, nil
	}

	opp, _ := NewDefaultProposalProcessor(pr, previous, args)

	m, err := opp.Process(context.Background(), nil)
	t.NoError(err)
	t.NotNil(m)

	t.Equal(1, writer.sts.Len())

	for i := range ophs {
		h := ophs[i][1]
		st := sts[h.String()]

		var bst base.StateValueMerger
		writer.sts.Traverse(func(_ string, v base.StateValueMerger) bool {
			if v.Key() == st.Key() {
				bst = v

				return false
			}

			return true
		})

		if i != 1 {
			t.Nil(bst)

			continue
		}

		t.NotNil(bst)

		nst, err := bst.CloseValue()
		t.NoError(err)

		bops := nst.Operations()
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
		if op.Fact().Hash().Equal(ophs[1][1]) {
			op.preprocess = func(ctx context.Context, _ base.GetStateFunc) (context.Context, base.OperationProcessReasonError, error) {
				return ctx, nil, errors.Errorf("findme: %q", op.Fact().Hash())
			}
		}

		ops[i] = op
	}

	pr := t.newproposal(NewProposalFact(point, t.Local.Address(), valuehash.RandomSHA256(), ophs))

	previous := base.NewDummyManifest(point.Height()-1, valuehash.RandomSHA256())
	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	writer, newwriterf := t.newBlockWriter()
	writer.manifest = manifest

	args := t.newargs(newwriterf)
	args.GetOperationFunc = func(_ context.Context, oph, fact util.Hash) (base.Operation, error) {
		op, found := ops[oph.String()]
		if !found {
			return nil, ErrOperationNotFoundInProcessor.WithStack()
		}

		return op, nil
	}

	opp, _ := NewDefaultProposalProcessor(pr, previous, args)

	m, err := opp.Process(context.Background(), nil)
	t.Error(err)
	t.Nil(m)

	t.ErrorContains(err, fmt.Sprintf("findme: %q", ophs[1][1]))
}

func (t *testDefaultProposalProcessor) TestCollectOperationButWithOperationReasonError() {
	point := base.RawPoint(33, 44)

	ophs, ops, _ := t.prepareOperations(point.Height()-1, 4)

	for i := range ops {
		i := i
		op := ops[i].(DummyOperation)
		op.preprocess = func(ctx context.Context, _ base.GetStateFunc) (context.Context, base.OperationProcessReasonError, error) {
			return ctx, nil, nil
		}

		ops[i] = op
	}

	pr := t.newproposal(NewProposalFact(point, t.Local.Address(), valuehash.RandomSHA256(), ophs))

	previous := base.NewDummyManifest(point.Height()-1, valuehash.RandomSHA256())
	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	writer, newwriterf := t.newBlockWriter()
	writer.manifest = manifest

	args := t.newargs(newwriterf)
	args.GetOperationFunc = func(_ context.Context, oph, fact util.Hash) (base.Operation, error) {
		if oph.Equal(ophs[1][0]) {
			return nil, ErrInvalidOperationInProcessor.Errorf("showme")
		}

		op, found := ops[oph.String()]
		if !found {
			return nil, ErrOperationNotFoundInProcessor.WithStack()
		}

		return op, nil
	}

	opp, _ := NewDefaultProposalProcessor(pr, previous, args)

	m, err := opp.Process(context.Background(), nil)
	t.NoError(err)
	t.NotNil(m)

	writer.opstreeg.Traverse(func(index uint64, n fixedtree.Node) (bool, error) {
		node := n.(base.OperationFixedtreeNode)

		switch {
		case index == 1:
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

func (t *testDefaultProposalProcessor) TestPreProcessButWithOperationReasonError() {
	point := base.RawPoint(33, 44)

	ophs, ops, _ := t.prepareOperations(point.Height()-1, 4)

	for i := range ops {
		i := i
		op := ops[i].(DummyOperation)
		op.preprocess = func(ctx context.Context, _ base.GetStateFunc) (context.Context, base.OperationProcessReasonError, error) {
			switch {
			case op.Fact().Hash().Equal(ophs[1][1]),
				op.Fact().Hash().Equal(ophs[3][1]):
				return ctx, base.NewBaseOperationProcessReasonf("showme, %q", op.Fact().Hash()), nil
			default:
				return ctx, nil, nil
			}
		}

		ops[i] = op
	}

	pr := t.newproposal(NewProposalFact(point, t.Local.Address(), valuehash.RandomSHA256(), ophs))

	previous := base.NewDummyManifest(point.Height()-1, valuehash.RandomSHA256())
	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	writer, newwriterf := t.newBlockWriter()
	writer.manifest = manifest

	args := t.newargs(newwriterf)
	args.GetOperationFunc = func(_ context.Context, oph, fact util.Hash) (base.Operation, error) {
		op, found := ops[oph.String()]
		if !found {
			return nil, ErrOperationNotFoundInProcessor.WithStack()
		}

		return op, nil
	}

	opp, _ := NewDefaultProposalProcessor(pr, previous, args)

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

func (t *testDefaultProposalProcessor) TestPreProcessContextCancel() {
	point := base.RawPoint(33, 44)

	ophs, ops, _ := t.prepareOperations(point.Height()-1, 4)

	startprocessedch := make(chan struct{}, 1)
	var startprocessedonece sync.Once

	for i := range ops {
		op := ops[i].(DummyOperation)
		if op.Fact().Hash().Equal(ophs[1][1]) {
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

	pr := t.newproposal(NewProposalFact(point, t.Local.Address(), valuehash.RandomSHA256(), ophs))

	previous := base.NewDummyManifest(point.Height()-1, valuehash.RandomSHA256())
	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	writer, newwriterf := t.newBlockWriter()
	writer.manifest = manifest

	args := t.newargs(newwriterf)
	args.GetOperationFunc = func(_ context.Context, oph, fact util.Hash) (base.Operation, error) {
		op, found := ops[oph.String()]
		if !found {
			return nil, ErrOperationNotFoundInProcessor.Errorf("operation not found")
		}

		return op, nil
	}

	opp, _ := NewDefaultProposalProcessor(pr, previous, args)

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

	t.ErrorIs(err, context.Canceled)
	t.ErrorContains(err, "pre process operation")
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

			if i == ophs[len(ophs)-1][0].String() {
				collected = index
			}

			ctx = context.WithValue(ctx, collectedstringkey, index+1)

			return ctx, nil, nil
		}

		ops[i] = op
	}

	pr := t.newproposal(NewProposalFact(point, t.Local.Address(), valuehash.RandomSHA256(), ophs))

	previous := base.NewDummyManifest(point.Height()-1, valuehash.RandomSHA256())
	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	writer, newwriterf := t.newBlockWriter()
	writer.manifest = manifest

	args := t.newargs(newwriterf)
	args.GetOperationFunc = func(_ context.Context, oph, fact util.Hash) (base.Operation, error) {
		op, found := ops[oph.String()]
		if !found {
			return nil, ErrOperationNotFoundInProcessor.Errorf("operation not found")
		}

		return op, nil
	}

	opp, _ := NewDefaultProposalProcessor(pr, previous, args)

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
		if !op.Fact().Hash().Equal(ophs[1][1]) && !op.Fact().Hash().Equal(ophs[3][1]) {
			// NOTE only will process, index 1 and 3 operation
			op.process = func(ctx context.Context, getStateFunc base.GetStateFunc) ([]base.StateMergeValue, base.OperationProcessReasonError, error) {
				return nil, base.ErrNotChangedOperationProcessReason, nil
			}
		}

		ops[i] = op
	}

	prevsts := make([]base.State, len(sts))
	prevsts[1] = base.NewBaseState(point.Height()-1, sts[ophs[1][1].String()].Key(), nil, valuehash.RandomSHA256(), nil)

	pr := t.newproposal(NewProposalFact(point, t.Local.Address(), valuehash.RandomSHA256(), ophs))

	previous := base.NewDummyManifest(point.Height()-1, valuehash.RandomSHA256())
	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	writer, newwriterf := t.newBlockWriter()
	writer.manifest = manifest

	args := t.newargs(newwriterf)

	args.GetStateFunc = func(key string) (base.State, bool, error) {
		switch key {
		case sts[ophs[1][1].String()].Key():
			return prevsts[1], true, nil
		default:
			return nil, false, nil
		}
	}

	args.GetOperationFunc = func(_ context.Context, oph, fact util.Hash) (base.Operation, error) {
		op, found := ops[oph.String()]
		if !found {
			return nil, ErrOperationNotFoundInProcessor.WithStack()
		}

		return op, nil
	}

	opp, _ := NewDefaultProposalProcessor(pr, previous, args)

	m, err := opp.Process(context.Background(), nil)
	t.NoError(err)
	t.NotNil(m)

	t.Equal(2, writer.sts.Len())

	for i := range ophs {
		h := ophs[i][1]
		st := sts[h.String()]

		var bst base.StateValueMerger
		writer.sts.Traverse(func(_ string, v base.StateValueMerger) bool {
			if v.Key() == st.Key() {
				bst = v

				return false
			}

			return true
		})

		if i != 1 && i != 3 {
			t.Nil(bst)

			continue
		}

		t.NotNil(bst)
		nst, err := bst.CloseValue()
		t.NoError(err)

		t.Equal(point.Height(), nst.Height())
		t.True(base.IsEqualStateValue(st, nst.Value()))

		bops := nst.Operations()
		t.Equal(1, len(bops))
		t.True(h.Equal(bops[0]))

		if i == 1 {
			t.True(prevsts[1].Hash().Equal(nst.Previous()))
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

	pr := t.newproposal(NewProposalFact(point, t.Local.Address(), valuehash.RandomSHA256(), ophs))

	previous := base.NewDummyManifest(point.Height()-1, valuehash.RandomSHA256())
	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	writer, newwriterf := t.newBlockWriter()
	writer.manifest = manifest

	args := t.newargs(newwriterf)
	args.GetOperationFunc = func(_ context.Context, oph, fact util.Hash) (base.Operation, error) {
		op, found := ops[oph.String()]
		if !found {
			return nil, ErrOperationNotFoundInProcessor.WithStack()
		}

		return op, nil
	}
	args.NewOperationProcessorFunc = func(_ base.Height, ht hint.Hint, getStatef base.GetStateFunc) (base.OperationProcessor, error) {
		if !ht.IsCompatible(DummyOperationHint) {
			return nil, nil
		}

		return &DummyOperationProcessor{
			preprocess: func(ctx context.Context, op base.Operation, _ base.GetStateFunc) (context.Context, base.OperationProcessReasonError, error) {
				return ctx, nil, nil
			},
			process: func(_ context.Context, op base.Operation, _ base.GetStateFunc) ([]base.StateMergeValue, base.OperationProcessReasonError, error) {
				switch h := op.Fact().Hash(); {
				case h.Equal(ophs[1][1]),
					h.Equal(ophs[3][1]): // NOTE only will process, index 1 and 3 operation
					return []base.StateMergeValue{sts[op.Fact().Hash().String()]}, nil, nil
				default:
					return nil, base.ErrNotChangedOperationProcessReason, nil
				}
			},
		}, nil
	}

	opp, _ := NewDefaultProposalProcessor(pr, previous, args)

	m, err := opp.Process(context.Background(), nil)
	t.NoError(err)
	t.NotNil(m)

	t.Equal(2, writer.sts.Len())

	for i := range ophs {
		h := ophs[i][1]
		st := sts[h.String()]

		var bst base.StateValueMerger
		writer.sts.Traverse(func(_ string, v base.StateValueMerger) bool {
			if v.Key() == st.Key() {
				bst = v

				return false
			}

			return true
		})

		if i != 1 && i != 3 {
			t.Nil(bst)

			continue
		}

		t.NotNil(bst)
		nst, err := bst.CloseValue()
		t.NoError(err)

		bops := nst.Operations()
		t.Equal(1, len(bops))
		t.True(h.Equal(bops[0]))
	}
}

func (t *testDefaultProposalProcessor) TestProcessButError() {
	point := base.RawPoint(33, 44)

	ophs, ops, _ := t.prepareOperations(point.Height()-1, 4)
	for i := range ops {
		op := ops[i].(DummyOperation)

		if op.Fact().Hash().Equal(ophs[1][1]) {
			op.process = func(ctx context.Context, getStateFunc base.GetStateFunc) ([]base.StateMergeValue, base.OperationProcessReasonError, error) {
				return nil, nil, errors.Errorf("findme: %q", op.Fact().Hash())
			}
		}

		ops[i] = op
	}

	pr := t.newproposal(NewProposalFact(point, t.Local.Address(), valuehash.RandomSHA256(), ophs))

	previous := base.NewDummyManifest(point.Height()-1, valuehash.RandomSHA256())
	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	writer, newwriterf := t.newBlockWriter()
	writer.manifest = manifest

	args := t.newargs(newwriterf)
	args.GetOperationFunc = func(_ context.Context, oph, fact util.Hash) (base.Operation, error) {
		op, found := ops[oph.String()]
		if !found {
			return nil, nil
		}

		return op, nil
	}

	opp, _ := NewDefaultProposalProcessor(pr, previous, args)

	m, err := opp.Process(context.Background(), nil)
	t.Error(err)
	t.Nil(m)
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
		if op.Fact().Hash().Equal(ophs[1][1]) { // NOTE only will process, index 1 and 3 operation
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

	pr := t.newproposal(NewProposalFact(point, t.Local.Address(), valuehash.RandomSHA256(), ophs))

	previous := base.NewDummyManifest(point.Height()-1, valuehash.RandomSHA256())
	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	writer, newwriterf := t.newBlockWriter()
	writer.manifest = manifest

	args := t.newargs(newwriterf)
	args.GetOperationFunc = func(_ context.Context, oph, fact util.Hash) (base.Operation, error) {
		op, found := ops[oph.String()]
		if !found {
			return nil, ErrOperationNotFoundInProcessor.WithStack()
		}

		return op, nil
	}

	opp, _ := NewDefaultProposalProcessor(pr, previous, args)

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

	t.ErrorIs(err, context.Canceled)
	t.ErrorContains(err, "process operation")
}

func (t *testDefaultProposalProcessor) TestProcessCancel() {
	point := base.RawPoint(33, 44)

	ophs, ops, _ := t.prepareOperations(point.Height()-1, 4)

	startprocessedch := make(chan struct{}, 1)
	var startprocessedonece sync.Once

	for i := range ops {
		op := ops[i].(DummyOperation)
		orig := op.process
		if op.Fact().Hash().Equal(ophs[1][1]) { // NOTE only will process, index 1 and 3 operation
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

	pr := t.newproposal(NewProposalFact(point, t.Local.Address(), valuehash.RandomSHA256(), ophs))

	previous := base.NewDummyManifest(point.Height()-1, valuehash.RandomSHA256())
	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	writer, newwriterf := t.newBlockWriter()
	writer.manifest = manifest

	args := t.newargs(newwriterf)
	args.GetOperationFunc = func(_ context.Context, oph, fact util.Hash) (base.Operation, error) {
		op, found := ops[oph.String()]
		if !found {
			return nil, ErrOperationNotFoundInProcessor.WithStack()
		}

		return op, nil
	}

	opp, _ := NewDefaultProposalProcessor(pr, previous, args)

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

	t.ErrorIs(err, context.Canceled)
	t.ErrorContains(err, "process operation")
}

func (t *testDefaultProposalProcessor) TestSave() {
	point := base.RawPoint(33, 44)

	ophs, ops, _ := t.prepareOperations(point.Height()-1, 4)

	indexExtendedOp := uint64(len(ops))
	extendedValue := util.UUID().String()

	{ // NOTE add extended operation
		op := t.newOperationExtended(point.Height()-1, "A", extendedValue)

		fact := op.Fact().Hash()

		st := t.newStateMergeValue(fact.String())

		op.preprocess = func(ctx context.Context, _ base.GetStateFunc) (context.Context, base.OperationProcessReasonError, error) {
			return ctx, nil, nil
		}
		op.process = func(context.Context, base.GetStateFunc) ([]base.StateMergeValue, base.OperationProcessReasonError, error) {
			return []base.StateMergeValue{st}, nil, nil
		}

		ophs = append(ophs, [2]util.Hash{op.Hash(), fact})
		ops[op.Hash().String()] = op
	}

	pr := t.newproposal(NewProposalFact(point, t.Local.Address(), valuehash.RandomSHA256(), ophs))

	previous := base.NewDummyManifest(point.Height()-1, valuehash.RandomSHA256())
	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())

	ifact := t.NewINITBallotFact(point.NextHeight(), previous.Hash(), pr.Fact().Hash())
	ivp, err := t.NewINITVoteproof(ifact, t.Local, []base.LocalNode{t.Local})
	t.NoError(err)

	t.Run("ok", func() {
		writer, newwriterf := t.newBlockWriter()
		writer.manifest = manifest

		savech := make(chan struct{}, 1)
		writer.savef = func(_ context.Context) (base.BlockMap, error) {
			savech <- struct{}{}

			return nil, nil
		}
		writer.setstatesf = func(ctx context.Context, index uint64, states []base.StateMergeValue, op base.Operation) error {
			if index == indexExtendedOp {
				t.Run("check extended value in operation", func() {
					b, err := util.MarshalJSON(op)
					t.NoError(err)

					t.T().Log("extended marshaled:", string(b))

					var m map[string]interface{}
					t.NoError(util.UnmarshalJSON(b, &m))

					v, found := m["A"]
					t.True(found)
					t.Equal(extendedValue, v)
				})
			}

			return writer.setStates(ctx, index, states, op)
		}

		args := t.newargs(newwriterf)
		args.GetOperationFunc = func(_ context.Context, oph, fact util.Hash) (base.Operation, error) {
			return ops[oph.String()], nil
		}

		opp, _ := NewDefaultProposalProcessor(pr, previous, args)

		m, err := opp.Process(context.Background(), ivp)
		t.NoError(err)
		t.NotNil(m)

		t.Equal(len(ops), writer.sts.Len())

		afact := t.NewACCEPTBallotFact(point.NextHeight(), pr.Fact().Hash(), m.Hash())
		avp, err := t.NewACCEPTVoteproof(afact, t.Local, []base.LocalNode{t.Local})
		t.NoError(err)

		_, err = opp.Save(context.Background(), avp)
		t.NoError(err)

		select {
		case <-time.After(time.Second * 2):
			t.Fail("failed to wait to save")
		case <-savech:
		}
	})

	t.Run("different manifest hash with majority", func() {
		writer, newwriterf := t.newBlockWriter()
		writer.manifest = manifest

		args := t.newargs(newwriterf)
		args.GetOperationFunc = func(_ context.Context, oph, fact util.Hash) (base.Operation, error) {
			return ops[oph.String()], nil
		}

		opp, _ := NewDefaultProposalProcessor(pr, previous, args)

		m, err := opp.Process(context.Background(), ivp)
		t.NoError(err)
		t.NotNil(m)

		// wrong avp
		afact := t.NewACCEPTBallotFact(point.NextHeight(), pr.Fact().Hash(), valuehash.RandomSHA256())
		avp, err := t.NewACCEPTVoteproof(afact, t.Local, []base.LocalNode{t.Local})
		t.NoError(err)

		_, err = opp.Save(context.Background(), avp)
		t.ErrorIs(err, ErrNotProposalProcessorProcessed)
		t.ErrorContains(err, "different manifest hash with majority")

		_, err = opp.Save(context.Background(), avp)
		t.ErrorIs(err, ErrProcessorAlreadySaved)
	})
}

func (t *testDefaultProposalProcessor) TestSaveFailed() {
	point := base.RawPoint(33, 44)

	ophs, ops, _ := t.prepareOperations(point.Height()-1, 4)

	pr := t.newproposal(NewProposalFact(point, t.Local.Address(), valuehash.RandomSHA256(), ophs))

	previous := base.NewDummyManifest(point.Height()-1, valuehash.RandomSHA256())
	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	writer, newwriterf := t.newBlockWriter()
	writer.manifest = manifest

	writer.savef = func(_ context.Context) (base.BlockMap, error) {
		return nil, errors.Errorf("killme")
	}

	args := t.newargs(newwriterf)
	args.GetOperationFunc = func(_ context.Context, oph, fact util.Hash) (base.Operation, error) {
		return ops[oph.String()], nil
	}

	opp, _ := NewDefaultProposalProcessor(pr, previous, args)

	m, err := opp.Process(context.Background(), nil)
	t.NoError(err)
	t.NotNil(m)

	t.Equal(4, writer.sts.Len())

	afact := t.NewACCEPTBallotFact(point.NextHeight(), pr.Fact().Hash(), m.Hash())
	avp, err := t.NewACCEPTVoteproof(afact, t.Local, []base.LocalNode{t.Local})
	t.NoError(err)

	_, err = opp.Save(context.Background(), avp)
	t.Error(err)
	t.ErrorContains(err, "killme")
}

func (t *testDefaultProposalProcessor) TestSaveAgain() {
	point := base.RawPoint(33, 44)

	ophs, ops, _ := t.prepareOperations(point.Height()-1, 4)

	pr := t.newproposal(NewProposalFact(point, t.Local.Address(), valuehash.RandomSHA256(), ophs))

	previous := base.NewDummyManifest(point.Height()-1, valuehash.RandomSHA256())
	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())

	ifact := t.NewINITBallotFact(point.NextHeight(), previous.Hash(), pr.Fact().Hash())
	ivp, err := t.NewINITVoteproof(ifact, t.Local, []base.LocalNode{t.Local})
	t.NoError(err)

	t.Run("no error", func() {
		writer, newwriterf := t.newBlockWriter()
		writer.manifest = manifest

		savech := make(chan struct{}, 1)
		writer.savef = func(_ context.Context) (base.BlockMap, error) {
			savech <- struct{}{}

			return nil, nil
		}

		args := t.newargs(newwriterf)
		args.GetOperationFunc = func(_ context.Context, oph, fact util.Hash) (base.Operation, error) {
			return ops[oph.String()], nil
		}

		opp, _ := NewDefaultProposalProcessor(pr, previous, args)

		m, err := opp.Process(context.Background(), ivp)
		t.NoError(err)
		t.NotNil(m)

		t.Equal(4, writer.sts.Len())

		afact := t.NewACCEPTBallotFact(point.NextHeight(), pr.Fact().Hash(), m.Hash())
		avp, err := t.NewACCEPTVoteproof(afact, t.Local, []base.LocalNode{t.Local})
		t.NoError(err)

		_, err = opp.Save(context.Background(), avp)
		t.NoError(err)

		select {
		case <-time.After(time.Second * 2):
			t.Fail("failed to wait to save")
		case <-savech:
		}

		_, err = opp.Save(context.Background(), avp)
		t.Error(err)
		t.ErrorIs(err, ErrProcessorAlreadySaved)
	})

	t.Run("different manifest hash with majority", func() {
		writer, newwriterf := t.newBlockWriter()
		writer.manifest = manifest

		args := t.newargs(newwriterf)
		args.GetOperationFunc = func(_ context.Context, oph, fact util.Hash) (base.Operation, error) {
			return ops[oph.String()], nil
		}

		opp, _ := NewDefaultProposalProcessor(pr, previous, args)

		m, err := opp.Process(context.Background(), ivp)
		t.NoError(err)
		t.NotNil(m)

		t.Equal(4, writer.sts.Len())

		// wrong avp
		afact := t.NewACCEPTBallotFact(point.NextHeight(), pr.Fact().Hash(), valuehash.RandomSHA256())
		avp, err := t.NewACCEPTVoteproof(afact, t.Local, []base.LocalNode{t.Local})
		t.NoError(err)

		_, err = opp.Save(context.Background(), avp)
		t.ErrorIs(err, ErrNotProposalProcessorProcessed)
		t.ErrorContains(err, "different manifest hash with majority")

		_, err = opp.Save(context.Background(), avp)
		t.ErrorIs(err, ErrProcessorAlreadySaved)
	})
}

func (t *testDefaultProposalProcessor) TestEmptyCollectOperationsEmptyProposalNoBlock() {
	point := base.RawPoint(33, 44)

	pr := t.newproposal(NewProposalFact(point, t.Local.Address(), valuehash.RandomSHA256(), nil))

	previous := base.NewDummyManifest(point.Height()-1, valuehash.RandomSHA256())
	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	writer, newwriterf := t.newBlockWriter()
	writer.manifest = manifest

	t.Run("false EmptyProposalNoBlock", func() {
		args := t.newargs(newwriterf)

		args.GetOperationFunc = func(_ context.Context, oph, fact util.Hash) (base.Operation, error) {
			return nil, ErrOperationNotFoundInProcessor.Errorf("operation not found")
		}

		opp, _ := NewDefaultProposalProcessor(pr, previous, args)

		m, err := opp.Process(context.Background(), nil)
		t.NoError(err)
		t.NotNil(m)
	})

	t.Run("EmptyProposalNoBlock", func() {
		args := t.newargs(newwriterf)
		args.EmptyProposalNoBlockFunc = func() bool {
			return true
		}

		args.GetOperationFunc = func(_ context.Context, oph, fact util.Hash) (base.Operation, error) {
			return nil, ErrOperationNotFoundInProcessor.Errorf("operation not found")
		}

		opp, _ := NewDefaultProposalProcessor(pr, previous, args)

		m, err := opp.Process(context.Background(), nil)
		t.Error(err)
		t.Nil(m)
		t.ErrorIs(err, ErrProposalProcessorEmptyOperations)
	})

	t.Run("EmptyProposalNoBlock, but not empty", func() {
		ophs, ops, _ := t.prepareOperations(point.Height()-1, 4)

		pr := t.newproposal(NewProposalFact(point, t.Local.Address(), valuehash.RandomSHA256(), ophs))

		args := t.newargs(newwriterf)
		args.EmptyProposalNoBlockFunc = func() bool {
			return true
		}

		var copslock sync.Mutex
		var cops []base.Operation

		args.GetOperationFunc = func(_ context.Context, oph, fact util.Hash) (base.Operation, error) {
			op, found := ops[oph.String()]
			if !found {
				return nil, ErrOperationNotFoundInProcessor.Errorf("operation not found")
			}

			copslock.Lock()
			cops = append(cops, op)
			copslock.Unlock()

			return op, nil
		}

		opp, _ := NewDefaultProposalProcessor(pr, previous, args)

		m, err := opp.Process(context.Background(), nil)
		t.NoError(err)
		t.NotNil(m)

		t.Equal(len(ophs), len(cops))
	})
}

func (t *testDefaultProposalProcessor) TestEmptyAfterProcessEmptyProposalNoBlock() {
	point := base.RawPoint(33, 44)
	ophs, ops, _ := t.prepareOperations(point.Height()-1, 4)

	previous := base.NewDummyManifest(point.Height()-1, valuehash.RandomSHA256())
	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	pr := t.newproposal(NewProposalFact(point, t.Local.Address(), valuehash.RandomSHA256(), ophs))

	t.Run("false EmptyProposalNoBlockFunc", func() {
		writer, newwriterf := t.newBlockWriter()
		writer.manifest = manifest
		args := t.newargs(newwriterf)
		args.GetOperationFunc = func(_ context.Context, oph, fact util.Hash) (base.Operation, error) {
			op, found := ops[oph.String()]
			if !found {
				return nil, ErrOperationNotFoundInProcessor.WithStack()
			}

			return op, nil
		}

		args.NewOperationProcessorFunc = func(_ base.Height, ht hint.Hint, getStatef base.GetStateFunc) (base.OperationProcessor, error) {
			if !ht.IsCompatible(DummyOperationHint) {
				return nil, nil
			}

			return &DummyOperationProcessor{
				preprocess: func(ctx context.Context, op base.Operation, _ base.GetStateFunc) (context.Context, base.OperationProcessReasonError, error) {
					return ctx, base.NewBaseOperationProcessReason("ignore"), nil
				},
				process: func(_ context.Context, op base.Operation, _ base.GetStateFunc) ([]base.StateMergeValue, base.OperationProcessReasonError, error) {
					return nil, base.ErrNotChangedOperationProcessReason, nil
				},
			}, nil
		}

		opp, _ := NewDefaultProposalProcessor(pr, previous, args)
		m, err := opp.Process(context.Background(), nil)
		t.NoError(err)
		t.NotNil(m)
	})

	t.Run("EmptyProposalNoBlockFunc", func() {
		writer, newwriterf := t.newBlockWriter()
		writer.manifest = manifest
		args := t.newargs(newwriterf)
		args.GetOperationFunc = func(_ context.Context, oph, fact util.Hash) (base.Operation, error) {
			op, found := ops[oph.String()]
			if !found {
				return nil, ErrOperationNotFoundInProcessor.WithStack()
			}

			return op, nil
		}

		args.NewOperationProcessorFunc = func(_ base.Height, ht hint.Hint, getStatef base.GetStateFunc) (base.OperationProcessor, error) {
			if !ht.IsCompatible(DummyOperationHint) {
				return nil, nil
			}

			return &DummyOperationProcessor{
				preprocess: func(ctx context.Context, op base.Operation, _ base.GetStateFunc) (context.Context, base.OperationProcessReasonError, error) {
					return ctx, nil, ErrSuspendOperation
				},
				process: func(_ context.Context, op base.Operation, _ base.GetStateFunc) ([]base.StateMergeValue, base.OperationProcessReasonError, error) {
					return nil, nil, nil
				},
			}, nil
		}

		args.EmptyProposalNoBlockFunc = func() bool {
			return true
		}

		opp, _ := NewDefaultProposalProcessor(pr, previous, args)
		m, err := opp.Process(context.Background(), nil)
		t.Error(err)
		t.Nil(m)
		t.ErrorIs(err, ErrProposalProcessorEmptyOperations)
	})
}

func TestDefaultProposalProcessor(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testDefaultProposalProcessor))
}
