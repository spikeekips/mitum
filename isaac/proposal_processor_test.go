package isaac

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

var (
	DummyOperationFactHint = hint.MustNewHint("dummy-operation-fact-v0.0.1")
	DummyOperationHint     = hint.MustNewHint("dummy-operation-v0.0.1")
)

type DummyOperationFact struct {
	h     util.Hash
	token base.Token
	v     util.Byter
}

func NewDummyOperationFact(token base.Token, v util.Byter) DummyOperationFact {
	fact := DummyOperationFact{
		token: token,
		v:     v,
	}
	fact.h = fact.hash()

	return fact
}

func (fact DummyOperationFact) Hint() hint.Hint {
	return DummyOperationFactHint
}

func (fact DummyOperationFact) IsValid([]byte) error {
	if err := util.CheckIsValid(nil, false, fact.h, fact.token); err != nil {
		return util.InvalidError.Wrapf(err, "invalid DummyOperationFact")
	}

	if !fact.h.Equal(fact.hash()) {
		return util.InvalidError.Errorf("DummyOperationFact hash does not match")
	}

	return nil
}

func (fact DummyOperationFact) Hash() util.Hash {
	return fact.h
}

func (fact DummyOperationFact) Token() base.Token {
	return fact.token
}

func (fact DummyOperationFact) hash() util.Hash {
	return valuehash.NewSHA256(util.ConcatByters(fact.v, util.BytesToByter(fact.token)))
}

type DummyOperation struct {
	fact   DummyOperationFact
	signed base.BaseSigned
}

func NewDummyOperation(fact DummyOperationFact, priv base.Privatekey, networkID base.NetworkID) (DummyOperation, error) {
	signed, err := base.BaseSignedFromFact(
		priv,
		networkID,
		fact,
	)
	if err != nil {
		return DummyOperation{}, errors.Wrap(err, "failed to sign DummyOperation")
	}

	return DummyOperation{fact: fact, signed: signed}, nil
}

func (op DummyOperation) Hint() hint.Hint {
	return DummyOperationHint
}

func (op DummyOperation) Signed() []base.Signed {
	return []base.Signed{op.signed}
}

func (op DummyOperation) Fact() base.Fact {
	return op.fact
}

func (op DummyOperation) HashBytes() []byte {
	return op.fact.h.Bytes()
}

func (op DummyOperation) IsValid([]byte) error {
	if err := op.fact.IsValid(nil); err != nil {
		return err
	}

	return nil
}

type DummyOperationProcessable struct {
	DummyOperation
	preprocess func(context.Context, base.StatePool) (bool, error)
	process    func(context.Context, base.StatePool) ([]base.State, error)
}

func NewDummyOperationProcessable(fact DummyOperationFact, priv base.Privatekey, networkID base.NetworkID) (DummyOperationProcessable, error) {
	op, err := NewDummyOperation(fact, priv, networkID)
	if err != nil {
		return DummyOperationProcessable{}, err
	}

	return DummyOperationProcessable{
		DummyOperation: op,
	}, nil
}

func (op DummyOperationProcessable) PreProcess(ctx context.Context, sp base.StatePool) (bool, error) {
	if op.preprocess == nil {
		return true, nil
	}

	return op.preprocess(ctx, sp)
}

func (op DummyOperationProcessable) Process(ctx context.Context, sp base.StatePool) ([]base.State, error) {
	if op.process == nil {
		return nil, nil
	}

	return op.process(ctx, sp)
}

type DummyBlockDataWriter struct {
	sync.RWMutex
	height      base.Height
	manifest    base.Manifest
	manifesterr error
	ophs        []util.Hash
	ops         []base.Operation
	sts         *util.LockedMap
}

func NewDummyBlockDataWriter(height base.Height) *DummyBlockDataWriter {
	return &DummyBlockDataWriter{
		height: height,
		sts:    util.NewLockedMap(),
	}
}

func (w *DummyBlockDataWriter) SetProposalOperations(ophs []util.Hash) error {
	w.Lock()
	defer w.Unlock()

	w.ophs = ophs

	return nil
}

func (w *DummyBlockDataWriter) SetOperation(i int, op base.Operation) error {
	w.Lock()
	defer w.Unlock()

	if len(w.ops) < 1 {
		w.ops = make([]base.Operation, len(w.ophs))
	}

	w.ops[i] = op

	return nil
}

func (w *DummyBlockDataWriter) SetStates(states []base.State, operation util.Hash) error {
	e := util.StringErrorFunc("failed to set states")

	for i := range states {
		st := states[i]

		j, _, _ := w.sts.Get(st.Key(), func() (interface{}, error) {
			return st.Merger(w.height), nil
		})
		merger := j.(base.StateValueMerger)

		if err := merger.Merge(st.Value(), []util.Hash{operation}); err != nil {
			return e(err, "failed to merge")
		}
	}

	return nil
}

func (w *DummyBlockDataWriter) SetManifest(base.Manifest) error {
	return nil
}

func (w *DummyBlockDataWriter) Manifest(context.Context) (base.Manifest, error) {
	return w.manifest, w.manifesterr
}

func (w *DummyBlockDataWriter) Save(context.Context, base.ACCEPTVoteproof) error {
	return nil
}

func (w *DummyBlockDataWriter) Cancel() error {
	return nil
}

type DummyOperationProcessor struct {
	preprocess func(context.Context, base.Operation, base.StatePool) (bool, error)
	process    func(context.Context, base.Operation, base.StatePool) ([]base.State, error)
}

func (p *DummyOperationProcessor) PreProcess(ctx context.Context, op base.Operation, sp base.StatePool) (bool, error) {
	if p.preprocess == nil {
		return false, nil
	}

	return p.preprocess(ctx, op, sp)
}

func (p *DummyOperationProcessor) Process(ctx context.Context, op base.Operation, sp base.StatePool) ([]base.State, error) {
	if p.process == nil {
		return nil, nil
	}

	return p.process(ctx, op, sp)
}

type testDefaultProposalProcessor struct {
	baseStateTestHandler
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
		)
	}

	return stts
}

func (t *testDefaultProposalProcessor) TestNew() {
	point := base.RawPoint(33, 44)

	fact := NewProposalFact(point, t.local.Address(), []util.Hash{valuehash.RandomSHA256()})

	opp := NewDefaultProposalProcessor(fact, nil, nil, nil, nil)
	_ = (interface{})(opp).(proposalProcessor)

	base.EqualProposalFact(t.Assert(), fact, opp.Proposal())
}

func (t *testDefaultProposalProcessor) TestCollectOperations() {
	point := base.RawPoint(33, 44)

	ops := map[string]base.Operation{}
	ophs := make([]util.Hash, 3)
	for i := range ophs {
		fact := NewDummyOperationFact(util.UUID().Bytes(), valuehash.RandomSHA256())
		op, err := NewDummyOperationProcessable(fact, t.local.Privatekey(), t.policy.NetworkID())
		t.NoError(err)

		ops[fact.Hash().String()] = op
		ophs[i] = fact.Hash()
	}

	fact := NewProposalFact(point, t.local.Address(), ophs)

	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	writer := &DummyBlockDataWriter{manifest: manifest}

	opp := NewDefaultProposalProcessor(fact, writer, nil, func(_ context.Context, facthash util.Hash) (base.Operation, error) {
		op, found := ops[facthash.String()]
		if !found {
			return nil, util.NotFoundError.Errorf("operation not found")
		}

		return op, nil
	},
		func(hint.Hint) (base.OperationProcessor, bool) { return nil, false },
	)

	m, err := opp.Process(context.Background())
	t.NoError(err)
	t.NotNil(m)

	for i := range ophs {
		a := ops[ophs[i].String()]
		b := writer.ops[i]

		t.NotNil(a)
		t.NotNil(b)

		base.EqualSignedFact(t.Assert(), a, b)
	}
}

func (t *testDefaultProposalProcessor) TestCollectOperationsFailed() {
	point := base.RawPoint(33, 44)

	ops := map[string]base.Operation{}
	ophs := make([]util.Hash, 4)
	for i := range ophs {
		fact := NewDummyOperationFact(util.UUID().Bytes(), valuehash.RandomSHA256())
		op, err := NewDummyOperationProcessable(fact, t.local.Privatekey(), t.policy.NetworkID())
		t.NoError(err)

		ophs[i] = fact.Hash()
		if i != 1 && i != 3 {
			ops[fact.Hash().String()] = op
		}
	}

	fact := NewProposalFact(point, t.local.Address(), ophs)

	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	writer := &DummyBlockDataWriter{manifest: manifest}

	opp := NewDefaultProposalProcessor(fact, writer, nil, func(_ context.Context, facthash util.Hash) (base.Operation, error) {
		op, found := ops[facthash.String()]
		if !found {
			return nil, util.NotFoundError.Errorf("operation not found")
		}

		return op, nil
	}, nil)

	m, err := opp.Process(context.Background())
	t.Error(err)
	t.Nil(m)

	t.True(errors.Is(err, util.NotFoundError))
	t.Contains(err.Error(), "failed to collect operations")
}

func (t *testDefaultProposalProcessor) TestCollectOperationsFailedButIgnored() {
	point := base.RawPoint(33, 44)

	ops := map[string]base.Operation{}
	ophs := make([]util.Hash, 4)
	for i := range ophs {
		fact := NewDummyOperationFact(util.UUID().Bytes(), valuehash.RandomSHA256())
		op, err := NewDummyOperationProcessable(fact, t.local.Privatekey(), t.policy.NetworkID())
		t.NoError(err)

		ophs[i] = fact.Hash()
		if i != 1 && i != 3 {
			ops[fact.Hash().String()] = op
		}
	}

	fact := NewProposalFact(point, t.local.Address(), ophs)

	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	writer := &DummyBlockDataWriter{manifest: manifest}

	opp := NewDefaultProposalProcessor(fact, writer, nil, func(_ context.Context, facthash util.Hash) (base.Operation, error) {
		op, found := ops[facthash.String()]
		if !found {
			return nil, IgnoreOperationInProcessorError.Errorf("operation not found")
		}

		return op, nil
	},
		func(hint.Hint) (base.OperationProcessor, bool) { return nil, false },
	)

	m, err := opp.Process(context.Background())
	t.NoError(err)
	t.NotNil(m)

	for i := range ophs {
		b := writer.ops[i]
		if i == 1 || i == 3 {
			t.Nil(b)

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

	ops := map[string]base.Operation{}
	ophs := make([]util.Hash, 4)
	sts := map[string]base.State{}

	for i := range ophs {
		i := i

		fact := NewDummyOperationFact(util.UUID().Bytes(), valuehash.RandomSHA256())
		op, err := NewDummyOperation(fact, t.local.Privatekey(), t.policy.NetworkID()) // NOTE not processable
		t.NoError(err)

		ophs[i] = fact.Hash()
		st := t.newStates(point.Height(), fact.Hash().String())[0]

		ops[fact.Hash().String()] = op
		sts[fact.Hash().String()] = st
	}

	fact := NewProposalFact(point, t.local.Address(), ophs)

	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	writer := NewDummyBlockDataWriter(point.Height() + 1)
	writer.manifest = manifest

	opp := NewDefaultProposalProcessor(fact, writer, nil, func(_ context.Context, facthash util.Hash) (base.Operation, error) {
		op, found := ops[facthash.String()]
		if !found {
			return nil, IgnoreOperationInProcessorError.Errorf("operation not found")
		}

		return op, nil
	},
		func(ht hint.Hint) (base.OperationProcessor, bool) {
			if !ht.IsCompatible(DummyOperationHint) {
				return nil, false
			}

			return &DummyOperationProcessor{
				preprocess: func(_ context.Context, op base.Operation, _ base.StatePool) (bool, error) {
					switch h := op.Fact().Hash(); {
					case h.Equal(ophs[1]),
						h.Equal(ophs[3]): // NOTE only will process, index 1 and 3 operation
						return true, nil
					default:
						return false, nil
					}
				},
				process: func(_ context.Context, op base.Operation, _ base.StatePool) ([]base.State, error) {
					return []base.State{sts[op.Fact().Hash().String()]}, nil
				},
			}, true
		},
	)

	m, err := opp.Process(context.Background())
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

		bops := bst.Operations()
		t.Equal(1, len(bops))
		t.True(h.Equal(bops[0]))
	}
}

func (t *testDefaultProposalProcessor) TestPreProcess() {
	point := base.RawPoint(33, 44)

	ops := map[string]base.Operation{}
	ophs := make([]util.Hash, 4)
	sts := map[string]base.State{}

	for i := range ophs {
		i := i

		fact := NewDummyOperationFact(util.UUID().Bytes(), valuehash.RandomSHA256())
		op, err := NewDummyOperationProcessable(fact, t.local.Privatekey(), t.policy.NetworkID())
		t.NoError(err)

		ophs[i] = fact.Hash()
		st := t.newStates(point.Height(), fact.Hash().String())[0]

		op.preprocess = func(ctx context.Context, _ base.StatePool) (bool, error) {
			return i == 1 || i == 3, nil // NOTE only will process, index 1 and 3 operation
		}
		op.process = func(ctx context.Context, _ base.StatePool) ([]base.State, error) {
			return []base.State{st}, nil
		}

		ops[fact.Hash().String()] = op
		sts[fact.Hash().String()] = st
	}

	fact := NewProposalFact(point, t.local.Address(), ophs)

	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	writer := NewDummyBlockDataWriter(point.Height() + 1)
	writer.manifest = manifest

	opp := NewDefaultProposalProcessor(fact, writer, nil, func(_ context.Context, facthash util.Hash) (base.Operation, error) {
		op, found := ops[facthash.String()]
		if !found {
			return nil, IgnoreOperationInProcessorError.Errorf("operation not found")
		}

		return op, nil
	},
		func(hint.Hint) (base.OperationProcessor, bool) { return nil, false },
	)

	m, err := opp.Process(context.Background())
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

		bops := bst.Operations()
		t.Equal(1, len(bops))
		t.True(h.Equal(bops[0]))
	}
}

func (t *testDefaultProposalProcessor) TestPreProcessButError() {
	point := base.RawPoint(33, 44)

	ops := map[string]base.Operation{}
	ophs := make([]util.Hash, 4)

	for i := range ophs {
		i := i

		fact := NewDummyOperationFact(util.UUID().Bytes(), valuehash.RandomSHA256())
		op, err := NewDummyOperationProcessable(fact, t.local.Privatekey(), t.policy.NetworkID())
		t.NoError(err)

		ophs[i] = fact.Hash()
		st := t.newStates(point.Height(), fact.Hash().String())[0]

		op.preprocess = func(ctx context.Context, _ base.StatePool) (bool, error) {
			if i == 1 {
				return true, errors.Errorf("findme: %q", fact.Hash())
			}

			return true, nil
		}
		op.process = func(ctx context.Context, _ base.StatePool) ([]base.State, error) {
			return []base.State{st}, nil
		}

		ops[fact.Hash().String()] = op
	}

	fact := NewProposalFact(point, t.local.Address(), ophs)

	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	writer := NewDummyBlockDataWriter(point.Height() + 1)
	writer.manifest = manifest

	opp := NewDefaultProposalProcessor(fact, writer, nil, func(_ context.Context, facthash util.Hash) (base.Operation, error) {
		op, found := ops[facthash.String()]
		if !found {
			return nil, IgnoreOperationInProcessorError.Errorf("operation not found")
		}

		return op, nil
	},
		func(hint.Hint) (base.OperationProcessor, bool) { return nil, false },
	)

	m, err := opp.Process(context.Background())
	t.Error(err)
	t.Nil(m)

	t.Contains(err.Error(), fmt.Sprintf("findme: %q", ophs[1]))
}

func (t *testDefaultProposalProcessor) TestPreProcessContextCancel() {
	point := base.RawPoint(33, 44)

	ops := map[string]base.Operation{}
	ophs := make([]util.Hash, 4)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	startprocessedch := make(chan struct{}, 1)
	var startprocessedonece sync.Once
	for i := range ophs {
		i := i

		fact := NewDummyOperationFact(util.UUID().Bytes(), valuehash.RandomSHA256())
		op, err := NewDummyOperationProcessable(fact, t.local.Privatekey(), t.policy.NetworkID())
		t.NoError(err)

		ophs[i] = fact.Hash()
		st := t.newStates(point.Height(), fact.Hash().String())[0]

		op.preprocess = func(ctx context.Context, _ base.StatePool) (bool, error) {
			defer startprocessedonece.Do(func() {
				startprocessedch <- struct{}{}
			})

			if i == 1 {
				select {
				case <-time.After(time.Minute):
				case <-ctx.Done():
					return false, ctx.Err()
				}
			}

			return true, nil
		}
		op.process = func(ctx context.Context, _ base.StatePool) ([]base.State, error) {
			return []base.State{st}, nil
		}

		ops[fact.Hash().String()] = op
	}

	fact := NewProposalFact(point, t.local.Address(), ophs)

	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	writer := NewDummyBlockDataWriter(point.Height() + 1)
	writer.manifest = manifest

	opp := NewDefaultProposalProcessor(fact, writer, nil, func(_ context.Context, facthash util.Hash) (base.Operation, error) {
		op, found := ops[facthash.String()]
		if !found {
			return nil, IgnoreOperationInProcessorError.Errorf("operation not found")
		}

		return op, nil
	},
		func(hint.Hint) (base.OperationProcessor, bool) { return nil, false },
	)

	donech := make(chan error)
	go func() {
		m, err := opp.Process(ctx)
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

	ops := map[string]base.Operation{}
	ophs := make([]util.Hash, 4)
	sts := map[string]base.State{}

	for i := range ophs {
		i := i

		fact := NewDummyOperationFact(util.UUID().Bytes(), valuehash.RandomSHA256())

		ophs[i] = fact.Hash()

		var op base.Operation
		if i == 1 || i == 3 {
			op, _ = NewDummyOperation(fact, t.local.Privatekey(), t.policy.NetworkID())
		} else {
			j, _ := NewDummyOperationProcessable(fact, t.local.Privatekey(), t.policy.NetworkID())
			j.preprocess = func(ctx context.Context, _ base.StatePool) (bool, error) {
				return true, nil
			}

			st := t.newStates(point.Height(), fact.Hash().String())[0]
			sts[fact.Hash().String()] = st

			j.process = func(ctx context.Context, _ base.StatePool) ([]base.State, error) {
				return []base.State{st}, nil
			}

			op = j
		}

		ops[fact.Hash().String()] = op
	}

	fact := NewProposalFact(point, t.local.Address(), ophs)

	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	writer := NewDummyBlockDataWriter(point.Height() + 1)
	writer.manifest = manifest

	opp := NewDefaultProposalProcessor(fact, writer, nil, func(_ context.Context, facthash util.Hash) (base.Operation, error) {
		op, found := ops[facthash.String()]
		if !found {
			return nil, IgnoreOperationInProcessorError.Errorf("operation not found")
		}

		return op, nil
	},
		func(hint.Hint) (base.OperationProcessor, bool) { return nil, false },
	)

	m, err := opp.Process(context.Background())
	t.NoError(err)
	t.NotNil(m)

	t.Equal(2, writer.sts.Len())

	for i := range ophs {
		h := ophs[i]
		st, found := sts[h.String()]
		if i == 1 || i == 3 {
			t.False(found)

			continue
		}

		var bst base.State
		writer.sts.Traverse(func(_, v interface{}) bool {
			k := v.(base.State)
			if k.Key() == st.Key() {
				bst = k

				return false
			}

			return true
		})

		t.NotNil(bst)

		bops := bst.Operations()
		t.Equal(1, len(bops))
		t.True(h.Equal(bops[0]))
	}
}

func (t *testDefaultProposalProcessor) TestProcess() {
	point := base.RawPoint(33, 44)

	ops := map[string]base.Operation{}
	ophs := make([]util.Hash, 4)
	sts := map[string]base.State{}

	for i := range ophs {
		i := i

		fact := NewDummyOperationFact(util.UUID().Bytes(), valuehash.RandomSHA256())
		op, err := NewDummyOperationProcessable(fact, t.local.Privatekey(), t.policy.NetworkID())
		t.NoError(err)

		ophs[i] = fact.Hash()
		st := t.newStates(point.Height(), fact.Hash().String())[0]

		op.preprocess = func(ctx context.Context, _ base.StatePool) (bool, error) {
			return true, nil
		}
		op.process = func(ctx context.Context, _ base.StatePool) ([]base.State, error) {
			if i == 1 || i == 3 { // NOTE only will process, index 1 and 3 operation
				return []base.State{st}, nil
			}

			return nil, nil
		}

		ops[fact.Hash().String()] = op
		sts[fact.Hash().String()] = st
	}

	fact := NewProposalFact(point, t.local.Address(), ophs)

	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	writer := NewDummyBlockDataWriter(point.Height() + 1)
	writer.manifest = manifest

	opp := NewDefaultProposalProcessor(fact, writer, nil, func(_ context.Context, facthash util.Hash) (base.Operation, error) {
		op, found := ops[facthash.String()]
		if !found {
			return nil, IgnoreOperationInProcessorError.Errorf("operation not found")
		}

		return op, nil
	},
		func(hint.Hint) (base.OperationProcessor, bool) { return nil, false },
	)

	m, err := opp.Process(context.Background())
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

		bops := bst.Operations()
		t.Equal(1, len(bops))
		t.True(h.Equal(bops[0]))
	}
}

func (t *testDefaultProposalProcessor) TestProcessWithOperationProcessor() {
	point := base.RawPoint(33, 44)

	ops := map[string]base.Operation{}
	ophs := make([]util.Hash, 4)
	sts := map[string]base.State{}

	for i := range ophs {
		i := i

		fact := NewDummyOperationFact(util.UUID().Bytes(), valuehash.RandomSHA256())
		op, err := NewDummyOperation(fact, t.local.Privatekey(), t.policy.NetworkID()) // NOTE not processable
		t.NoError(err)

		ophs[i] = fact.Hash()
		st := t.newStates(point.Height(), fact.Hash().String())[0]

		ops[fact.Hash().String()] = op
		sts[fact.Hash().String()] = st
	}

	fact := NewProposalFact(point, t.local.Address(), ophs)

	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	writer := NewDummyBlockDataWriter(point.Height() + 1)
	writer.manifest = manifest

	opp := NewDefaultProposalProcessor(fact, writer, nil, func(_ context.Context, facthash util.Hash) (base.Operation, error) {
		op, found := ops[facthash.String()]
		if !found {
			return nil, IgnoreOperationInProcessorError.Errorf("operation not found")
		}

		return op, nil
	},
		func(ht hint.Hint) (base.OperationProcessor, bool) {
			if !ht.IsCompatible(DummyOperationHint) {
				return nil, false
			}

			return &DummyOperationProcessor{
				preprocess: func(_ context.Context, op base.Operation, _ base.StatePool) (bool, error) {
					return true, nil
				},
				process: func(_ context.Context, op base.Operation, _ base.StatePool) ([]base.State, error) {
					switch h := op.Fact().Hash(); {
					case h.Equal(ophs[1]),
						h.Equal(ophs[3]): // NOTE only will process, index 1 and 3 operation
						return []base.State{sts[op.Fact().Hash().String()]}, nil
					default:
						return nil, nil
					}
				},
			}, true
		},
	)

	m, err := opp.Process(context.Background())
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

		bops := bst.Operations()
		t.Equal(1, len(bops))
		t.True(h.Equal(bops[0]))
	}
}

func (t *testDefaultProposalProcessor) TestProcessButError() {
	point := base.RawPoint(33, 44)

	ops := map[string]base.Operation{}
	ophs := make([]util.Hash, 4)

	for i := range ophs {
		i := i

		fact := NewDummyOperationFact(util.UUID().Bytes(), valuehash.RandomSHA256())
		op, err := NewDummyOperationProcessable(fact, t.local.Privatekey(), t.policy.NetworkID())
		t.NoError(err)

		ophs[i] = fact.Hash()
		st := t.newStates(point.Height(), fact.Hash().String())[0]

		op.preprocess = func(ctx context.Context, _ base.StatePool) (bool, error) {
			return true, nil
		}
		op.process = func(ctx context.Context, _ base.StatePool) ([]base.State, error) {
			if i == 1 {
				return nil, errors.Errorf("findme: %q", fact.Hash())
			}

			return []base.State{st}, nil
		}

		ops[fact.Hash().String()] = op
	}

	fact := NewProposalFact(point, t.local.Address(), ophs)

	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	writer := NewDummyBlockDataWriter(point.Height() + 1)
	writer.manifest = manifest

	opp := NewDefaultProposalProcessor(fact, writer, nil, func(_ context.Context, facthash util.Hash) (base.Operation, error) {
		op, found := ops[facthash.String()]
		if !found {
			return nil, IgnoreOperationInProcessorError.Errorf("operation not found")
		}

		return op, nil
	},
		func(hint.Hint) (base.OperationProcessor, bool) { return nil, false },
	)

	m, err := opp.Process(context.Background())
	t.Error(err)
	t.Nil(m)

	t.Contains(err.Error(), fmt.Sprintf("findme: %q", ophs[1]))
}

func (t *testDefaultProposalProcessor) TestProcessContextCancel() {
	point := base.RawPoint(33, 44)

	ops := map[string]base.Operation{}
	ophs := make([]util.Hash, 4)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	startprocessedch := make(chan struct{}, 1)
	var startprocessedonece sync.Once
	for i := range ophs {
		i := i

		fact := NewDummyOperationFact(util.UUID().Bytes(), valuehash.RandomSHA256())
		op, err := NewDummyOperationProcessable(fact, t.local.Privatekey(), t.policy.NetworkID())
		t.NoError(err)

		ophs[i] = fact.Hash()
		st := t.newStates(point.Height(), fact.Hash().String())[0]

		op.preprocess = func(ctx context.Context, _ base.StatePool) (bool, error) {
			return true, nil
		}

		op.process = func(ctx context.Context, _ base.StatePool) ([]base.State, error) {
			defer startprocessedonece.Do(func() {
				startprocessedch <- struct{}{}
			})

			if i == 1 {
				select {
				case <-time.After(time.Minute):
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}

			return []base.State{st}, nil
		}

		ops[fact.Hash().String()] = op
	}

	fact := NewProposalFact(point, t.local.Address(), ophs)

	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	writer := NewDummyBlockDataWriter(point.Height() + 1)
	writer.manifest = manifest

	opp := NewDefaultProposalProcessor(fact, writer, nil, func(_ context.Context, facthash util.Hash) (base.Operation, error) {
		op, found := ops[facthash.String()]
		if !found {
			return nil, IgnoreOperationInProcessorError.Errorf("operation not found")
		}

		return op, nil
	},
		func(hint.Hint) (base.OperationProcessor, bool) { return nil, false },
	)

	donech := make(chan error)
	go func() {
		m, err := opp.Process(ctx)
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

	ops := map[string]base.Operation{}
	ophs := make([]util.Hash, 4)

	startprocessedch := make(chan struct{}, 1)
	var startprocessedonece sync.Once
	for i := range ophs {
		i := i

		fact := NewDummyOperationFact(util.UUID().Bytes(), valuehash.RandomSHA256())
		op, err := NewDummyOperationProcessable(fact, t.local.Privatekey(), t.policy.NetworkID())
		t.NoError(err)

		ophs[i] = fact.Hash()
		st := t.newStates(point.Height(), fact.Hash().String())[0]

		op.preprocess = func(ctx context.Context, _ base.StatePool) (bool, error) {
			return true, nil
		}

		op.process = func(ctx context.Context, _ base.StatePool) ([]base.State, error) {
			defer startprocessedonece.Do(func() {
				startprocessedch <- struct{}{}
			})

			if i == 1 {
				select {
				case <-time.After(time.Minute):
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}

			return []base.State{st}, nil
		}

		ops[fact.Hash().String()] = op
	}

	fact := NewProposalFact(point, t.local.Address(), ophs)

	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	writer := NewDummyBlockDataWriter(point.Height() + 1)
	writer.manifest = manifest

	opp := NewDefaultProposalProcessor(fact, writer, nil, func(_ context.Context, facthash util.Hash) (base.Operation, error) {
		op, found := ops[facthash.String()]
		if !found {
			return nil, IgnoreOperationInProcessorError.Errorf("operation not found")
		}

		return op, nil
	},
		func(hint.Hint) (base.OperationProcessor, bool) { return nil, false },
	)

	donech := make(chan error)
	go func() {
		m, err := opp.Process(context.Background())
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

func TestDefaultProposalProcessor(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testDefaultProposalProcessor))
}
