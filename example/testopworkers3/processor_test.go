package main

import (
	"cmp"
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
	"gonum.org/v1/gonum/stat/combin"
)

type baseTestOperationProcessor struct {
	baseTestAccountStateMerger
	networkID base.NetworkID
}

func (t *baseTestOperationProcessor) SetupSuite() {
	t.BaseTestSuite.SetupSuite()

	t.networkID = base.NetworkID([]byte("showme"))
}

func (t *baseTestOperationProcessor) newProcessor(ctx context.Context) *OperationProcessor {
	args := NewOperationProcessorArgs(t.networkID)
	args.SaveOperationFunc = func(uint64, Operation) error { return nil }

	opsr, err := NewOperationProcessor(ctx, args)
	t.NoError(err)

	return opsr
}

func (t *baseTestOperationProcessor) newOp(issuer base.Privatekey, receiver base.Publickey, balance int64) Op {
	op := NewOp(NewOpFact(
		util.UUID().Bytes(),
		balance,
		issuer.Publickey(),
		receiver,
	))

	t.NoError(op.Sign(issuer, t.networkID))

	return op
}

func (t *baseTestOperationProcessor) randomOp(balance int64) Op {
	return t.newOp(
		base.NewDummyPrivatekey(util.UUID().String()),
		base.NewDummyPublickey(util.UUID().String()),
		balance,
	)
}

type testOperationProcessor struct {
	baseTestOperationProcessor
}

func (t *testOperationProcessor) TestCancel() {
	t.Run("no process", func() {
		opsr := t.newProcessor(context.Background())
		opsr.Cancel()

		err := opsr.ctx().Err()
		t.Error(err)
		t.ErrorIs(err, context.Canceled)
	})

	t.Run("parent ctx", func() {
		ctx, ctxcancel := context.WithCancel(context.Background())

		opsr := t.newProcessor(ctx)

		ctxcancel()

		err := opsr.ctx().Err()
		t.Error(err)
		t.ErrorIs(err, context.Canceled)
	})

	t.Run("canceled by process error", func() {
		ctx, ctxcancel := context.WithCancel(context.Background())
		defer ctxcancel()

		opsr := t.newProcessor(ctx)
		opsr.args.GetOperationFunc = func(ctx context.Context, operation util.Hash) (Operation, bool, error) {
			return nil, false, errors.Errorf("showme")
		}

		process, done, _, err := opsr.Process()
		t.NoError(err)

		for i := uint64(0); i < 333; i++ {
			err := process(i, valuehash.RandomSHA256(), valuehash.RandomSHA256())
			if err != nil {
				t.ErrorContains(context.Cause(opsr.ctx()), "showme")
			}
		}

		done()

		err = opsr.Wait()
		t.Error(err)
		t.ErrorContains(context.Cause(opsr.ctx()), "showme")

		err = opsr.ctx().Err()
		t.Error(err)
		t.ErrorIs(err, context.Canceled)
		t.ErrorContains(context.Cause(opsr.ctx()), "showme")
	})

	t.Run("cancel() of process before done", func() {
		ctx, ctxcancel := context.WithCancel(context.Background())
		defer ctxcancel()

		opsr := t.newProcessor(ctx)
		opsr.args.GetOperationFunc = func(ctx context.Context, operation util.Hash) (Operation, bool, error) {
			select {
			case <-ctx.Done():
				return nil, false, ctx.Err()
			case <-time.After(time.Second * 333):
			}

			return nil, false, nil
		}

		process, _, cancel, err := opsr.Process()
		t.NoError(err)

		for i := uint64(0); i < 33; i++ {
			t.NoError(process(i, valuehash.RandomSHA256(), valuehash.RandomSHA256()))
		}
		cancel(nil)

		err = opsr.ctx().Err()
		t.Error(err)
		t.ErrorIs(err, context.Canceled)
	})

	t.Run("cancel() of process after done", func() {
		ctx, ctxcancel := context.WithCancel(context.Background())
		defer ctxcancel()

		opsr := t.newProcessor(ctx)
		opsr.args.GetOperationFunc = func(ctx context.Context, operation util.Hash) (Operation, bool, error) {
			select {
			case <-ctx.Done():
				return nil, false, ctx.Err()
			case <-time.After(time.Second * 333):
			}

			return nil, false, nil
		}

		process, done, cancel, err := opsr.Process()
		t.NoError(err)

		for i := uint64(0); i < 33; i++ {
			t.NoError(process(i, valuehash.RandomSHA256(), valuehash.RandomSHA256()))
		}

		done()

		cancel(nil)

		err = opsr.ctx().Err()
		t.Error(err)
		t.ErrorIs(err, context.Canceled)
	})

	t.Run("cancel() of parent ctx", func() {
		ctx, ctxcancel := context.WithCancel(context.Background())
		defer ctxcancel()

		opsr := t.newProcessor(ctx)
		opsr.args.GetOperationFunc = func(ctx context.Context, operation util.Hash) (Operation, bool, error) {
			select {
			case <-ctx.Done():
				return nil, false, ctx.Err()
			case <-time.After(time.Second * 333):
			}

			return nil, false, nil
		}

		process, _, _, err := opsr.Process()
		t.NoError(err)

		for i := uint64(0); i < 33; i++ {
			t.NoError(process(i, valuehash.RandomSHA256(), valuehash.RandomSHA256()))
		}

		ctxcancel()

		err = opsr.ctx().Err()
		t.Error(err)
		t.ErrorIs(err, context.Canceled)
	})
}

func (t *testOperationProcessor) TestWait() {
	t.Run("no operations", func() {
		opsr := t.newProcessor(context.Background())

		waitch := make(chan error, 1)
		go func() {
			waitch <- opsr.Wait()
		}()

		_, done, _, err := opsr.Process()
		t.NoError(err)
		done()

		select {
		case <-time.After(time.Second * 3):
			t.Fail("expected wait done")
		case err := <-waitch:
			t.NoError(err)
		}
	})

	t.Run("canceled without done", func() {
		opsr := t.newProcessor(context.Background())

		waitch := make(chan error, 1)
		go func() {
			waitch <- opsr.Wait()
		}()

		_, _, cancel, err := opsr.Process()
		t.NoError(err)

		<-time.After(time.Second)
		cancel(nil)

		select {
		case <-time.After(time.Second * 3):
			t.Fail("expected wait done")
		case err := <-waitch:
			t.Error(err)
			t.ErrorIs(err, context.Canceled)
		}
	})
}

func (t *testOperationProcessor) testProcess(nops uint64) {
	opsr := t.newProcessor(context.Background())

	opsr.args.NewJobWorker = func(ctx context.Context) (util.JobWorker, error) {
		return util.NewBaseJobWorker(ctx, int64(nops))
	}

	addOps, doneOps := util.SortCompactAppendSlice[uint64](int(nops), cmp.Less[uint64])
	addProcessedOps, doneProcessedOps := util.SortCompactAppendSlice[uint64](int(nops), cmp.Less[uint64])

	ops := map[string]Operation{}
	opsi := map[uint64]Operation{}
	values := map[uint64][]StateMergeValue{}

	for opindex := uint64(0); opindex < nops; opindex++ {
		op := t.randomOp(1)

		ops[op.Hash().String()] = op
		opsi[opindex] = op

		vs, reason, err := op.ProcessNew(context.Background())
		t.NoError(err)
		t.Nil(reason)
		t.NotEmpty(vs)

		values[opindex] = vs
	}

	opsr.args.GetOperationFunc = func(_ context.Context, operation util.Hash) (Operation, bool, error) {
		i, found := ops[operation.String()]

		return i, found, nil
	}

	opsr.args.MergeStateMergeValueFunc = func(opindex uint64, fact util.Hash, vs []StateMergeValue) error {
		addProcessedOps(opindex)

		// check values
		switch expeectedValues, found := values[opindex]; {
		case !found:
			return util.ErrNotFound
		case len(vs) != len(expeectedValues):
			return errors.Errorf("values, %d != expeected, %d)", len(values), len(expeectedValues))
		default:
			return util.TraverseSliceV1(vs, func(_ int, v StateMergeValue) (bool, error) {
				av := v.(AccountStateMergeValue)

				var bv AccountStateMergeValue

				for i := range expeectedValues {
					if j := expeectedValues[i]; j.Key() == av.Key() {
						bv = j.(AccountStateMergeValue)

						break
					}
				}

				switch {
				case av.Key() != bv.Key():
					return false, errors.Errorf("expected value key not found")
				case !av.Address.Equal(bv.Address):
					return false, errors.Errorf("expected value address not found")
				case av.Balance != bv.Balance:
					return false, errors.Errorf("expected value balance not found")
				default:
					return true, nil
				}
			})
		}
	}

	waitch := make(chan error, 1)
	go func() {
		waitch <- opsr.Wait()
	}()

	process, done, _, err := opsr.Process()
	t.NoError(err)

	for i := uint64(0); i < nops; i++ {
		opindex := i

		op := opsi[opindex]

		addOps(opindex)

		t.NoError(process(opindex, op.Hash(), op.Fact().Hash()))
	}

	done()

	select {
	case <-time.After(time.Second * 3):
		t.Fail("expected wait done")
	case err := <-waitch:
		t.NoError(err)
	}

	t.Equal(doneOps(), doneProcessedOps())
}

func (t *testOperationProcessor) TestProcess() {
	for i := uint64(0); i < 1<<13; i = i + 1000 {
		t.Run(fmt.Sprintf("nops: %d", i), func() { t.testProcess(i) })
	}
}

func TestOperationProcessor(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testOperationProcessor))
}

type testProcessWorker struct {
	baseTestOperationProcessor
}

func (t *testProcessWorker) newWorker(ctx context.Context, local string) *ProcessWorker {
	args := NewProcessWorkerArgs(t.networkID)
	args.SaveOperationFuncs = func(context.Context) (processWorkerSaveOperationFunc, writerCloseSaveFunc, error) {
		return func(uint64, Operation) error { return nil }, func(bool) (string, error) { return "", nil }, nil
	}
	args.SaveStateFuncs = func(context.Context) (processWorkerSaveStateFunc, writerCloseSaveFunc, error) {
		return func(base.State) error { return nil }, func(bool) (string, error) { return "", nil }, nil
	}

	pw, err := NewProcessWorker(ctx, args, local, 33)
	t.NoError(err)

	return pw
}

func (t *testProcessWorker) TestBroadcastCanceled() {
	t.Run("by Cancel()", func() {
		pw := t.newWorker(context.Background(), "w0")

		ch := make(chan struct{}, 1)
		pw.args.BroadcastCanceled = func() error {
			ch <- struct{}{}

			return nil
		}

		go func() {
			_ = pw.MergeWait()
		}()

		<-time.After(time.Second)
		t.T().Log("Cancel")
		pw.Cancel()

		<-ch
	})

	t.Run("by errProcessWorkerDone", func() {
		pw := t.newWorker(context.Background(), "w0")

		ch := make(chan struct{}, 1)
		pw.args.BroadcastCanceled = func() error {
			ch <- struct{}{}

			return nil
		}

		go func() {
			_ = pw.MergeWait()
		}()

		<-time.After(time.Second)
		pw.ctxCancel(errProcessWorkerDone)

		select {
		case <-time.After(time.Second):
		case <-ch:
			t.Fail("unexpected BroadcastCanceled")
		}
	})

	t.Run("by errProcessWorkerDone", func() {
		pw := t.newWorker(context.Background(), "w0")

		ch := make(chan struct{}, 1)
		pw.args.BroadcastCanceled = func() error {
			ch <- struct{}{}

			return nil
		}

		go func() {
			_ = pw.MergeWait()
		}()

		<-time.After(time.Second)
		pw.ctxCancel(errProcessWorkerDone)

		select {
		case <-time.After(time.Second):
		case <-ch:
			t.Fail("unexpected BroadcastCanceled")
		}
	})

	t.Run("by CancelByManager()", func() {
		pw := t.newWorker(context.Background(), "w0")

		ch := make(chan struct{}, 1)
		pw.args.BroadcastCanceled = func() error {
			ch <- struct{}{}

			return nil
		}

		go func() {
			_ = pw.MergeWait()
		}()

		<-time.After(time.Second)
		pw.CancelByManager()

		select {
		case <-time.After(time.Second):
		case <-ch:
			t.Fail("unexpected BroadcastCanceled")
		}
	})
}

func (t *testProcessWorker) TestMergeWaitCancel() {
	t.Run("before NewOperations", func() {
		pw := t.newWorker(context.Background(), "w0")

		waitch := make(chan error, 1)

		t.T().Log("MergeWait")

		go func() {
			waitch <- pw.MergeWait()
		}()

		<-time.After(time.Second)
		t.T().Log("Cancel")
		pw.Cancel()

		select {
		case <-time.After(time.Second * 2):
			t.Fail("wait MergeWait")
		case err := <-waitch:
			t.Error(err)
			t.ErrorIs(err, context.Canceled)
		}
	})

	t.Run("during NewOperations", func() {
		pw := t.newWorker(context.Background(), "w0")
		pw.args.NOps = 1 << 3

		processch := make(chan error, 1)
		pw.args.GetOperationFunc = func(ctx context.Context, operation util.Hash) (Operation, bool, error) {
			select {
			case <-ctx.Done():
				t.LockedLog("process canceled: %+v", ctx.Err())

				processch <- ctx.Err()

				return nil, false, ctx.Err()
			case <-time.After(time.Second * 33):
				processch <- nil

				return nil, false, nil
			}
		}

		process, _, cancel, err := pw.NewOperations()
		t.NoError(err)
		defer cancel(nil)

		go func() {
			process(0, valuehash.RandomSHA256(), valuehash.RandomSHA256())
		}()

		waitch := make(chan error, 1)

		t.T().Log("MergeWait")
		go func() {
			waitch <- pw.MergeWait()
		}()

		<-time.After(time.Second)
		pw.Cancel()
		t.T().Log("Canceled")

		select {
		case <-time.After(time.Second * 2):
			t.Fail("wait MergeWait")
		case err := <-waitch:
			t.Error(err)
			t.ErrorIs(err, context.Canceled)
		}

		select {
		case <-time.After(time.Second * 2):
			t.Fail("wait process cancel")
		case err := <-processch:
			t.Error(err)
			t.ErrorIs(err, context.Canceled)
		}
	})
}

func (t *testProcessWorker) TestWaitCancel() {
	t.Run("MergeWait and then Wait", func() {
		pw := t.newWorker(context.Background(), "w0")
		defer pw.Cancel()

		pw.args.NOps = 1 << 3
		pw.args.IsEnoughWorkers = func(n uint64) bool { return n > 0 }
		pw.args.UpdateMergeDoneFunc = func(context.Context) (
			updateState func(base.State) error,
			updateReason func(opindex uint64, reason string) error,
			done func() error,
			_ error,
		) {
			return func(base.State) error { return nil },
				func(uint64, string) error { return nil },
				func() error { return nil },
				nil
		}
		pw.args.UpdateStoreDoneFunc = func(ctx context.Context, _ map[base.BlockItemType]string) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Second * 2):
				return nil
			}
		}

		_, done, _, err := pw.NewOperations()
		t.NoError(err)
		done()

		t.NoError(pw.MergeWait())
		pw.Cancel()

		err = pw.Done()
		t.Error(err)
		t.ErrorIs(err, context.Canceled)
	})

	t.Run("Wait, Cancel without MergeWait", func() {
		pw := t.newWorker(context.Background(), "w0")
		defer pw.Cancel()

		pw.args.IsEnoughWorkers = func(n uint64) bool { return n > 0 }

		pw.Cancel()

		err := pw.Done()
		t.Error(err)
		t.ErrorIs(err, context.Canceled)
	})
}

func (t *testProcessWorker) TestProcessAndMerge() {
	priv0 := base.NewDummyPrivatekey("0")
	priv1 := base.NewDummyPrivatekey("1")
	priv2 := base.NewDummyPrivatekey("2")
	pub0 := priv0.Publickey()
	pub1 := priv1.Publickey()
	pub2 := priv2.Publickey()

	key0 := AccountStateKey(pub0)
	key1 := AccountStateKey(pub1)
	key2 := AccountStateKey(pub2)

	newState := func(stateKey string, pub base.Publickey) base.State {
		return base.NewBaseState(
			33,
			stateKey,
			NewAccountStateValue(pub, 1),
			nil,
			[]util.Hash{valuehash.RandomSHA256()},
		)
	}

	sts := map[string]base.State{
		key0: newState(key0, pub0),
		key1: newState(key1, pub1),
		key2: newState(key2, pub2),
	}

	ops := []Op{
		t.newOp(priv0, pub1, 1),
		t.newOp(priv0, pub2, 1), // ignored
		t.newOp(priv1, pub0, 1),
		t.newOp(priv1, pub2, 1), // ignored
		t.newOp(priv2, pub0, 1),
		t.newOp(priv2, pub1, 1), // ignored
	}

	ophs := map[string]uint64{}
	opfhs := map[string]uint64{}
	for i := range ops {
		ophs[ops[i].Hash().String()] = uint64(i)
		opfhs[ops[i].Fact().Hash().String()] = uint64(i)
	}

	expectedMergedOps := map[ /* state key */ string][]uint64{
		key0: {0, 2, 4},
		key1: {0, 2},
		key2: {4},
	}
	expectedUnmergedOps := map[uint64]string{
		1: "previous issuer found",
		3: "previous issuer found",
		5: "previous issuer found",
	}

	getOperationFunc := func(ctx context.Context, operation util.Hash) (Operation, bool, error) {
		switch opindex, found := ophs[operation.String()]; {
		case !found:
			return nil, false, nil
		default:
			return ops[opindex], true, nil
		}
	}

	getState := func(_ context.Context, key string) (base.State, bool, error) {
		i, found := sts[key]

		return i, found, nil
	}

	run := func(seq []int) {
		pw := t.newWorker(context.Background(), "w0")
		defer pw.Cancel()

		pw.args.NOps = uint64(len(ops))
		pw.args.NewJobWorker = func(ctx context.Context) (util.JobWorker, error) {
			return util.NewBaseJobWorker(ctx, 1) // <- only one process at a time
		}

		pw.args.StateOwners = func(context.Context, string) ([]string, error) {
			return []string{pw.local}, nil
		}
		pw.args.GetOperationFunc = getOperationFunc
		pw.args.GetStateFunc = getState
		pw.args.IsEnoughWorkers = func(n uint64) bool { return n > 0 }

		resultMergedOps := util.NewSingleLockedMap[string, []uint64]()
		resultUnmergedOps := util.NewSingleLockedMap[uint64, string]()

		pw.args.UpdateMergeDoneFunc = func(context.Context) (
			updateState func(base.State) error,
			updateReason func(opindex uint64, reason string) error,
			done func() error,
			_ error,
		) {
			return func(st base.State) error {
					add, done := util.CompactAppendSlice[uint64](len(st.Operations()))
					if err := util.TraverseSliceV1(st.Operations(), func(_ int, i util.Hash) (bool, error) {
						j, found := opfhs[i.String()]
						if !found {
							return false, util.ErrNotFound.Errorf("op")
						}

						add(j)

						return true, nil
					}); err != nil {
						return err
					}

					resultMergedOps.SetValue(st.Key(), done())

					return nil
				},
				func(opindex uint64, reason string) error {
					resultUnmergedOps.SetValue(opindex, reason)

					return nil
				},
				func() error { return nil },
				nil
		}

		process, done, _, err := pw.NewOperations()
		t.NoError(err)

		for i := range seq {
			opindex := uint64(seq[i])
			op := ops[opindex]

			t.NoError(process(opindex, op.Hash(), op.Fact().Hash()))
		}

		done()

		t.NoError(pw.MergeWait())

		t.Equal(expectedMergedOps, resultMergedOps.Map())
		t.Equal(expectedUnmergedOps, resultUnmergedOps.Map())
	}

	gen := combin.NewPermutationGenerator(len(ops), len(ops))
	nextseq := func() bool {
		return gen.Next()
	}
	getseq := func() []int {
		return gen.Permutation(nil)
	}

	for nextseq() {
		seq := getseq()

		t.T().Log("seq:", seq)

		run(seq)
	}
}

func (t *testProcessWorker) TestSaveOperation() {
	nops := 33
	ops := make([]Op, nops)
	ophs := map[string]uint64{}

	for i := range ops {
		opindex := uint64(i)
		op := t.randomOp(1)
		ops[opindex] = op
		ophs[op.Hash().String()] = opindex
	}

	pw := t.newWorker(context.Background(), "w0")
	defer pw.Cancel()

	pw.args.NOps = uint64(nops)
	pw.args.UpdateMergeDoneFunc = func(context.Context) (
		updateState func(base.State) error,
		updateReason func(opindex uint64, reason string) error,
		done func() error,
		_ error,
	) {
		return func(base.State) error { return nil },
			func(uint64, string) error { return nil },
			func() error { return nil },
			nil
	}

	updatestorech := make(chan map[base.BlockItemType]string, 1)
	pw.args.UpdateStoreDoneFunc = func(_ context.Context, m map[base.BlockItemType]string) error {
		updatestorech <- m

		return nil
	}

	pw.args.StateOwners = func(context.Context, string) ([]string, error) {
		return []string{pw.local}, nil
	}
	pw.args.GetOperationFunc = func(ctx context.Context, operation util.Hash) (Operation, bool, error) {
		switch opindex, found := ophs[operation.String()]; {
		case !found:
			return nil, false, nil
		default:
			return ops[opindex], true, nil
		}
	}
	pw.args.IsEnoughWorkers = func(n uint64) bool { return n > 0 }

	opsfilename := util.UUID().String()
	storedch := make(chan bool, 1)
	storedOps := util.NewSingleLockedMap[uint64, Operation]()
	pw.args.SaveOperationFuncs = func(context.Context) (processWorkerSaveOperationFunc, writerCloseSaveFunc, error) {
		return func(opindex uint64, op Operation) error {
				storedOps.SetValue(opindex, op)

				return nil
			}, func(ok bool) (string, error) {
				storedch <- ok

				return opsfilename, nil
			},
			nil
	}

	process, done, _, err := pw.NewOperations()
	t.NoError(err)

	for i := range ops {
		opindex := uint64(i)
		op := ops[opindex]

		t.NoError(process(opindex, op.Hash(), op.Fact().Hash()))
	}

	done()

	t.NoError(pw.MergeWait())
	t.NoError(pw.Done())

	t.Equal(nops, storedOps.Len())
	storedOps.Traverse(func(opindex uint64, sop Operation) bool {
		base.EqualOperation(t.Assert(), ops[opindex], sop)

		return true
	})

	select {
	case <-time.After(time.Second):
		t.Fail("wait to close save")
	case ok := <-storedch:
		t.True(ok)
	}

	select {
	case <-time.After(time.Second):
		t.Fail("wait to save block files")
	case m := <-updatestorech:
		t.Equal(map[base.BlockItemType]string{base.BlockItemOperations: opsfilename}, m)
	}
}

func (t *testProcessWorker) Test3Workers() {
	npriv := 6 // NOTE should be at least 3

	privs := make([]base.Privatekey, npriv)
	pubs := make([]base.Publickey, len(privs))
	sts := map[string]base.State{}
	stateKeys := make([]string, len(privs))
	for i := range privs {
		priv := base.NewDummyPrivatekey(fmt.Sprintf("%03d", i))
		privs[i] = priv
		pubs[i] = priv.Publickey()

		stateKey := AccountStateKey(priv.Publickey())
		stateKeys[i] = stateKey
		sts[stateKey] = base.NewBaseState(
			33,
			stateKey,
			NewAccountStateValue(priv.Publickey(), 1),
			nil,
			[]util.Hash{valuehash.RandomSHA256()},
		)

	}

	ops := make([]Operation, npriv*3)
	ophs := map[string]uint64{}
	opfhs := map[string]uint64{}
	for i := range privs {
		issuer := privs[i]
		other0 := privs[(i+1)%len(privs)].Publickey()
		other1 := privs[(i+2)%len(privs)].Publickey()

		op := t.newOp(issuer, other0, 1)
		opindex := uint64(i * 3)
		ops[opindex] = op
		ophs[op.Hash().String()] = opindex
		opfhs[op.Fact().Hash().String()] = opindex

		op = t.newOp(issuer, other0, 1)
		opindex = uint64((i * 3) + 1)
		ops[opindex] = op
		ophs[op.Hash().String()] = opindex
		opfhs[op.Fact().Hash().String()] = opindex

		opindex = uint64((i * 3) + 2)
		op = t.newOp(issuer, other1, 1)
		ops[opindex] = op
		ophs[op.Hash().String()] = opindex
		opfhs[op.Fact().Hash().String()] = opindex
	}

	getOperationFunc := func(ctx context.Context, op util.Hash) (Operation, bool, error) {
		switch opindex, found := ophs[op.String()]; {
		case !found:
			return nil, false, nil
		default:
			return ops[opindex], true, nil
		}
	}
	getState := func(_ context.Context, key string) (base.State, bool, error) {
		i, found := sts[key]

		return i, found, nil
	}

	var expectedMergedOps map[string][]uint64
	var expectedUnmergedOps map[uint64]string

	t.Run("one worker", func() {
		pw := t.newWorker(context.Background(), "w0")
		defer pw.Cancel()

		pw.args.NOps = uint64(len(ops))
		pw.args.StateOwners = func(_ context.Context, stateKey string) ([]string, error) {
			return []string{"w0"}, nil
		}
		pw.args.NewJobWorker = func(ctx context.Context) (util.JobWorker, error) {
			return util.NewBaseJobWorker(ctx, 1) // <- only one process at a time
		}
		pw.args.GetOperationFunc = getOperationFunc
		pw.args.GetStateFunc = getState
		pw.args.IsEnoughWorkers = func(n uint64) bool { return n > 0 }

		resultMergedOps := util.NewSingleLockedMap[string, []uint64]()
		resultUnmergedOps := util.NewSingleLockedMap[uint64, string]()

		pw.args.UpdateMergeDoneFunc = func(context.Context) (
			updateState func(base.State) error,
			updateReason func(opindex uint64, reason string) error,
			done func() error,
			_ error,
		) {
			return func(st base.State) error {
					add, done := util.CompactAppendSlice[uint64](len(st.Operations()))
					if err := util.TraverseSliceV1(st.Operations(), func(_ int, i util.Hash) (bool, error) {
						j, found := opfhs[i.String()]
						if !found {
							return false, util.ErrNotFound.Errorf("op")
						}

						add(j)

						return true, nil
					}); err != nil {
						return err
					}

					resultMergedOps.SetValue(st.Key(), done())

					return nil
				},
				func(opindex uint64, reason string) error {
					resultUnmergedOps.SetValue(opindex, reason)

					return nil
				},
				func() error { return nil },
				nil
		}

		process, done, _, err := pw.NewOperations()
		t.NoError(err)

		for i := range ops {
			opindex := uint64(i)
			op := ops[opindex]

			t.NoError(process(opindex, op.Hash(), op.Fact().Hash()))
		}

		done()

		t.NoError(pw.MergeWait())

		t.T().Log("merge result")
		expectedMergedOps = resultMergedOps.Map()
		expectedUnmergedOps = resultUnmergedOps.Map()

		addk, donek := util.SortCompactAppendSlice[string](len(expectedMergedOps), cmp.Less[string])
		for i := range expectedMergedOps {
			addk(i)
		}
		util.TraverseSliceV1(donek(), func(_ int, i string) (bool, error) {
			t.T().Logf("state_key=%q merged_ops=%v", i, expectedMergedOps[i])

			return true, nil
		})

		addu, doneu := util.SortCompactAppendSlice[uint64](len(expectedUnmergedOps), cmp.Less[uint64])
		for i := range expectedUnmergedOps {
			addu(i)
		}
		util.TraverseSliceV1(doneu(), func(_ int, i uint64) (bool, error) {
			t.T().Logf("unmerge_op=%d reason=%q", i, expectedUnmergedOps[i])

			return true, nil
		})
	})

	{
		addM, doneM := util.SortUniqCompactAppendSlice[uint64](len(ops), cmp.Less[uint64])
		for i := range expectedMergedOps {
			addM(expectedMergedOps[i]...)
		}

		addU, doneU := util.SortUniqCompactAppendSlice[uint64](len(ops), cmp.Less[uint64])
		for i := range expectedUnmergedOps {
			addU(i)
		}

		t.T().Logf("expected   merged ops=%v", doneM())
		t.T().Logf("expected unmerged ops=%v", doneU())
	}

	t.Run("3 workers", func() {
		nworkers := 3
		nshared := 2

		workers := make([]string, nworkers)
		for i := range workers {
			workers[i] = fmt.Sprintf("w%02d", i)
		}

		workerOps := map[string][]uint64{}
		for i := range ops {
			worker := workers[i%len(workers)]
			workerOps[worker] = append(workerOps[worker], uint64(i))
		}

		stateOwners := map[string][]string{}
		util.TraverseSliceV1(stateKeys, func(i int, stateKey string) (bool, error) {
			n := nshared
			if j := len(workers); n > j {
				n = j
			}

			owners := make([]string, n)
			for j := range owners {
				owners[j] = workers[(i+j)%len(workers)]
			}

			stateOwners[stateKey] = owners

			t.T().Logf("state owner: state_key=%q workers=%v", stateKey, owners)

			return true, nil
		})

		pws := map[string]*ProcessWorker{}
		resultMergedOps := util.NewSingleLockedMap[string, []uint64]()
		resultUnmergedOps := util.NewSingleLockedMap[uint64, string]()

		for i := range workers {
			local := workers[i]

			pw := t.newWorker(context.Background(), local)
			defer pw.Cancel()

			pw.SetLogging(logging.TestNilLogging)

			pws[local] = pw

			pw.args.NOps = uint64(len(ops))
			pw.args.StateOwners = func(_ context.Context, stateKey string) ([]string, error) {
				owners, found := stateOwners[stateKey]
				if !found {
					panic(fmt.Sprintf("state, %q owner not found", stateKey))
				}

				return owners, nil
			}
			pw.args.NewJobWorker = func(ctx context.Context) (util.JobWorker, error) {
				return util.NewBaseJobWorker(ctx, 1<<9) // <- only one process at a time
			}
			pw.args.GetOperationFunc = getOperationFunc
			pw.args.GetStateFunc = getState
			pw.args.IsEnoughWorkers = func(n uint64) bool { return n >= uint64(nworkers) }

			pw.args.UpdateMergeDoneFunc = func(context.Context) (
				updateState func(base.State) error,
				updateReason func(opindex uint64, reason string) error,
				done func() error,
				_ error,
			) {
				return func(st base.State) error {
						add, done := util.CompactAppendSlice[uint64](len(st.Operations()))
						if err := util.TraverseSliceV1(st.Operations(), func(_ int, i util.Hash) (bool, error) {
							j, found := opfhs[i.String()]
							if !found {
								return false, util.ErrNotFound.Errorf("op")
							}

							add(j)

							return true, nil
						}); err != nil {
							return err
						}

						resultMergedOps.SetValue(st.Key(), done())

						return nil
					},
					func(opindex uint64, reason string) error {
						if !resultUnmergedOps.Exists(opindex) && len(reason) > 0 {
							resultUnmergedOps.SetValue(opindex, reason)
						}

						return nil
					},
					func() error { return nil },
					nil
			}

			pw.args.BroadcastInsertValuesDoneFunc = func(context.Context) error {
				for i := range workers {
					worker := workers[i]
					if worker == local {
						continue
					}

					err := pws[worker].WorkerInsertValuesDone(local)
					t.LockedLog("broadcast insert values done: local=%q worker=%q error=%+v", local, worker, err)
					if err != nil {
						return err
					}
				}

				return nil
			}
			pw.args.BroadcastInsertOpFunc = func(_ context.Context, worker string, opindex uint64, fact util.Hash, values []StateMergeValue, owners map[string][]string) error {
				addK, doneK := util.SortCompactAppendSlice[string](len(values), cmp.Less[string])
				util.TraverseSliceV1(values, func(_ int, v StateMergeValue) (bool, error) {
					addK(v.Key())

					return true, nil
				})

				t.LockedLog("insert values: local=%q worker=%q opindex=%d state_keys=%v", local, worker, opindex, doneK())

				return pws[worker].InsertValues(local, opindex, fact, values, owners)
			}

			pw.args.BroadcastPhaseDoneFunc = func(_ context.Context, phase int, mergedOps, unmergedOps []uint64) error {
				t.LockedLog("broadcast phase done: local=%q phase=%d merged_ops=%v unmerged_ops=%v",
					local, phase, mergedOps, unmergedOps)

				for i := range workers {
					worker := workers[i]
					if worker == local {
						continue
					}

					err := pws[worker].WorkerPhaseDone(local, phase, mergedOps, unmergedOps)
					t.LockedLog(
						"\tphase done: local=%q worker=%q phase=%d merged_ops=%v unmerged_ops=%v error=%+v",
						local, worker, phase, mergedOps, unmergedOps, err)
					if err != nil {
						return err
					}
				}

				return nil
			}
		}

		t.T().Log("process operations")
		var wg sync.WaitGroup
		wg.Add(nworkers)

		errch := make(chan error, 2)
		for worker := range workerOps {
			worker := worker
			wops := workerOps[worker]

			go func() {
				defer wg.Done()

				process, done, _, err := pws[worker].NewOperations()
				if err != nil {
					errch <- err

					return
				}

				for i := range wops {
					opindex := wops[i]
					t.LockedLog("process: worker=%q opindex=%d", worker, opindex)

					op := ops[opindex]

					if err := process(opindex, op.Hash(), op.Fact().Hash()); err != nil {
						errch <- err

						return
					}
				}

				done()
			}()
		}

		wg.Wait()
		errch <- nil
		t.NoError(<-errch)

		for i := range workers {
			worker := workers[i]

			t.T().Logf("MergeWait: worker=%q", worker)
			t.NoError(pws[worker].MergeWait())
		}

		t.Equal(expectedMergedOps, resultMergedOps.Map())
		t.Equal(expectedUnmergedOps, resultUnmergedOps.Map())
	})
}

func TestProcessWorker(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testProcessWorker))
}
