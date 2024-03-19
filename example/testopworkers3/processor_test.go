package testopsworkers

import (
	"cmp"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type testOperationProcessor struct {
	baseTestAccountStateMerger
	networkID base.NetworkID
}

func (t *testOperationProcessor) SetupSuite() {
	t.networkID = base.NetworkID([]byte("showme"))
}

func (t *testOperationProcessor) newProcessor(ctx context.Context) *OperationProcessor {
	args := NewOperationProcessorArgs(t.networkID)

	opsr, err := NewOperationProcessor(ctx, args)
	t.NoError(err)

	return opsr
}

func (t *testOperationProcessor) randomOp() Op {
	return NewOp(NewOpFact(
		util.UUID().Bytes(),
		1,
		base.NewDummyPublickey(util.UUID().String()),
		base.NewDummyPublickey(util.UUID().String()),
	))
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

	priv := base.NewDummyPrivatekey(util.UUID().String())

	for opindex := uint64(0); opindex < nops; opindex++ {
		op := t.randomOp()
		t.NoError(op.Sign(priv, t.networkID))

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

	opsr.args.MergeStateMergeValueFunc = func(opindex uint64, vs []StateMergeValue) error {
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
