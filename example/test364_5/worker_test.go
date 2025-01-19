package main

import (
	"context"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type testWorker struct {
	dutil.BaseSuite
	baseTestAccountStateValueMerger
	baseTestWorkers
}

func (t *testWorker) TestClose() {
	t.Run("ok", func() {
		local := "local"
		workers := []string{local}
		ops := t.baseTestAccountStateValueMerger.randomAddOps(1, 1)

		getStateOwners, stateOwnersClose, err := stateOwnersFuncByCount(workers, 1)
		t.NoError(err)
		defer stateOwnersClose()

		ctx, cancel := t.Ctx()
		defer cancel(nil)

		initialInsert, _, _, wait, closeWorker, err := newFuncWorker(
			ctx,
			t.newWorkerOptions(local),
			func(stateKey string) (base.State, bool, error) { return nil, false, nil },
			nil,
			util.UUID().String(),
			workers, workers,
			1, 1, 1,
		)
		t.WorkerIfErrPanic(local, err)

		initialerr := make(chan error, 1)
		go func() {
			initialerr <- initialInsert(
				getStateOwners,
				t.defaultDistributeValuesFunc(local),
				func(ctx context.Context, _ uint64, _, _ util.Hash) (Operation, bool, string, error) {
					select {
					case <-ctx.Done():
						return nil, false, "", ctx.Err()
					case <-time.After(time.Second * 3):
						return ops[0], true, "", nil
					}
				},
				t.ophsSeq2(ops),
				nil,
			)
		}()

		waiterr := make(chan error, 1)
		go func() {
			waiterr <- wait(
				func(context.Context, string, string, string, []uint64) error { return nil },
				func(context.Context, string, string, map[uint64]string, uint64,
					dutil.InErrorFunc[dutil.InErrorFunc[base.State]],
				) error {
					return nil
				},
			)
		}()

		closeWorker(nil)
		t.T().Log("close worker")

		t.T().Log("wait: initial insert")
		select {
		case <-time.After(time.Second * 5):
			t.Fail("unexpected initial insert done")
		case err := <-initialerr:
			t.NoError(err)
		}

		t.T().Log("wait: worker wait")
		select {
		case <-time.After(time.Second * 5):
			t.Fail("unexpected wait done")
		case err := <-waiterr:
			t.NoError(err)
		}
	})

	t.Run("close with error", func() {
		workers := t.workers(3)
		local := workers[0]
		ops := t.baseTestAccountStateValueMerger.randomAddOps(1, 1)

		getStateOwners, stateOwnersClose, err := stateOwnersFuncByCount(workers, 1)
		t.NoError(err)
		defer stateOwnersClose()

		ctx, cancel := t.Ctx()
		defer cancel(nil)

		initialInsert, _, _, wait, closeWorker, err := newFuncWorker(
			ctx,
			t.newWorkerOptions(local),
			func(stateKey string) (base.State, bool, error) { return nil, false, nil },
			nil,
			util.UUID().String(),
			workers, workers,
			1, 1, 1,
		)
		t.WorkerIfErrPanic(local, err)

		initialerr := make(chan error, 1)
		go func() {
			initialerr <- initialInsert(
				getStateOwners,
				t.defaultDistributeValuesFunc(local),
				func(ctx context.Context, _ uint64, _, _ util.Hash) (Operation, bool, string, error) {
					select {
					case <-ctx.Done():
						return nil, false, "", ctx.Err()
					case <-time.After(time.Minute):
						return nil, true, "", nil
					}
				},
				t.ophsSeq2(ops),
				nil,
			)
		}()

		waiterr := make(chan error, 1)
		go func() {
			waiterr <- wait(
				func(context.Context, string, string, string, []uint64) error { return nil },
				func(context.Context, string, string, map[uint64]string, uint64,
					dutil.InErrorFunc[dutil.InErrorFunc[base.State]],
				) error {
					return nil
				},
			)
		}()

		closeWorker(errors.Errorf("eatme"))

		t.T().Log("wait: initial insert")
		select {
		case <-time.After(time.Second * 2):
			t.Fail("unexpected initial insert done")
		case err := <-initialerr:
			t.ErrorIs(err, context.Canceled)
		}

		t.T().Log("wait: worker wait")
		select {
		case <-time.After(time.Second * 2):
			t.Fail("unexpected wait done")
		case err := <-waiterr:
			t.ErrorContains(err, "eatme")
			t.ErrorIs(err, context.Canceled)
		}
	})
}

func TestWorker(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testWorker))
}
