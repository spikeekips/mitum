package main

import (
	"context"
	"slices"
	"sync"
	"testing"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type testManagerWorkersOps struct {
	suite.Suite
	baseTestMergeOp
	baseTestWorkers
}

func (t *testManagerWorkersOps) randomOphs(n int) [][2]util.Hash {
	h := make([][2]util.Hash, n)
	for i := range n {
		h[i] = [2]util.Hash{
			valuehash.RandomSHA256(),
			valuehash.RandomSHA256(),
		}
	}

	return h
}

func (t *testManagerWorkersOps) workerSendOpFunc() newWorkerSendOpFunc {
	return func(_ context.Context, worker string) (opindexOpHashesFunc, inErrorFunc[error], error) {
		return func(opindex uint64, oph, fact util.Hash) error {
				t.println("send op: worker=%q opindex=%d", worker, opindex)

				return nil
			},
			func(err error) error {
				t.println("send ops done: worker=%q err=%v", worker, err)

				return err
			},
			nil
	}
}

func (t *testManagerWorkersOps) TestCancel() {
	t.Run("before new", func() {
		workers := t.workers(3)
		ophs := t.randomOphs(3)

		ctx, cancel := context.WithCancel(context.Background())

		cancel()

		err := newFuncManagerWorkersOps(
			ctx,
			newFuncNewJobWorkerFunc(1<<9),
			workers, 1,
			ophs,
			t.workerSendOpFunc(),
		)
		t.ErrorIs(err, context.Canceled)
	})

	t.Run("during sending", func() {
		workers := t.workers(3)
		ophs := t.randomOphs(3)

		ctx, cancel := context.WithCancel(context.Background())

		send := t.workerSendOpFunc()

		err := newFuncManagerWorkersOps(
			ctx,
			newFuncNewJobWorkerFunc(1<<9),
			workers, 1,
			ophs,
			func(ctx context.Context, worker string) (opindexOpHashesFunc, inErrorFunc[error], error) {
				o, d, err := send(ctx, worker)
				if err != nil {
					return nil, nil, err
				}

				return func(opindex uint64, oph, fact util.Hash) error {
						if opindex == 1 {
							t.println("send op cancel: worker=%q opindex=%d", worker, opindex)

							cancel()
						}

						return o(opindex, oph, fact)
					},
					func(err error) error {
						return d(err)
					},
					nil
			},
		)
		t.ErrorIs(err, context.Canceled)
	})
}

func (t *testManagerWorkersOps) TestSend() {
	t.Run("empty ophs", func() {
		workers := t.workers(3)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		err := newFuncManagerWorkersOps(
			ctx,
			newFuncNewJobWorkerFunc(1<<9),
			workers, 1,
			nil,
			t.workerSendOpFunc(),
		)
		t.ErrorContains(err, "empty ophs")
	})

	t.Run("ophs > workers", func() {
		workers := t.workers(3)
		ophs := t.randomOphs(33)
		var opsShare uint64 = 1

		var expectedOpsWorkers []string
		expected := map[string][][3]interface{}{}
		{
			w, d, err := opsNShareWorkers(workers, opsShare, ophs)
			t.NoError(err)

			expectedOpsWorkers = w()
			slices.Sort(expectedOpsWorkers)

			traverseSlice(workers, func(_ int, worker string) (bool, error) {
				var ops [][3]interface{}

				d(worker, func(opindex uint64, oph, fact util.Hash) error {
					ops = append(ops, [3]interface{}{opindex, oph.String(), fact.String()})

					return nil
				})

				if len(ops) > 0 {
					slices.SortFunc(ops, func(a, b [3]interface{}) int {
						return int(a[0].(uint64) - b[0].(uint64))
					})

					expected[worker] = ops
				}

				return true, nil
			})
		}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		send := t.workerSendOpFunc()

		sentOps, err := util.NewLockedMap[string, [][3]interface{}](1<<9, nil)
		t.NoError(err)

		t.NoError(newFuncManagerWorkersOps(
			ctx,
			newFuncNewJobWorkerFunc(1<<9),
			workers, opsShare,
			ophs,
			func(ctx context.Context, worker string) (opindexOpHashesFunc, inErrorFunc[error], error) {
				o, d, err := send(ctx, worker)
				if err != nil {
					return nil, nil, err
				}

				var ops [][3]interface{}

				return func(opindex uint64, oph, fact util.Hash) error {
						ops = append(ops, [3]interface{}{opindex, oph.String(), fact.String()})

						return o(opindex, oph, fact)
					},
					func(err error) error {
						if err != nil {
							return err
						}

						slices.SortFunc(ops, func(a, b [3]interface{}) int {
							return int(a[0].(uint64) - b[0].(uint64))
						})

						sentOps.SetValue(worker, ops)

						return d(err)
					},
					nil
			},
		))

		t.Equal(len(expectedOpsWorkers), len(sentOps.Map()))
		t.Equal(expected, sentOps.Map())
	})
}

func (t *testManagerWorkersOps) TestFailed() {
	baseRunFailedNewSendFunc := func(
		workers []string,
		failedWorker string,
		opsShare uint64,
		ophs [][2]util.Hash,
		sendf newWorkerSendOpFunc,
	) error {
		var expectedOpsWorkers []string
		expected := map[string][][3]interface{}{}
		{
			w, d, err := opsNShareWorkers(workers, opsShare, ophs)
			t.NoError(err)

			expectedOpsWorkers = w()
			slices.Sort(expectedOpsWorkers)

			expectedOpsWorkers = util.FilterSlice(expectedOpsWorkers, func(a string) bool { return a != failedWorker })

			traverseSlice(workers, func(_ int, worker string) (bool, error) {
				if worker == failedWorker {
					return true, nil
				}

				var ops [][3]interface{}

				d(worker, func(opindex uint64, oph, fact util.Hash) error {
					ops = append(ops, [3]interface{}{opindex, oph.String(), fact.String()})

					return nil
				})

				if len(ops) > 0 {
					slices.SortFunc(ops, func(a, b [3]interface{}) int {
						return int(a[0].(uint64) - b[0].(uint64))
					})

					expected[worker] = ops
				}

				return true, nil
			})
		}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		sentOps, err := util.NewLockedMap[string, [][3]interface{}](1<<9, nil)
		t.NoError(err)

		err = newFuncManagerWorkersOps(
			ctx,
			newFuncNewJobWorkerFunc(1<<9),
			workers, opsShare,
			ophs,
			func(ctx context.Context, worker string) (opindexOpHashesFunc, inErrorFunc[error], error) {
				o, d, err := sendf(ctx, worker)
				if err != nil {
					return nil, nil, err
				}

				var ops [][3]interface{}

				return func(opindex uint64, oph, fact util.Hash) error {
						ops = append(ops, [3]interface{}{opindex, oph.String(), fact.String()})

						return o(opindex, oph, fact)
					},
					func(err error) error {
						if err != nil {
							return err
						}

						if err := d(err); err != nil {
							return err
						}

						slices.SortFunc(ops, func(a, b [3]interface{}) int {
							return int(a[0].(uint64) - b[0].(uint64))
						})

						sentOps.SetValue(worker, ops)

						return nil
					},
					nil
			},
		)

		if err == nil {
			t.Equal(len(expectedOpsWorkers), len(sentOps.Map()))
			t.Equal(expected, sentOps.Map())
		}

		return err
	}

	runFailedNewSendFunc := func(
		workers []string,
		failedWorker string,
		opsShare uint64,
		ophs [][2]util.Hash,
	) error {
		return baseRunFailedNewSendFunc(
			workers,
			failedWorker,
			opsShare,
			ophs,
			func(_ context.Context, worker string) (opindexOpHashesFunc, inErrorFunc[error], error) {
				if worker == failedWorker {
					return nil, nil, errors.Errorf("showme")
				}

				return func(opindex uint64, oph, fact util.Hash) error {
						t.println("send op: worker=%q opindex=%d", worker, opindex)

						return nil
					},
					func(err error) error {
						t.println("send ops done: worker=%q err=%v", worker, err)

						return err
					},
					nil
			},
		)
	}

	t.Run("failed new send func", func() {
		t.Run("no failed", func() {
			workers := t.workers(3)
			ophs := t.randomOphs(33)
			var opsShare uint64 = 2

			t.NoError(runFailedNewSendFunc(workers, "", opsShare, ophs))
		})

		t.Run("under max failed", func() {
			workers := t.workers(3)
			failedWorker := workers[2]
			ophs := t.randomOphs(33)
			var opsShare uint64 = 2

			t.NoError(runFailedNewSendFunc(workers, failedWorker, opsShare, ophs))
		})

		t.Run("over max failed", func() {
			workers := t.workers(3)
			failedWorker := workers[2]
			var opsShare uint64 = 1

			err := runFailedNewSendFunc(workers, failedWorker, opsShare, t.randomOphs(33))
			t.ErrorIs(err, errWorkersFailed)
			t.ErrorContains(err, "showme")
		})
	})

	runFailedSendFunc := func(
		workers []string,
		failedWorker string,
		opsShare uint64,
		ophs [][2]util.Hash,
	) error {
		return baseRunFailedNewSendFunc(
			workers,
			failedWorker,
			opsShare,
			ophs,
			func(_ context.Context, worker string) (opindexOpHashesFunc, inErrorFunc[error], error) {
				var once sync.Once

				return func(opindex uint64, oph, fact util.Hash) error {
						var err error

						once.Do(func() {
							if worker == failedWorker {
								err = errors.Errorf("showme")
							}
						})
						if err != nil {
							return err
						}

						t.println("send op: worker=%q opindex=%d", worker, opindex)

						return nil
					},
					func(err error) error {
						t.println("send ops done: worker=%q err=%v", worker, err)

						return err
					},
					nil
			},
		)
	}

	t.Run("failed in send func", func() {
		t.Run("no failed", func() {
			workers := t.workers(3)
			ophs := t.randomOphs(33)
			var opsShare uint64 = 2

			t.NoError(runFailedSendFunc(workers, "", opsShare, ophs))
		})

		t.Run("under max failed", func() {
			workers := t.workers(3)
			failedWorker := workers[2]
			ophs := t.randomOphs(33)
			var opsShare uint64 = 2

			t.NoError(runFailedSendFunc(workers, failedWorker, opsShare, ophs))
		})

		t.Run("over max failed", func() {
			workers := t.workers(3)
			failedWorker := workers[2]
			var opsShare uint64 = 1

			err := runFailedSendFunc(workers, failedWorker, opsShare, t.randomOphs(33))
			t.ErrorIs(err, errWorkersFailed)
			t.ErrorContains(err, "showme")
		})
	})

	runFailedSendDoneFunc := func(
		workers []string,
		failedWorker string,
		opsShare uint64,
		ophs [][2]util.Hash,
	) error {
		return baseRunFailedNewSendFunc(
			workers,
			failedWorker,
			opsShare,
			ophs,
			func(_ context.Context, worker string) (opindexOpHashesFunc, inErrorFunc[error], error) {
				return func(opindex uint64, oph, fact util.Hash) error {
						t.println("send op: worker=%q opindex=%d", worker, opindex)

						return nil
					},
					func(err error) error {
						if err == nil && worker == failedWorker {
							err = errors.Errorf("showme")
						}

						t.println("send ops done: worker=%q err=%v", worker, err)

						return err
					},
					nil
			},
		)
	}

	t.Run("failed in send func", func() {
		t.Run("no failed", func() {
			workers := t.workers(3)
			ophs := t.randomOphs(33)
			var opsShare uint64 = 2

			t.NoError(runFailedSendDoneFunc(workers, "", opsShare, ophs))
		})

		t.Run("under max failed", func() {
			workers := t.workers(3)
			failedWorker := workers[2]
			ophs := t.randomOphs(33)
			var opsShare uint64 = 2

			t.NoError(runFailedSendDoneFunc(workers, failedWorker, opsShare, ophs))
		})

		t.Run("over max failed", func() {
			workers := t.workers(3)
			failedWorker := workers[2]
			var opsShare uint64 = 1

			err := runFailedSendDoneFunc(workers, failedWorker, opsShare, t.randomOphs(33))
			t.ErrorIs(err, errWorkersFailed)
			t.ErrorContains(err, "showme")
		})
	})
}

func TestManagerWorkersOps(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testManagerWorkersOps))
}
