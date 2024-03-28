package testopsworkers

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

func TestSerialStateOwnersFunc(tt *testing.T) {
	t := new(suite.Suite)
	t.SetT(tt)

	t.Run("empty workers", func() {
		var workers []string
		var share uint8 = 2

		_, err := serialStateOwnersFunc(workers, share)
		t.Error(err)
		t.ErrorContains(err, "empty workers")
	})

	t.Run("zero share", func() {
		workers := []string{"w0", "w1", "w2"}
		var share uint8 = 0

		_, err := serialStateOwnersFunc(workers, share)
		t.Error(err)
		t.ErrorContains(err, "zero share")
	})

	t.Run("same state key", func() {
		workers := []string{"w0", "w1", "w2"}
		var share uint8 = 2

		f, err := serialStateOwnersFunc(workers, share)
		t.NoError(err)

		key := util.UUID().String()
		ws, err := f(key)
		t.NoError(err)
		t.Equal([]string{"w0", "w1"}, ws, "0")

		ws, err = f(key)
		t.NoError(err)
		t.Equal([]string{"w2", "w0"}, ws, "1")

		ws, err = f(key)
		t.NoError(err)
		t.Equal([]string{"w1", "w2"}, ws, "2")
	})

	t.Run("workers > share", func() {
		workers := []string{"w0", "w1", "w2"}
		var share uint8 = 2

		f, err := serialStateOwnersFunc(workers, share)
		t.NoError(err)

		ws, err := f(util.UUID().String())
		t.NoError(err)
		t.Equal([]string{"w0", "w1"}, ws, "0")

		ws, err = f(util.UUID().String())
		t.NoError(err)
		t.Equal([]string{"w2", "w0"}, ws, "1")

		ws, err = f(util.UUID().String())
		t.NoError(err)
		t.Equal([]string{"w1", "w2"}, ws, "2")
	})

	t.Run("workers < share", func() {
		workers := []string{"w0", "w1", "w2"}
		var share uint8 = 4

		f, err := serialStateOwnersFunc(workers, share)
		t.NoError(err)

		ws, err := f(util.UUID().String())
		t.NoError(err)
		t.Equal([]string{"w0", "w1", "w2"}, ws, "0")

		ws, err = f(util.UUID().String())
		t.NoError(err)
		t.Equal([]string{"w0", "w1", "w2"}, ws, "1")

		ws, err = f(util.UUID().String())
		t.NoError(err)
		t.Equal([]string{"w0", "w1", "w2"}, ws, "2")
	})

	t.Run("workers == share", func() {
		workers := []string{"w0", "w1", "w2"}
		var share uint8 = 3

		f, err := serialStateOwnersFunc(workers, share)
		t.NoError(err)

		ws, err := f(util.UUID().String())
		t.NoError(err)
		t.Equal([]string{"w0", "w1", "w2"}, ws, "0")

		ws, err = f(util.UUID().String())
		t.NoError(err)
		t.Equal([]string{"w0", "w1", "w2"}, ws, "1")

		ws, err = f(util.UUID().String())
		t.NoError(err)
		t.Equal([]string{"w0", "w1", "w2"}, ws, "2")
	})
}

func TestSerialOpOwnersFunc(tt *testing.T) {
	t := new(suite.Suite)
	t.SetT(tt)

	t.Run("empty workers", func() {
		var workers []string
		var share uint8 = 2

		_, err := serialOpOwnersFunc(workers, share)
		t.Error(err)
		t.ErrorContains(err, "empty workers")
	})

	t.Run("zero share", func() {
		workers := []string{"w0", "w1", "w2"}
		var share uint8 = 0

		_, err := serialOpOwnersFunc(workers, share)
		t.Error(err)
		t.ErrorContains(err, "zero share")
	})

	t.Run("same opindex", func() {
		workers := []string{"w0", "w1", "w2"}
		var share uint8 = 2

		f, err := serialOpOwnersFunc(workers, share)
		t.NoError(err)

		ws, err := f(0, nil, nil)
		t.NoError(err)
		t.Equal([]string{"w0", "w1"}, ws, "0")

		ws, err = f(0, nil, nil)
		t.NoError(err)
		t.Equal([]string{"w0", "w1"}, ws, "0")

		ws, err = f(0, nil, nil)
		t.NoError(err)
		t.Equal([]string{"w0", "w1"}, ws, "0")
	})

	t.Run("workers > share", func() {
		workers := []string{"w0", "w1", "w2"}
		var share uint8 = 2

		f, err := serialOpOwnersFunc(workers, share)
		t.NoError(err)

		ws, err := f(0, nil, nil)
		t.NoError(err)
		t.Equal([]string{"w0", "w1"}, ws, "0")

		ws, err = f(1, nil, nil)
		t.NoError(err)
		t.Equal([]string{"w2", "w0"}, ws, "1")

		ws, err = f(2, nil, nil)
		t.NoError(err)
		t.Equal([]string{"w1", "w2"}, ws, "2")
	})

	t.Run("workers < share", func() {
		workers := []string{"w0", "w1", "w2"}
		var share uint8 = 4

		f, err := serialOpOwnersFunc(workers, share)
		t.NoError(err)

		ws, err := f(0, nil, nil)
		t.NoError(err)
		t.Equal([]string{"w0", "w1", "w2"}, ws, "0")

		ws, err = f(1, nil, nil)
		t.NoError(err)
		t.Equal([]string{"w0", "w1", "w2"}, ws, "1")

		ws, err = f(2, nil, nil)
		t.NoError(err)
		t.Equal([]string{"w0", "w1", "w2"}, ws, "2")
	})

	t.Run("workers == share", func() {
		workers := []string{"w0", "w1", "w2"}
		var share uint8 = 3

		f, err := serialOpOwnersFunc(workers, share)
		t.NoError(err)

		ws, err := f(0, nil, nil)
		t.NoError(err)
		t.Equal([]string{"w0", "w1", "w2"}, ws, "0")

		ws, err = f(1, nil, nil)
		t.NoError(err)
		t.Equal([]string{"w0", "w1", "w2"}, ws, "1")

		ws, err = f(2, nil, nil)
		t.NoError(err)
		t.Equal([]string{"w0", "w1", "w2"}, ws, "2")
	})
}

type testProcessManager struct {
	baseTestOperationProcessor
}

func (t *testProcessManager) TestNew() {
	t.Run("empty workers", func() {
		args := NewProcessManagerArgs()
		_, err := NewProcessManager(context.Background(), args, nil, nil)
		t.Error(err)
		t.ErrorContains(err, "empty workers")
	})

	t.Run("zero op share", func() {
		args := NewProcessManagerArgs()
		args.NOpShare = 0

		_, err := NewProcessManager(context.Background(), args, []string{"w0"}, nil)
		t.Error(err)
		t.ErrorContains(err, "zero operation share")
	})

	t.Run("zero state share", func() {
		args := NewProcessManagerArgs()
		args.NStateShare = 0

		_, err := NewProcessManager(context.Background(), args, []string{"w0"}, nil)
		t.Error(err)
		t.ErrorContains(err, "zero state share")
	})

	t.Run("NewJobWorker error", func() {
		args := NewProcessManagerArgs()
		args.NewJobWorker = func(context.Context) (util.JobWorker, error) { return nil, errors.Errorf("showme") }

		_, err := NewProcessManager(context.Background(), args, []string{"w0"}, nil)
		t.Error(err)
		t.ErrorContains(err, "showme")
	})

	t.Run("zero MapSize", func() {
		args := NewProcessManagerArgs()
		args.MapSize = 0

		_, err := NewProcessManager(context.Background(), args, []string{"w0"}, nil)
		t.Error(err)
		t.ErrorContains(err, "empty size")
	})
}

func (t *testProcessManager) TestArgs() {
	t.Run("op share > workers", func() {
		workers := []string{"w0"}
		args := NewProcessManagerArgs()
		args.NOpShare = 2

		_, err := NewProcessManager(context.Background(), args, workers, nil)
		t.NoError(err)
		t.Equal(uint8(len(workers)), args.NOpShare)
	})

	t.Run("state share > workers", func() {
		workers := []string{"w0"}
		args := NewProcessManagerArgs()
		args.NStateShare = 2

		_, err := NewProcessManager(context.Background(), args, workers, nil)
		t.NoError(err)
		t.Equal(uint8(len(workers)), args.NStateShare)
	})

	t.Run("empty ops", func() {
		workers := []string{"w0", "w1"}
		args := NewProcessManagerArgs()
		args.NStateShare = 1

		pm, err := NewProcessManager(context.Background(), args, workers, nil)
		t.NoError(err)
		t.Equal(workers[:args.NStateShare], pm.workers)
	})

	t.Run("IsEnoughWorkers: state share = 1", func() {
		workers := []string{"w0", "w1"}
		args := NewProcessManagerArgs()
		args.NStateShare = 1

		pm, err := NewProcessManager(context.Background(), args, workers, [][2]util.Hash{{valuehash.RandomSHA256(), valuehash.RandomSHA256()}})
		t.NoError(err)

		t.True(pm.args.IsEnoughWorkers(uint64(len(workers))))
		t.False(pm.args.IsEnoughWorkers(uint64(len(workers) - 1)))
	})

	t.Run("IsEnoughWorkers: state share = len(workers)", func() {
		workers := []string{"w0", "w1"}
		args := NewProcessManagerArgs()
		args.NStateShare = uint8(len(workers))

		pm, err := NewProcessManager(context.Background(), args, workers, [][2]util.Hash{{valuehash.RandomSHA256(), valuehash.RandomSHA256()}})
		t.NoError(err)

		t.True(pm.args.IsEnoughWorkers(uint64(len(workers))))
		t.True(pm.args.IsEnoughWorkers(uint64(len(workers) - 1)))
	})

	t.Run("IsEnoughWorkers: empty ops, state share = 1", func() {
		workers := []string{"w0", "w1"}
		args := NewProcessManagerArgs()
		args.NStateShare = 1

		pm, err := NewProcessManager(context.Background(), args, workers, nil)
		t.NoError(err)

		t.Equal([]string{"w0"}, pm.workers)
		t.True(pm.args.IsEnoughWorkers(2))
		t.True(pm.args.IsEnoughWorkers(1))
	})

	t.Run("IsEnoughWorkers: empty ops, state share = len(workers)", func() {
		workers := []string{"w0", "w1"}
		args := NewProcessManagerArgs()
		args.NStateShare = uint8(len(workers))

		pm, err := NewProcessManager(context.Background(), args, workers, nil)
		t.NoError(err)

		t.Equal(workers, pm.workers)
		t.True(pm.args.IsEnoughWorkers(2))
		t.True(pm.args.IsEnoughWorkers(1))
	})
}

func (t *testProcessManager) TestCancel() {
	t.Run("before process, and then process", func() {
		workers := []string{"w0", "w1"}
		args := NewProcessManagerArgs()

		pm, err := NewProcessManager(context.Background(), args, workers, nil)
		t.NoError(err)

		pm.Cancel()

		err = pm.Process()
		t.ErrorIs(err, context.Canceled)
	})

	t.Run("cancel during process", func() {
		workers := []string{"w0", "w1"}
		args := NewProcessManagerArgs()

		startprocessch := make(chan uint64)
		args.WorkerNewOperations = func(ctx context.Context, worker string) (
			workerNewOperationsFunc,
			func() error,
			func(),
			error,
		) {
			return func(opindex uint64, _, _ util.Hash) error {
					startprocessch <- opindex

					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-time.After(time.Second * 33):
						return nil
					}
				},
				func() error { return nil },
				func() {},
				nil
		}

		ops := [][2]util.Hash{{valuehash.RandomSHA256(), valuehash.RandomSHA256()}}

		pm, err := NewProcessManager(context.Background(), args, workers, ops)
		t.NoError(err)
		defer pm.Cancel()

		processerrch := make(chan error, 1)
		go func() {
			processerrch <- pm.Process()
		}()

		<-startprocessch
		pm.Cancel()

		err = <-processerrch
		t.ErrorIs(err, context.Canceled)
	})

	t.Run("ManifestDone; cancel", func() {
		workers := []string{"w0", "w1"}
		args := NewProcessManagerArgs()

		args.WorkerNewOperations = func(ctx context.Context, worker string) (
			workerNewOperationsFunc,
			func() error,
			func(),
			error,
		) {
			return func(uint64, util.Hash, util.Hash) error { return nil },
				func() error { return nil },
				func() {},
				nil
		}

		ops := [][2]util.Hash{{valuehash.RandomSHA256(), valuehash.RandomSHA256()}}

		pm, err := NewProcessManager(context.Background(), args, workers, ops)
		t.NoError(err)
		defer pm.Cancel()

		processerrch := make(chan error, 1)
		go func() {
			processerrch <- pm.Process()
		}()

		t.NoError(<-processerrch)

		doneerrch := make(chan error, 1)
		go func() {
			_, err := pm.ManifestDone()

			doneerrch <- err
		}()

		<-time.After(time.Second)
		pm.Cancel()

		err = <-doneerrch
		t.ErrorIs(err, context.Canceled)
	})

	t.Run("StoreDone; cancel", func() {
		workers := []string{"w0", "w1"}
		args := NewProcessManagerArgs()

		args.WorkerNewOperations = func(ctx context.Context, worker string) (
			workerNewOperationsFunc,
			func() error,
			func(),
			error,
		) {
			return func(uint64, util.Hash, util.Hash) error { return nil },
				func() error { return nil },
				func() {},
				nil
		}

		ops := [][2]util.Hash{{valuehash.RandomSHA256(), valuehash.RandomSHA256()}}

		pm, err := NewProcessManager(context.Background(), args, workers, ops)
		t.NoError(err)
		defer pm.Cancel()

		processerrch := make(chan error, 1)
		go func() {
			processerrch <- pm.Process()
		}()

		t.NoError(<-processerrch)

		doneerrch := make(chan error, 1)
		go func() {
			doneerrch <- pm.StoreDone()
		}()

		<-time.After(time.Second)
		pm.Cancel()

		err = <-doneerrch
		t.ErrorIs(err, context.Canceled)
	})
}

func (t *testProcessManager) TestWorkerNewOperations() {
	workers := []string{"w0", "w1", "w3"}
	ops := make([][2]util.Hash, 33)
	for i := range ops {
		ops[i] = [2]util.Hash{valuehash.RandomSHA256(), valuehash.RandomSHA256()}
	}

	args := NewProcessManagerArgs()
	args.NOpShare = 1

	collectedWorkers := util.NewSingleLockedMap[string, []uint64]()
	collectedOps := util.NewSingleLockedMap[uint64, util.Hash]()
	collectedFacts := util.NewSingleLockedMap[uint64, util.Hash]()

	donech := map[string]chan struct{}{}
	for i := range workers {
		donech[workers[i]] = make(chan struct{}, 1)
	}

	args.WorkerNewOperations = func(ctx context.Context, worker string) (
		workerNewOperationsFunc,
		func() error,
		func(),
		error,
	) {
		return func(opindex uint64, op, fact util.Hash) error {
				if _, _, err := collectedWorkers.Set(worker, func(i []uint64, _ bool) ([]uint64, error) {
					if slices.Index(i, opindex) >= 0 {
						return nil, util.ErrFound.Errorf("workers: opindex=%d", opindex)
					}

					i = append(i, opindex)

					return i, nil
				}); err != nil {
					return err
				}

				if _, _, err := collectedOps.Set(opindex, func(_ util.Hash, found bool) (util.Hash, error) {
					if found {
						return nil, util.ErrFound.Errorf("ops: opindex=%d", opindex)
					}

					return op, nil
				}); err != nil {
					return err
				}

				if _, _, err := collectedFacts.Set(opindex, func(_ util.Hash, found bool) (util.Hash, error) {
					if found {
						return nil, util.ErrFound.Errorf("facts: opindex=%d", opindex)
					}

					return fact, nil
				}); err != nil {
					return err
				}

				return nil
			},
			func() error {
				donech[worker] <- struct{}{}

				return nil
			},
			func() {},
			nil
	}

	pm, err := NewProcessManager(context.Background(), args, workers, ops)
	t.NoError(err)
	defer pm.Cancel()

	err = pm.Process()
	t.NoError(err)

	for i := range donech {
		<-donech[i]
	}

	expected := map[string][]uint64{
		"w0": {0, 3, 6, 9, 12, 15, 18, 21, 24, 27, 30},
		"w1": {1, 4, 7, 10, 13, 16, 19, 22, 25, 28, 31},
		"w3": {2, 5, 8, 11, 14, 17, 20, 23, 26, 29, 32},
	}

	t.Equal(expected, collectedWorkers.Map())
	t.Equal(len(ops), collectedOps.Len())
	t.Equal(len(ops), collectedFacts.Len())
	collectedOps.Traverse(func(opindex uint64, op util.Hash) bool {
		t.True(ops[opindex][0].Equal(op))

		return true
	})
	collectedFacts.Traverse(func(opindex uint64, fact util.Hash) bool {
		t.True(ops[opindex][1].Equal(fact))

		return true
	})
}

func (t *testProcessManager) TestWorkerNewOperationsWithEmptyOps() {
	workers := []string{"w0", "w1", "w3"}

	args := NewProcessManagerArgs()
	args.NOpShare = 1
	args.NStateShare = uint8(len(workers)) // prevent to shrink workers

	donech := map[string]chan struct{}{}
	for i := range workers {
		donech[workers[i]] = make(chan struct{}, 1)
	}

	args.WorkerNewOperations = func(ctx context.Context, worker string) (
		workerNewOperationsFunc,
		func() error,
		func(),
		error,
	) {
		return func(uint64, util.Hash, util.Hash) error {
				return nil
			},
			func() error {
				donech[worker] <- struct{}{}

				return nil
			},
			func() {},
			nil
	}

	pm, err := NewProcessManager(context.Background(), args, workers, nil)
	t.NoError(err)
	defer pm.Cancel()

	err = pm.Process()
	t.NoError(err)

	for i := range donech {
		<-donech[i]
	}
}

func (t *testProcessManager) TestProcessOneWorker() {
	workers := []string{"w0"}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pwargs := NewProcessWorkerArgs(t.networkID)
	pw, err := NewProcessWorker(ctx, pwargs, "w0", 33)
	t.NoError(err)

	ops := make([]Operation, 11)
	opi := make([][2]util.Hash, len(ops))
	ophs := map[string]uint64{}
	opfhs := map[string]uint64{}
	states := map[string]base.State{}
	for i := range ops {
		opindex := uint64(i)
		op := t.randomOp(1)
		ops[i] = op
		opi[i] = [2]util.Hash{op.Hash(), op.Fact().Hash()}
		ophs[op.Hash().String()] = opindex
		opfhs[op.Fact().Hash().String()] = opindex

		issuer := op.Fact().(OpFact).Issuer
		stateKey := AccountStateKey(issuer)
		states[stateKey] = base.NewBaseState(
			33,
			stateKey,
			NewAccountStateValue(issuer, 1),
			nil,
			[]util.Hash{valuehash.RandomSHA256()},
		)
	}

	args := NewProcessManagerArgs()
	pm, err := NewProcessManager(ctx, args, workers, opi)
	t.NoError(err)
	defer pm.Cancel()

	pw.args.GetOperationFunc = func(ctx context.Context, op util.Hash) (Operation, bool, error) {
		switch opindex, found := ophs[op.String()]; {
		case !found:
			return nil, false, nil
		default:
			return ops[opindex], true, nil
		}
	}
	pw.args.GetStateFunc = func(_ context.Context, key string) (base.State, bool, error) {
		i, found := states[key]

		return i, found, nil
	}

	pw.args.IsEnoughWorkers = pm.args.IsEnoughWorkers

	resultMergedOps := util.NewSingleLockedMap[string, []uint64]()
	resultUnmergedOps := util.NewSingleLockedMap[uint64, string]()

	pw.args.UpdateMergeDoneFunc = func(context.Context) (
		updateState func(base.State) error,
		updateReason func(opindex uint64, reason string) error,
		done func() error,
		_ error,
	) {
		pmUpdateState, pmUpdateReason, pmDone, err := pm.UpdateMerged(pw.local)
		if err != nil {
			return nil, nil, nil, err
		}

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

				return pmUpdateState(st.Key(), st.Hash())
			},
			func(opindex uint64, reason string) error {
				if !resultUnmergedOps.Exists(opindex) && len(reason) > 0 {
					resultUnmergedOps.SetValue(opindex, reason)
				}

				return pmUpdateReason(opindex, reason)
			},
			func() error {
				return pmDone()
			},
			nil
	}

	args.WorkerNewOperations = func(ctx context.Context, worker string) (
		workerNewOperationsFunc,
		func() error,
		func(),
		error,
	) {
		process, done, cancel, err := pw.NewOperations()
		if err != nil {
			return nil, nil, nil, err
		}
		return process, func() error {
				done()
				return nil
			}, func() {
				cancel(nil)
			}, nil
	}

	pwmergedoneerrch := make(chan error, 1)
	go func() {
		pwmergedoneerrch <- pw.MergeWait()
	}()

	err = pm.Process()
	t.NoError(err)

	t.NoError(<-pwmergedoneerrch)

	_, err = pm.ManifestDone()
	t.NoError(err)

	t.Equal(len(ops)*2, len(resultMergedOps.Map()))
	t.Empty(resultUnmergedOps.Map())

	// FIXME check manifest
}

func TestProcessManager(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testProcessManager))
}
