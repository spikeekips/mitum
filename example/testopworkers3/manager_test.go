package testopsworkers

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
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

func (t *testProcessManager) workers(n uint8) (workers []string) {
	workers = make([]string, n)
	for i := range workers {
		workers[i] = fmt.Sprintf("w%02d", i)
	}

	return workers
}

func (t *testProcessManager) newDummyManangerWithError(
	ctx context.Context,
	workers []string,
) (*ProcessManager, error) {
	args := NewProcessManagerArgs(workers)
	args.WorkerCancel = func(context.Context, string) error { return nil }
	args.WorkerPrepare = func(context.Context, string, processWorkerPrepareArgs) error { return nil }

	pr := t.newProposal(33, nil)

	return NewProcessManager(ctx, args, valuehash.RandomSHA256(), valuehash.RandomSHA256(), pr)
}

func (t *testProcessManager) newDummyMananger(
	ctx context.Context,
	workers []string,
) *ProcessManager {
	pm, err := t.newDummyManangerWithError(ctx, workers)
	t.NoError(err)

	return pm
}

func (t *testProcessManager) newProposal(height base.Height, ops [][2]util.Hash) base.ProposalSignFact {
	priv := base.NewDummyPrivatekey("proposer")

	sf := isaac.NewProposalSignFact(
		isaac.NewProposalFact(
			base.NewPoint(height, 0),
			base.SimpleAddress("proposer"),
			valuehash.RandomSHA256(),
			ops,
		),
	)

	t.NoError(sf.Sign(priv, t.networkID))

	return sf
}

func (t *testProcessManager) TestNew() {
	t.Run("empty workers", func() {
		args := NewProcessManagerArgs(nil)
		pr := t.newProposal(33, nil)
		_, err := NewProcessManager(context.Background(), args, valuehash.RandomSHA256(), valuehash.RandomSHA256(), pr)
		t.Error(err)
		t.ErrorContains(err, "empty workers")
	})

	t.Run("zero op share", func() {
		args := NewProcessManagerArgs([]string{"w0"})
		args.NOpShare = 0

		pr := t.newProposal(33, nil)
		_, err := NewProcessManager(context.Background(), args, valuehash.RandomSHA256(), valuehash.RandomSHA256(), pr)
		t.Error(err)
		t.ErrorContains(err, "zero operation share")
	})

	t.Run("zero state share", func() {
		args := NewProcessManagerArgs([]string{"w0"})
		args.NStateShare = 0

		pr := t.newProposal(33, nil)
		_, err := NewProcessManager(context.Background(), args, valuehash.RandomSHA256(), valuehash.RandomSHA256(), pr)
		t.Error(err)
		t.ErrorContains(err, "zero state share")
	})

	t.Run("NewJobWorker error", func() {
		args := NewProcessManagerArgs([]string{"w0"})
		args.NewJobWorker = func(context.Context) (util.JobWorker, error) { return nil, errors.Errorf("showme") }

		pr := t.newProposal(33, nil)
		_, err := NewProcessManager(context.Background(), args, valuehash.RandomSHA256(), valuehash.RandomSHA256(), pr)
		t.Error(err)
		t.ErrorContains(err, "showme")
	})

	t.Run("zero MapSize", func() {
		args := NewProcessManagerArgs([]string{"w0"})
		args.MapSize = 0

		pr := t.newProposal(33, nil)
		_, err := NewProcessManager(context.Background(), args, valuehash.RandomSHA256(), valuehash.RandomSHA256(), pr)
		t.Error(err)
		t.ErrorContains(err, "empty size")
	})
}

func (t *testProcessManager) TestArgs() {
	t.Run("op share > workers", func() {
		workers := []string{"w0"}
		args := NewProcessManagerArgs(workers)
		args.WorkerCancel = func(context.Context, string) error { return nil }
		args.NOpShare = 2

		pr := t.newProposal(33, nil)
		pm, err := NewProcessManager(context.Background(), args, valuehash.RandomSHA256(), valuehash.RandomSHA256(), pr)
		t.NoError(err)
		defer pm.Cancel()

		t.Equal(uint8(len(workers)), args.NOpShare)
	})

	t.Run("state share > workers", func() {
		workers := []string{"w0"}
		args := NewProcessManagerArgs(workers)
		args.WorkerCancel = func(context.Context, string) error { return nil }
		args.NStateShare = 2

		pr := t.newProposal(33, nil)
		pm, err := NewProcessManager(context.Background(), args, valuehash.RandomSHA256(), valuehash.RandomSHA256(), pr)
		t.NoError(err)
		defer pm.Cancel()

		t.Equal(uint8(len(workers)), args.NStateShare)
	})

	t.Run("empty ops", func() {
		workers := []string{"w0", "w1"}
		args := NewProcessManagerArgs(workers)
		args.WorkerCancel = func(context.Context, string) error { return nil }
		args.NStateShare = 1

		pr := t.newProposal(33, nil)
		pm, err := NewProcessManager(context.Background(), args, valuehash.RandomSHA256(), valuehash.RandomSHA256(), pr)
		t.NoError(err)
		defer pm.Cancel()

		t.Equal(workers[:args.NStateShare], pm.args.Workers)
	})

	t.Run("IsEnoughWorkers: state share = 1", func() {
		workers := []string{"w0", "w1"}
		args := NewProcessManagerArgs(workers)
		args.WorkerCancel = func(context.Context, string) error { return nil }
		args.NStateShare = 1

		pr := t.newProposal(33, [][2]util.Hash{{valuehash.RandomSHA256(), valuehash.RandomSHA256()}})
		pm, err := NewProcessManager(context.Background(), args, valuehash.RandomSHA256(), valuehash.RandomSHA256(), pr)
		t.NoError(err)
		defer pm.Cancel()

		t.True(pm.args.IsEnoughWorkers(uint64(len(workers))))
		t.False(pm.args.IsEnoughWorkers(uint64(len(workers) - 1)))
	})

	t.Run("IsEnoughWorkers: state share = len(workers)", func() {
		workers := []string{"w0", "w1"}
		args := NewProcessManagerArgs(workers)
		args.WorkerCancel = func(context.Context, string) error { return nil }
		args.NStateShare = uint8(len(workers))

		pr := t.newProposal(33, [][2]util.Hash{{valuehash.RandomSHA256(), valuehash.RandomSHA256()}})
		pm, err := NewProcessManager(context.Background(), args, valuehash.RandomSHA256(), valuehash.RandomSHA256(), pr)
		t.NoError(err)
		defer pm.Cancel()

		t.True(pm.args.IsEnoughWorkers(uint64(len(workers))))
		t.True(pm.args.IsEnoughWorkers(uint64(len(workers) - 1)))
	})

	t.Run("IsEnoughWorkers: empty ops, state share = 1", func() {
		workers := []string{"w0", "w1"}
		args := NewProcessManagerArgs(workers)
		args.WorkerCancel = func(context.Context, string) error { return nil }
		args.NStateShare = 1

		pr := t.newProposal(33, nil)
		pm, err := NewProcessManager(context.Background(), args, valuehash.RandomSHA256(), valuehash.RandomSHA256(), pr)
		t.NoError(err)
		defer pm.Cancel()

		t.Equal([]string{"w0"}, pm.args.Workers)
		t.True(pm.args.IsEnoughWorkers(2))
		t.True(pm.args.IsEnoughWorkers(1))
	})

	t.Run("IsEnoughWorkers: empty ops, state share = len(workers)", func() {
		workers := []string{"w0", "w1"}
		args := NewProcessManagerArgs(workers)
		args.WorkerCancel = func(context.Context, string) error { return nil }
		args.NStateShare = uint8(len(workers))

		pr := t.newProposal(33, nil)
		pm, err := NewProcessManager(context.Background(), args, valuehash.RandomSHA256(), valuehash.RandomSHA256(), pr)
		t.NoError(err)
		defer pm.Cancel()

		t.Equal(workers, pm.args.Workers)
		t.True(pm.args.IsEnoughWorkers(2))
		t.True(pm.args.IsEnoughWorkers(1))
	})
}

func (t *testProcessManager) TestCancel() {
	t.Run("cancel ctx", func() {
		ctx, cancel := context.WithCancel(context.Background())

		workers := []string{"w0", "w1"}
		pm := t.newDummyMananger(ctx, workers)

		cancel()

		err := pm.Process()
		t.ErrorIs(err, context.Canceled)
	})

	t.Run("before process, and then process", func() {
		workers := []string{"w0", "w1"}
		pm := t.newDummyMananger(context.Background(), workers)

		pm.Cancel()

		err := pm.Process()
		t.ErrorIs(err, context.Canceled)
	})

	t.Run("cancel during process", func() {
		workers := []string{"w0", "w1"}
		args := NewProcessManagerArgs(workers)
		args.WorkerCancel = func(context.Context, string) error { return nil }
		args.WorkerPrepare = func(context.Context, string, processWorkerPrepareArgs) error { return nil }

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
		pr := t.newProposal(33, ops)

		pm, err := NewProcessManager(context.Background(), args, valuehash.RandomSHA256(), valuehash.RandomSHA256(), pr)
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
		args := NewProcessManagerArgs(workers)

		args.WorkerCancel = func(context.Context, string) error { return nil }
		args.WorkerPrepare = func(context.Context, string, processWorkerPrepareArgs) error { return nil }
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
		pr := t.newProposal(33, ops)

		pm, err := NewProcessManager(context.Background(), args, valuehash.RandomSHA256(), valuehash.RandomSHA256(), pr)
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
		args := NewProcessManagerArgs(workers)

		args.WorkerCancel = func(context.Context, string) error { return nil }
		args.WorkerPrepare = func(context.Context, string, processWorkerPrepareArgs) error { return nil }
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
		pr := t.newProposal(33, ops)

		pm, err := NewProcessManager(context.Background(), args, valuehash.RandomSHA256(), valuehash.RandomSHA256(), pr)
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

	t.Run("cancel workers", func() {
		workers := t.workers(2)
		args := NewProcessManagerArgs(workers)

		workercancelch := make(chan string, len(workers))
		args.WorkerCancel = func(_ context.Context, worker string) error {
			workercancelch <- worker

			return nil
		}

		args.WorkerPrepare = func(context.Context, string, processWorkerPrepareArgs) error { return nil }
		args.WorkerNewOperations = func(ctx context.Context, worker string) (
			workerNewOperationsFunc,
			func() error,
			func(),
			error,
		) {
			return func(uint64, util.Hash, util.Hash) error { return nil },
				func() error {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-time.After(time.Second * 3):
						return nil
					}
				},
				func() {},
				nil
		}

		ops := [][2]util.Hash{{valuehash.RandomSHA256(), valuehash.RandomSHA256()}}
		pr := t.newProposal(33, ops)

		pm, err := NewProcessManager(context.Background(), args, valuehash.RandomSHA256(), valuehash.RandomSHA256(), pr)
		t.NoError(err)
		defer pm.Cancel()

		processerrch := make(chan error, 1)
		go func() {
			processerrch <- pm.Process()
		}()

		<-time.After(time.Second)
		pm.Cancel()

		err = <-processerrch
		t.ErrorIs(err, context.Canceled)

		addc, donec := util.SortCompactAppendSlice[string](len(workers), cmp.Less[string])
		for worker := range workercancelch {
			if addc(worker) {
				break
			}
		}
		t.Equal(workers, donec())
	})
}

func (t *testProcessManager) TestWorkerPrepare() {
	ops := [][2]util.Hash{{valuehash.RandomSHA256(), valuehash.RandomSHA256()}}

	workerNewOperations := func(context.Context, string) (
		workerNewOperationsFunc,
		func() error,
		func(),
		error,
	) {
		return func(uint64, util.Hash, util.Hash) error {
				return nil
			},
			func() error { return nil },
			func() {},
			nil
	}

	t.Run("ok", func() {
		workers := t.workers(2)

		args := NewProcessManagerArgs(workers)
		args.WorkerCancel = func(context.Context, string) error { return nil }
		args.WorkerNewOperations = workerNewOperations
		args.WorkerPrepare = func(context.Context, string, processWorkerPrepareArgs) error {
			return nil
		}
		args.NOpShare = 1

		pr := t.newProposal(33, ops)
		pm, err := NewProcessManager(context.Background(), args, valuehash.RandomSHA256(), valuehash.RandomSHA256(), pr)
		t.NoError(err)
		defer pm.Cancel()

		t.NoError(pm.Process())
	})

	t.Run("all fails", func() {
		workers := t.workers(2)

		args := NewProcessManagerArgs(workers)
		args.WorkerCancel = func(context.Context, string) error { return nil }
		args.WorkerNewOperations = workerNewOperations
		args.WorkerPrepare = func(context.Context, string, processWorkerPrepareArgs) error {
			return errors.Errorf("showme")
		}
		args.NOpShare = 2

		pr := t.newProposal(33, ops)
		pm, err := NewProcessManager(context.Background(), args, valuehash.RandomSHA256(), valuehash.RandomSHA256(), pr)
		t.NoError(err)
		defer pm.Cancel()

		err = pm.Process()
		t.ErrorContains(err, "failed to prepare")
	})

	t.Run("safe fails, but under NOpShare", func() {
		workers := t.workers(3)

		args := NewProcessManagerArgs(workers)
		args.WorkerCancel = func(context.Context, string) error { return nil }
		args.WorkerNewOperations = workerNewOperations
		args.WorkerPrepare = func(_ context.Context, worker string, _ processWorkerPrepareArgs) error {
			if slices.Index(workers, worker) == 2 {
				return errors.Errorf("showme")
			}

			return nil
		}
		args.NOpShare = 1
		args.NStateShare = 2

		pr := t.newProposal(33, ops)
		pm, err := NewProcessManager(context.Background(), args, valuehash.RandomSHA256(), valuehash.RandomSHA256(), pr)
		t.NoError(err)
		defer pm.Cancel()

		err = pm.Process()
		t.ErrorContains(err, "failed to prepare")
	})

	t.Run("safe fails, but under NStateShare", func() {
		workers := t.workers(3)

		args := NewProcessManagerArgs(workers)
		args.WorkerCancel = func(context.Context, string) error { return nil }
		args.WorkerNewOperations = workerNewOperations
		args.WorkerPrepare = func(_ context.Context, worker string, _ processWorkerPrepareArgs) error {
			if slices.Index(workers, worker) == 2 {
				return errors.Errorf("showme")
			}

			return nil
		}
		args.NOpShare = 2
		args.NStateShare = 1

		pr := t.newProposal(33, ops)
		pm, err := NewProcessManager(context.Background(), args, valuehash.RandomSHA256(), valuehash.RandomSHA256(), pr)
		t.NoError(err)
		defer pm.Cancel()

		err = pm.Process()
		t.ErrorContains(err, "failed to prepare")
	})
}

func (t *testProcessManager) TestWorkerNewOperations() {
	workers := []string{"w0", "w1", "w3"}
	ops := make([][2]util.Hash, 33)
	for i := range ops {
		ops[i] = [2]util.Hash{valuehash.RandomSHA256(), valuehash.RandomSHA256()}
	}

	args := NewProcessManagerArgs(workers)
	args.WorkerCancel = func(context.Context, string) error { return nil }
	args.WorkerPrepare = func(context.Context, string, processWorkerPrepareArgs) error { return nil }
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

	pr := t.newProposal(33, ops)
	pm, err := NewProcessManager(context.Background(), args, valuehash.RandomSHA256(), valuehash.RandomSHA256(), pr)
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

	previousManifest := valuehash.RandomSHA256()
	previousSuffrage := valuehash.RandomSHA256()
	pr := t.newProposal(33, nil)

	args := NewProcessManagerArgs(workers)
	args.NOpShare = 1
	args.NStateShare = uint8(len(workers)) // prevent to shrink workers
	args.WorkerCancel = func(context.Context, string) error { return nil }
	args.WorkerPrepare = func(context.Context, string, processWorkerPrepareArgs) error { return nil }

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
				return nil
			},
			func() {},
			nil
	}

	pm, err := NewProcessManager(context.Background(), args, previousManifest, previousSuffrage, pr)
	t.NoError(err)
	defer pm.Cancel()

	err = pm.Process()
	t.NoError(err)

	manifest, err := pm.ManifestDone()
	t.NoError(err)

	t.T().Log("check manifest")
	t.NotNil(manifest)
	t.NoError(manifest.IsValid(nil))
	t.Equal(pr.Point().Height(), manifest.Height())
	t.True(previousManifest.Equal(manifest.Previous()))
	t.True(previousSuffrage.Equal(manifest.Suffrage()))
	t.Equal(pr.ProposalFact().ProposedAt(), manifest.ProposedAt())
	t.Nil(manifest.OperationsTree())
	t.Nil(manifest.StatesTree())
}

func (t *testProcessManager) newProcessWorker(
	ctx context.Context,
	local string,
	height base.Height,
	other func(string) *ProcessWorker,
	others func() []*ProcessWorker,
	getOperationFunc func(context.Context, util.Hash) (Operation, bool, error),
	getFactFunc func(util.Hash) (uint64, bool),
	getStateFunc func(context.Context, string) (base.State, bool, error),
	isEnoughWorkers func(uint64) bool,
	stateOwners func(string) ([]string, error),
	resultMergedOps util.LockedMap[string, []uint64],
	resultUnmergedOps util.LockedMap[uint64, string],
	updateMerged func(string) (
		updateState func(string, util.Hash) error,
		updateReason func(uint64, string) error,
		done func() error,
		_ error,
	),
) (*ProcessWorker, error) {
	pwargs := NewProcessWorkerArgs(t.networkID)
	pw, err := NewProcessWorker(ctx, pwargs, local, height)
	if err != nil {
		return nil, err
	}

	pw.args.GetOperationFunc = getOperationFunc
	pw.args.GetStateFunc = getStateFunc
	pw.args.IsEnoughWorkers = isEnoughWorkers
	pw.args.StateOwners = func(_ context.Context, stateKey string) ([]string, error) {
		return stateOwners(stateKey)
	}
	pw.args.BroadcastInsertValuesDoneFunc = func(context.Context) error {
		return util.TraverseSliceV1(others(), func(_ int, pw *ProcessWorker) (bool, error) {
			return true, pw.WorkerInsertValuesDone(local)
		})
	}
	pw.args.BroadcastInsertOpFunc = func(_ context.Context, worker string, opindex uint64, fact util.Hash, values []StateMergeValue) error {
		return other(worker).InsertValues(local, opindex, fact, values)
	}

	pw.args.BroadcastPhaseDoneFunc = func(_ context.Context, phase int, mergedOps, unmergedOps []uint64) error {
		return util.TraverseSliceV1(others(), func(_ int, pw *ProcessWorker) (bool, error) {
			return true, pw.WorkerPhaseDone(local, phase, mergedOps, unmergedOps)
		})
	}

	pw.args.UpdateMergeDoneFunc = func(_ context.Context) (
		updateState func(base.State) error,
		updateReason func(opindex uint64, reason string) error,
		done func() error,
		_ error,
	) {
		pmUpdateState, pmUpdateReason, pmDone, err := updateMerged(local)
		if err != nil {
			return nil, nil, nil, err
		}

		return func(st base.State) error {
				add, done := util.CompactAppendSlice[uint64](len(st.Operations()))
				if err := util.TraverseSliceV1(st.Operations(), func(_ int, i util.Hash) (bool, error) {
					j, found := getFactFunc(i)
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

	return pw, nil
}

func (t *testProcessManager) testNWorkers(
	n uint8,
	pr base.ProposalSignFact,
	ops []Operation,
	states map[string]base.State,
	previousManifest, previousSuffrage util.Hash,
) (
	resultMergedOps util.LockedMap[string, []uint64],
	resultUnmergedOps util.LockedMap[uint64, string],
	manifest base.Manifest,
) {
	workers := t.workers(n)

	ophs := map[string]uint64{}
	opfhs := map[string]uint64{}
	for i := range ops {
		opindex := uint64(i)
		op := ops[i]

		ops[i] = op
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
	getStateFunc := func(_ context.Context, key string) (base.State, bool, error) {
		i, found := states[key]

		return i, found, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	args := NewProcessManagerArgs(workers)

	pm, err := NewProcessManager(ctx, args, previousManifest, previousSuffrage, pr)
	t.NoError(err)
	defer pm.Cancel()

	resultMergedOps = util.NewSingleLockedMap[string, []uint64]()
	resultUnmergedOps = util.NewSingleLockedMap[uint64, string]()

	pws := util.NewSingleLockedMap[string, *ProcessWorker]()

	args.WorkerCancel = func(_ context.Context, worker string) error {
		i, found := pws.Value(worker)
		if found {
			i.Cancel()
		}

		return nil
	}
	args.WorkerPrepare = func(_ context.Context, worker string, args processWorkerPrepareArgs) error {
		pw, err := t.newProcessWorker(
			ctx,
			worker,
			args.Height,
			func(other string) *ProcessWorker {
				pw, _ := pws.Value(other)

				return pw
			},
			func() []*ProcessWorker {
				add, done := util.CompactAppendSlice[*ProcessWorker](len(workers))
				pws.Traverse(func(other string, pw *ProcessWorker) bool {
					if other != worker {
						add(pw)
					}

					return true
				})

				return done()
			},
			getOperationFunc,
			func(fact util.Hash) (uint64, bool) {
				i, found := opfhs[fact.String()]
				return i, found
			},
			getStateFunc,
			newIsEnoughWorkers(uint64(len(workers)), uint64(args.NStateShare)),
			pm.StateOwners,
			resultMergedOps,
			resultUnmergedOps,
			pm.UpdateMerged,
		)
		if err != nil {
			return err
		}

		pws.SetValue(worker, pw)

		return nil
	}

	args.WorkerNewOperations = func(ctx context.Context, worker string) (
		workerNewOperationsFunc,
		func() error,
		func(),
		error,
	) {
		pw, found := pws.Value(worker)
		if !found {
			return nil, nil, nil, util.ErrNotFound.Errorf("worker, %q", worker)
		}

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

	err = pm.Process()
	t.NoError(err)

	for i := range workers {
		worker := workers[i]

		pw, found := pws.Value(worker)
		if !found {
			panic(util.ErrNotFound.Errorf("worker, %q not found", worker))
		}

		t.NoError(pw.MergeWait())
	}

	manifest, err = pm.ManifestDone()
	t.NoError(err)

	return resultMergedOps, resultUnmergedOps, manifest
}

func (t *testProcessManager) TestWorkers() {
	previousManifest := valuehash.RandomSHA256()
	previousSuffrage := valuehash.RandomSHA256()

	ops := make([]Operation, 11)
	opi := make([][2]util.Hash, len(ops))
	states := map[string]base.State{}
	for i := range ops {
		op := t.randomOp(1)
		ops[i] = op
		opi[i] = [2]util.Hash{op.Hash(), op.Fact().Hash()}

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

	pr := t.newProposal(33, opi)

	var manifest base.Manifest
	var expectedMergedOps map[string][]uint64
	var expectedUnmergedOps map[uint64]string

	t.Run("one worker", func() {
		rm, ru, m := t.testNWorkers(1, pr, ops, states, previousManifest, previousSuffrage)

		manifest = m

		expectedMergedOps = rm.Map()
		expectedUnmergedOps = ru.Map()

		t.Equal(len(ops)*2, len(expectedMergedOps))
		t.Empty(expectedUnmergedOps)

		t.T().Log("check manifest")
		t.NotNil(m)
		t.NoError(m.IsValid(nil))
		t.Equal(pr.Point().Height(), m.Height())
		t.True(previousManifest.Equal(m.Previous()))
		t.True(previousSuffrage.Equal(m.Suffrage()))
		t.Equal(pr.ProposalFact().ProposedAt(), m.ProposedAt())
		t.NotNil(m.OperationsTree())
		t.NotNil(m.StatesTree())
	})

	t.Run("3 worker", func() {
		rm, ru, m := t.testNWorkers(1, pr, ops, states, previousManifest, previousSuffrage)

		t.Equal(expectedMergedOps, rm.Map())
		t.Equal(expectedUnmergedOps, ru.Map())

		base.EqualManifest(t.Assert(), manifest, m)
	})
}

func TestProcessManager(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testProcessManager))
}
