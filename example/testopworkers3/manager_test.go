package testopsworkers

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/pkg/errors"
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

func (t *testProcessManager) newManager(
	ctx context.Context,
	workers []string,
	ops [][2]util.Hash,
) *ProcessManager {
	args := NewProcessManagerArgs()
	pm, err := NewProcessManager(ctx, args, workers, ops)
	t.NoError(err)

	return pm
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

func (t *testProcessManager) TestWorkrNewOperations() {
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

	args.WorkerNewOperations = func(ctx context.Context, worker string) (
		workerNewOperationsFunc,
		func() error,
		func(),
		error,
	) {
		return func(opindex uint64, op, fact util.Hash) error {
				_, _, err := collectedWorkers.Set(worker, func(i []uint64, _ bool) ([]uint64, error) {
					if slices.Index(i, opindex) >= 0 {
						return nil, util.ErrFound.Errorf("workers: opindex=%d", opindex)
					}

					i = append(i, opindex)

					return i, nil
				})

				_, _, err := collectedOps.Set(opindex, func(_ util.Hash, found bool) (util.Hash, error) {
					if found {
						return nil, util.ErrFound.Errorf("ops: opindex=%d", opindex)
					}

					return op, nil
				})

				_, _, err := collectedFacts.Set(opindex, func(_ util.Hash, found bool) (util.Hash, error) {
					if found {
						return nil, util.ErrFound.Errorf("facts: opindex=%d", opindex)
					}

					return fact, nil
				})

				return err
			},
			func() error { return nil },
			func() {},
			nil
	}

	pm, err := NewProcessManager(context.Background(), args, workers, ops)
	t.NoError(err)
	defer pm.Cancel()

	err = pm.Process()
	t.NoError(err)

	expected := map[string][]uint64{
		"w0": {0, 3, 6, 9, 12, 15, 18, 21, 24, 27, 30},
		"w1": {1, 4, 7, 10, 13, 16, 19, 22, 25, 28, 31},
		"w3": {2, 5, 8, 11, 14, 17, 20, 23, 26, 29, 32},
	}

	t.Equal(expected, collectedWorkers.Map())
}

func TestProcessManager(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testProcessManager))
}
