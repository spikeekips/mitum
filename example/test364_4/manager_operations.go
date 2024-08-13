package main

import (
	"context"
	"math"
	"slices"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/fixedtree"
	"github.com/spikeekips/mitum/util/logging"
)

type (
	managerWorkerOperationFunc                 func(opindex uint64, oph, fact util.Hash) error
	managerWorkerOperationsFunc                func(_ context.Context, worker string, _ func(managerWorkerOperationFunc) error) error
	managerWorkersOperationsWorkerUnmergedFunc func(worker string, unmerge []uint64) (bool, error)
	managerWorkersOperationsFuncs              func(_ context.Context, workers []string, minEnough, maxFailed uint64, ophs [][2]util.Hash) (
		func(managerWorkerOperationsFunc) error,
		managerWorkersOperationsWorkerUnmergedFunc,
		func(func(fixedtree.Tree) error) error,
		error,
	)
	managerWorkersOperationsOpsFunc        func(func(managerWorkerOperationFunc) error) error
	managerWorkersOperationsWorkerOpsFunc  func(worker string, _ managerWorkersOperationsOpsFunc) error
	managerWorkersOperationsWorkersOpsFunc func(workers []string, ophs [][2]util.Hash) (managerWorkersOperationsWorkerOpsFunc, error)
)

type managerWorkersOperationsArgs struct {
	*logging.Logging
	NewCheckWorkersFunc          checkEnoughFailedWorkersFuncs
	NewWorkersOpsFunc            managerWorkersOperationsWorkersOpsFunc
	NewOperationsTreeWriterFuncs newOperationsTreeFuncs
}

func newManagerWorkersOperationsArgs() *managerWorkersOperationsArgs {
	return &managerWorkersOperationsArgs{
		Logging: logging.NewLogging(func(zctx zerolog.Context) zerolog.Context {
			return zctx.Str("module", "manager-workers-operations")
		}),
		NewCheckWorkersFunc: func(minEnough, maxFailed uint64) (
			checkEnoughFailedWorkerFunc,
			func(),
			error,
		) {
			return nil, nil, util.ErrNotImplemented.Errorf("NewCheckWorkers")
		},
		NewWorkersOpsFunc: func([]string, [][2]util.Hash) (managerWorkersOperationsWorkerOpsFunc, error) {
			return nil, util.ErrNotImplemented.Errorf("NewWorkersOpsFunc")
		},
		NewOperationsTreeWriterFuncs: func(context.Context, [][2]util.Hash) (newOperationsTreeNotInStateFunc, errorFunc, newOperationsTreeNewTreeFunc, error) {
			return nil, nil, nil, util.ErrNotImplemented.Errorf("NewOperationsTreeWriterFuncs")
		},
	}
}

func newManagerWorkersOperationsFuncs(
	args *managerWorkersOperationsArgs,
) managerWorkersOperationsFuncs {
	return func(ctx context.Context, workers []string, minEnough, maxFailed uint64, ophs [][2]util.Hash) (
		func(managerWorkerOperationsFunc) error,
		managerWorkersOperationsWorkerUnmergedFunc,
		func(func(fixedtree.Tree) error) error,
		error,
	) {
		switch w := uint64(len(workers)); { // FIXME move to higher level
		case w < 1:
			return nil, nil, nil, util.ErrNotFound.Errorf("empty workers")
		case minEnough < 1:
			return nil, nil, nil, util.ErrInvalid.Errorf("zero min enough")
		case minEnough > w:
			return nil, nil, nil, util.ErrInvalid.Errorf("invalid min enough; min enough=%d > workers=%d", minEnough, w)
		case maxFailed >= w:
			return nil, nil, nil, util.ErrInvalid.Errorf("invalid max failed; max failed=%d >= workers=%d", maxFailed, w)
		}

		wctx, wctxCancel := ContextWithCancelCause(ctx)

		var checkWorker checkEnoughFailedWorkerFunc
		var checkWorkerDone func()

		switch c, d, err := args.NewCheckWorkersFunc(minEnough, maxFailed); {
		case err != nil:
			return nil, nil, nil, err
		default:
			checkWorker = c
			checkWorkerDone = d
		}

		var setNotInState newOperationsTreeNotInStateFunc
		var opsTreeJobDone errorFunc
		var opsTreeDone newOperationsTreeNewTreeFunc

		switch s, j, d, err := args.NewOperationsTreeWriterFuncs(wctx, ophs); {
		case err != nil:
			return nil, nil, nil, err
		default:
			setNotInState = s
			opsTreeJobDone = j
			opsTreeDone = d
		}

		closef := func(err error) error {
			wctxCancel(err)
			checkWorkerDone()
			opsTreeDone(err)

			return err
		}

		return func(f managerWorkerOperationsFunc) error {
				if err := distributeOpsToWorkers(
					wctx,
					args,
					workers,
					minEnough, maxFailed,
					ophs,
					f,
				); err != nil {
					return closef(err)
				}

				return nil
			},
			func(worker string, unmerged []uint64) (added bool, _ error) {
				switch ok, enough, _, err := checkWorker(worker, nil); {
				case err != nil:
					return false, closef(err)
				case ok, enough:
					added = ok

					if ok && len(unmerged) > 0 {
						_ = setNotInState(unmerged)
					}

					if enough {
						if err := opsTreeJobDone(nil); err != nil {
							return false, closef(err)
						}
					}
				}

				return added, nil
			},
			func(f func(fixedtree.Tree) error) error {
				switch tr, err := opsTreeDone(nil); {
				case err != nil:
					return closef(err)
				default:
					return closef(f(tr))
				}
			},
			nil
	}
}

func distributeOpsToWorkers(
	ctx context.Context,
	args *managerWorkersOperationsArgs,
	workers []string,
	minEnough, maxFailed uint64,
	ophs [][2]util.Hash,
	wf managerWorkerOperationsFunc,
) error {
	var checkWorker checkEnoughFailedWorkerFunc

	switch c, d, err := args.NewCheckWorkersFunc(minEnough, maxFailed); {
	case err != nil:
		return err
	default:
		defer d()

		checkWorker = c
	}

	var workerOps managerWorkersOperationsWorkerOpsFunc

	switch f, err := args.NewWorkersOpsFunc(workers, ophs); {
	case err != nil:
		return err
	default:
		workerOps = f
	}

	var jw JobWorker

	switch i, err := NewBaseJobWorker(ctx, int64(len(workers))); {
	case err != nil:
		return err
	default:
		defer i.Close()

		jw = i
	}

	if err := util.TraverseSliceV1(workers, func(_ int, worker string) (bool, error) {
		return true, jw.NewJob(func(ctx context.Context) error {
			return workerOps(worker, func(ofs func(managerWorkerOperationFunc) error) error {
				added, err := distributeOpsToWorker(ctx, worker, ofs, wf, checkWorker)

				args.Log().Debug().Str("worker", worker).Bool("added", added).Msg("worker ops")

				return err
			})
		})
	}); err != nil {
		return err
	}

	jw.JobDone()

	switch err := jw.Wait(); {
	case errors.Is(err, errCheckWorkersEnough):
		return nil
	default:
		return err
	}
}

func distributeOpsToWorker(
	ctx context.Context,
	worker string,
	ofs func(managerWorkerOperationFunc) error,
	wf managerWorkerOperationsFunc,
	checkWorker checkEnoughFailedWorkerFunc,
) (bool, error) {
	switch ok, enough, failed, err := checkWorker(worker, func() error {
		return wf(ctx, worker, func(of managerWorkerOperationFunc) error {
			return ofs(of)
		})
	}); {
	case !ok:
		return ok, err
	case enough:
		return true, errCheckWorkersEnough.Wrap(err)
	case failed:
		return true, errCheckWorkersFailed.Wrap(err)
	default:
		return true, err
	}
}

func serialNoShareWorkerOps(workers []string, ophs [][2]util.Hash) (
	managerWorkersOperationsWorkerOpsFunc,
	error,
) {
	o := uint64(len(ophs))

	switch {
	case len(workers) < 1:
		return nil, errors.Errorf("empty workers")
	case o < 1:
		return func(worker string, f managerWorkersOperationsOpsFunc) error {
			return f(nil)
		}, nil
	}

	ws := uint64(len(workers))
	d := o / ws
	if o%ws > 0 {
		d += 1
	}

	return func(worker string, f managerWorkersOperationsOpsFunc) error {
			wi := uint64(slices.Index(workers, worker))
			if wi < 0 {
				return util.ErrNotFound.Errorf(worker)
			}

			start := wi * d
			if start >= o {
				return f(nil)
			}

			end := (wi + 1) * d
			if end >= o {
				end = o
			}

			return f(func(of managerWorkerOperationFunc) error {
				if of == nil {
					return nil
				}

				for opindex := start; opindex < end; opindex++ {
					hs := ophs[opindex]

					if err := of(opindex, hs[0], hs[1]); err != nil {
						return err
					}
				}

				return nil
			})
		},
		nil
}

type (
	distributeItemsSerialNShareCallbackFunc func(func(func(uint64) error) error) error
	distributeItemsSerialNShareFunc         func(worker uint64, f distributeItemsSerialNShareCallbackFunc) error
)

func newDistributeItemsSerialNShareFunc(workers, items, share uint64) (
	distributeItemsSerialNShareFunc,
	error,
) {
	switch {
	case workers < 1:
		return nil, errors.Errorf("zero workers")
	case items < 1:
		return distributeItemsSerialNShareEmptyFunc(workers), nil
	case share < 1:
		return nil, errors.Errorf("zero share")
	case share > workers:
		share = workers
	}

	pos := func(worker, item uint64) uint64 {
		return uint64(math.Floor(
			float64((workers*item)+worker) / float64(share),
		))
	}

	workerMaxItems := distributeItemsSerialNShareWorkerMaxItemsFunc(workers, items, share)

	return func(worker uint64, f distributeItemsSerialNShareCallbackFunc) error {
			switch {
			case worker >= workers:
				return util.ErrNotFound.Errorf("unknown worker=%d", worker)
			case f == nil:
				return nil
			}

			maxItems := workerMaxItems(worker)
			if maxItems < 1 {
				return distributeItemsSerialNShareEmptyFunc(workers)(worker, f)
			}

			return f(func(of func(uint64) error) error {
				for item := uint64(0); item < maxItems; item++ {
					if err := of(pos(worker, item)); err != nil {
						return err
					}
				}

				return nil
			})
		},
		nil
}

func distributeItemsSerialNShareEmptyFunc(workers uint64) distributeItemsSerialNShareFunc {
	return func(worker uint64, f distributeItemsSerialNShareCallbackFunc) error {
		if worker >= workers {
			return util.ErrNotFound.Errorf("unknown worker=%d", worker)
		}

		return f(func(func(uint64) error) error {
			return nil
		})
	}
}

func distributeItemsSerialNShareWorkerMaxItemsFunc(workers, items, share uint64) func(worker uint64) uint64 {
	total := items * share

	i := uint64(math.Floor(float64(total) / float64(workers)))
	j := total - (i * workers)

	return func(worker uint64) uint64 {
		if worker >= j {
			return i
		}

		return i + 1
	}
}
