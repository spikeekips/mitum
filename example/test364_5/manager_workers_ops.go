package main

import (
	"context"

	"github.com/spikeekips/mitum/util"
)

type (
	newWorkerSendOpFunc func(_ context.Context, worker string) (opindexOpHashesFunc, inErrorFunc[error], error)
)

func newFuncManagerWorkersOps(
	ctx context.Context,
	newJobWorker newJobWorkerFunc,
	workers []string,
	opsShare uint64,
	ophs [][2]util.Hash,
	newWorkerSendOp newWorkerSendOpFunc,
) (errorFunc, errorFunc, error) { // FIXME return minEnough, maxFailed for newFuncManagerWorkersOpsWait
	if len(ophs) < 1 {
		return nil, nil, util.ErrInvalid.Errorf("empty ophs")
	}

	log := loadContextValueLogger(ctx)

	l := log.Debug().
		Strs("workers", workers).
		Uint64("ops_share", opsShare).
		Int("ophs", len(ophs))

	var opsWorkers []string
	var workerOps workerOperationsFunc

	switch w, d, err := opsNShareWorkers(workers, opsShare, ophs); {
	case err != nil:
		return nil, nil, err
	default:
		opsWorkers = w()
		workerOps = d

		l = l.Strs("ops_workers", opsWorkers)
	}

	opsWorkersSize := uint64(len(opsWorkers))

	l = l.Uint64("ops_workers_size", opsWorkersSize)

	var checkWorker checkFailedWorkerFunc
	var checkWorkerClose func()

	switch _, maxFailed, err := minEnoughMaxFailed(opsWorkersSize, opsShare); {
	case err != nil:
		return nil, nil, err
	default:
		l = l.Uint64("max_failed", maxFailed)

		switch w, c, err := newFuncCheckFailedWorkers(opsWorkersSize, maxFailed); {
		case err != nil:
			return nil, nil, err
		default:
			checkWorker = w
			checkWorkerClose = c
		}
	}

	l.Msg("send ops to workers")

	var newJob jobWorkerNewJobFunc
	var jobDone func()
	var jobWait errorFunc

	switch n, d, w, err := newJobWorker(ctx); {
	case err != nil:
		return nil, nil, err
	default:
		newJob, jobDone, jobWait = n, d, w
	}

	sendWorker := func(ctx context.Context, worker string) error {
		switch s, d, err := newWorkerSendOp(ctx, worker); {
		case err != nil:
			return err
		default:
			return d(workerOps(worker, func(opindex uint64, oph, fact util.Hash) error {
				return s(opindex, oph, fact)
			}))
		}
	}

	return func() error {
			defer jobDone()

			return traverseSlice(opsWorkers, func(_ int, worker string) (bool, error) {
				return true, newJob(func(ctx context.Context) error {
					l := log.With().Str("worker", worker).Logger()

					gerr := sendWorker(ctx, worker)

					switch _, failed, err := checkWorker(worker, func() error { return gerr }); {
					case failed:
						l.Error().Err(err).Msg("sent; check workers: failed")

						return err
					case err != nil: // NOTE ignore
						l.Debug().Err(err).Msg("sent")
					}

					return nil
				})
			})
		},
		func() error {
			defer checkWorkerClose()

			return jobWait()
		},
		nil
}
