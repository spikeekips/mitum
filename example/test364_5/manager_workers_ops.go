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
) error {
	log := newModuleLoggerFromContext(ctx, "manager-workers-ops", nil)

	if len(ophs) < 1 {
		return util.ErrInvalid.Errorf("empty ophs")
	}

	var opsWorkers []string
	var workerOps workerOperationsFunc

	switch w, d, err := opsNShareWorkers(workers, opsShare, ophs); {
	case err != nil:
		return err
	default:
		opsWorkers = w()
		workerOps = d
	}

	return elapsedLogger(
		log.With().Strs("ops_workers", opsWorkers).Logger(),
		"sent ops to workers",
	)(managerDistributeWorkersOps(
		ctx,
		newJobWorker,
		newWorkerSendOp,
		opsWorkers,
		opsShare,
		workerOps,
	))
}

func managerDistributeWorkersOps(
	ctx context.Context,
	newJobWorker newJobWorkerFunc,
	newWorkerSendOp newWorkerSendOpFunc,
	opsWorkers []string,
	opsShare uint64,
	workerOps workerOperationsFunc,
) error {
	log := loadContextValueLogger(ctx)

	opsWorkersSize := uint64(len(opsWorkers))

	var maxFailed uint64

	switch _, f, err := minEnoughMaxFailed(opsWorkersSize, opsShare); {
	case err != nil:
		return err
	default:
		maxFailed = f
	}

	log.Debug().
		Uint64("ops_workers_size", opsWorkersSize).
		Uint64("max_failed", maxFailed).
		Msg("distribute ops to workers")

	var checkWorker checkFailedWorkerFunc

	switch w, c, err := newFuncCheckFailedWorkers(opsWorkersSize, maxFailed); {
	case err != nil:
		return err
	default:
		defer c()

		checkWorker = w
	}

	var newJob jobWorkerNewJobFunc
	var jobDone func()
	var jobWait errorFunc

	switch n, d, w, err := newJobWorker(ctx); {
	case err != nil:
		return err
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

	if err := traverseSlice(opsWorkers, func(_ int, worker string) (bool, error) {
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
	}); err != nil {
		return err
	}

	jobDone()

	return jobWait()
}
