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
	var minEnough, maxFailed uint64

	switch e, f, err := minEnoughMaxFailed(uint64(len(opsWorkers)), opsShare); {
	case err != nil:
		return err
	default:
		minEnough, maxFailed = e, f
	}

	var checkWorker checkEnoughFailedWorkerFunc

	switch w, c, err := newFuncCheckEnoughFailedWorkers(minEnough, maxFailed); {
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

	log := loadContextValueLogger(ctx)

	if err := traverseSlice(opsWorkers, func(_ int, worker string) (bool, error) {
		return true, newJob(func(ctx context.Context) error {
			l := log.With().Str("worker", worker).Logger()

			switch ok, enough, failed, err := checkWorker(worker, func() error { return sendWorker(ctx, worker) }); {
			case failed:
				l.Error().Err(err).Msg("send; check workers: failed")

				return err
			case err != nil: // NOTE ignore
				l.Debug().Err(err).Msg("send")

				return nil
			case !ok:
				l.Debug().Msg("sent; check workers: not ok")
			case enough:
				l.Debug().Msg("sent; check workers: enough")
			}

			return nil
		})
	}); err != nil {
		return err
	}

	jobDone()

	return jobWait()
}
