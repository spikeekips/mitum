package main

import (
	"context"
	"slices"

	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
)

type (
	newWorkerSendOpFunc func(_ context.Context, worker string) (opindexOpHashesFunc, dutil.InErrorFunc[error], error)
)

func newFuncManagerWorkersOps(
	ctx context.Context,
	newJobWorker dutil.NewJobWorkerFunc,
	workers []string,
	opsShare uint64,
	ophs [][2]util.Hash,
) (
	opsWorkers []string,
	minEnough, maxFailed uint64,
	_ dutil.InErrorFunc[newWorkerSendOpFunc],
	_ dutil.ErrorFunc,
	_ error,
) {
	if len(ophs) < 1 {
		return nil, 0, 0, nil, nil, util.ErrInvalid.Errorf("empty ophs")
	}

	log := dutil.NewModuleLoggerFromContext(ctx, "manager-wokers-ops", nil)

	log.Debug().
		Strs("workers", workers).
		Uint64("ops_share", opsShare).
		Int("ophs", len(ophs)).
		Msg("new")

	l := log.Debug()

	var workerOps workerOperationsFunc

	switch w, d, err := opsNShareWorkers(workers, opsShare, ophs); {
	case err != nil:
		return nil, 0, 0, nil, nil, err
	default:
		opsWorkers, workerOps = w(), d

		l = l.Strs("ops_workers", opsWorkers)
	}

	opsWorkersSize := uint64(len(opsWorkers))

	l = l.Uint64("ops_workers_size", opsWorkersSize)

	var checkWorker checkFailedWorkerFunc
	var checkWorkerClose func()

	switch me, mf, err := minEnoughMaxFailed(opsWorkersSize, opsShare); {
	case err != nil:
		return nil, 0, 0, nil, nil, err
	default:
		l = l.Uint64("max_failed", maxFailed)

		switch w, c, err := newFuncCheckFailedWorkers(opsWorkersSize, mf); {
		case err != nil:
			return nil, 0, 0, nil, nil, err
		default:
			checkWorker, checkWorkerClose = w, c
		}

		minEnough = me
		maxFailed = mf
	}

	l.Msg("send ops to workers")

	var newJob dutil.JobWorkerNewJobFunc
	var jobDone func()
	var jobWait dutil.InErrorFunc[error]

	switch n, d, w, err := newJobWorker(ctx, opsWorkersSize); {
	case err != nil:
		return nil, 0, 0, nil, nil, err
	default:
		newJob, jobDone, jobWait = n, d, w
	}

	sendWorker := func(ctx context.Context, newWorkerSendOp newWorkerSendOpFunc, worker string) error {
		switch s, d, err := newWorkerSendOp(ctx, worker); {
		case err != nil:
			return err
		default:
			return d(workerOps(worker, func(opindex uint64, oph, fact util.Hash) error {
				return s(opindex, oph, fact)
			}))
		}
	}

	return opsWorkers, minEnough, maxFailed,
		func(newWorkerSendOp newWorkerSendOpFunc) error {
			defer jobDone()

			for worker := range slices.Values(opsWorkers) {
				if err := newJob(func(ctx context.Context) error {
					l := log.With().Str("worker", worker).Logger()

					gerr := sendWorker(ctx, newWorkerSendOp, worker)

					switch _, failed, err := checkWorker(worker, func() error { return gerr }); {
					case failed:
						l.Error().Err(err).Msg("sent; check workers: failed")

						return err
					case err != nil: // NOTE ignore
						l.Debug().Err(err).Msg("sent")
					}

					return nil
				}); err != nil {
					return err
				}
			}

			return nil
		},
		func() error {
			defer checkWorkerClose()

			return jobWait(nil)
		},
		nil
}
