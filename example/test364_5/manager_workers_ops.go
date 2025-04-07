package dop5

import (
	"context"
	"slices"

	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
)

type (
	newWorkerSendOpFunc func(_ context.Context, worker string, maxItems uint64) (
		OpindexOpHashesFunc, dutil.InErrorFunc[error], error)
)

func newFuncManagerWorkersOps( //nolint:funlen
	ctx context.Context,
	newJobWorker dutil.NewJobWorkerFunc,
	workers []string,
	opsShare uint64,
	ophs [][2]util.Hash,
) (
	[]string, /* ops workers */
	dutil.InErrorFunc[newWorkerSendOpFunc],
	dutil.ErrorFunc,
	error,
) {
	if len(ophs) < 1 {
		return nil, nil, nil, util.ErrInvalid.Errorf("empty ophs")
	}

	log := dutil.NewModuleLoggerFromContext(ctx, "manager-wokers-ops", nil)

	log.Debug().
		Strs("workers", workers).
		Uint64("ops_share", opsShare).
		Int("ophs", len(ophs)).
		Msg("new")

	l := log.Debug()

	var workerOps workerOperationsFunc
	var opsWorkerMaxItems map[string]uint64

	var opsWorkers []string

	switch w, d, err := opsNShareWorkers(workers, opsShare, ophs); {
	case err != nil:
		return nil, nil, nil, err
	default:
		opsWorkers = make([]string, 0, len(workers))
		opsWorkerMaxItems = map[string]uint64{}

		for worker, maxItems := range w {
			opsWorkers = append(opsWorkers, worker)
			opsWorkerMaxItems[worker] = maxItems
		}

		workerOps = d

		l = l.Strs("ops_workers", opsWorkers).Interface("max_items", opsWorkerMaxItems)
	}

	opsWorkersSize := uint64(len(opsWorkers))

	l = l.Uint64("ops_workers_size", opsWorkersSize)

	var checkWorker checkFailedWorkersFunc

	switch w, err := newFuncCheckFailedWorkers(opsWorkersSize, opsShare, nil); {
	case err != nil:
		return nil, nil, nil, err
	default:
		checkWorker = w
	}

	l.Msg("send ops to workers")

	var newJob dutil.JobWorkerNewJobFunc
	var jobDone func()
	var jobWait dutil.InErrorFunc[error]

	switch n, d, w, err := newJobWorker(ctx, dutil.SafeUint64toInt64(opsWorkersSize)); {
	case err != nil:
		return nil, nil, nil, err
	default:
		newJob, jobDone, jobWait = n, d, w
	}

	sendWorker := func(ctx context.Context, newWorkerSendOp newWorkerSendOpFunc, worker string, maxItems uint64) error {
		switch s, d, err := newWorkerSendOp(ctx, worker, maxItems); {
		case err != nil:
			return err
		default:
			return d(workerOps(worker, s))
		}
	}

	return opsWorkers,
		func(newWorkerSendOp newWorkerSendOpFunc) error {
			defer jobDone()

			for worker := range slices.Values(opsWorkers) {
				if err := newJob(func(ctx context.Context) error {
					l := log.With().Str("worker", worker).Logger()

					err := sendWorker(ctx, newWorkerSendOp, worker, opsWorkerMaxItems[worker])

					switch _, _, failed := checkWorker(worker, err); {
					case failed != nil:
						l.Error().Err(failed).Msg("sent; check workers: failed")

						return failed
					default:
						l.Debug().Err(err).Msg("send op to worker")
					}

					return nil
				}); err != nil {
					return err
				}
			}

			return nil
		},
		func() error {
			return jobWait(nil)
		},
		nil
}
