package main

import (
	"context"
	"slices"

	"github.com/pkg/errors"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
)

func newFuncManagerCollectOwners(
	ctx context.Context,
	opsWorkers []string,
	workersSize, opsShare uint64,
) (
	dutil.In2ErrorFunc[string /* worker */, []string /* owners */],
	dutil.OutErrorFunc[[]string /* owners */],
	error,
) {
	log := dutil.NewModuleLoggerFromContext(ctx, "manager-collect-owners", nil)

	log.Debug().
		Strs("ops_workers", opsWorkers).
		Uint64("workers_size", workersSize).
		Uint64("ops_share", opsShare).
		Msg("new")

	var checkWorker checkFailedWorkersFunc

	switch w, err := newFuncCheckFailedWorkers(uint64(len(opsWorkers)), opsShare, nil); {
	case err != nil:
		return nil, nil, err
	default:
		checkWorker = w
	}

	octx, ownersDone, parentContextError, ownersWait := dutil.NewContextDone(ctx)

	var add dutil.AppendSliceAddFunc[string]
	var done dutil.OutFunc[[]string]

	switch a, d, err := dutil.NewLockComparableUniqueAppendSlice[string](workersSize, 0); {
	case err != nil:
		return nil, nil, err
	default:
		add, done = a, d
	}

	checkParentContextError := dutil.NewFuncCheckParentContextError(parentContextError, dutil.DummyErrorInFunc)

	return func(worker string, owners []string) error {
			if ok, err := checkParentContextError(); err != nil || !ok {
				return err
			}

			if slices.Index(opsWorkers, worker) < 0 {
				return errors.Errorf("worker, %q not ops worker", worker)
			}

			return dutil.ContextCauseError(octx, nil, func(context.Context) error {
				l := log.With().Str("worker", worker).Logger()

				_, _ = add(owners...)

				switch ok, enough, failed := checkWorker(worker, nil); {
				case failed != nil:
					l.Error().Err(failed).Msg("worker states owners: failed")

					return ownersDone(failed)
				case !ok:
				case enough:
					l.Debug().Msg("worker states owners; enough")

					return ownersDone(nil)
				}

				return nil
			})
		},
		func() ([]string, error) {
			if err := ownersWait(nil); err != nil {
				return nil, err
			}

			return done(), nil
		},
		nil
}
