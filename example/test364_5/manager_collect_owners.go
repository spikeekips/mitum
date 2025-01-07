package main

import (
	"context"

	dutil "github.com/spikeekips/mitum/example/test364_5/util"
)

func newFuncManagerCollectOwners(
	ctx context.Context,
	workersSize, minEnough uint64,
) (
	dutil.In2ErrorFunc[string /* worker */, []string /* owners */],
	dutil.OutErrorFunc[[]string /* owners */],
	error,
) {
	log := dutil.NewModuleLoggerFromContext(ctx, "manager-collect-owners", nil)

	log.Debug().
		Uint64("workers", workersSize).
		Uint64("min_enough", minEnough).
		Msg("new")

	var checkWorker checkEnoughFailedWorkerFunc
	var checkWorkerClose func()

	switch w, c, err := newFuncCheckEnoughFailedWorkers(minEnough, workersSize); {
	case err != nil:
		return nil, nil, err
	default:
		checkWorker, checkWorkerClose = w, c
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

			return dutil.ContextCauseError(octx, nil, func(context.Context) error {
				l := log.With().Str("worker", worker).Logger()

				switch ok, enough, _, err := checkWorker(worker, func() error {
					if len(owners) < 1 {
						return nil
					}

					_, _ = add(owners...)

					return nil
				}); {
				case err != nil:
					l.Error().Err(err).Msg("worker state owners: failed")

					return ownersDone(err)
				case !ok:
				case enough:
					l.Debug().Msg("worker state owners; enough")

					return ownersDone(nil)
				}

				return nil
			})
		},
		func() ([]string, error) {
			defer checkWorkerClose()

			if err := ownersWait(nil); err != nil {
				return nil, err
			}

			return done(), nil
		},
		nil
}
