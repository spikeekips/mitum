package main

import (
	"context"
)

func newFuncManagerCollectOwners(
	ctx context.Context,
	workersSize, minEnough uint64,
) (
	in2ErrorFunc[string /* worker */, []string /* owners */],
	inErrorFunc[inErrorFunc[[]string /* owners */]],
	error,
) {
	log := loadContextValueLogger(ctx)

	log.Debug().
		Uint64("workers", workersSize).
		Uint64("min_enough", minEnough).
		Msg("managerCollectOwners")

	var checkWorker checkEnoughFailedWorkerFunc
	var checkWorkerClose func()

	switch w, c, err := newFuncCheckEnoughFailedWorkers(minEnough, workersSize); {
	case err != nil:
		return nil, nil, err
	default:
		checkWorker = w
		checkWorkerClose = c
	}

	_, ownersDone, ownersWait := newContextDone(ctx)

	var add appendSliceAddFunc[string]
	var done appendSliceDoneFunc[string]

	if err := rUniqueAppendSlice[string, string](workersSize, 0, func(s string) string { return s }, &add, &done); err != nil {
		return nil, nil, err
	}

	return func(worker string, owners []string) error {
			return contextCauseError(ctx, dummyErrorInFunc, func(context.Context) error {
				l := log.With().Str("worker", worker).Logger()

				switch ok, enough, _, err := checkWorker(worker, func() error {
					if len(owners) < 1 {
						return nil
					}

					_, _ = add(owners[0], owners[1:]...)

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
		func(f inErrorFunc[[]string]) error {
			defer func() {
				checkWorkerClose()
				done(nil)
			}()

			if err := ownersWait(); err != nil {
				return err
			}

			return done(f)
		},
		nil
}
