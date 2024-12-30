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
	dutil.InErrorFunc[dutil.InErrorFunc[[]string /* owners */]],
	error,
) {
	log := dutil.LoadContextValueLogger(ctx)

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

	_, ownersDone, _, ownersWait := dutil.NewContextDone(ctx)

	var add dutil.AppendSliceAddFunc[string]
	var done dutil.AppendSliceDoneFunc[string]

	if err := dutil.RUniqueAppendSlice[string, string](workersSize, 0, func(s string) string { return s }, &add, &done); err != nil {
		return nil, nil, err
	}

	return func(worker string, owners []string) error {
			return dutil.ContextCauseError(ctx, nil, func(context.Context) error {
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
		func(f dutil.InErrorFunc[[]string]) error {
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
