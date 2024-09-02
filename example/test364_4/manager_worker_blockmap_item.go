package main

import (
	"context"

	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
)

type (
	managerWorkerBlockMapItemsWorkerBlockMapItemChunksFunc   func(worker string, _ []blockMapItemChunk) error
	managerWorkerBlockMapItemsReceiveWorkerBlockMapItemsFunc func(_ context.Context, worker string, _ func(workerSaveBlockMapItemChunkFunc) error) error
	managerWorkerBlockMapItemsDoneFunc                       func() error
	managerWorkerBlockMapItemsFunc                           func(
		_ context.Context,
		minEnough, maxFailed uint64,
		_ managerWorkerBlockMapItemsWorkerBlockMapItemChunksFunc,
	) (
		managerWorkerBlockMapItemsReceiveWorkerBlockMapItemsFunc,
		managerWorkerBlockMapItemsDoneFunc,
		error,
	)
)

type managerWorkerBlockMapItemsArgs struct {
	*logging.Logging
}

func newManagerWorkerBlockMapItemsArgs() *managerWorkerBlockMapItemsArgs {
	return &managerWorkerBlockMapItemsArgs{
		Logging: logging.NewLogging(func(zctx zerolog.Context) zerolog.Context {
			return zctx.Str("module", "manager-blockmap-item")
		}),
	}
}

func newFuncManagerWorkerBlockMapItems(
	args *managerWorkerBlockMapItemsArgs,
	newCheckWorkersFunc checkEnoughFailedWorkersFuncs,
) managerWorkerBlockMapItemsFunc {
	return func(
		ctx context.Context,
		minEnough, maxFailed uint64,
		saveBlockMapItemChunks managerWorkerBlockMapItemsWorkerBlockMapItemChunksFunc,
	) (
		managerWorkerBlockMapItemsReceiveWorkerBlockMapItemsFunc,
		managerWorkerBlockMapItemsDoneFunc,
		error,
	) {
		wctx, wctxCancel := ContextWithCancelCause(ctx)

		closef := OnceFuncCallback(func(err error) error {
			wctxCancel(err)

			return err
		})

		var receiveWorkerBlockMapItem managerWorkerBlockMapItemsReceiveWorkerBlockMapItemsFunc
		var receiveWorkerBlockMapItemWait func() error

		switch i, w, err := newManagerWorkerWorkerBlockMapItem(
			wctx,
			minEnough, maxFailed,
			newCheckWorkersFunc,
			saveBlockMapItemChunks,
		); {
		case err != nil:
			return nil, nil, closef(err)
		default:
			receiveWorkerBlockMapItem = i
			receiveWorkerBlockMapItemWait = w
		}

		return func(ctx context.Context, worker string, f func(workerSaveBlockMapItemChunkFunc) error) error {
				return contextCauseError(wctx, ifErrErrorFunc(closef), func() error {
					return receiveWorkerBlockMapItem(ctx, worker, f)
				})
			},
			func() error {
				return contextCauseError(wctx, closef, receiveWorkerBlockMapItemWait)
			},
			nil
	}
}

func newManagerWorkerWorkerBlockMapItem(
	ctx context.Context,
	minEnough, maxFailed uint64,
	newCheckWorkersFunc checkEnoughFailedWorkersFuncs,
	saveBlockMapItemChunks managerWorkerBlockMapItemsWorkerBlockMapItemChunksFunc,
) (
	managerWorkerBlockMapItemsReceiveWorkerBlockMapItemsFunc,
	func() error,
	error,
) {
	var checkWorker checkEnoughFailedWorkerFunc
	var checkWorkerClose func()

	switch c, d, err := newCheckWorkersFunc(minEnough, maxFailed); {
	case err != nil:
		return nil, nil, err
	default:
		checkWorker = c
		checkWorkerClose = d
	}

	_, done, wait := NewDoneContextFunc(ctx)

	return func(ctx context.Context, worker string, f func(workerSaveBlockMapItemChunkFunc) error) error {
			var add util.CompactAppendSliceAddFunc[blockMapItemChunk]
			var collect util.CompactAppendSliceDoneFunc[blockMapItemChunk]

			switch a, c, err := util.CompactAppendSliceV1[blockMapItemChunk](1<<3, 1<<3); {
			case err != nil:
				return err
			default:
				defer c(nil)

				add = a
				collect = c
			}

			switch ok, enough, failed, err := checkWorker(
				worker,
				func() error {
					return f(func(item blockMapItemChunk) error {
						_, _ = add(item)

						return nil
					})
				},
			); {
			case failed:
				return done(err)
			case err != nil:
				return nil
			case !ok:
			default:
				if err := collect(func(s []blockMapItemChunk) error {
					return saveBlockMapItemChunks(worker, s)
				}); err != nil {
					return err
				}

				if enough {
					return done(nil)
				}
			}

			return nil
		},
		func() error {
			defer checkWorkerClose()

			return wait()
		},
		nil
}
