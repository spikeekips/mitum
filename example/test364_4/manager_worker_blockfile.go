package main

import (
	"context"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
)

type (
	managerWorkerBlockFileWorkerFilesFunc      func(worker string, _ map[base.BlockItemType] /* []checksum */ []string) error
	managerWorkerBlockFileReceiveChecksumsFunc func(base.BlockItemType /* checksums */, []string) error
	managerWorkerBlockFileReceiveFunc          func(_ context.Context, worker string, _ func(managerWorkerBlockFileReceiveChecksumsFunc) error) error
	managerWorkerBlockFilesDoneFunc            func() error
	managerWorkerBlockFilesFunc                func(
		_ context.Context,
		minEnough, maxFailed uint64,
		_ managerWorkerBlockFileWorkerFilesFunc,
	) (
		managerWorkerBlockFileReceiveFunc,
		managerWorkerBlockFilesDoneFunc,
		error,
	)
)

type managerWorkerBlockFilesArgs struct {
	*logging.Logging
}

func newManagerWorkerBlockFilesArgs() *managerWorkerBlockFilesArgs {
	return &managerWorkerBlockFilesArgs{
		Logging: logging.NewLogging(func(zctx zerolog.Context) zerolog.Context {
			return zctx.Str("module", "manager-worker-blockfile")
		}),
	}
}

func newFuncManagerWorkerBlockFiles(
	args *managerWorkerBlockFilesArgs,
	newCheckWorkersFunc checkEnoughFailedWorkersFuncs,
) managerWorkerBlockFilesFunc {
	return func(
		ctx context.Context,
		minEnough, maxFailed uint64,
		saveBlockFiles managerWorkerBlockFileWorkerFilesFunc,
	) (
		managerWorkerBlockFileReceiveFunc,
		managerWorkerBlockFilesDoneFunc,
		error,
	) {
		wctx, wctxCancel := ContextWithCancelCause(ctx)

		closef := OnceFuncCallback(func(err error) error {
			wctxCancel(err)

			return err
		})

		var receiveWorkerBlockFiles managerWorkerBlockFileReceiveFunc
		var receiveWorkerBlockFilesWait func() error

		switch i, w, err := newManagerWorkerWorkerBlockFiles(
			wctx,
			minEnough, maxFailed,
			newCheckWorkersFunc,
			saveBlockFiles,
		); {
		case err != nil:
			return nil, nil, closef(err)
		default:
			receiveWorkerBlockFiles = i
			receiveWorkerBlockFilesWait = w
		}

		return func(ctx context.Context, worker string, f func(managerWorkerBlockFileReceiveChecksumsFunc) error) error {
				// FIXME handle the delayed lazy workers
				return contextCauseError(wctx, ifErrErrorFunc(closef), func() error {
					return receiveWorkerBlockFiles(ctx, worker, f)
				})
			},
			func() error {
				return contextCauseError(wctx, closef, receiveWorkerBlockFilesWait)
			},
			nil
	}
}

func newManagerWorkerWorkerBlockFiles(
	ctx context.Context,
	minEnough, maxFailed uint64,
	newCheckWorkersFunc checkEnoughFailedWorkersFuncs,
	saveBlockFiles managerWorkerBlockFileWorkerFilesFunc,
) (
	managerWorkerBlockFileReceiveFunc,
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

	return func(ctx context.Context, worker string, f func(managerWorkerBlockFileReceiveChecksumsFunc) error) error {
			checksums := util.NewSingleLockedMap[base.BlockItemType, []string]()
			defer checksums.Close()

			switch ok, enough, failed, err := checkWorker(
				worker,
				func() error {
					return f(func(it base.BlockItemType, cs []string) error {
						switch {
						case util.IsDuplicatedSlice(cs, func(s string) (bool, string) { return true, s }):
							return errors.Errorf("duplicated checksums")
						case len(cs) < 1:
							return nil
						default:
							_ = checksums.SetValue(it, cs)

							return nil
						}
					})
				},
			); {
			case failed:
				return done(err)
			case err != nil:
				return nil
			case !ok:
			default:
				if err := saveBlockFiles(worker, checksums.Map()); err != nil {
					return done(err)
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
