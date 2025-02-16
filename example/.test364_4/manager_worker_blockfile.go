package main

import (
	"context"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
)

type (
	managerWorkerBlockFileWorkerFilesFunc func(
		worker string,
		_ map[base.BlockItemType] /* []checksum */ []string,
	) error
	managerWorkerBlockFileReceiveChecksumsFunc func(_ base.BlockItemType, checksums []string) error
	managerWorkerBlockFileReceiveFunc          func(worker string) (
		managerWorkerBlockFileReceiveChecksumsFunc,
		errorFunc,
		error,
	)
	managerWorkerBlockFilesDoneFunc func() error
	managerWorkerBlockFilesFunc     func(
		_ context.Context,
		minEnough, maxFailed uint64,
		_ managerWorkerBlockFileWorkerFilesFunc,
	) (
		managerWorkerBlockFileReceiveFunc,
		managerWorkerBlockFilesDoneFunc,
		error,
	)
)

func newFuncManagerWorkerBlockFiles(
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
		var checkWorker checkEnoughFailedWorkerFunc
		var checkWorkerClose func()

		switch c, d, err := newCheckWorkersFunc(minEnough, maxFailed); {
		case err != nil:
			return nil, nil, err
		default:
			checkWorker = c
			checkWorkerClose = d
		}

		_, done, wait := newDoneContextFunc(ctx)

		return func(worker string) (
				managerWorkerBlockFileReceiveChecksumsFunc,
				errorFunc,
				error,
			) {
				wctx, wctxCancel := contextWithCancelCause(ctx)

				m := util.NewSingleLockedMap[base.BlockItemType, []string]()

				return func(it base.BlockItemType, checksums []string) error {
						if err := context.Cause(wctx); err != nil {
							return errors.WithStack(err)
						}

						var gerr error

						switch {
						case util.IsDuplicatedSlice(checksums, func(s string) (bool, string) { return true, s }):
							gerr = errors.Errorf("duplicated checksums")
						case len(checksums) < 1:
							return nil
						case !m.SetValue(it, checksums):
							gerr = util.ErrFound.Errorf("%q already set", it)
						}

						return ifErrErrorFunc(wctxCancel)(gerr)
					},
					func(err error) error {
						defer m.Close()

						gerr := err

						if gerr == nil {
							if err := context.Cause(wctx); err != nil {
								gerr = err
							}
						}

						if gerr == nil {
							gerr = saveBlockFiles(worker, m.Map())
						}

						switch _, enough, failed, err := checkWorker(worker, func() error { return gerr }); {
						case failed:
							return done(errCheckWorkersFailed.Wrap(err))
						case err != nil:
							return nil
						case enough:
							return done(nil)
						default:
							return gerr
						}
					},
					nil
			},
			func() error {
				defer func() {
					checkWorkerClose()
				}()

				return wait()
			},
			context.Cause(ctx)
	}
}
