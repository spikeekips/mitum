package main

import (
	"context"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/fixedtree"
	"github.com/spikeekips/mitum/util/logging"
)

type managerWorkersWorkerStatesFunc func(_ context.Context, worker string, _ func(func(stateKey string) error) error) error

type managerWorkersStatesArgs struct {
	*logging.Logging
	NewCheckWorkersFunc checkEnoughFailedWorkersFuncs
	NewTreeWriterFuncs  newStatesTreeFuncs
}

func newManagerWorkersStatesArgs() *managerWorkersStatesArgs {
	return &managerWorkersStatesArgs{
		Logging: logging.NewLogging(func(zctx zerolog.Context) zerolog.Context {
			return zctx.Str("module", "manager-workers-states")
		}),
		NewCheckWorkersFunc: func(minEnough, maxFailed uint64) (
			checkEnoughFailedWorkerFunc,
			func(),
			error,
		) {
			return nil, nil, util.ErrNotImplemented.Errorf("NewCheckWorkers")
		},
		NewTreeWriterFuncs: func(context.Context) (newStatesTreeAddNodeFunc, errorFunc, newStatesTreeNewTreeFunc, error) {
			return nil, nil, nil, util.ErrNotImplemented.Errorf("NewTreeWriterFuncs")
		},
	}
}

func newManagerWorkersStates(
	ctx context.Context,
	args *managerWorkersStatesArgs,
	workers []string,
	minEnough, maxFailed uint64,
) (
	managerWorkersWorkerStatesFunc,
	managerWorkersTreeFunc,
	error,
) {
	wctx, wctxCancel := ContextWithCancelCause(ctx)

	var checkWorker checkEnoughFailedWorkerFunc
	var checkWorkerDone func()

	switch c, d, err := args.NewCheckWorkersFunc(minEnough, maxFailed); {
	case err != nil:
		return nil, nil, err
	default:
		checkWorker = c
		checkWorkerDone = d
	}

	closef := OnceFuncCallback(func(err error) error {
		wctxCancel(err)
		checkWorkerDone()

		return err
	})

	var treeAdd newStatesTreeAddNodeFunc
	var treeJobDone errorFunc
	var treeDone newStatesTreeNewTreeFunc

	switch a, j, d, err := args.NewTreeWriterFuncs(wctx); {
	case err != nil:
		return nil, nil, err
	default:
		treeAdd = a
		treeJobDone = j
		treeDone = d
	}

	errClosed := errors.New(util.UUID().String())

	return func(ctx context.Context, worker string, f func(func(stateKey string) error) error) error {
			if err := context.Cause(wctx); err != nil {
				return errors.WithStack(err)
			}

			return contextCauseError(
				ctx,
				ifErrErrorFunc(closef),
				func() error {
					var gerr error

					if f != nil {
						switch err := f(func(stateKey string) error {
							switch _, closed, err := treeAdd(stateKey); {
							case err != nil:
								return err
							case closed:
								return errClosed
							default:
								return nil
							}
						}); {
						case errors.Is(err, errClosed):
							return nil
						default:
							gerr = err
						}
					}

					switch ok, enough, failed, err := checkWorker(worker, func() error {
						return gerr
					}); {
					case failed:
						return err
					case ok, enough:
						if enough {
							if err := treeJobDone(nil); err != nil {
								return err
							}
						}
					}

					return nil
				},
			)
		},
		func(f func(fixedtree.Tree) error) error {
			return contextCauseError(
				wctx,
				closef,
				func() error {
					switch tr, err := treeDone(nil); {
					case err != nil:
						return err
					default:
						return f(tr)
					}
				},
			)
		},
		nil
}
