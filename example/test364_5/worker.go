package dop5

import (
	"context"
	"iter"
	"slices"
	"sync"

	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
)

type (
	newWorkerDistributeSMVsFunc func(_ context.Context, worker string) (
		WorkerProcessInsertSMVsFunc,
		dutil.In2ErrorFunc[[]string /* subs */, []string /* affected */],
		error,
	)
	WorkerInitialInsertFunc = func(
		getStateOwnersFunc,
		newWorkerDistributeSMVsFunc,
		getOperationFactFunc,
		iter.Seq2[uint64 /* opindex */, [2]util.Hash /* {op, fact} */],
		WorkerReportStatesOwnersFunc,
	) error
	workerBroadcastUnmerged = func(
		_ context.Context,
		local,
		worker string,
		unmerged []uint64,
	) error
	WorkerReportStatesOwnersFunc = func(
		_ context.Context,
		local string, // FIXME remove
		owners []string,
	) error
	workerReportResultFunc func(
		_ context.Context,
		local string,
		reasons map[uint64]string,
		statesSize uint64,
		_ dutil.InErrorFunc[dutil.InErrorFunc[base.State]],
	) error
	WorkerWaitFunc = func(
		workerBroadcastUnmerged,
		workerReportResultFunc,
	) error
	WorkerCloseFunc = dutil.InErrorFunc[error]
)

type WorkerOptions struct {
	Local                       string
	JobWorkerMaxSemSize         uint64
	MaxExtractStateKeyCacheSize int
}

func NewWorkerOptions(local string) *WorkerOptions {
	return &WorkerOptions{
		Local:                       local,
		JobWorkerMaxSemSize:         1 << 9,
		MaxExtractStateKeyCacheSize: 1 << 13,
	}
}

func NewFuncWorker(
	ctx context.Context,
	options *WorkerOptions,
	getState getStateFunc,
	createState CreateStateFunc,
	reportCancelWork dutil.InErrorFunc[error],
	workers, opsWorkers []string,
	opsShare, statesShare uint64,
	maxOpsSize uint64,
) (
	WorkerInitialInsertFunc,
	WorkerProcessWorkerInsertSMVsFunc,
	WorkerUnmergedFunc,
	WorkerWaitFunc,
	WorkerCloseFunc,
	error,
) {
	log := dutil.NewModuleLoggerFromContext(ctx, "worker", func(zctx zerolog.Context) zerolog.Context {
		return zctx.
			Str("local", options.Local)
	})

	log.Debug().
		Interface("options", options).
		Uint64("max_ops_size", maxOpsSize).
		Dict("workers", zerolog.Dict().
			Strs("all", workers).
			Strs("ops", opsWorkers),
		).
		Dict("share", zerolog.Dict().
			Uint64("ops", opsShare).
			Uint64("states", statesShare),
		).
		Msg("newFuncWorker")

	if err := isValidWorkerArgs(
		options.Local,
		workers, opsWorkers,
		opsShare, statesShare,
	); err != nil {
		return nil, nil, nil, nil, nil, err
	}

	wctx, wctxCancel := dutil.ContextWithCancelCause(ctx)

	cancelWork := newFuncWorkerCancelWork(wctxCancel, reportCancelWork)

	ifErrorCancelWork := dutil.NewIfErrErrorInFunc(func(err error) error {
		return cancelWork(err, true)
	})

	_, closeWorker, _, closeWorkerWait := dutil.NewContextWait(wctx)

	var initialInsert workerProcessInitialInsertFunc
	var workerInsert WorkerProcessWorkerInsertSMVsFunc
	var workerUnmerge WorkerUnmergedFunc
	var processWait workerProcessWaitFunc

	switch i, w, u, p, err := newFuncWorkerProcess(
		wctx,
		options.Local,
		dutil.NewFuncNewJobWorkerFunc(options.JobWorkerMaxSemSize),
		getState,
		createState,
		options.MaxExtractStateKeyCacheSize,
		workers, opsWorkers,
		opsShare, statesShare,
		maxOpsSize,
	); {
	case err != nil:
		return nil, nil, nil, nil, nil, err
	default:
		initialInsert, workerInsert, workerUnmerge, processWait = i, w, u, p
	}

	var bgNewJob dutil.JobWorkerNewJobFunc
	var bgJobWait dutil.InErrorFunc[error]

	switch n, d, w, err := dutil.NewFuncJobWorker(wctx, uint64(len(workers))); {
	case err != nil:
		return nil, nil, nil, nil, nil, err
	default:
		var bgJobDoneOnce sync.Once

		bgNewJob, bgJobWait = n, func(err error) error {
			bgJobDoneOnce.Do(func() {
				d()
			})

			return w(err)
		}
	}

	return func(
			getStateOwners getStateOwnersFunc,
			distribute newWorkerDistributeSMVsFunc,
			getOperationFact getOperationFactFunc,
			ophs iter.Seq2[uint64, [2]util.Hash],
			reportStatesOwners WorkerReportStatesOwnersFunc,
		) error {
			delayedDistribute := newFuncDelayedWorkerDistributeSMVs(
				distribute,
				func(ctx context.Context) (
					dutil.InErrorFunc[interface{}],
					func(),
					iter.Seq[interface{}],
				) {
					return dutil.NewContextUnlimitedChans[interface{}](ctx, maxOpsSize*2, maxOpsSize*2)
				},
			)

			return ifErrorCancelWork(initialInsert(
				getStateOwners,
				func(ctx context.Context, worker string) (
					WorkerProcessInsertSMVsFunc,
					dutil.In2ErrorFunc[[]string, []string],
					error,
				) {
					switch p, o, w, err := delayedDistribute(ctx, worker); {
					case err != nil, p == nil:
						return nil, nil, err
					default:
						if err := bgNewJob(func(context.Context) error {
							_ = w() // NOTE ignore error

							return nil
						}); err != nil {
							return nil, nil, err
						}

						return p, o, err
					}
				},
				getOperationFact,
				func(f OpindexOpHashesFunc) error {
					for opindex, hs := range ophs {
						if err := f(opindex, hs[0], hs[1]); err != nil {
							return err
						}
					}

					return nil
				},
				func(owners []string, _ map[uint64]string) error {
					if reportStatesOwners != nil {
						if slices.Index(opsWorkers, options.Local) >= 0 {
							return bgNewJob(func(ctx context.Context) error {
								return reportStatesOwners(ctx, options.Local, owners)
							})
						}
					}

					return nil
				},
			))
		},
		workerInsert,
		workerUnmerge,
		func(
			broadcastUnmerged workerBroadcastUnmerged,
			reportResult workerReportResultFunc,
		) error {
			var savedStates *dutil.SetOnlyLockedMap[string, base.State]

			defer func() {
				_ = wctxCancel(nil)

				if savedStates != nil {
					savedStates.Close()
				}
				_ = bgJobWait(context.Canceled)
			}()

			if err := processWait(
				func(_ context.Context, worker string, unmerged []uint64) error {
					return bgNewJob(func(ctx context.Context) error {
						return broadcastUnmerged(ctx, options.Local, worker, unmerged)
					})
				},
				func(
					ctx context.Context,
					reasons map[uint64]string,
					statesSize uint64,
					reportStates dutil.InErrorFunc[dutil.InErrorFunc[base.State]],
				) error {
					if statesSize > 0 {
						switch i, err := dutil.NewLockedMap[string, base.State](nil, statesSize, statesSize); {
						case err != nil:
							return err
						default:
							savedStates = dutil.NewSetOnlyLockedMap(i, 1<<9)
						}
					}

					return reportResult(ctx, options.Local, reasons, statesSize,
						func(f dutil.InErrorFunc[base.State]) error {
							return reportStates(func(st base.State) error {
								_ = savedStates.SetValue(st.Key(), st)

								return f(st)
							})
						})
				},
			); err != nil {
				return cancelWork(err, true)
			}

			// NOTE operation should be saved in outisde newFuncWorker

			_ = savedStates // FIXME save states

			switch err := closeWorkerWait(); {
			case err != nil:
				log.Error().Err(err).Msg("worker canceled")

				return cancelWork(err, true)
			default:
				log.Debug().Msg("worker closed")

				_ = bgJobWait(nil) // NOTE ignore background jobs error

				return nil
			}
		},
		func(err error) error {
			if err != nil {
				return cancelWork(err, false)
			}

			return closeWorker(nil)
		},
		nil
}

func isValidWorkerArgs(
	local string,
	workers, opsWorkers []string,
	opsShare, statesShare uint64,
) error {
	switch {
	case local == "":
		return util.ErrInvalid.Errorf("empty local worker name")
	case opsShare < 1:
		return util.ErrInvalid.Errorf("zero ops share")
	case statesShare < 1:
		return util.ErrInvalid.Errorf("zero states share")
	case len(workers) < 1:
		return util.ErrInvalid.Errorf("empty workers")
	case len(opsWorkers) < 1:
		return util.ErrInvalid.Errorf("empty ops workers")
	case len(workers) < len(opsWorkers):
		return util.ErrInvalid.Errorf("invalid workers size; < ops workers")
	}

	for worker := range slices.Values(opsWorkers) {
		if slices.Index(workers, worker) < 0 {
			return util.ErrInvalid.Errorf("unknown ops worker, %q", worker)
		}
	}

	return nil
}

func newFuncWorkerCancelWork(
	cancel dutil.InErrorFunc[error],
	reportCancelWork dutil.InErrorFunc[error],
) dutil.In2ErrorFunc[error, bool] {
	var once sync.Once

	return func(err error, isInternal bool) error {
		once.Do(func() {
			_ = cancel(err)

			if isInternal && reportCancelWork != nil { // NOTE report error to manager
				_ = reportCancelWork(err)
			}
		})

		return err
	}
}
