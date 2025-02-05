package main

import (
	"context"
	"maps"
	"slices"
	"strings"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/example/test364_5/fixedtree"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
)

type (
	managerInsertWorkerResultFunc = func(worker string) (
		dutil.InErrorFunc[map[uint64]string /* reasons  */],
		dutil.In2ErrorFunc[string /* state key */, util.Hash /* state hash */],
		dutil.InErrorFunc[error],
		error,
	)
	managerCollectRersultWaitFunc = func(fixedtree.GenerateNodesHashFunc) (
		*fixedtree.Tree, /* operations fixedtree tree */
		*fixedtree.Tree, /* states fixedtree tree */
		error,
	)
	generateFixedTreeFunc               = func(fixedtree.GenerateNodesHashFunc) (*fixedtree.Tree, error)
	managerCollectResultOwnersReadyFunc = dutil.In2ErrorFunc[[]string /* states owners */, uint64 /* states share */]
)

func newFuncManagerCollectResult(
	ctx context.Context,
	workersSize, approxStatesSize uint64,
	ophs [][2]util.Hash,
) (
	managerInsertWorkerResultFunc,
	managerCollectResultOwnersReadyFunc,
	managerCollectRersultWaitFunc,
	error,
) {
	if len(ophs) < 1 {
		return nil, nil, nil, errors.Errorf("empty ophs")
	}

	log := dutil.NewModuleLoggerFromContext(ctx, "manager-collect-result", nil)

	log.Debug().
		Uint64("workers_size", workersSize).
		Uint64("approx_states_size", approxStatesSize).
		Msg("new")

	var opsNewJob dutil.JobWorkerNewJobFunc
	var opsJobDone func()
	var opsJobWait dutil.InErrorFunc[error]

	switch n, d, w, err := dutil.NewFuncJobWorker(ctx, approxStatesSize); {
	case err != nil:
		return nil, nil, nil, err
	default:
		opsNewJob, opsJobDone, opsJobWait = n, d, w
	}

	_, baseOpsDone, _, baseOpsWait := dutil.NewContextWait(ctx)

	var unmergedOp dutil.In2ErrorFunc[uint64, string]
	var doneOpsTree generateFixedTreeFunc

	switch startOpsFixedNodes, u, d, err := newFuncGenerateOperationFixedtree(ctx, ophs, opsNewJob); {
	case err != nil:
		return nil, nil, nil, err
	default:
		unmergedOp, doneOpsTree = u, d

		go func() {
			_ = baseOpsDone(startOpsFixedNodes())
		}()
	}

	var workerResult managerInsertWorkerResultFunc
	var ownersReady managerCollectResultOwnersReadyFunc
	var workersWait dutil.ErrorFunc
	var workerStatesTree generateFixedTreeFunc

	switch r, o, w, t, err := newFuncManagerWorkersResult(
		ctx,
		workersSize,
		approxStatesSize,
		opsNewJob,
		func(reasons map[uint64]string) error {
			for opindex, reason := range maps.All(reasons) {
				if err := unmergedOp(opindex, reason); err != nil {
					return err
				}
			}

			return nil
		},
	); {
	case err != nil:
		return nil, nil, nil, err
	default:
		workerResult, ownersReady, workersWait, workerStatesTree = r, o, w, t
	}

	return workerResult,
		ownersReady,
		func(g fixedtree.GenerateNodesHashFunc) (opsTree, statesTree *fixedtree.Tree, _ error) {
			defer func() {
				_ = opsJobWait(context.Canceled)
				_ = baseOpsDone(nil)
			}()

			if g == nil {
				return nil, nil, nil
			}

			if err := workersWait(); err != nil {
				return nil, nil, err
			}

			err := dutil.FuncsJobWorker(
				ctx,
				func(context.Context) error {
					if err := baseOpsWait(); err != nil {
						return err
					}

					opsJobDone()

					if err := opsJobWait(nil); err != nil {
						return err
					}

					switch tr, err := doneOpsTree(g); {
					case err != nil:
						return err
					default:
						opsTree = tr

						return nil
					}
				},
				func(context.Context) error {
					switch tr, err := workerStatesTree(g); {
					case err != nil:
						return err
					default:
						statesTree = tr

						return nil
					}
				},
			)

			return opsTree, statesTree, err
		},
		nil
}

func newFuncGenerateOperationFixedtree(
	ctx context.Context,
	ophs [][2]util.Hash,
	newJob dutil.JobWorkerNewJobFunc,
) (
	_ dutil.ErrorFunc,
	unmerge dutil.In2ErrorFunc[uint64 /* opindex */, string /* reason */],
	_ generateFixedTreeFunc,
	_ error,
) {
	var pool dutil.LockedMap[uint64, *operationTreeNode]

	switch i, err := dutil.NewLockedMap[uint64, *operationTreeNode](nil, uint64(len(ophs)), uint64(len(ophs))); {
	case err != nil:
		return nil, nil, nil, err
	default:
		pool = i
	}

	return func() error {
			for i, hs := range slices.All(ophs) {
				if err := newJob(func(context.Context) error {
					_, _ = pool.Set(uint64(i), func(n *operationTreeNode, found bool) (*operationTreeNode, error) {
						if found {
							return nil, dutil.ErrLockedSetIgnore
						}

						return newOperationTreeNode(hs[0], hs[1], "")
					})

					return nil
				}); err != nil {
					return err
				}
			}

			return nil
		},
		func(opindex uint64, reason string) error {
			if reason == "" {
				return nil
			}

			_, _ = pool.Set(opindex, func(n *operationTreeNode, found bool) (*operationTreeNode, error) {
				switch {
				case !found:
					hs := ophs[opindex]

					return newOperationTreeNode(hs[0], hs[1], reason)
				default:
					n.SetReason(reason)
				}

				return nil, dutil.ErrLockedSetIgnore
			})

			return nil
		},
		func(g fixedtree.GenerateNodesHashFunc) (*fixedtree.Tree, error) {
			defer pool.Close()

			nnodes := uint64(pool.Len())

			if nnodes < 1 {
				return nil, nil
			}

			nodes := make([]fixedtree.Node, nnodes)

			for opindex, n := range pool.Seq2 {
				nodes[opindex] = n
			}

			if err := g(ctx, nodes, nil); err != nil {
				return nil, err
			}

			return newOperationsTree(nodes), nil
		},
		nil
}

func newFuncManagerWorkersResult(
	ctx context.Context,
	workersSize,
	approxStatesSize uint64,
	opsNewjob dutil.JobWorkerNewJobFunc,
	workerReasons dutil.InErrorFunc[map[uint64]string],
) (
	_ managerInsertWorkerResultFunc,
	ready managerCollectResultOwnersReadyFunc,
	insertDone dutil.ErrorFunc,
	_ generateFixedTreeFunc,
	_ error,
) {
	log := dutil.LoadContextValueLogger(ctx)

	var newJob dutil.JobWorkerNewJobFunc
	var jobDone func()
	var jobWait dutil.InErrorFunc[error]

	switch n, d, w, err := dutil.NewFuncJobWorker(ctx, approxStatesSize); {
	case err != nil:
		return nil, nil, nil, nil, err
	default:
		newJob, jobDone, jobWait = n, d, w
	}

	wsctx, workersDone, workersDoneContextError, workersWait := dutil.NewContextWait(ctx)

	var addState dutil.AppendSliceAddFunc[fixedtree.Node]
	var doneState dutil.OutFunc[[]fixedtree.Node]

	switch a, d, err := dutil.NewLockUniqueAppendSlice[fixedtree.Node, string](approxStatesSize, 0,
		func(n fixedtree.Node) string { return n.Key() }); {
	case err != nil:
		return nil, nil, nil, nil, err
	default:
		addState, doneState = a, d
	}

	var checkWorker checkFailedWorkersFunc

	workerErrPool := dutil.NewSingleLockedMap[string, error]()

	checkWorkerDone := func(ctx context.Context, cw checkFailedWorkersFunc, worker string, err error) (bool, error) {
		l := log.With().Str("worker", worker).Logger()

		switch ok, enough, failed := cw(worker, err); {
		case failed != nil:
			l.Error().Err(failed).Msg("failed")

			return true, workersDone(failed)
		case !ok:
		case enough:
			l.Debug().Msg("enough")

			workersDone(nil)

			return false, nil
		default:
			l.Debug().Msg("checked failed")
		}

		return false, nil
	}

	workerDone := func(ctx context.Context, worker string, err error) error {
		l := log.With().Str("worker", worker).Logger()

		var cw checkFailedWorkersFunc

		switch created, _ := workerErrPool.Set(worker, func(_ error, found bool) (error, error) {
			switch {
			case checkWorker != nil:
				cw = checkWorker

				return nil, dutil.ErrLockedSetIgnore
			case found:
				return nil, dutil.ErrLockedSetIgnore
			default:
				return err, nil
			}
		}); {
		case created:
			l.Debug().Msg("not yet ready")

			return err
		case cw == nil:
			return nil
		default:
			_, err := checkWorkerDone(ctx, cw, worker, err)

			return err
		}
	}

	return func(worker string) (
			dutil.InErrorFunc[map[uint64]string],
			dutil.In2ErrorFunc[string, util.Hash],
			dutil.InErrorFunc[error],
			error,
		) {
			if ok, err := workersDoneContextError(); err != nil || !ok {
				return nil, nil, nil, err
			}

			wctx, cancel := dutil.ContextWithCancelCause(wsctx)
			checkContextError := dutil.NewFuncCheckContextError(workersDoneContextError, cancel)

			return func(reasons map[uint64]string) error {
					if ok, err := checkContextError(); err != nil || !ok {
						return err
					}

					switch err := dutil.ContextCauseError(wctx, dutil.NewIfErrErrorInFunc(cancel), func(context.Context) error {
						return opsNewjob(func(context.Context) error {
							return workerReasons(reasons)
						})
					}); {
					case errors.Is(err, dutil.ErrJobWorkerDone),
						errors.Is(err, context.Canceled):
						return nil // ignore error
					default:
						return err
					}
				},
				func(stateKey string, stateHash util.Hash) error {
					if ok, err := checkContextError(); err != nil || !ok {
						return err
					}

					switch err := dutil.ContextCauseError(wctx, dutil.NewIfErrErrorInFunc(cancel), func(context.Context) error {
						return newJob(func(context.Context) error {
							switch n, err := newStateTreeNode(stateKey, stateHash); {
							case err != nil:
								return err
							default:
								_, _ = addState(n)

								return nil
							}
						})
					}); {
					case errors.Is(err, dutil.ErrJobWorkerDone),
						errors.Is(err, context.Canceled):
						return nil // ignore error
					default:
						return err
					}
				},
				func(err error) error {
					defer cancel(nil)

					if ok, err := checkContextError(); err != nil || !ok {
						return err
					}

					switch err := workerDone(wctx, worker, err); {
					case errors.Is(err, dutil.ErrJobWorkerDone),
						errors.Is(err, context.Canceled):
						return nil // ignore error
					default:
						return err
					}
				},
				nil
		},
		func(statesOwners []string, statesShare uint64) error {
			if len(statesOwners) < 1 {
				return workersDone(nil)
			}

			return dutil.ContextCauseError(wsctx, dutil.NewIfErrErrorInFunc(workersDone), func(context.Context) error {
				if err := workerErrPool.Get(util.UUID().String(), func(error, bool) error {
					if checkWorker != nil {
						return nil
					}

					log.Debug().
						Strs("state_owners", statesOwners).
						Uint64("states_share", statesShare).
						Msg("ready")

					switch w, err := newFuncCheckFailedWorkers(uint64(len(statesOwners)), statesShare, nil); {
					case err != nil:
						return err
					default:
						checkWorker = func(worker string, err error) (ok, enough bool, failed error) {
							if slices.Index(statesOwners, worker) < 0 {
								return false, false, nil
							}

							return w(worker, err)
						}

						return nil
					}
				}); err != nil {
					return err
				}

				// NOTE exhaust errors in pool
				for worker, err := range workerErrPool.Seq2 {
					if _, gerr := checkWorkerDone(ctx, checkWorker, worker, err); gerr != nil {
						return gerr
					}
				}

				return nil
			})
		},
		func() error {
			defer func() {
				jobDone()

				workerErrPool.Close()
			}()

			return workersWait()
		},
		func(g fixedtree.GenerateNodesHashFunc) (tr *fixedtree.Tree, _ error) {
			defer func() {
				_ = jobWait(context.Canceled)
			}()

			return tr, dutil.ContextCauseError(ctx, workersDone, func(context.Context) error {
				if err := jobWait(nil); err != nil {
					return err
				}

				nodes := doneState()

				slices.SortFunc(nodes, func(a, b fixedtree.Node) int {
					return strings.Compare(a.Key(), b.Key())
				})

				if err := g(ctx, nodes, nil); err != nil {
					return err
				}

				tr = newStatesTree(nodes)

				return nil
			})
		},
		nil
}
