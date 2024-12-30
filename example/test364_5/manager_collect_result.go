package main

import (
	"context"
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
	managerCollectRersultWaitFunc = dutil.In3ErrorFunc[
		fixedtree.GenerateNodesHashFunc,
		dutil.InErrorFunc[*fixedtree.Tree /* operations fixedtree tree */],
		dutil.InErrorFunc[*fixedtree.Tree /* states fixedtree tree */],
	]
	managerCollectResultOwnersReadyFunc = dutil.In2ErrorFunc[[]string /* stateOwners */, uint64 /* statesShare */]
)

func newFuncManagerCollectResult(
	ctx context.Context,
	maxStatesSize uint64,
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
		Uint64("max_states_size", maxStatesSize).
		Msg("new")

	var newJob dutil.JobWorkerNewJobFunc
	var jobDone func()
	var jobWait dutil.ErrorFunc

	switch n, d, w, err := dutil.NewFuncJobWorker(ctx, maxStatesSize); {
	case err != nil:
		return nil, nil, nil, err
	default:
		newJob, jobDone, jobWait = n, d, w
	}

	_, baseOpsDone, _, baseOpsWait := dutil.NewContextDone(ctx)

	var unmergedOp dutil.In2ErrorFunc[uint64, string]
	var doneOpsTree dutil.In2ErrorFunc[fixedtree.GenerateNodesHashFunc, dutil.InErrorFunc[*fixedtree.Tree]]

	switch startOpsFixedNodes, u, d, err := newFuncGenerateOperationFixedtree(ctx, ophs, newJob); {
	case err != nil:
		return nil, nil, nil, err
	default:
		unmergedOp = u
		doneOpsTree = d

		go func() {
			_ = baseOpsDone(startOpsFixedNodes())
		}()
	}

	var workerResult managerInsertWorkerResultFunc
	var ownersReady managerCollectResultOwnersReadyFunc
	var workersWait dutil.ErrorFunc
	var workersDone dutil.In2ErrorFunc[fixedtree.GenerateNodesHashFunc, dutil.InErrorFunc[*fixedtree.Tree]]

	switch r, o, d, w, err := newFuncManagerWorkersResult(
		ctx,
		maxStatesSize,
		newJob,
		func(reasons map[uint64]string) error {
			return dutil.TraverseMap(reasons, func(opindex uint64, reason string) (bool, error) {
				return true, unmergedOp(opindex, reason)
			})
		},
	); {
	case err != nil:
		return nil, nil, nil, err
	default:
		workerResult, ownersReady, workersWait, workersDone = r, o, d, w
	}

	return workerResult,
		ownersReady,
		func(g fixedtree.GenerateNodesHashFunc, opsTree dutil.InErrorFunc[*fixedtree.Tree], statesTree dutil.InErrorFunc[*fixedtree.Tree]) error {
			if err := baseOpsWait(); err != nil {
				return err
			}

			if err := workersWait(); err != nil {
				return err
			}

			jobDone()

			if err := jobWait(); err != nil {
				return err
			}

			return dutil.FuncsJobWorker(
				ctx,
				func(context.Context) error {
					return doneOpsTree(g, opsTree)
				},
				func(context.Context) error {
					return workersDone(g, statesTree)
				},
			)
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
	_ dutil.In2ErrorFunc[fixedtree.GenerateNodesHashFunc, dutil.InErrorFunc[*fixedtree.Tree]],
	_ error,
) {
	var pool util.LockedMap[uint64, *OperationTreeNode]

	switch i, err := util.NewLockedMap[uint64, *OperationTreeNode](uint64(len(ophs)), nil); {
	case err != nil:
		return nil, nil, nil, err
	default:
		pool = i
	}

	return func() error {
			return dutil.TraverseSlice(ophs, func(i int, hs [2]util.Hash) (bool, error) {
				return true, newJob(func(context.Context) error {
					_, _, _ = pool.Set(uint64(i), func(n *OperationTreeNode, found bool) (*OperationTreeNode, error) {
						if found {
							return nil, util.ErrLockedSetIgnore
						}

						return NewOperationTreeNode(hs[0], hs[1], ""), nil
					})

					return nil
				})
			})
		},
		func(opindex uint64, reason string) error {
			if reason == "" {
				return nil
			}

			_, _, _ = pool.Set(opindex, func(n *OperationTreeNode, found bool) (*OperationTreeNode, error) {
				switch {
				case !found:
					hs := ophs[opindex]

					return NewOperationTreeNode(hs[0], hs[1], reason), nil
				default:
					n.SetReason(reason)
				}

				return nil, util.ErrLockedSetIgnore
			})

			return nil
		},
		func(g fixedtree.GenerateNodesHashFunc, f dutil.InErrorFunc[*fixedtree.Tree]) error {
			defer pool.Close()

			if f == nil {
				return nil
			}

			nnodes := uint64(pool.Len())

			if nnodes < 1 {
				return f(nil)
			}

			nodes := make([]fixedtree.Node, nnodes)

			_ = pool.Traverse(func(opindex uint64, n *OperationTreeNode) bool {
				nodes[opindex] = n

				return true
			})

			if g == nil {
				g = fixedtree.NewFuncAsyncGenerateNodesHashByFollowingParent(nnodes)
			}

			if err := g(ctx, nodes, nil); err != nil {
				return err
			}

			return f(newOperationsTree(nodes))
		},
		nil
}

func newFuncManagerWorkersResult(
	ctx context.Context,
	maxStatesSize uint64,
	newJob dutil.JobWorkerNewJobFunc,
	workerReasons dutil.InErrorFunc[map[uint64]string],
) (
	_ managerInsertWorkerResultFunc,
	ready managerCollectResultOwnersReadyFunc,
	insertDone dutil.ErrorFunc,
	_ dutil.In2ErrorFunc[fixedtree.GenerateNodesHashFunc, dutil.InErrorFunc[*fixedtree.Tree]],
	_ error,
) {
	log := dutil.LoadContextValueLogger(ctx)

	wsctx, workersDone, parentContextError, workersWait := dutil.NewContextDone(ctx)

	var addState dutil.AppendSliceAddFunc[fixedtree.Node]
	var doneState dutil.AppendSliceDoneFunc[fixedtree.Node]

	if err := dutil.RUniqueAppendSlice[fixedtree.Node, string](maxStatesSize, 0,
		func(n fixedtree.Node) string { return n.Key() },
		&addState, &doneState); err != nil {
		return nil, nil, nil, nil, err
	}

	var checkWorker checkEnoughFailedWorkerFunc
	var checkWorkerClose func()

	workerErrPool := util.NewSingleLockedMap[string, error]()

	checkWorkerDone := func(ctx context.Context, cw checkEnoughFailedWorkerFunc, worker string, err error) (bool, error) {
		l := log.With().Str("worker", worker).Logger()

		switch ok, enough, failed, err := cw(worker, func() error { return err }); {
		case failed:
			l.Error().Err(err).Msg("failed")

			return true, workersDone(err)
		case err != nil:
			l.Error().Err(err).Msg("err")

			return false, err
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

		var cw checkEnoughFailedWorkerFunc

		switch _, created, _ := workerErrPool.Set(worker, func(_ error, found bool) (error, error) {
			switch {
			case checkWorker != nil:
				cw = checkWorker

				return nil, util.ErrLockedSetIgnore
			case found:
				return nil, util.ErrLockedSetIgnore
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

	checkParentContextError := func() error {
		if ok, err := parentContextError(); err != nil || !ok {
			return err
		}

		return nil
	}

	return func(worker string) (
			dutil.InErrorFunc[map[uint64]string],
			dutil.In2ErrorFunc[string, util.Hash],
			dutil.InErrorFunc[error],
			error,
		) {
			wctx, cancel := dutil.ContextWithCancelCause(wsctx)

			return func(reasons map[uint64]string) error {
					if err := dutil.NewIfErrErrorInFunc(cancel)(checkParentContextError()); err != nil {
						return err
					}

					return dutil.ContextCauseError(wctx, dutil.NewIfErrErrorInFunc(cancel), func(context.Context) error {
						return newJob(func(context.Context) error {
							return workerReasons(reasons)
						})
					})
				},
				func(stateKey string, stateHash util.Hash) error {
					if err := dutil.NewIfErrErrorInFunc(cancel)(checkParentContextError()); err != nil {
						return err
					}

					return dutil.ContextCauseError(wctx, dutil.NewIfErrErrorInFunc(cancel), func(context.Context) error {
						return newJob(func(context.Context) error {
							_, _ = addState(NewStateTreeNode(stateKey, stateHash))

							return nil
						})
					})
				},
				func(err error) error {
					defer cancel(nil)

					if err := checkParentContextError(); err != nil {
						return err
					}

					return workerDone(wctx, worker, err)
				},
				nil
		},
		func(stateOwners []string, statesShare uint64) error {
			if len(stateOwners) < 1 {
				return workersDone(nil)
			}

			return dutil.ContextCauseError(wsctx, dutil.NewIfErrErrorInFunc(workersDone), func(context.Context) error {
				return workerErrPool.Get(util.UUID().String(), func(error, bool) error {
					if checkWorker != nil {
						return nil
					}

					switch minEnough, maxFailed, err := minEnoughMaxFailed(uint64(len(stateOwners)), statesShare); {
					case err != nil:
						return err
					default:
						log.Debug().
							Uint64("states_share", statesShare).
							Uint64("min_enough", minEnough).
							Uint64("max_failed", maxFailed).
							Msg("ready")

						switch w, c, err := newFuncCheckEnoughFailedWorkers(minEnough, maxFailed); {
						case err != nil:
							return err
						default:
							checkWorker = func(worker string, f func() error) (bool, bool, bool, error) {
								if slices.Index(stateOwners, worker) < 0 {
									return false, false, false, util.ErrNotFound.Errorf("unknown worker, %q", worker)
								}

								return w(worker, f)
							}
							checkWorkerClose = c
						}
					}

					var gerr error

					// NOTE exhaust  errors in pool
					_ = workerErrPool.Traverse(func(worker string, err error) bool {
						if failed, err := checkWorkerDone(ctx, checkWorker, worker, err); failed {
							gerr = err
						}

						return true
					})

					return gerr
				})
			})
		},
		func() error {
			defer workerErrPool.Close()

			return workersWait()
		},
		func(
			g fixedtree.GenerateNodesHashFunc,
			statesTree dutil.InErrorFunc[*fixedtree.Tree],
		) error {
			defer func() {
				_ = workersDone(nil)
				_ = workerErrPool.Get(util.UUID().String(), func(error, bool) error {
					if checkWorkerClose != nil {
						checkWorkerClose()
					}

					return nil
				})
				doneState(nil)
			}()

			return dutil.ContextCauseError(ctx, workersDone, func(context.Context) error {
				return doneState(func(s []fixedtree.Node) error {
					if statesTree == nil {
						return nil
					}

					nodes := slices.Clone(s)

					slices.SortFunc(nodes, func(a, b fixedtree.Node) int {
						return strings.Compare(a.Key(), b.Key())
					})

					if g == nil {
						g = fixedtree.NewFuncAsyncGenerateNodesHashByFollowingParent(uint64(len(nodes)))
					}

					if err := g(ctx, nodes, nil); err != nil {
						return err
					}

					return statesTree(newStatesTree(nodes))
				})
			})
		},
		nil
}
