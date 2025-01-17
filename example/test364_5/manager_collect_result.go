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
	managerCollectResultOwnersReadyFunc = dutil.In2ErrorFunc[[]string /* stateOwners */, uint64 /* statesShare */]
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

	_, baseOpsDone, _, baseOpsWait := dutil.NewContextDone(ctx)

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
				_ = baseOpsWait(context.Canceled)
			}()

			if g == nil {
				return nil, nil, nil
			}

			if err := workersWait(); err != nil {
				return nil, nil, err
			}

			return opsTree, statesTree, dutil.FuncsJobWorker(ctx, func(context.Context) error {
				if err := baseOpsWait(nil); err != nil {
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
				})
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
	var pool util.LockedMap[uint64, *OperationTreeNode]

	switch i, err := util.NewLockedMap[uint64, *OperationTreeNode](uint64(len(ophs)), nil); {
	case err != nil:
		return nil, nil, nil, err
	default:
		pool = i
	}

	return func() error {
			for i, hs := range slices.All(ophs) {
				if err := newJob(func(context.Context) error {
					_, _, _ = pool.Set(uint64(i), func(n *OperationTreeNode, found bool) (*OperationTreeNode, error) {
						if found {
							return nil, util.ErrLockedSetIgnore
						}

						return NewOperationTreeNode(hs[0], hs[1], ""), nil
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
		func(g fixedtree.GenerateNodesHashFunc) (*fixedtree.Tree, error) {
			defer pool.Close()

			nnodes := uint64(pool.Len())

			if nnodes < 1 {
				return nil, nil
			}

			nodes := make([]fixedtree.Node, nnodes)

			_ = pool.Traverse(func(opindex uint64, n *OperationTreeNode) bool {
				nodes[opindex] = n

				return true
			})

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

	wsctx, workersDone, parentContextError, workersWait := dutil.NewContextDone(ctx)

	var addState dutil.AppendSliceAddFunc[fixedtree.Node]
	var doneState dutil.OutFunc[[]fixedtree.Node]

	switch a, d, err := dutil.NewLockUniqueAppendSlice[fixedtree.Node, string](approxStatesSize, 0,
		func(n fixedtree.Node) string { return n.Key() }); {
	case err != nil:
		return nil, nil, nil, nil, err
	default:
		addState, doneState = a, d
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

	return func(worker string) (
			dutil.InErrorFunc[map[uint64]string],
			dutil.In2ErrorFunc[string, util.Hash],
			dutil.InErrorFunc[error],
			error,
		) {
			if ok, err := parentContextError(); err != nil || !ok {
				return nil, nil, nil, err
			}

			wctx, cancel := dutil.ContextWithCancelCause(wsctx)
			checkParentContextError := dutil.NewFuncCheckParentContextError(parentContextError, cancel)

			return func(reasons map[uint64]string) error {
					if ok, err := checkParentContextError(); err != nil || !ok {
						return err
					}

					return dutil.ContextCauseError(wctx, dutil.NewIfErrErrorInFunc(cancel), func(context.Context) error {
						return opsNewjob(func(context.Context) error {
							return workerReasons(reasons)
						})
					})
				},
				func(stateKey string, stateHash util.Hash) error {
					if ok, err := checkParentContextError(); err != nil || !ok {
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

					if ok, err := checkParentContextError(); err != nil || !ok {
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
				if err := workerErrPool.Get(util.UUID().String(), func(error, bool) error {
					if checkWorker != nil {
						return nil
					}

					switch minEnough, maxFailed, err := minEnoughMaxFailed(workersSize, statesShare); {
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
							checkWorker = w
							checkWorkerClose = c

							return nil
						}
					}
				}); err != nil {
					return err
				}

				var gerr error

				// NOTE exhaust  errors in pool
				_ = workerErrPool.Traverse(func(worker string, err error) bool {
					if failed, err := checkWorkerDone(ctx, checkWorker, worker, err); failed {
						gerr = err
					}

					return gerr == nil
				})

				return gerr
			})
		},
		func() error {
			defer func() {
				jobDone()

				workerErrPool.Close()
			}()

			return workersWait(nil)
		},
		func(g fixedtree.GenerateNodesHashFunc) (tr *fixedtree.Tree, _ error) {
			defer func() {
				_ = workerErrPool.Get(util.UUID().String(), func(error, bool) error {
					if checkWorkerClose != nil {
						checkWorkerClose()
					}

					return nil
				})
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
