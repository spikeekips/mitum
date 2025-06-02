package dop6

import (
	"context"
	"errors"
	"slices"
	"strings"

	dop5 "github.com/spikeekips/mitum/example/test364_5"
	"github.com/spikeekips/mitum/example/test364_5/fixedtree"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
)

type (
	managerWorkerStatesFunc = func(worker string) (
		dutil.In2ErrorFunc[string /* state key */, util.Hash /* state hash */],
		dutil.InErrorFunc[error],
	)
	managerFixedTreeWaitFunc = func(fixedtree.GenerateNodesHashFunc) (*fixedtree.Tree, error)
)

func newManagerStates(
	ctx context.Context,
	approxStatesSize uint64,
	checkWorker dop5.CheckFailedWorkersFunc,
) (
	managerWorkerStatesFunc,
	managerFixedTreeWaitFunc,
	error,
) {
	var addState dutil.AppendSliceAddFunc[fixedtree.Node]
	var doneState dutil.OutFunc[[]fixedtree.Node]

	switch a, d, err := dutil.NewLockUniqueAppendSlice[fixedtree.Node, string](approxStatesSize, 1<<9, //nolint:mnd
		func(n fixedtree.Node) string { return n.Key() }); {
	case err != nil:
		return nil, nil, err
	default:
		addState, doneState = a, d
	}

	var newJob dutil.JobWorkerNewJobFunc
	var jobDone func()
	var jobWait dutil.InErrorFunc[error]

	switch n, d, w, err := dutil.NewFuncJobWorker(ctx, dutil.SafeUint64ToInt64(approxStatesSize)); {
	case err != nil:
		return nil, nil, err
	default:
		newJob, jobDone, jobWait = n, d, w
	}

	return func(worker string) (
			dutil.In2ErrorFunc[string, util.Hash],
			dutil.InErrorFunc[error],
		) {
			return func(stateKey string, stateHash util.Hash) error {
					switch err := newJob(func(context.Context) error {
						switch n, err := dop5.NewStateTreeNode(stateKey, stateHash); {
						case err != nil:
							return err
						default:
							_, _ = addState(n)

							return nil
						}
					}); {
					case err == nil:
					case !errors.Is(err, dutil.ErrJobWorkerDone):
						return err
					}

					return nil
				},
				func(err error) error {
					switch ok, enough, failed := checkWorker(worker, err); {
					case !ok:
					case failed != nil:
						return jobWait(failed)
					case enough:
						jobDone()
					}

					return nil
				}
		},
		func(g fixedtree.GenerateNodesHashFunc) (*fixedtree.Tree, error) {
			if err := jobWait(nil); err != nil {
				return nil, err
			}

			nodes := doneState()

			slices.SortFunc(nodes, func(a, b fixedtree.Node) int {
				return strings.Compare(a.Key(), b.Key())
			})

			if err := g(ctx, nodes, nil); err != nil {
				return nil, err
			}

			return dop5.NewStatesTree(nodes), nil
		},
		nil
}
