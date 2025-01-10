package main

import (
	"context"
	"slices"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/example/test364_5/fixedtree"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
)

var managerName = "manager"

type (
	newManagerWorkerSendOpFunc = func(_ context.Context, workID, worker string) (
		opindexOpHashesFunc, dutil.InErrorFunc[error], error)
	newManagerManifestFunc = func(opsTree, statesTree util.Hash) (base.Manifest, error)
	managerWaitFunc        = dutil.In2ErrorFunc[dutil.InContextErrorFunc[string], newManagerManifestFunc]
)

type managerOptions struct {
	JobWorkerMaxSemSize   uint64
	GenerateNodesHashFunc fixedtree.GenerateNodesHashFunc
}

func newManagerOptions() *managerOptions {
	return &managerOptions{
		JobWorkerMaxSemSize:   1 << 9,
		GenerateNodesHashFunc: fixedtree.NewFuncAsyncGenerateNodesHashByFollowingParent(1 << 9),
	}
}

func newFuncManager(
	ctx context.Context,
	options *managerOptions,
	workID string,
	workers []string,
	opsShare, statesShare uint64,
	ophs [][2]util.Hash,
) (
	_ dutil.InErrorFunc[newManagerWorkerSendOpFunc],
	workerOwners dutil.In2ErrorFunc[string, []string],
	_ managerInsertWorkerResultFunc,
	wait managerWaitFunc,
	_ error,
) {
	switch o, s, err := isValidManagerArgs(workID, workers, opsShare, statesShare, ophs); {
	case err != nil:
		return nil, nil, nil, nil, err
	default:
		opsShare, statesShare = o, s
	}

	log := dutil.NewModuleLoggerFromContext(ctx, "manager", nil)

	log.Debug().
		Interface("options", options).
		Strs("workers", workers).
		Uint64("ops_share", opsShare).
		Uint64("states_share", statesShare).
		Msg("new")

	newJobWorker := dutil.NewFuncNewJobWorkerFunc(options.JobWorkerMaxSemSize)

	var sendOps dutil.InErrorFunc[newWorkerSendOpFunc]
	var workerResult managerInsertWorkerResultFunc
	var processWait managerProcessWaitFunc

	switch s, o, r, w, err := newFuncManagerProcess(
		ctx,
		newJobWorker,
		workers,
		opsShare, statesShare,
		ophs,
	); {
	case err != nil:
		return nil, nil, nil, nil, err
	default:
		sendOps, workerOwners, workerResult, processWait = s, o, r, w
	}

	return func(f newManagerWorkerSendOpFunc) error {
			return sendOps(func(ctx context.Context, worker string) (opindexOpHashesFunc, dutil.InErrorFunc[error], error) {
				return f(ctx, workID, worker)
			})
		},
		workerOwners,
		workerResult,
		func(
			workerDone dutil.InContextErrorFunc[string],
			mf newManagerManifestFunc,
		) error {
			var opsTree, statesTree *fixedtree.Tree

			switch otr, str, err := processWait(
				options.GenerateNodesHashFunc,
				func(owners []string) error {
					// NOTE manager done to workers outside owners
					return newFuncWorkerManagerDone(
						ctx,
						newJobWorker,
						workers,
						owners,
						workerDone,
					)
				},
			); {
			case err != nil:
				return err
			default:
				opsTree, statesTree = otr, str
			}

			switch manifest, err := mf(opsTree.Root(), statesTree.Root()); {
			case err != nil:
				return err
			default:
				_ = manifest
			}

			// FIXME save,
			// - block map
			// - trees

			return nil
		},
		nil
}

func isValidManagerArgs(
	workID string,
	workers []string,
	opsShare, statesShare uint64,
	ophs [][2]util.Hash,
) (uint64, uint64, error) {
	nworkers := uint64(len(workers))

	var err error

	switch {
	case workID == "":
		err = errors.Errorf("empty work id")
	case len(ophs) < 1:
		err = errors.Errorf("empty ophs")
	case nworkers < 1:
		err = errors.Errorf("empty workers")
	case opsShare < 1:
		err = errors.Errorf("zero ops share")
	case statesShare < 1:
		err = errors.Errorf("zero states share")
	}

	if err != nil {
		return 0, 0, err
	}

	if opsShare >= nworkers {
		opsShare = nworkers
	}

	if statesShare >= nworkers {
		statesShare = nworkers
	}

	return opsShare, statesShare, nil
}

func newFuncWorkerManagerDone(
	ctx context.Context,
	newJobWorker dutil.NewJobWorkerFunc,
	workers, owners []string,
	workerDone dutil.InContextErrorFunc[string],
) error {
	var newJob dutil.JobWorkerNewJobFunc
	var jobDone func()
	var jobWait dutil.InErrorFunc[error]

	switch n, d, w, err := newJobWorker(ctx, uint64(len(workers))); {
	case err != nil:
		return err
	default:
		newJob, jobDone, jobWait = n, d, w
	}

	for worker := range dutil.FilterSlice(slices.Values(workers), func(i string) bool {
		return slices.Index(owners, i) < 0
	}) {
		if err := newJob(func(ctx context.Context) error {
			return workerDone(ctx, worker)
		}); err != nil {
			return jobWait(err)
		}
	}

	jobDone()

	return jobWait(nil)
}
