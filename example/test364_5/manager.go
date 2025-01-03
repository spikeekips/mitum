package main

import (
	"context"

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
	managerWaitFunc        = dutil.InErrorFunc[newManagerManifestFunc]
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

func newManager(
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

	var sendOp dutil.InErrorFunc[newWorkerSendOpFunc]
	var workerResult managerInsertWorkerResultFunc
	var processWait managerProcessWaitFunc

	switch s, o, r, w, err := newManagerProcess(
		ctx,
		dutil.NewFuncNewJobWorkerFunc(options.JobWorkerMaxSemSize),
		workers,
		opsShare, statesShare,
		ophs,
	); {
	case err != nil:
		return nil, nil, nil, nil, err
	default:
		sendOp, workerOwners, workerResult, processWait = s, o, r, w
	}

	return func(f newManagerWorkerSendOpFunc) error {
			return sendOp(func(ctx context.Context, worker string) (opindexOpHashesFunc, dutil.InErrorFunc[error], error) {
				return f(ctx, workID, worker)
			})
		},
		workerOwners,
		workerResult,
		func(mf newManagerManifestFunc) error {
			var opsTree, statesTree *fixedtree.Tree

			switch otr, str, err := processWait(
				options.GenerateNodesHashFunc,
				nil,
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

	switch {
	case workID == "":
		return 0, 0, errors.Errorf("empty work id")
	case len(ophs) < 1:
		return 0, 0, errors.Errorf("empty ophs")
	case nworkers < 1:
		return 0, 0, errors.Errorf("empty workers")
	case opsShare < 1:
		return 0, 0, errors.Errorf("zero ops share")
	case statesShare < 1:
		return 0, 0, errors.Errorf("zero states share")
	}

	if opsShare >= nworkers {
		opsShare = nworkers
	}
	if statesShare >= nworkers {
		statesShare = nworkers
	}

	return opsShare, statesShare, nil
}
