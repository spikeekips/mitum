package main

import (
	"context"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/example/test364_5/fixedtree"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
)

type managerProcessWaitFunc = dutil.In4ErrorFunc[
	fixedtree.GenerateNodesHashFunc,
	dutil.InErrorFunc[[]string /* owners */],
	dutil.InErrorFunc[*fixedtree.Tree /* operations fixedtree tree */],
	dutil.InErrorFunc[*fixedtree.Tree /* states fixedtree tree */],
]

func newManagerProcess(
	ctx context.Context,
	newJobWorker dutil.NewJobWorkerFunc,
	workers []string,
	opsShare, statesShare uint64,
	ophs [][2]util.Hash,
) (
	_ dutil.InErrorFunc[newWorkerSendOpFunc],
	workerOwners dutil.In2ErrorFunc[string, []string],
	_ managerInsertWorkerResultFunc,
	_ managerProcessWaitFunc,
	_ error,
) {
	switch { // FIXME move to manager
	case len(ophs) < 1:
		return nil, nil, nil, nil, errors.Errorf("empty ophs")
	case len(workers) < 1:
		return nil, nil, nil, nil, errors.Errorf("empty workers")
	case opsShare < 1:
		return nil, nil, nil, nil, errors.Errorf("zero ops share")
	case statesShare < 1:
		return nil, nil, nil, nil, errors.Errorf("zero states share")
	}

	log := dutil.NewModuleLoggerFromContext(ctx, "manager-process", nil)

	var sendOps dutil.InErrorFunc[newWorkerSendOpFunc]
	var opsWorkers []string
	var opsMinEnough uint64
	var opsWait dutil.ErrorFunc

	switch ow, me, _, f, w, err := newFuncManagerWorkersOps(ctx, newJobWorker, workers, opsShare, ophs); {
	case err != nil:
		return nil, nil, nil, nil, err
	default:
		opsWorkers, opsMinEnough, sendOps, opsWait = ow, me, f, w
	}

	log.Debug().
		Strs("workers", workers).
		Uint64("ops_share", opsShare).
		Uint64("statess_share", statesShare).
		Int("ophs", len(ophs)).
		Msg("new")

	var workerOwnersWait dutil.InErrorFunc[dutil.InErrorFunc[[]string]]

	switch o, w, err := newFuncManagerCollectOwners(ctx, uint64(len(opsWorkers)), opsMinEnough); {
	case err != nil:
		return nil, nil, nil, nil, err
	default:
		workerOwners, workerOwnersWait = o, w
	}

	var workerResult managerInsertWorkerResultFunc
	var ownersReady managerCollectResultOwnersReadyFunc
	var resultWait managerCollectRersultWaitFunc

	switch wr, r, w, err := newFuncManagerCollectResult(ctx, uint64(len(ophs))*2, ophs); { // NOTE approximate states size
	case err != nil:
		return nil, nil, nil, nil, err
	default:
		workerResult, ownersReady, resultWait = wr, r, w
	}

	return sendOps,
		workerOwners,
		workerResult,
		func(
			g fixedtree.GenerateNodesHashFunc,
			ownersf dutil.InErrorFunc[[]string],
			opsTree dutil.InErrorFunc[*fixedtree.Tree],
			statesTree dutil.InErrorFunc[*fixedtree.Tree],
		) error {
			defer func() {
				_ = workerOwnersWait(nil)
				_ = resultWait(nil, nil, nil)
			}()

			if err := opsWait(); err != nil {
				return err
			}

			if err := workerOwnersWait(func(owners []string) error {
				if ownersf != nil {
					if err := ownersf(owners); err != nil {
						return err
					}
				}

				return ownersReady(owners, statesShare)
			}); err != nil {
				return err
			}

			return resultWait(g, opsTree, statesTree)
		},
		nil
}
