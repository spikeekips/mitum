package main

import (
	"context"

	"github.com/spikeekips/mitum/example/test364_5/fixedtree"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
)

type managerProcessWaitFunc = func(
	fixedtree.GenerateNodesHashFunc,
	dutil.InErrorFunc[[]string /* owners */],
) (
	*fixedtree.Tree, /* operations fixedtree tree */
	*fixedtree.Tree, /* states fixedtree tree */
	error,
)

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

	var workerOwnersWait dutil.OutErrorFunc[[]string]

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
		) (*fixedtree.Tree, *fixedtree.Tree, error) {
			defer func() {
				_, _, _ = resultWait(nil)
			}()

			if err := opsWait(); err != nil {
				return nil, nil, err
			}

			switch owners, err := workerOwnersWait(); {
			case err != nil:
				return nil, nil, err
			default:
				if ownersf != nil {
					if err := ownersf(owners); err != nil {
						return nil, nil, err
					}
				}

				if err := ownersReady(owners, statesShare); err != nil {
					return nil, nil, err
				}
			}

			return resultWait(g)
		},
		nil
}
