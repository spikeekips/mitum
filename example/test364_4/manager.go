package main

import (
	"context"
	"slices"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/fixedtree"
	"github.com/spikeekips/mitum/util/logging"
)

type (
	managerStartWorkerFunc             func(_ context.Context, worker string) error
	managerManifestFunc                func(opsTree, statesTree fixedtree.Tree) (base.Manifest, error)
	managerSaveWithACCEPTVoteproofFunc func(
		_ base.ACCEPTVoteproof,
		_ blockFileWriterProposalFunc,
		_ blockFileWriterVoteproofsFunc,
		opsTreeWriter, statesTreeWriter blockFileWriterTreeFunc,
		_ blockFileWriterMapFunc,
	) (BlockMap, error)
	managerACCEPVoteproofFunc   func(managerManifestFunc) (managerSaveWithACCEPTVoteproofFunc, error)
	managerTreeWithProposalFunc func(base.ProposalSignFact, base.INITVoteproof) (
		managerWorkersOperationsFunc,
		managerWorkerUnmergedFunc,
		managerWorkerStatesFunc,
		managerACCEPVoteproofFunc,
		managerWorkerBlockFileReceiveFunc,
		error,
	)
	managerStartFunc func(
		_ context.Context,
		workers []string,
		opsShare, statesShare uint64,
		workID string,
		startWorker managerStartWorkerFunc,
	) (
		managerTreeWithProposalFunc,
		error,
	)
)

type managerArgs struct {
	*logging.Logging
	NewCheckWorkersFunc checkEnoughFailedWorkersFuncs
}

func newManagerArgs() *managerArgs {
	return &managerArgs{
		Logging: NewModuleLogging("manager", nil),
		NewCheckWorkersFunc: func(_, _ uint64) (
			checkEnoughFailedWorkerFunc,
			func(),
			error,
		) {
			return nil, nil, util.ErrNotImplemented.Errorf("NewCheckWorkers")
		},
	}
}

func newFuncManager(
	args *managerArgs,
	wtargs *managerWorkersTreesArgs,
	sargs *managerSaveArgs,
) (
	managerStartFunc,
	error,
) {
	doTree := newFuncManagerTrees(wtargs, args.NewCheckWorkersFunc)
	workerBlockItems := newFuncManagerWorkerBlockFiles(args.NewCheckWorkersFunc)
	doSave := newFuncManagerSave(sargs)

	return func(
			ctx context.Context,
			workers []string,
			opsShare, statesShare uint64,
			workID string,
			startWorker managerStartWorkerFunc,
		) (
			managerTreeWithProposalFunc,
			error,
		) {
			if len(workID) < 1 {
				return nil, errors.Errorf("empty work id")
			}

			var minEnough, maxFailed uint64

			switch e, f, so, ss, err := managerMinEnoughMaxFailed(uint64(len(workers)), opsShare, statesShare); {
			case err != nil:
				return nil, err
			default:
				minEnough = e
				maxFailed = f
				opsShare = so
				statesShare = ss
			}

			wctx, wctxCancel := contextWithCancelCause(ctx)

			// NOTE start workers
			if err := contextCauseError(wctx, ifErrErrorFunc(wctxCancel), func(ctx context.Context) error {
				return newManagerStartWorkers(
					ctx,
					workers,
					maxFailed,
					args.NewCheckWorkersFunc,
					startWorker,
				)
			}); err != nil {
				return nil, err
			}

			return managerTreesWithProposal(
					wctx,
					wctxCancel,
					workers,
					opsShare, statesShare, minEnough, maxFailed,
					workID,
					doTree,
					workerBlockItems,
					doSave,
				),
				nil
		},
		nil
}

func managerMinEnoughMaxFailed(
	nworkers,
	opsShare, statesShare uint64,
) (minEnough, maxFailed, newOpsShare, newStatesShare uint64, _ error) {
	if nworkers < 1 {
		return minEnough, maxFailed, newOpsShare, newStatesShare, util.ErrNotFound.Errorf("empty workers")
	}

	switch e, f, s, err := minEnoughMaxFailed(nworkers, opsShare); {
	case err != nil:
		return minEnough, maxFailed, newOpsShare, newStatesShare, err
	default:
		newOpsShare = s
		maxFailed = f
		minEnough = e
	}

	switch e, f, s, err := minEnoughMaxFailed(nworkers, statesShare); {
	case err != nil:
		return minEnough, maxFailed, newOpsShare, newStatesShare, err
	default:
		newStatesShare = s

		if f < maxFailed {
			maxFailed = f
		}

		if e > minEnough {
			minEnough = e
		}
	}

	return minEnough, maxFailed, newOpsShare, newStatesShare, nil
}

func operationsFromINITVoteproof(pr base.ProposalSignFact, _ base.INITVoteproof) [][2]util.Hash {
	// FIXME expels from INITVoteproof
	return pr.ProposalFact().Operations()
}

func handleReceivedBlockFiles() (
	managerWorkerBlockFileWorkerFilesFunc,
	func(managerSaveSetBlockMapItem) error,
) {
	itemTypeChecksums := util.NewSingleLockedMap[base.BlockItemType, []string]()

	return func(_ string, m map[base.BlockItemType][]string) error {
			_ = TraverseMap(m, func(it base.BlockItemType, checksums []string) (bool, error) {
				_, _, err := itemTypeChecksums.Set(it, func(prev []string, found bool) ([]string, error) {
					switch {
					case len(checksums) < 1:
						return nil, util.ErrLockedSetIgnore
					case !found:
						return checksums, nil
					}

					var add util.CompactAppendSliceAddFunc[string]
					var done util.CompactAppendSliceDoneFunc[string]

					switch a, d, err := util.CompactAppendSliceV1[string](uint64(len(checksums))); {
					case err != nil:
						return nil, err
					default:
						defer d(nil)

						add = a
						done = d
					}

					util.TraverseSliceV1(checksums, func(_ int, checksum string) (bool, error) {
						if slices.Index(prev, checksum) < 1 {
							add(checksum)
						}

						return true, nil
					})

					var merged []string

					_ = done(func(s []string) error {
						merged = make([]string, len(prev)+len(s))
						copy(merged, prev)
						copy(merged[len(prev):], s)

						return nil
					})

					return merged, nil
				})

				return true, err
			})

			return nil
		},
		func(f managerSaveSetBlockMapItem) error {
			defer itemTypeChecksums.Close()

			return TraverseMap(itemTypeChecksums.Map(), func(it base.BlockItemType, checksums []string) (bool, error) {
				if len(checksums) < 1 {
					return true, nil
				}

				slices.Sort(checksums)

				return true, f(newBaseBlockMapItem(it, baseBlockMapItemChecksum(checksums), true))
			})
		}
}

func managerTreesWithProposal(
	ctx context.Context,
	ctxCancel errorFunc,
	workers []string,
	opsShare, statesShare, minEnough, maxFailed uint64,
	workID string,
	doTree managerTreesFuncs,
	workerBlockItems managerWorkerBlockFilesFunc,
	doSave managerSaveFunc,
) managerTreeWithProposalFunc {
	return func(pr base.ProposalSignFact, ivp base.INITVoteproof) (
		managerWorkersOperationsFunc,
		managerWorkerUnmergedFunc,
		managerWorkerStatesFunc,
		managerACCEPVoteproofFunc,
		managerWorkerBlockFileReceiveFunc,
		error,
	) {
		ophs := operationsFromINITVoteproof(pr, ivp)

		var workersOperations managerWorkersOperationsFunc
		var workersUnmerged managerWorkerUnmergedFunc
		var workersStates managerWorkerStatesFunc
		var treesDone managerTreesDoneFunc

		switch o, u, s, d, err := doTree(
			ctx,
			workers,
			opsShare, statesShare,
			ophs,
		); {
		case err != nil:
			return nil, nil, nil, nil, nil, err
		default:
			workersOperations = o
			workersUnmerged = u
			workersStates = s
			treesDone = d
		}

		var workerBlockFileReceive managerWorkerBlockFileReceiveFunc
		var workerBlockFilesDone managerWorkerBlockFilesDoneFunc

		workerBlockFileReceiveWorkerFile, workerBlockItemsDone := handleReceivedBlockFiles()

		switch r, d, err := workerBlockItems(ctx, minEnough, maxFailed, workerBlockFileReceiveWorkerFile); {
		case err != nil:
			return nil, nil, nil, nil, nil, err
		default:
			workerBlockFileReceive = r
			workerBlockFilesDone = d
		}

		return workersOperations, workersUnmerged, workersStates,
			func(f managerManifestFunc) (managerSaveWithACCEPTVoteproofFunc, error) {
				var opsTree, statesTree fixedtree.Tree
				var manifest base.Manifest

				if err := contextCauseError(ctx, ifErrErrorFunc(ctxCancel), func(context.Context) error {
					return treesDone(func(ops, states fixedtree.Tree) error {
						switch m, err := f(ops, states); {
						case err != nil:
							return err
						default:
							opsTree = ops
							statesTree = states
							manifest = m

							return nil
						}
					})
				}); err != nil {
					return nil, err
				}

				return managerSaveWithACCEPTVoteproof(
						ctx,
						ctxCancel,
						workID,
						pr,
						ivp,
						workerBlockFilesDone,
						workerBlockItemsDone,
						doSave,
						opsTree, statesTree,
						manifest,
					),
					nil
			},
			workerBlockFileReceive,
			nil
	}
}

func managerSaveWithACCEPTVoteproof(
	ctx context.Context,
	ctxCancel errorFunc,
	workID string,
	pr base.ProposalSignFact,
	ivp base.INITVoteproof,
	workerBlockFilesDone managerWorkerBlockFilesDoneFunc,
	workerBlockItemsDone func(managerSaveSetBlockMapItem) error,
	doSave managerSaveFunc,
	opsTree, statesTree fixedtree.Tree,
	manifest base.Manifest,
) managerSaveWithACCEPTVoteproofFunc {
	return func(
		avp base.ACCEPTVoteproof,
		proposalWriter blockFileWriterProposalFunc,
		voteproofsWriter blockFileWriterVoteproofsFunc,
		opsTreeWriter, statesTreeWriter blockFileWriterTreeFunc,
		mapWriter blockFileWriterMapFunc,
	) (bm BlockMap, _ error) {
		defer func() {
			opsTree.Close()
			statesTree.Close()
		}()

		return bm, contextCauseError(ctx, ctxCancel, func(context.Context) error {
			if err := workerBlockFilesDone(); err != nil {
				return err
			}

			var saveACCEPTVoteproof managerSaveACCEPTVoteproofFunc

			switch saveProposal, saveINITVoteproof, saveOpsTree, saveStatesTree,
				saveManifest, savp, setBlockMapItem, err := doSave(
				ctx,
				workID,
				proposalWriter,
				voteproofsWriter,
				opsTreeWriter, statesTreeWriter,
				mapWriter,
			); {
			case err != nil:
				return err
			default:
				if err := saveProposal(pr); err != nil {
					return err
				}

				if err := saveINITVoteproof(ivp); err != nil {
					return err
				}

				if err := saveOpsTree(opsTree); err != nil {
					return err
				}

				if err := saveStatesTree(statesTree); err != nil {
					return err
				}

				if err := saveManifest(manifest); err != nil {
					return err
				}

				if err := workerBlockItemsDone(func(it BlockMapItem) error {
					return setBlockMapItem(it)
				}); err != nil {
					return err
				}

				saveACCEPTVoteproof = savp
			}

			switch i, err := saveACCEPTVoteproof(avp); {
			case err != nil:
				return err
			default:
				bm = i

				return nil
			}
		})
	}
}
