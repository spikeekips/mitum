package main

import (
	"context"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/fixedtree"
	"github.com/spikeekips/mitum/util/logging"
)

type (
	managerSaveProposalFunc        func(base.ProposalSignFact) error
	managerSaveINITVoteproofFunc   func(base.INITVoteproof) error
	managerSaveOperationsTreeFunc  func(fixedtree.Tree) error
	managerSaveStatesTreeFunc      func(fixedtree.Tree) error
	managerSaveManifestFunc        func(base.Manifest) error
	managerSaveACCEPTVoteproofFunc func(base.ACCEPTVoteproof) (BlockMap, error)
	managerSaveSetBlockMapItem     func(BlockMapItem) error
	managerSaveFunc                func(
		_ context.Context,
		workID string,
		proposalWriter blockFileWriterProposalFunc,
		voteproofsWriter blockFileWriterVoteproofsFunc,
		opsTreeWriter, statesTreeWriter blockFileWriterTreeFunc,
		mapWriter blockFileWriterMapFunc,
	) (
		managerSaveProposalFunc,
		managerSaveINITVoteproofFunc,
		managerSaveOperationsTreeFunc,
		managerSaveStatesTreeFunc,
		managerSaveManifestFunc,
		managerSaveACCEPTVoteproofFunc,
		managerSaveSetBlockMapItem,
		error,
	)
)

type managerSaveArgs struct {
	*logging.Logging
	BlockMapExpired time.Duration
}

func newManagerSaveArgs() *managerSaveArgs {
	return &managerSaveArgs{
		Logging:         NewModuleLogging("manager-save", nil),
		BlockMapExpired: time.Hour,
	}
}

func newFuncManagerSave(
	args *managerSaveArgs,
) managerSaveFunc {
	return func(
		ctx context.Context,
		workID string,
		proposalWriter blockFileWriterProposalFunc,
		voteproofsWriter blockFileWriterVoteproofsFunc,
		opsTreeWriter, statesTreeWriter blockFileWriterTreeFunc,
		mapWriter blockFileWriterMapFunc,
	) (
		managerSaveProposalFunc,
		managerSaveINITVoteproofFunc,
		managerSaveOperationsTreeFunc,
		managerSaveStatesTreeFunc,
		managerSaveManifestFunc,
		managerSaveACCEPTVoteproofFunc,
		managerSaveSetBlockMapItem,
		error,
	) {
		wctx, wctxCancel := contextWithCancelCause(ctx)

		var jw JobWorker

		switch i, err := NewBaseJobWorker(wctx, 6); {
		case err != nil:
			return nil, nil, nil, nil, nil, nil, nil, err
		default:
			jw = i
		}

		closef := func(err error) error {
			jw.Close()
			wctxCancel(err)

			return err
		}

		var ivpWriter func(base.INITVoteproof) error
		var avpWriter func(base.ACCEPTVoteproof) (BlockMapItem, error)

		switch i, a, err := voteproofsWriter(wctx, workID); {
		case err != nil:
			return nil, nil, nil, nil, nil, nil, nil, err
		default:
			ivpWriter = i
			avpWriter = a
		}

		setManifest, setItem, mapDone := newMakeBlockMapFunc(args.BlockMapExpired)

		return func(pr base.ProposalSignFact) error {
				return contextCauseErrorWithNewJob(wctx, ifErrErrorFunc(closef), jw.NewJob, func(ctx context.Context) error {
					switch i, err := proposalWriter(ctx, workID, pr); {
					case err != nil:
						return err
					default:
						return setItem(i)
					}
				})
			},

			func(ivp base.INITVoteproof) error {
				return contextCauseErrorWithNewJob(wctx, ifErrErrorFunc(closef), jw.NewJob, func(context.Context) error {
					return ivpWriter(ivp)
				})
			},
			func(tr fixedtree.Tree) error {
				return managerSaveTreeWithNewJob(wctx, workID, ifErrErrorFunc(closef), jw.NewJob, base.BlockItemOperationsTree, tr, opsTreeWriter, setItem)
			},
			func(tr fixedtree.Tree) error {
				return managerSaveTreeWithNewJob(wctx, workID, ifErrErrorFunc(closef), jw.NewJob, base.BlockItemStatesTree, tr, statesTreeWriter, setItem)
			},
			func(manifest base.Manifest) error {
				return contextCauseError(
					wctx,
					ifErrErrorFunc(closef),
					func(context.Context) error {
						return setManifest(manifest)
					},
				)
			},
			func(avp base.ACCEPTVoteproof) (bm BlockMap, _ error) {
				return bm, contextCauseError(wctx, closef, func(ctx context.Context) error {
					jw.JobDone()

					if err := jw.Wait(); err != nil {
						return err
					}

					switch i, err := avpWriter(avp); {
					case err != nil:
						return err
					default:
						if err := setItem(i); err != nil {
							return err
						}
					}

					switch m, err := mapDone(); {
					case err != nil:
						return err
					default:
						bm = m

						return mapWriter(ctx, workID, bm)
					}
				})
			},
			setItem,
			nil
	}
}

func newMakeBlockMapFunc(expired time.Duration) (
	func(base.Manifest) error,
	managerSaveSetBlockMapItem,
	func() (BlockMap, error),
) {
	var l sync.Mutex

	var manifest base.Manifest
	items := map[base.BlockItemType]BlockMapItem{}

	return func(m base.Manifest) error {
			if m == nil {
				return errors.Errorf("nil manifest")
			}

			l.Lock()
			defer l.Unlock()

			manifest = m

			return nil
		},
		func(item BlockMapItem) error {
			if item == nil {
				return errors.Errorf("nil blockmap item")
			}

			l.Lock()
			defer l.Unlock()

			if _, found := items[item.Type()]; found {
				return util.ErrFound.Errorf("item=%q", item.Type().String())
			}

			items[item.Type()] = item

			return nil
		},
		func() (BlockMap, error) {
			if manifest == nil {
				clear(items)
				items = nil

				return nil, util.ErrNotFound.Errorf("manifest")
			}

			return newBaseBlockMap(manifest, items, expired), nil
		}
}

func managerSaveTreeWithNewJob(
	ctx context.Context,
	workID string,
	cancelf errorFunc,
	newJob func(ContextWorkerCallback) error,
	it base.BlockItemType,
	tr fixedtree.Tree,
	writer blockFileWriterTreeFunc,
	setItem managerSaveSetBlockMapItem,
) error {
	return contextCauseErrorWithNewJob(ctx, cancelf, newJob, func(context.Context) error {
		switch cs, err := writer(ctx, workID, tr); {
		case err != nil:
			return err
		case len(cs) < 1:
			return nil
		default:
			return setItem(newBlockMapItemFromChunks(it, cs))
		}
	})
}

func newBlockMapItemFromChunks(it base.BlockItemType, cs []blockFileChunk) BlockMapItem {
	if len(cs) < 1 {
		return nil
	}

	slices.SortFunc(cs, func(a, b blockFileChunk) int {
		return strings.Compare(a.Checksum(), b.Checksum())
	})

	w, d := defaultChecksumWrite()

	for i := range cs {
		_, _ = w([]byte(cs[i].Checksum()))
	}

	return newBaseBlockMapItem(it, d(), true)
}
