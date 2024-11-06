package main

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/fixedtree"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type testManagerWorkers struct {
	baseTestManager
}

func (t *testManagerWorkers) newArgs() (
	args *managerArgs,
	wtargs *managerWorkersTreesArgs,
	sargs *managerSaveArgs,
) {
	args = newManagerArgs()
	args.NewCheckWorkersFunc = func(minEnough, maxFailed uint64) (
		checkEnoughFailedWorkerFunc,
		func(),
		error,
	) {
		return newCheckEnoughFailedWorkers(minEnough, maxFailed)
	}

	wtargs = newManagerWorkersTreesArgs()
	wtargs.NewWorkersOpsFunc = distributeNShareWorkerOps
	wtargs.NewOperationsTreeWriterFuncs = newFuncOperationsTreeWriter()
	wtargs.NewStatesTreeWriterFuncs = newFuncStatesTreeWriter(1<<9, 1<<9)

	sargs = newManagerSaveArgs()

	args.SetLogging(logging.TestNilLogging)
	wtargs.SetLogging(logging.TestNilLogging)
	sargs.SetLogging(logging.TestNilLogging)

	return
}

func (t *testManagerWorkers) workerStartWorkerFunc(f map[string]func(context.Context) error) func(context.Context, string) error {
	return func(ctx context.Context, worker string) error {
		fmt.Printf("start: worker=%q\n", worker)

		return f[worker](ctx)
	}
}

func (t *testManagerWorkers) workerDistributeOpsFunc(wbs map[string]*workerBroker) managerWorkerOperationsFunc {
	return func(_ context.Context, worker string, f func(managerWorkerOperationFunc) error) error {
		fmt.Printf("distribute: worker=%q\n", worker)

		return wbs[worker].localInsert(func(of operationsWorkerLocalInsertOpFunc) error {
			return f(func(opindex uint64, oph, fact util.Hash) error {
				fmt.Printf("distribute op: worker=%q opindex=%d\n", worker, opindex)

				return of(opindex, oph, fact)
			})
		})
	}
}

func (t *testManagerWorkers) workerArgs(local string) (
	*phaseMergerArgs,
	*initialValuesWorkerMergerArgs,
	*statesWorkerMergerArgs,
	*operationsWorkerMergerArgs,
	*workerSaveArgs,
	util.LockedMap[uint64, []StateMergeValue],
	util.LockedMap[string, StateMerger],
	util.LockedMap[string, base.State],
) {
	t.T().Helper()

	valuesPool, err := util.NewShardedMap[uint64, []StateMergeValue](1<<9, nil)
	t.NoError(err)

	mergersPool, err := util.NewShardedMap[string, StateMerger](1<<9, nil)
	t.NoError(err)

	statesPool, err := util.NewShardedMap[string, base.State](1<<9, nil)
	t.NoError(err)

	pargs := newPhaseMergerArgs()

	iargs := newInitialValuesWorkerMergerArgs()
	iargs.NewValuesPool = func() (util.LockedMap[uint64, []StateMergeValue], error) {
		return valuesPool, nil
	}

	sargs := newStatesWorkerMergerArgs()
	oargs := newOperationsWorkerMergerArgs()
	args := newWorkerSaveArgs()

	return pargs, iargs, sargs, oargs, args, valuesPool, mergersPool, statesPool
}

type managerBroker struct {
	distributeOps    managerWorkersOperationsFunc
	workerUnmerge    managerWorkerUnmergedFunc
	workerStates     managerWorkerStatesFunc
	manifest         managerACCEPVoteproofFunc
	workerBlockFiles managerWorkerBlockFileReceiveFunc
}

type workerBroker struct {
	localInsert     operationsWorkerLocalInsertOpsFuncs
	workerInsert    operationsWorkerInitialInsertOpsFuncs
	workerPhaseDone workerMergerPhaseDoneFunc
	mergeWait       func() error
	saveWait        func() error
}

func (t *testManagerWorkers) broadcastStateMergeValuesFunc(
	local string,
	workers []string,
	wbs map[string]*workerBroker,
	stateOwners stateMergeValueOwnerFunc,
) operationsWorkerMergerBroadcastStateMergeValueFuncs {
	others := util.FilterSlice(workers, func(s string) bool {
		return s != local
	})

	valuesOwners := newStateMergeValueOwners(local, stateOwners)

	return func(ctx context.Context) (
		operationsWorkerInitialInsertOpFunc,
		func() error,
		error,
	) {
		broadcast, broadcastDone, err := newBroadcastStateMergeValues(
			ctx,
			others,
			func(ctx context.Context, worker string) (
				operationsWorkerInitialInsertOpFunc,
				func() error,
				error,
			) {
				bs := wbs[worker]
				if bs == nil {
					return nil, nil, util.ErrNotFound.Errorf("worker, %q", worker)
				}

				return bs.workerInsert(local)
			},
			func(worker string, err error) error {
				if err != nil {
					fmt.Sprintf("broadcast error: local=%q worker=%q err=%+v", local, worker, err)
				}

				return nil // NOTE ignore
			},
		)
		if err != nil {
			return nil, nil, err
		}

		return func(opindex uint64, oph util.Hash, values []StateMergeValue) error {
				return valuesOwners(values, func(worker string, values []StateMergeValue) error {
					return broadcast(worker, opindex, oph, values)
				})
			},
			func() error {
				switch err := broadcastDone(); {
				case errors.Is(err, context.Canceled),
					errors.Is(err, util.ErrJobWorkerDone):
					return nil
				default:
					return err
				}
			},
			nil
	}
}

func (t *testManagerWorkers) makeManager(
	ctx context.Context,
	workers []string,
	workID string,
	startWorkers func(context.Context, string) error,
	opsShare, statesShare uint64,
	pr base.ProposalSignFact,
	ivp base.INITVoteproof,
	mb *managerBroker,
) error {
	args, wtargs, sargs := t.newArgs()

	f, err := newFuncManager(args, wtargs, sargs)
	if err != nil {
		return err
	}

	p, err := f(ctx, workers, opsShare, statesShare, workID, startWorkers)
	if err != nil {
		return err
	}

	distributeOps, workerUnmerge, workerStates, saveTree, workerBlockFiles, err := p(pr, ivp)
	if err != nil {
		return err
	}

	mb.distributeOps = distributeOps
	mb.workerUnmerge = workerUnmerge
	mb.workerStates = workerStates
	mb.manifest = saveTree
	mb.workerBlockFiles = workerBlockFiles

	return nil
}

func (t *testManagerWorkers) makeWorker(
	ctx context.Context,
	local string,
	workers []string,
	share uint64,
	maxOps uint64,
	mb *managerBroker,
	wbs map[string]*workerBroker,
	workID string,
	height base.Height,
	initialStates []base.State,
	ops []Operation,
) error {
	nworkers := uint64(len(workers))
	if share > nworkers {
		share = nworkers
	}

	minEnough := nworkers - share + 1

	log := func(a ...interface{}) {
		b := make([]interface{}, len(a)+1)
		b[0] = "worker: " + local + ":"
		copy(b[1:], a)
		fmt.Println(b...)
	}

	pargs, iargs, sargs, oargs, args, valuesPool, mergersPool, statesPool := t.workerArgs(local)
	_, _ = valuesPool, mergersPool // FIXME remove

	util.TraverseSliceV1(initialStates, func(_ int, st base.State) (bool, error) {
		statesPool.SetValue(st.Key(), st)

		return true, nil
	})

	stateOwners, err := stateOwnersFuncDistributeByCount(workers, share)
	if err != nil {
		return err
	}

	f := newFuncWorkerSave(args, iargs, sargs, oargs, pargs)

	wb := new(workerBroker)

	localInsert, workerInsert, workerPhaseDone, mergeWait, saveWait, err := f(
		ctx,
		nworkers, minEnough, maxOps,
		func(context.Context, []uint64) error { return nil },
		newGetStateFunc(statesPool),
		func(stateKey string) bool {
			return slices.Index(stateOwners(stateKey), local) >= 0
		},
		t.broadcastStateMergeValuesFunc(local, workers, wbs, stateOwners),
		newGetOperationFunc(ops),
		func() (newStateFunc, error) {
			return func(stateKey string, value base.StateValue, ophs []util.Hash) (base.State, error) {
				return base.NewBaseState(height, stateKey, value, nil, ophs), nil
			}, nil
		},
		func(_ context.Context, f workerSaveMergeDoneFunc) error {
			log("send merge done")

			return f(
				func(sf workerSaveMergeStatesFuncs) error {
					return mb.workerStates(
						ctx,
						localWorkerName,
						func(msf func(stateKey string) error) error {
							return sf(func(stateKey string, _ util.Hash) error {
								return msf(stateKey)
							})
						},
					)
				},
				func(unmerged []uint64) error {
					log("unmerged:", unmerged)

					_, err := mb.workerUnmerge(localWorkerName, unmerged)

					return err
				},
			)
		},
		func(ctx context.Context, f func(operationsWorkerSaveOperationsFunc) error) (bc []blockFileChunk, _ error) {
			w, _ := t.newCreateWriter()
			bw := t.newOperationsWriter(w)

			return bc, f(func(total uint64) operationsWorkerSaveOperationsCallbackFunc {
				return func(of func(operationsWorkerSaveOperationFunc) error) error {
					i, err := bw(ctx, workID, total, of)

					bc = i

					log("save operation:", extractChecksumsFromBlockFileChunks(bc))

					return err
				}
			})
		},

		func(ctx context.Context, f func(workerSaveStatesFunc) error) (bc []blockFileChunk, _ error) {
			w, _ := t.newCreateWriter()
			bw := t.newStatesWriter(w)

			return bc, f(func(total uint64) workerSaveStatesCallbackFunc {
				return func(of func(workerSaveStateFunc) error) error {
					i, err := bw(ctx, workID, total, of)

					bc = i

					log("save states:", bc)

					return err
				}
			})
		},
		func(reasons map[uint64]string) error {
			log("save reasons:", reasons)

			return nil
		},
		func(ctx context.Context, f func(workerSaveBlockFileChunksFunc) error) error {
			r, d, err := mb.workerBlockFiles(localWorkerName)
			if err != nil {
				return err
			}

			err = f(func(it base.BlockItemType, bs []blockFileChunk) error {
				return r(it, extractChecksumsFromBlockFileChunks(bs))
			})

			return d(err)
		},
	)
	if err != nil {
		return err
	}

	wb.localInsert, wb.workerInsert, wb.workerPhaseDone, wb.mergeWait, wb.saveWait = localInsert, workerInsert, workerPhaseDone, mergeWait, saveWait

	wbs[local] = wb

	return nil
}

func (t *testManagerWorkers) managerManifest(
	pr base.ProposalSignFact,
	f managerACCEPVoteproofFunc,
	callback func(base.Manifest) error,
) managerACCEPVoteproofFunc {
	return func(m managerManifestFunc) (managerSaveWithACCEPTVoteproofFunc, error) {
		return f(func(opsTree, statesTree fixedtree.Tree) (base.Manifest, error) {
			fmt.Printf("trees: ops=%q states=%q\n", opsTree.Root(), statesTree.Root())

			switch i, err := m(opsTree, statesTree); {
			case err != nil:
				return nil, err
			case i != nil:
				return i, nil
			default:
				dm := t.newManifest()
				dm.SetHeight(pr.Point().Height())
				dm.SetProposal(pr.Fact().Hash())
				dm.SetProposedAt(pr.ProposalFact().ProposedAt())

				dm.SetOperationsTree(opsTree.Root())
				dm.SetStatesTree(statesTree.Root())

				return dm, callback(dm)
			}
		})
	}
}

func (t *testManagerWorkers) managerSaveAVP(
	avp base.ACCEPTVoteproof,
	f managerSaveWithACCEPTVoteproofFunc,
) (BlockMap, error) {
	fmt.Println("manager: new blockmap:")

	pw, _ := t.newCreateWriter()
	vw, _ := t.newCreateWriter()
	ow, _ := t.newCreateWriter()
	sw, _ := t.newCreateWriter()
	mw, _ := t.newCreateWriter()

	bm, err := f(
		avp,
		t.newProposalWriter(pw),
		t.newVoteproofsWriter(vw),
		t.newOperationsTreeWriter(ow),
		t.newStatesTreeWriter(sw),
		t.newMapWriter(mw),
	)
	if err != nil {
		fmt.Printf("manager: blockmap: err=%+v\n", err)
	}

	return bm, err
}

func (t *testManagerWorkers) waitWorkers(wbs map[string]*workerBroker) map[string]error {
	werrs := map[string]error{}

	var workerWaitWG sync.WaitGroup

	TraverseMap(wbs, func(worker string, wb *workerBroker) (bool, error) {
		workerWaitWG.Add(1)

		go func() {
			defer workerWaitWG.Done()

			if err := wb.mergeWait(); err != nil {
				werrs[worker] = err

				return
			}

			if err := wb.saveWait(); err != nil {
				werrs[worker] = err

				return
			}
		}()

		return true, nil
	})

	workerWaitWG.Wait()

	return werrs
}

func (t *testManagerWorkers) Test1Workers() {
	workers := []string{localWorkerName}
	workID := util.UUID().String()
	ops, pr, ivp, avp, expectedOpsTree, expectedStatesTree, expectedManifest, preStates := t.baseItems(33)

	mb := new(managerBroker)
	wbs := map[string]*workerBroker{}
	workerStartWorkerFuncs := map[string]func(context.Context) error{}

	// worker
	{
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		workerStartWorkerFuncs[localWorkerName] = func(context.Context) error {
			return t.makeWorker(
				ctx,
				localWorkerName,
				workers,
				1,
				1<<9,
				mb,
				wbs,
				workID,
				pr.Point().Height(),
				preStates,
				ops,
			)
		}
	}

	// manager
	{
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		t.NoError(t.makeManager(
			ctx,
			workers,
			workID,
			t.workerStartWorkerFunc(workerStartWorkerFuncs),
			1, 1,
			pr,
			ivp,
			mb,
		))
	}

	go ifErrorPanic(mb.distributeOps(t.workerDistributeOpsFunc(wbs)))

	tch := make(chan [2]fixedtree.Tree, 1)
	mch := make(chan base.Manifest, 1)
	bmch := make(chan interface{}, 1)

	go func() {
		saveAVP, err := t.managerManifest(
			pr,
			mb.manifest,
			func(m base.Manifest) error {
				mch <- m

				return nil
			},
		)(func(opsTree, statesTree fixedtree.Tree) (base.Manifest, error) {
			tch <- [2]fixedtree.Tree{opsTree, statesTree}

			return nil, nil
		})
		fmt.Printf("manager: manifest: err=%+v\n", err)
		if err != nil {
			bmch <- err

			return
		}

		bm, err := t.managerSaveAVP(
			avp,
			saveAVP,
		)
		if err != nil {
			bmch <- err

			return
		}

		bmch <- bm
	}()

	if werrs := t.waitWorkers(wbs); len(werrs) > 0 {
		TraverseMap(werrs, func(worker string, err error) (bool, error) {
			fmt.Printf("worker=%q err=%+v\n", worker, err)

			return true, nil
		})

		t.Fail("workers wait failed")
	}

	select {
	case <-time.After(time.Second * 2):
		t.Fail("wait trees")
	case ts := <-tch:
		t.T().Log("manager: trees:", t.Marshal(ts))

		t.True(ts[0].Root().Equal(expectedOpsTree.Root()))
		t.True(ts[1].Root().Equal(expectedStatesTree.Root()), "%q != %q", ts[1].Root(), expectedStatesTree.Root())
	}

	select {
	case <-time.After(time.Second * 2):
		t.Fail("wait manifest")
	case m := <-mch:
		t.T().Log("manager: manifest:", t.Marshal(m))

		base.IsEqualManifest(expectedManifest, m)
	}

	select {
	case <-time.After(time.Second * 2):
		t.Fail("wait blockmap")
	case i := <-bmch:
		switch j := i.(type) {
		case error:
			t.NoError(j)
		default:
			t.T().Log("manager: blockmap:", t.Marshal(j))
		}
	}
}

func TestManagerWorkers(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testManagerWorkers))
}

func extractChecksumsFromBlockFileChunks(bs []blockFileChunk) (checksums []string) {
	checksums = make([]string, len(bs))

	for i := range bs {
		checksums[i] = bs[i].Checksum()
	}

	return
}

func stateOwnersFuncDistributeByCount(workers []string, share uint64) (stateMergeValueOwnerFunc, error) {
	var f func(k string) []uint64

	switch i, _, err := distributeByCount(uint64(len(workers)), share); {
	case err != nil:
		return nil, err
	default:
		f = i
	}

	return func(stateKey string) (owners []string) {
			p := f(stateKey)

			owners = make([]string, len(p))
			for i := range p {
				owners[i] = workers[p[i]]
			}

			return owners
		},
		nil
}
