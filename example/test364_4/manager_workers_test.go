package main

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

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

func (t *testManagerWorkers) workerDistributeOpsFunc(d map[string]operationsWorkerLocalInsertOpsFuncs) managerWorkerOperationsFunc {
	return func(_ context.Context, worker string, f func(managerWorkerOperationFunc) error) error {
		fmt.Printf("distribute: worker=%q\n", worker)

		return d[worker](func(of operationsWorkerLocalInsertOpFunc) error {
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

func (t *testManagerWorkers) Test1Workers() {
	workers := []string{localWorkerName}
	workID := util.UUID().String()
	ops, pr, ivp, avp, expectedOpsTree, expectedStatesTree, expectedManifest := t.baseItems(33)

	var sts []base.State
	util.TraverseSliceV1(ops, func(_ int, op Operation) (bool, error) {
		fact := op.Fact().(OpFact)
		sts = append(sts, t.newState(fact.Issuer, fact.Amount))

		return true, nil
	})

	workerStartWorkerFuncs := map[string]func(context.Context) error{}
	workerWaitFuncs := map[string]func() error{}

	type managerBroker struct {
		distributeOps    managerWorkersOperationsFunc
		workerUnmerge    managerWorkerUnmergedFunc
		workerStates     managerWorkerStatesFunc
		saveTree         managerACCEPVoteproofFunc
		workerBlockFiles managerWorkerBlockFileReceiveFunc
	}

	mb := new(managerBroker)

	// workers
	workerDistributeOpsFuncs := map[string]operationsWorkerLocalInsertOpsFuncs{}

	{
		pargs, iargs, sargs, oargs, args, valuesPool, mergersPool, statesPool := t.workerArgs(localWorkerName)
		_, _, _ = valuesPool, mergersPool, statesPool

		util.TraverseSliceV1(sts, func(_ int, st base.State) (bool, error) {
			statesPool.SetValue(st.Key(), st)

			return true, nil
		})

		ctx, cancel := context.WithCancel(context.Background())
		defer func() {
			cancel()
			fmt.Println("worker canceled")
		}()

		workerStartWorkerFuncs[localWorkerName] = func(context.Context) error {
			f := newFuncWorkerSave(args, iargs, sargs, oargs, pargs)

			insert, _, _, mergeWait, saveWait, err := f(
				ctx,
				1,
				1<<9,
				func(context.Context, []uint64) error { return nil },
				newGetStateFunc(statesPool),
				func(string) bool { return true },
				func(context.Context) (operationsworkerInitialInsertOpFunc, func() error, error) {
					// ignore: one worker
					return func(uint64, util.Hash, []StateMergeValue) error { return nil },
						func() error { return nil },
						nil
				},
				newGetOperationFunc(ops),
				func() (newStateFunc, error) {
					return func(stateKey string, value base.StateValue, ophs []util.Hash) (base.State, error) {
						return base.NewBaseState(pr.Point().Height(), stateKey, value, nil, ophs), nil
					}, nil
				},
				func(_ context.Context, f workerSaveMergeDoneFunc) error {
					fmt.Println("worker: send merge done")

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
							fmt.Println("worker: unmerged:", unmerged)

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

							fmt.Println("worker: save operation:", extractChecksumsFromBlockFileChunks(bc))

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

							fmt.Println("worker: save states:", bc)

							return err
						}
					})
				},
				func(reasons map[uint64]string) error {
					fmt.Println("worker: save reasons:", reasons)

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

			workerDistributeOpsFuncs[localWorkerName] = insert
			workerWaitFuncs[localWorkerName] = func() error {
				if err := mergeWait(); err != nil {
					return err
				}

				return saveWait()
			}

			return err
		}
	}

	// manager
	{
		args, wtargs, sargs := t.newArgs()

		f, err := newFuncManager(args, wtargs, sargs)
		t.NoError(err)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		p, err := f(ctx, workers, 1, 1, workID, t.workerStartWorkerFunc(workerStartWorkerFuncs))
		t.NoError(err)

		distributeOps, workerUnmerge, workerStates, saveTree, workerBlockFiles, err := p(pr, ivp)
		t.NoError(err)

		mb.distributeOps = distributeOps
		mb.workerUnmerge = workerUnmerge
		mb.workerStates = workerStates
		mb.saveTree = saveTree
		mb.workerBlockFiles = workerBlockFiles
	}

	go ifErrorPanic(mb.distributeOps(t.workerDistributeOpsFunc(workerDistributeOpsFuncs)))

	tch := make(chan [2]fixedtree.Tree, 1)
	mch := make(chan base.Manifest, 1)
	bmch := make(chan interface{}, 1)

	go func() {
		saveAVP, err := mb.saveTree(func(opsTree, statesTree fixedtree.Tree) (base.Manifest, error) {
			fmt.Printf("trees: ops=%q states=%q\n", opsTree.Root(), statesTree.Root())

			tch <- [2]fixedtree.Tree{opsTree, statesTree}

			dm := t.newManifest()
			dm.SetHeight(pr.Point().Height())
			dm.SetProposal(pr.Fact().Hash())
			dm.SetProposedAt(pr.ProposalFact().ProposedAt())

			dm.SetOperationsTree(opsTree.Root())
			dm.SetStatesTree(statesTree.Root())

			mch <- dm

			return dm, nil
		})
		fmt.Printf("manager: manifest: err=%+v\n", err)
		if err != nil {
			bmch <- err

			return
		}

		fmt.Println("manager: new blockmap:")
		pw, _ := t.newCreateWriter()
		vw, _ := t.newCreateWriter()
		ow, _ := t.newCreateWriter()
		sw, _ := t.newCreateWriter()
		mw, _ := t.newCreateWriter()

		bm, err := saveAVP(
			avp,
			t.newProposalWriter(pw),
			t.newVoteproofsWriter(vw),
			t.newOperationsTreeWriter(ow),
			t.newStatesTreeWriter(sw),
			t.newMapWriter(mw),
		)
		fmt.Printf("manager: blockmap: err=%+v\n", err)
		if err != nil {
			bmch <- err

			return
		}

		bmch <- bm
	}()

	var workerWaitWG sync.WaitGroup

	TraverseMap(workerWaitFuncs, func(worker string, f func() error) (bool, error) {
		workerWaitWG.Add(1)

		go func() {
			defer workerWaitWG.Done()

			if err := f(); err != nil {
				fmt.Println("worker: wait:", err)
				panic(err)
			}
		}()

		return true, nil
	})

	workerWaitWG.Wait()

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
