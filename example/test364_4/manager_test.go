package main

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/fixedtree"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type baseTestManager struct {
	isaac.BaseTestBallots
	baseTestBlock
	enc encoder.Encoder
}

func (t *baseTestManager) SetupSuite() {
	t.enc = jsonenc.NewEncoder()
}

func (t *baseTestManager) workers(n int) []string {
	w := make([]string, n)

	for i := 0; i < n; i++ {
		w[i] = fmt.Sprintf("w%d", i)
	}

	return w
}

func (t *baseTestManager) ophs(n int) [][2]util.Hash {
	ophs := make([][2]util.Hash, n)

	for i := 0; i < n; i++ {
		ophs[i] = [2]util.Hash{valuehash.RandomSHA256(), valuehash.RandomSHA256()}
	}

	return ophs
}

func (t *baseTestManager) stateKeys(n int) []string {
	sts := make([]string, n)

	for i := 0; i < n; i++ {
		sts[i] = util.ULID().String()
	}

	return sts
}

func (t *baseTestManager) stateKeysUnordered(n int) []string {
	sts := make([]string, n)

	for i := 0; i < n; i++ {
		sts[i] = util.UUID().String()
	}

	return sts
}

func (t *baseTestManager) newCreateWriter() (
	blockFileWriterCreateWriterFunc,
	func() ([]byte, error),
) {
	w := bytes.NewBuffer(nil)

	return func(base.BlockItemType, string) (string, ioWriteFunc, blockFileDoneFileFunc, error) {
			return "", w.Write, func(err error, _ string) (string, error) { return util.UUID().String(), err }, nil
		},
		func() ([]byte, error) {
			defer w.Reset()

			return w.Bytes(), nil
		}
}

func (t *baseTestManager) newProposalWriter(createWriter blockFileWriterCreateWriterFunc) blockFileWriterProposalFunc {
	return newFuncBlockFileProposalWriter(t.enc, createWriter)
}

func (t *baseTestManager) newVoteproofsWriter(createWriter blockFileWriterCreateWriterFunc) blockFileWriterVoteproofsFunc {
	return newFuncBlockFileVoteproofsWriter(t.enc, createWriter)
}

func (t *baseTestManager) newOperationsWriter(createWriter blockFileWriterCreateWriterFunc) blockFileWriterOperationsFunc {
	return newFuncBlockFileOperationsWriter(t.enc, createWriter, nil, nil)
}

func (t *baseTestManager) newStatesWriter(createWriter blockFileWriterCreateWriterFunc) blockFileWriterStatesFunc {
	return newFuncBlockFileStatesWriter(t.enc, createWriter, nil, nil)
}

func (t *baseTestManager) newMapWriter(createWriter blockFileWriterCreateWriterFunc) blockFileWriterMapFunc {
	return newFuncBlockFileMapWriter(t.enc, createWriter)
}

func (t *baseTestManager) newOperationsTreeWriter(createWriter blockFileWriterCreateWriterFunc) blockFileWriterTreeFunc {
	return newFuncBlockFileOperationsTreeWriter(t.enc, createWriter, nil, nil)
}

func (t *baseTestManager) newStatesTreeWriter(createWriter blockFileWriterCreateWriterFunc) blockFileWriterTreeFunc {
	return newFuncBlockFileStatesTreeWriter(t.enc, createWriter, nil, nil)
}

func (t *baseTestManager) newProposal(n int) base.ProposalSignFact {
	ophs := make([][2]util.Hash, n)

	for i := range ophs {
		ophs[i] = [2]util.Hash{valuehash.RandomSHA256(), valuehash.RandomSHA256()}
	}

	fact := t.NewProposalFact(base.RawPoint(33, 44), t.Local, ophs)

	return t.NewProposal(t.Local, fact)
}

func (t *baseTestManager) newINITVoteproof() base.INITVoteproof {
	fact := t.NewINITBallotFact(base.RawPoint(33, 0), valuehash.RandomSHA256(), valuehash.RandomSHA256())
	vp, err := t.NewINITVoteproof(fact, t.Local, []base.LocalNode{t.Local})
	t.NoError(err)

	return vp
}

func (t *baseTestManager) newACCEPTVoteproof() base.ACCEPTVoteproof {
	fact := t.NewACCEPTBallotFact(base.RawPoint(33, 0), valuehash.RandomSHA256(), valuehash.RandomSHA256())

	vp, err := t.NewACCEPTVoteproof(fact, t.Local, []base.LocalNode{t.Local})
	t.NoError(err)

	return vp
}

func (t *baseTestManager) newOperationsTree() fixedtree.Tree {
	ops := make([]util.Hash, 33)
	for i := range ops {
		ops[i] = valuehash.RandomSHA256()
	}

	return t.newOperationsTreeWithOps(ops)
}

func (t *baseTestManager) newOperationsTreeWithOps(ops []util.Hash) fixedtree.Tree {
	tw, err := fixedtree.NewWriter(base.OperationFixedtreeHint, uint64(len(ops)))
	if err != nil {
		panic(err)
	}

	for i := 0; i < tw.Len(); i++ {
		t.NoError(tw.Add(uint64(i), base.NewInStateOperationFixedtreeNode(ops[i], "")))
	}

	tr, err := tw.Tree()
	t.NoError(err)

	return tr
}

func (t *baseTestManager) newStatesTree(n uint64) fixedtree.Tree {
	tw, err := fixedtree.NewWriter(base.StateFixedtreeHint, n)
	if err != nil {
		panic(err)
	}

	for i := 0; i < tw.Len(); i++ {
		t.NoError(tw.Add(uint64(i), fixedtree.NewBaseNode(util.ULID().String())))
	}

	tr, err := tw.Tree()
	t.NoError(err)

	return tr
}

func (t *baseTestManager) checksums(n int) []string {
	c := make([]string, n)
	for i := range c {
		c[i] = util.ULID().String()
	}

	return c
}

func (t *baseTestManager) newCheckWorkersFunc() checkEnoughFailedWorkersFuncs {
	return func(minEnough, maxFailed uint64) (
		checkEnoughFailedWorkerFunc,
		func(),
		error,
	) {
		return newCheckEnoughFailedWorkers(minEnough, maxFailed)
	}
}

func (t *baseTestManager) baseItems(nops int) (
	pr base.ProposalSignFact,
	ivp base.INITVoteproof,
	avp base.ACCEPTVoteproof,
	opsTree, statesTree fixedtree.Tree,
	manifest base.Manifest,
) {
	pr = t.newProposal(nops)
	ivp = t.newINITVoteproof()
	avp = t.newACCEPTVoteproof()

	ophs := make([]util.Hash, len(pr.ProposalFact().Operations()))
	util.TraverseSliceV1(pr.ProposalFact().Operations(), func(i int, h [2]util.Hash) (bool, error) {
		ophs[i] = h[1]

		return true, nil
	})

	opsTree = t.newOperationsTreeWithOps(ophs)
	statesTree = t.newStatesTree(33)

	dm := t.newManifest()
	dm.SetHeight(pr.Point().Height())
	dm.SetProposal(pr.Fact().Hash())
	dm.SetProposedAt(pr.ProposalFact().ProposedAt())

	dm.SetOperationsTree(opsTree.Root())
	dm.SetStatesTree(statesTree.Root())

	manifest = dm

	return
}

type testManager struct {
	baseTestManager
}

func (t *testManager) newProposalWriter() blockFileWriterProposalFunc {
	f, _ := t.newCreateWriter()

	return t.baseTestManager.newProposalWriter(f)
}

func (t *testManager) newVoteproofsWriter() blockFileWriterVoteproofsFunc {
	f, _ := t.newCreateWriter()

	return t.baseTestManager.newVoteproofsWriter(f)
}

func (t *testManager) newOperationsWriter() blockFileWriterOperationsFunc {
	f, _ := t.newCreateWriter()

	return t.baseTestManager.newOperationsWriter(f)
}

func (t *testManager) newStatesWriter() blockFileWriterStatesFunc {
	f, _ := t.newCreateWriter()

	return t.baseTestManager.newStatesWriter(f)
}

func (t *testManager) newMapWriter() blockFileWriterMapFunc {
	f, _ := t.newCreateWriter()

	return t.baseTestManager.newMapWriter(f)
}

func (t *testManager) newOperationsTreeWriter() blockFileWriterTreeFunc {
	f, _ := t.newCreateWriter()

	return t.baseTestManager.newOperationsTreeWriter(f)
}

func (t *testManager) newStatesTreeWriter() blockFileWriterTreeFunc {
	f, _ := t.newCreateWriter()

	return t.baseTestManager.newStatesTreeWriter(f)
}

func (t *testManager) newArgs() (
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

func (t *testManager) newFunc() managerStartFunc {
	args, wtargs, sargs := t.newArgs()

	f, err := newFuncManager(args, wtargs, sargs)
	t.NoError(err)

	return f
}

func (t *testManager) TestError() {
	f := t.newFunc()

	t.Run("empty work id", func() {
		_, err := f(context.Background(), nil, 1, 1, "", nil)
		t.ErrorContains(err, "empty work id")
	})

	t.Run("empty workers", func() {
		_, err := f(context.Background(), nil, 1, 1, util.UUID().String(), nil)
		t.ErrorContains(err, "empty workers")
	})

	t.Run("zero share", func() {
		_, err := f(context.Background(), t.workers(3), 0, 1, util.UUID().String(), nil)
		t.ErrorContains(err, "zero share")

		_, err = f(context.Background(), t.workers(3), 1, 0, util.UUID().String(), nil)
		t.ErrorContains(err, "zero share")
	})
}

func (t *testManager) TestStart() {
	t.Run("worker failed; over max failed", func() {
		workers := t.workers(3)

		f := t.newFunc()
		_, err := f(context.Background(), workers, 1, 1, util.UUID().String(), func(_ context.Context, worker string) error {
			if worker == workers[2] {
				fmt.Printf("start: worker=%q error\n", worker)

				return errors.Errorf("findme")
			}

			fmt.Printf("start: worker=%q\n", worker)

			return nil
		})
		t.ErrorIs(err, errCheckWorkersFailed)
		t.ErrorContains(err, "findme")
	})

	t.Run("worker failed, but under max failed", func() {
		workers := t.workers(3)

		f := t.newFunc()
		_, err := f(context.Background(), workers, 2, 2, util.UUID().String(), func(_ context.Context, worker string) error {
			if worker == workers[2] {
				fmt.Printf("start: worker=%q error\n", worker)

				return errors.Errorf("findme")
			}

			fmt.Printf("start: worker=%q\n", worker)

			return nil
		})
		t.NoError(err)
	})
}

func (t *testManager) baseItems() (
	pr base.ProposalSignFact,
	ivp base.INITVoteproof,
	avp base.ACCEPTVoteproof,
	opsTree, statesTree fixedtree.Tree,
	manifest base.Manifest,
) {
	return t.baseTestManager.baseItems(3)
}

func (t *testManager) blockMapItems(
	workID string,
	nworkers int,
	pr base.ProposalSignFact,
	ivp base.INITVoteproof,
	avp base.ACCEPTVoteproof,
	opsTree, statesTree fixedtree.Tree,
) (
	checksums map[base.BlockItemType][]string,
	blockMapItemProposal,
	blockMapItemOperations,
	blockMapItemStates,
	blockMapItemVoteproofs,
	blockMapItemOpsTree,
	blockMapItemStatesTree BlockMapItem,
) {
	checksums = map[base.BlockItemType][]string{}
	checksums[base.BlockItemOperations] = t.checksums(nworkers * 3)
	checksums[base.BlockItemStates] = t.checksums(nworkers * 3)

	blockMapItemOperations = newBaseBlockMapItem(base.BlockItemOperations, baseBlockMapItemChecksum(checksums[base.BlockItemOperations]), true)
	blockMapItemStates = newBaseBlockMapItem(base.BlockItemStates, baseBlockMapItemChecksum(checksums[base.BlockItemStates]), true)

	{
		bi, err := t.newProposalWriter()(context.Background(), workID, pr)
		t.NoError(err)
		blockMapItemProposal = bi
	}

	{
		sivp, savp, err := t.newVoteproofsWriter()(context.Background(), workID)
		t.NoError(err)
		t.NoError(sivp(ivp))
		bi, err := savp(avp)
		t.NoError(err)

		blockMapItemVoteproofs = bi
	}

	{
		chunks, err := t.newOperationsTreeWriter()(context.Background(), workID, opsTree)
		t.NoError(err)
		blockMapItemOpsTree = newBlockMapItemFromChunks(base.BlockItemOperationsTree, chunks)
	}

	{
		chunks, err := t.newStatesTreeWriter()(context.Background(), workID, statesTree)
		t.NoError(err)
		blockMapItemStatesTree = newBlockMapItemFromChunks(base.BlockItemStatesTree, chunks)
	}

	return
}

func (t *testManager) defaultStartWorker() func(context.Context, string) error {
	return func(_ context.Context, worker string) error {
		fmt.Printf("start: worker=%q\n", worker)

		return nil
	}
}

func (t *testManager) defaultDistributeOps() managerWorkerOperationsFunc {
	return func(_ context.Context, worker string, f func(managerWorkerOperationFunc) error) error {
		fmt.Printf("distribute: worker=%q\n", worker)

		return f(func(opindex uint64, oph, _ util.Hash) error {
			fmt.Printf("distribute op: worker=%q opindex=%d oph=%v\n", worker, opindex, oph)

			return nil
		})
	}
}

func (t *testManager) defaultWorkersUnmerge(workers []string, unmerge managerWorkerUnmergedFunc) error {
	return util.TraverseSliceV1(workers, func(_ int, worker string) (bool, error) {
		fmt.Printf("worker unmerge: worker=%q\n", worker)

		return unmerge(worker, nil)
	})
}

func (t *testManager) defaultWorkersStates(
	workers []string,
	statesTree fixedtree.Tree,
	w managerWorkerStatesFunc,
) error {
	nodes := statesTree.Nodes()

	return util.TraverseSliceV1(workers, func(_ int, worker string) (bool, error) {
		fmt.Printf("worker states: worker=%q\n", worker)

		d := statesTree.Len() / len(workers)
		s := slices.Index(workers, worker) * d

		return true, w(context.Background(), worker, func(f func(string) error) error {
			return util.TraverseSliceV1(nodes[s:s+d], func(_ int, n fixedtree.Node) (bool, error) {
				return true, f(n.Key())
			})
		})
	})
}

func (t *testManager) defaultWorkersBlockFiles(
	workers []string,
	checksums map[base.BlockItemType][]string,
	w managerWorkerBlockFileReceiveFunc,
) error {
	return util.TraverseSliceV1(workers, func(_ int, worker string) (bool, error) {
		return true, w(context.Background(), worker, func(bf managerWorkerBlockFileReceiveChecksumsFunc) error {
			fmt.Printf("worker block files: worker=%q\n", worker)

			for it := range checksums {
				cs := checksums[it]
				d := len(cs) / len(workers)
				s := slices.Index(workers, worker) * d

				if err := bf(it, cs[s:s+d]); err != nil {
					return err
				}
			}

			return nil
		})
	})
}

func (t *testManager) TestManifestAndSave() {
	t.Run("ok: 3 workers", func() {
		workID := util.UUID().String()
		workers := t.workers(3)

		pr, ivp, avp, expectedOpsTree, expectedStatesTree, expectedManifest := t.baseItems()

		expectedChecksums, expectedBlockItemProposal, expectedBlockItemOperations, expectedBlockItemStates, expectedBlockItemVoteproofs, expectedBlockItemOpsTree, expectedBlockItemStatesTree := t.blockMapItems(workID, len(workers), pr, ivp, avp, expectedOpsTree, expectedStatesTree)

		f := t.newFunc()

		p, err := f(context.Background(), workers, 1, 1, workID, t.defaultStartWorker())
		t.NoError(err)

		distributeOps, workerUnmerge, workerStates, mf, workerBlockFiles, err := p(pr, ivp)
		t.NoError(err)

		go ifErrorPanic(distributeOps(t.defaultDistributeOps()))
		go ifErrorPanic(t.defaultWorkersUnmerge(workers, workerUnmerge))
		go ifErrorPanic(t.defaultWorkersStates(workers, expectedStatesTree, workerStates))
		go ifErrorPanic(t.defaultWorkersBlockFiles(workers, expectedChecksums, workerBlockFiles))

		save, err := mf(func(opsTree, statesTree fixedtree.Tree) (base.Manifest, error) {
			fmt.Printf("trees: ops=%v states=%v\n", opsTree.Root(), statesTree.Root())

			t.Equal(expectedOpsTree.Root(), opsTree.Root())
			t.Equal(expectedStatesTree.Root(), statesTree.Root())

			m := t.newManifest()
			m.SetHash(expectedManifest.Hash())
			m.SetHeight(pr.Point().Height())
			m.SetProposal(pr.Fact().Hash())
			m.SetProposedAt(pr.ProposalFact().ProposedAt())

			m.SetOperationsTree(opsTree.Root())
			m.SetStatesTree(statesTree.Root())

			base.IsEqualManifest(expectedManifest, m)

			return m, nil
		})
		t.NoError(err)

		bm, err := save(
			avp,
			t.newProposalWriter(),
			t.newVoteproofsWriter(),
			t.newOperationsTreeWriter(),
			t.newStatesTreeWriter(),
			t.newMapWriter(),
		)
		t.NoError(err)

		base.IsEqualManifest(expectedManifest, bm.Manifest())

		itempr, found := bm.Item(base.BlockItemProposal)
		t.True(found)
		base.IsEqualBlockMapItem(expectedBlockItemProposal, itempr)

		itemvps, found := bm.Item(base.BlockItemVoteproofs)
		t.True(found)
		base.IsEqualBlockMapItem(expectedBlockItemVoteproofs, itemvps)

		itemo, found := bm.Item(base.BlockItemOperations)
		t.True(found)
		base.IsEqualBlockMapItem(expectedBlockItemOperations, itemo)

		items, found := bm.Item(base.BlockItemStates)
		t.True(found)
		base.IsEqualBlockMapItem(expectedBlockItemStates, items)

		itemot, found := bm.Item(base.BlockItemOperationsTree)
		t.True(found)
		base.IsEqualBlockMapItem(expectedBlockItemOpsTree, itemot)

		itemst, found := bm.Item(base.BlockItemStatesTree)
		t.True(found)
		base.IsEqualBlockMapItem(expectedBlockItemStatesTree, itemst)
	})

	t.Run("cancel before trees", func() {
		workID := util.UUID().String()
		workers := t.workers(3)

		pr, ivp, avp, expectedOpsTree, expectedStatesTree, _ := t.baseItems()

		expectedChecksums, _, _, _, _, _, _ := t.blockMapItems(workID, len(workers), pr, ivp, avp, expectedOpsTree, expectedStatesTree)

		f := t.newFunc()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		p, err := f(ctx, workers, 1, 1, workID, t.defaultStartWorker())
		t.NoError(err)

		distributeOps, workerUnmerge, workerStates, mf, workerBlockFiles, err := p(pr, ivp)
		t.NoError(err)

		doneDistribute := make(chan struct{}, 1)
		go func() {
			ifErrorPanic(distributeOps(t.defaultDistributeOps()))

			close(doneDistribute)
		}()

		doneUnmerge := make(chan struct{}, 1)
		go func() {
			ifErrorPanic(t.defaultWorkersUnmerge(workers, workerUnmerge))

			close(doneUnmerge)
		}()

		doneStates := make(chan struct{}, 1)
		go func() {
			ifErrorPanic(t.defaultWorkersStates(workers, expectedStatesTree, workerStates))

			close(doneStates)
		}()

		doneBlockFiles := make(chan struct{}, 1)
		go func() {
			ifErrorPanic(t.defaultWorkersBlockFiles(workers, expectedChecksums, workerBlockFiles))

			close(doneBlockFiles)
		}()

		select {
		case <-time.After(time.Second * 2):
			t.Fail("wait distribute done")
		case <-doneDistribute:
		}

		select {
		case <-time.After(time.Second * 2):
			t.Fail("wait unmerge done")
		case <-doneUnmerge:
		}

		select {
		case <-time.After(time.Second * 2):
			t.Fail("wait states done")
		case <-doneStates:
		}

		select {
		case <-time.After(time.Second * 2):
			t.Fail("wait blockfiles done")
		case <-doneBlockFiles:
		}

		cancel()

		_, err = mf(func(opsTree, statesTree fixedtree.Tree) (base.Manifest, error) {
			fmt.Printf("trees: ops=%v states=%v\n", opsTree.Root(), statesTree.Root())

			return t.newManifest(), nil
		})
		t.ErrorIs(err, context.Canceled)
	})

	t.Run("cancel after trees", func() {
		workID := util.UUID().String()
		workers := t.workers(3)

		pr, ivp, avp, expectedOpsTree, expectedStatesTree, _ := t.baseItems()

		expectedChecksums, _, _, _, _, _, _ := t.blockMapItems(workID, len(workers), pr, ivp, avp, expectedOpsTree, expectedStatesTree)

		f := t.newFunc()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		p, err := f(ctx, workers, 1, 1, workID, t.defaultStartWorker())
		t.NoError(err)

		distributeOps, workerUnmerge, workerStates, mf, workerBlockFiles, err := p(pr, ivp)
		t.NoError(err)

		go ifErrorPanic(distributeOps(t.defaultDistributeOps()))
		go ifErrorPanic(t.defaultWorkersUnmerge(workers, workerUnmerge))
		go ifErrorPanic(t.defaultWorkersStates(workers, expectedStatesTree, workerStates))
		go ifErrorPanic(t.defaultWorkersBlockFiles(workers, expectedChecksums, workerBlockFiles))

		save, err := mf(func(opsTree, statesTree fixedtree.Tree) (base.Manifest, error) {
			fmt.Printf("trees: ops=%v states=%v\n", opsTree.Root(), statesTree.Root())

			cancel()

			return t.newManifest(), nil
		})
		t.NoError(err)

		_, err = save(
			avp,
			t.newProposalWriter(),
			t.newVoteproofsWriter(),
			t.newOperationsTreeWriter(),
			t.newStatesTreeWriter(),
			t.newMapWriter(),
		)
		t.ErrorIs(err, context.Canceled)
	})
}

func (t *testManager) TestWorkersFails() {
	checkError := func(err error, expected ...error) {
		if err == nil {
			return
		}

		var found bool

		for i := range expected {
			if errors.Is(err, expected[i]) {
				found = true
			}
		}

		if !found {
			panic(err)
		}
	}

	t.Run("distribute: over max failed", func() {
		workID := util.UUID().String()
		workers := t.workers(3)

		pr, ivp, avp, expectedOpsTree, expectedStatesTree, _ := t.baseItems()

		expectedChecksums, _, _, _, _, _, _ := t.blockMapItems(workID, len(workers), pr, ivp, avp, expectedOpsTree, expectedStatesTree)

		f := t.newFunc()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		p, err := f(ctx, workers, 1, 1, workID, t.defaultStartWorker())
		t.NoError(err)

		distributeOps, workerUnmerge, workerStates, mf, workerBlockFiles, err := p(pr, ivp)
		t.NoError(err)

		err = distributeOps(func(_ context.Context, worker string, f func(managerWorkerOperationFunc) error) error {
			fmt.Printf("distribute: worker=%q\n", worker)

			if worker == workers[2] {
				return errors.Errorf("showme")
			}

			return f(func(opindex uint64, oph, _ util.Hash) error {
				fmt.Printf("distribute op: worker=%q opindex=%d oph=%v\n", worker, opindex, oph)

				return nil
			})
		})
		switch {
		case err == nil:
			panic("distribute")
		case !strings.Contains(err.Error(), "showme"):
			panic(err)
		}

		go checkError(t.defaultWorkersUnmerge(workers, workerUnmerge), errCheckWorkersFailed)
		go checkError(t.defaultWorkersStates(workers, expectedStatesTree, workerStates), errCheckWorkersFailed)
		go checkError(t.defaultWorkersBlockFiles(workers, expectedChecksums, workerBlockFiles), errCheckWorkersFailed)

		_, err = mf(func(opsTree, statesTree fixedtree.Tree) (base.Manifest, error) {
			fmt.Printf("trees: ops=%v states=%v\n", opsTree.Root(), statesTree.Root())

			return t.newManifest(), nil
		})
		t.ErrorIs(err, errCheckWorkersFailed)
		t.ErrorContains(err, "showme")
	})

	t.Run("worker states: over max failed", func() {
		workID := util.UUID().String()
		workers := t.workers(3)

		pr, ivp, avp, expectedOpsTree, expectedStatesTree, _ := t.baseItems()

		expectedChecksums, _, _, _, _, _, _ := t.blockMapItems(workID, len(workers), pr, ivp, avp, expectedOpsTree, expectedStatesTree)

		f := t.newFunc()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		p, err := f(ctx, workers, 1, 1, workID, t.defaultStartWorker())
		t.NoError(err)

		distributeOps, workerUnmerge, workerStates, mf, workerBlockFiles, err := p(pr, ivp)
		t.NoError(err)

		go checkError(distributeOps(t.defaultDistributeOps()), errCheckWorkersFailed)
		go checkError(t.defaultWorkersUnmerge(workers, workerUnmerge), errCheckWorkersFailed)
		go func() {
			nodes := expectedStatesTree.Nodes()

			util.TraverseSliceV1(workers, func(_ int, worker string) (bool, error) {
				fmt.Printf("worker states: worker=%q\n", worker)

				d := expectedStatesTree.Len() / len(workers)
				s := slices.Index(workers, worker) * d

				err := workerStates(context.Background(), worker, func(f func(string) error) error {
					if worker == workers[2] {
						return errors.Errorf("showme")
					}

					return util.TraverseSliceV1(nodes[s:s+d], func(_ int, n fixedtree.Node) (bool, error) {
						return true, f(n.Key())
					})
				})
				switch {
				case err == nil:
				case !strings.Contains(err.Error(), "showme"):
					panic(err)
				}

				return true, nil
			})
		}()

		go checkError(t.defaultWorkersBlockFiles(workers, expectedChecksums, workerBlockFiles), errCheckWorkersFailed)

		_, err = mf(func(opsTree, statesTree fixedtree.Tree) (base.Manifest, error) {
			fmt.Printf("trees: ops=%v states=%v\n", opsTree.Root(), statesTree.Root())

			return t.newManifest(), nil
		})
		t.ErrorIs(err, errCheckWorkersFailed)
		t.ErrorContains(err, "showme")
	})

	t.Run("block files: over max failed", func() {
		workID := util.UUID().String()
		workers := t.workers(3)

		pr, ivp, avp, expectedOpsTree, expectedStatesTree, _ := t.baseItems()

		expectedChecksums, _, _, _, _, _, _ := t.blockMapItems(workID, len(workers), pr, ivp, avp, expectedOpsTree, expectedStatesTree)

		f := t.newFunc()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		p, err := f(ctx, workers, 1, 1, workID, t.defaultStartWorker())
		t.NoError(err)

		distributeOps, workerUnmerge, workerStates, mf, workerBlockFiles, err := p(pr, ivp)
		t.NoError(err)

		go checkError(distributeOps(t.defaultDistributeOps()), errCheckWorkersFailed)
		go checkError(t.defaultWorkersUnmerge(workers, workerUnmerge), errCheckWorkersFailed)
		go checkError(t.defaultWorkersStates(workers, expectedStatesTree, workerStates), errCheckWorkersFailed)
		go func() {
			util.TraverseSliceV1(workers, func(_ int, worker string) (bool, error) {
				err := workerBlockFiles(context.Background(), worker, func(bf managerWorkerBlockFileReceiveChecksumsFunc) error {
					fmt.Printf("worker block files: worker=%q\n", worker)

					if worker == workers[2] {
						return errors.Errorf("showme")
					}

					for it := range expectedChecksums {
						cs := expectedChecksums[it]
						d := len(cs) / len(workers)
						s := slices.Index(workers, worker) * d

						if err := bf(it, cs[s:s+d]); err != nil {
							return err
						}
					}

					return nil
				})
				switch {
				case err == nil:
				case !strings.Contains(err.Error(), "showme"):
					panic(err)
				}

				return true, nil
			})
		}()

		save, err := mf(func(opsTree, statesTree fixedtree.Tree) (base.Manifest, error) {
			fmt.Printf("trees: ops=%v states=%v\n", opsTree.Root(), statesTree.Root())

			return t.newManifest(), nil
		})
		t.NoError(err)

		_, err = save(
			avp,
			t.newProposalWriter(),
			t.newVoteproofsWriter(),
			t.newOperationsTreeWriter(),
			t.newStatesTreeWriter(),
			t.newMapWriter(),
		)
		t.ErrorIs(err, errCheckWorkersFailed)
		t.ErrorContains(err, "showme")
	})
}

func (t *testManager) TestSaveError() {
	prepare := func() func(
		blockFileWriterProposalFunc,
		blockFileWriterVoteproofsFunc,
		blockFileWriterTreeFunc,
		blockFileWriterTreeFunc,
		blockFileWriterMapFunc,
	) error {
		workID := util.UUID().String()
		workers := t.workers(3)

		pr, ivp, avp, expectedOpsTree, expectedStatesTree, _ := t.baseItems()

		expectedChecksums, _, _, _, _, _, _ := t.blockMapItems(workID, len(workers), pr, ivp, avp, expectedOpsTree, expectedStatesTree)

		f := t.newFunc()

		p, err := f(context.Background(), workers, 1, 1, workID, t.defaultStartWorker())
		t.NoError(err)

		distributeOps, workerUnmerge, workerStates, mf, workerBlockFiles, err := p(pr, ivp)
		t.NoError(err)

		go ifErrorPanic(distributeOps(t.defaultDistributeOps()))
		go ifErrorPanic(t.defaultWorkersUnmerge(workers, workerUnmerge))
		go ifErrorPanic(t.defaultWorkersStates(workers, expectedStatesTree, workerStates))
		go ifErrorPanic(t.defaultWorkersBlockFiles(workers, expectedChecksums, workerBlockFiles))

		save, err := mf(func(opsTree, statesTree fixedtree.Tree) (base.Manifest, error) {
			fmt.Printf("trees: ops=%v states=%v\n", opsTree.Root(), statesTree.Root())

			return t.newManifest(), nil
		})
		t.NoError(err)

		return func(
			pf blockFileWriterProposalFunc,
			vf blockFileWriterVoteproofsFunc,
			of blockFileWriterTreeFunc,
			sf blockFileWriterTreeFunc,
			mf blockFileWriterMapFunc,
		) error {
			_, err := save(avp, pf, vf, of, sf, mf)

			return err
		}
	}

	t.Run("proposal", func() {
		save := prepare()

		err := save(
			func(context.Context, string, base.ProposalSignFact) (BlockMapItem, error) {
				return nil, errors.Errorf("showme")
			},
			t.newVoteproofsWriter(),
			t.newOperationsTreeWriter(),
			t.newStatesTreeWriter(),
			t.newMapWriter(),
		)
		t.ErrorContains(err, "showme")
	})

	t.Run("voteproofs: init voteproof", func() {
		save := prepare()

		err := save(
			t.newProposalWriter(),
			func(context.Context, string) (
				func(base.INITVoteproof) error,
				func(base.ACCEPTVoteproof) (BlockMapItem, error),
				error,
			) {
				return func(base.INITVoteproof) error {
						return errors.Errorf("showme")
					},
					func(base.ACCEPTVoteproof) (BlockMapItem, error) {
						return nil, nil
					},
					nil
			},
			t.newOperationsTreeWriter(),
			t.newStatesTreeWriter(),
			t.newMapWriter(),
		)
		t.ErrorContains(err, "showme")
	})

	t.Run("ops tree", func() {
		save := prepare()

		err := save(
			t.newProposalWriter(),
			t.newVoteproofsWriter(),
			func(context.Context, string, fixedtree.Tree) ([]blockFileChunk, error) {
				return nil, errors.Errorf("showme")
			},
			t.newStatesTreeWriter(),
			t.newMapWriter(),
		)
		t.ErrorContains(err, "showme")
	})

	t.Run("states tree", func() {
		save := prepare()

		err := save(
			t.newProposalWriter(),
			t.newVoteproofsWriter(),
			t.newOperationsTreeWriter(),
			func(context.Context, string, fixedtree.Tree) ([]blockFileChunk, error) {
				return nil, errors.Errorf("showme")
			},
			t.newMapWriter(),
		)
		t.ErrorContains(err, "showme")
	})

	t.Run("map", func() {
		save := prepare()

		err := save(
			t.newProposalWriter(),
			t.newVoteproofsWriter(),
			t.newOperationsTreeWriter(),
			t.newStatesTreeWriter(),
			func(context.Context, string, BlockMap) error {
				return errors.Errorf("showme")
			},
		)
		t.ErrorContains(err, "showme")
	})
}

func TestManager(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testManager))
}

func ifErrorPanic(err error) {
	if err != nil {
		panic(err)
	}
}
