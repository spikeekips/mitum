package main

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"testing"

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
	swargs *managerStartWorkersArgs,
	targs *managerTreesArgs,
	wtargs *managerWorkersTreesArgs,
	bargs *managerWorkerBlockFilesArgs,
	sargs *managerSaveArgs,
) {
	args = newManagerArgs()
	swargs = newManagerStartWorkersArgs()
	swargs.NewCheckWorkersFunc = func(minEnough, maxFailed uint64) (
		checkEnoughFailedWorkerFunc,
		func(),
		error,
	) {
		return newCheckEnoughFailedWorkers(minEnough, maxFailed)
	}

	targs = newManagerTreesArgs()
	wtargs = newManagerWorkersTreesArgs()
	wtargs.NewCheckWorkersFunc = func(minEnough, maxFailed uint64) (
		checkEnoughFailedWorkerFunc,
		func(),
		error,
	) {
		return newCheckEnoughFailedWorkers(minEnough, maxFailed)
	}
	wtargs.NewWorkersOpsFunc = distributeNShareWorkerOps
	wtargs.NewOperationsTreeWriterFuncs = newFuncOperationsTreeWriter()
	wtargs.NewStatesTreeWriterFuncs = newFuncStatesTreeWriter(1<<9, 1<<9)

	bargs = newManagerWorkerBlockFilesArgs()
	sargs = newManagerSaveArgs()

	args.SetLogging(logging.TestNilLogging)
	swargs.SetLogging(logging.TestNilLogging)
	targs.SetLogging(logging.TestNilLogging)
	wtargs.SetLogging(logging.TestNilLogging)
	bargs.SetLogging(logging.TestNilLogging)
	sargs.SetLogging(logging.TestNilLogging)

	return
}

func (t *testManager) newFunc() managerStartFunc {
	args, swargs, targs, wtargs, bargs, sargs := t.newArgs()

	f, err := newFuncManager(args, swargs, targs, wtargs, bargs, sargs)
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

func (t *testManager) TestManifest() {
	t.Run("ok: 3 workers", func() {
		workID := util.UUID().String()
		workers := t.workers(3)

		pr := t.newProposal(3)
		ivp := t.newINITVoteproof()
		avp := t.newACCEPTVoteproof()

		ophs := make([]util.Hash, len(pr.ProposalFact().Operations()))
		util.TraverseSliceV1(pr.ProposalFact().Operations(), func(i int, h [2]util.Hash) (bool, error) {
			ophs[i] = h[1]

			return true, nil
		})

		expectedOpsTree := t.newOperationsTreeWithOps(ophs)
		expectedStatesTree := t.newStatesTree(33)
		t.T().Log("ops tree:", expectedOpsTree.Root())
		t.T().Log("state tree:", expectedStatesTree.Root())

		expectedManifest := t.newManifest()
		expectedManifest.SetHeight(pr.Point().Height())
		expectedManifest.SetProposal(pr.Fact().Hash())
		expectedManifest.SetProposedAt(pr.ProposalFact().ProposedAt())

		expectedManifest.SetOperationsTree(expectedOpsTree.Root())
		expectedManifest.SetStatesTree(expectedStatesTree.Root())

		expectedChecksums := map[base.BlockItemType][]string{}
		expectedChecksums[base.BlockItemOperations] = t.checksums(len(workers) * 3)
		expectedChecksums[base.BlockItemStates] = t.checksums(len(workers) * 3)

		expectedBlockItemOperations := newBaseBlockMapItem(base.BlockItemOperations, baseBlockMapItemChecksum(expectedChecksums[base.BlockItemOperations]), true)
		expectedBlockItemStates := newBaseBlockMapItem(base.BlockItemStates, baseBlockMapItemChecksum(expectedChecksums[base.BlockItemStates]), true)

		proposalWriter := t.newProposalWriter()
		expectedBlockItemProposal, err := proposalWriter(context.Background(), workID, pr)
		t.NoError(err)

		voteproofsWriter := t.newVoteproofsWriter()
		var expectedBlockItemVoteproofs BlockMapItem
		{
			sivp, savp, err := voteproofsWriter(context.Background(), workID)
			t.NoError(err)
			t.NoError(sivp(ivp))
			expectedBlockItemVoteproofs, err = savp(avp)
			t.NoError(err)
		}

		opsTreeWriter := t.newOperationsTreeWriter()
		var expectedBlockItemOpsTree BlockMapItem
		{
			chunks, err := opsTreeWriter(context.Background(), workID, expectedOpsTree)
			t.NoError(err)
			expectedBlockItemOpsTree = newBlockMapItemFromChunks(base.BlockItemOperationsTree, chunks)
		}

		statesTreeWriter := t.newStatesTreeWriter()
		var expectedBlockItemstatesTree BlockMapItem
		{
			chunks, err := statesTreeWriter(context.Background(), workID, expectedStatesTree)
			t.NoError(err)
			expectedBlockItemstatesTree = newBlockMapItemFromChunks(base.BlockItemStatesTree, chunks)
		}

		f := t.newFunc()

		p, err := f(context.Background(), workers, 1, 1, workID, func(_ context.Context, worker string) error {
			fmt.Printf("start: worker=%q\n", worker)

			return nil
		})
		t.NoError(err)

		distributeOps, workerUnmerge, workerStates, mf, workerBlockFiles, err := p(pr, ivp)
		t.NoError(err)

		t.NoError(distributeOps(func(_ context.Context, worker string, f func(managerWorkerOperationFunc) error) error {
			fmt.Printf("distribute: worker=%q\n", worker)

			return f(func(opindex uint64, oph, _ util.Hash) error {
				fmt.Printf("distribute op: worker=%q opindex=%d oph=%v\n", worker, opindex, oph)

				return nil
			})
		}))

		go func() {
			err := util.TraverseSliceV1(workers, func(_ int, worker string) (bool, error) {
				fmt.Printf("worker unmerge: worker=%q\n", worker)

				return workerUnmerge(worker, nil)
			})
			if err != nil {
				panic(err)
			}
		}()

		go func() {
			nodes := expectedStatesTree.Nodes()

			err := util.TraverseSliceV1(workers, func(_ int, worker string) (bool, error) {
				fmt.Printf("worker states: worker=%q\n", worker)

				d := expectedStatesTree.Len() / len(workers)
				s := slices.Index(workers, worker) * d

				return true, workerStates(context.Background(), worker, func(f func(string) error) error {
					return util.TraverseSliceV1(nodes[s:s+d], func(_ int, n fixedtree.Node) (bool, error) {
						return true, f(n.Key())
					})
				})
			})
			if err != nil {
				panic(err)
			}
		}()

		go func() {
			err := util.TraverseSliceV1(workers, func(_ int, worker string) (bool, error) {
				return true, workerBlockFiles(context.Background(), worker, func(bf managerWorkerBlockFileReceiveChecksumsFunc) error {
					fmt.Printf("worker block files: worker=%q\n", worker)

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
			})
			if err != nil {
				panic(err)
			}
		}()

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
			proposalWriter,
			voteproofsWriter,
			opsTreeWriter,
			statesTreeWriter,
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
		base.IsEqualBlockMapItem(expectedBlockItemstatesTree, itemst)
	})
}

func TestManager(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testManager))
}
