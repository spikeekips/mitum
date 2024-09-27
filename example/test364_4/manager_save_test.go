package main

import (
	"context"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/fixedtree"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type testManagerSave struct {
	baseTestManager
	baseTestBlock
}

func (t *testManagerSave) newArgs() *managerSaveArgs {
	args := newManagerSaveArgs()

	return args
}

func (t *testManagerSave) TestCancel() {
	t.Run("before proposal", func() {
		ctx, cancel := context.WithCancel(context.Background())

		f := newFuncManagerSave(t.newArgs())

		cancel()

		pf, ivpf, otf, stf, mf, avpf, _, err := f(
			ctx,
			util.UUID().String(),
			func(context.Context, string, base.ProposalSignFact) (BlockMapItem, error) {
				return nil, errors.Errorf("proposal")
			},
			func(context.Context, string) (
				func(base.INITVoteproof) error,
				func(base.ACCEPTVoteproof) (BlockMapItem, error),
				error,
			) {
				return func(base.INITVoteproof) error { return errors.Errorf("ivp") },
					func(base.ACCEPTVoteproof) (BlockMapItem, error) { return nil, errors.Errorf("avp") },
					nil
			},
			func(context.Context, string, fixedtree.Tree) ([]blockFileChunk, error) {
				return nil, errors.Errorf("operations tree")
			},
			func(context.Context, string, fixedtree.Tree) ([]blockFileChunk, error) {
				return nil, errors.Errorf("states tree")
			},
			func(context.Context, string, BlockMap) error { return errors.Errorf("map") },
		)
		t.NoError(err)

		t.ErrorIs(pf(t.newProposal(3)), context.Canceled)
		t.ErrorIs(ivpf(t.newINITVoteproof()), context.Canceled)
		t.ErrorIs(otf(t.newOperationsTree()), context.Canceled)
		t.ErrorIs(stf(t.newStatesTree(33)), context.Canceled)
		t.ErrorIs(mf(t.newManifest()), context.Canceled)

		bm, err := avpf(t.newACCEPTVoteproof())
		t.ErrorIs(err, context.Canceled)
		t.Nil(bm)
	})

	t.Run("before init voteproof", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		f := newFuncManagerSave(t.newArgs())

		pf, ivpf, otf, stf, mf, avpf, _, err := f(
			ctx,
			util.UUID().String(),
			func(context.Context, string, base.ProposalSignFact) (BlockMapItem, error) {
				return t.newItem(base.BlockItemProposal), nil
			},
			func(context.Context, string) (
				func(base.INITVoteproof) error,
				func(base.ACCEPTVoteproof) (BlockMapItem, error),
				error,
			) {
				return func(base.INITVoteproof) error { return errors.Errorf("ivp") },
					func(base.ACCEPTVoteproof) (BlockMapItem, error) { return nil, errors.Errorf("avp") },
					nil
			},
			func(context.Context, string, fixedtree.Tree) ([]blockFileChunk, error) {
				return nil, errors.Errorf("operations tree")
			},
			func(context.Context, string, fixedtree.Tree) ([]blockFileChunk, error) {
				return nil, errors.Errorf("states tree")
			},
			func(context.Context, string, BlockMap) error { return errors.Errorf("map") },
		)
		t.NoError(err)

		t.NoError(pf(t.newProposal(3)))

		cancel()

		t.ErrorIs(ivpf(t.newINITVoteproof()), context.Canceled)
		t.ErrorIs(otf(t.newOperationsTree()), context.Canceled)
		t.ErrorIs(stf(t.newStatesTree(33)), context.Canceled)
		t.ErrorIs(mf(t.newManifest()), context.Canceled)

		bm, err := avpf(t.newACCEPTVoteproof())
		t.ErrorIs(err, context.Canceled)
		t.Nil(bm)
	})

	t.Run("before operations tree", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		f := newFuncManagerSave(t.newArgs())

		pf, ivpf, otf, stf, mf, avpf, _, err := f(
			ctx,
			util.UUID().String(),
			func(context.Context, string, base.ProposalSignFact) (BlockMapItem, error) {
				return t.newItem(base.BlockItemProposal), nil
			},
			func(context.Context, string) (
				func(base.INITVoteproof) error,
				func(base.ACCEPTVoteproof) (BlockMapItem, error),
				error,
			) {
				return func(base.INITVoteproof) error { return nil },
					func(base.ACCEPTVoteproof) (BlockMapItem, error) { return nil, errors.Errorf("avp") },
					nil
			},
			func(context.Context, string, fixedtree.Tree) ([]blockFileChunk, error) {
				return nil, errors.Errorf("operations tree")
			},
			func(context.Context, string, fixedtree.Tree) ([]blockFileChunk, error) {
				return nil, errors.Errorf("states tree")
			},
			func(context.Context, string, BlockMap) error { return errors.Errorf("map") },
		)
		t.NoError(err)

		t.NoError(pf(t.newProposal(3)))
		t.NoError(ivpf(t.newINITVoteproof()))

		cancel()

		t.ErrorIs(otf(t.newOperationsTree()), context.Canceled)
		t.ErrorIs(stf(t.newStatesTree(33)), context.Canceled)
		t.ErrorIs(mf(t.newManifest()), context.Canceled)

		bm, err := avpf(t.newACCEPTVoteproof())
		t.ErrorIs(err, context.Canceled)
		t.Nil(bm)
	})

	t.Run("before states tree", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		f := newFuncManagerSave(t.newArgs())

		pf, ivpf, otf, stf, mf, avpf, _, err := f(
			ctx,
			util.UUID().String(),
			func(context.Context, string, base.ProposalSignFact) (BlockMapItem, error) {
				return t.newItem(base.BlockItemProposal), nil
			},
			func(context.Context, string) (
				func(base.INITVoteproof) error,
				func(base.ACCEPTVoteproof) (BlockMapItem, error),
				error,
			) {
				return func(base.INITVoteproof) error { return nil },
					func(base.ACCEPTVoteproof) (BlockMapItem, error) { return nil, errors.Errorf("avp") },
					nil
			},
			func(context.Context, string, fixedtree.Tree) ([]blockFileChunk, error) {
				return nil, nil
			},
			func(context.Context, string, fixedtree.Tree) ([]blockFileChunk, error) {
				return nil, errors.Errorf("states tree")
			},
			func(context.Context, string, BlockMap) error { return errors.Errorf("map") },
		)
		t.NoError(err)

		t.NoError(pf(t.newProposal(3)))
		t.NoError(ivpf(t.newINITVoteproof()))
		t.NoError(otf(t.newOperationsTree()))

		cancel()

		t.ErrorIs(stf(t.newStatesTree(33)), context.Canceled)
		t.ErrorIs(mf(t.newManifest()), context.Canceled)

		bm, err := avpf(t.newACCEPTVoteproof())
		t.ErrorIs(err, context.Canceled)
		t.Nil(bm)
	})

	t.Run("before manifest", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		f := newFuncManagerSave(t.newArgs())

		pf, ivpf, otf, stf, mf, avpf, _, err := f(
			ctx,
			util.UUID().String(),
			func(context.Context, string, base.ProposalSignFact) (BlockMapItem, error) {
				return t.newItem(base.BlockItemProposal), nil
			},
			func(context.Context, string) (
				func(base.INITVoteproof) error,
				func(base.ACCEPTVoteproof) (BlockMapItem, error),
				error,
			) {
				return func(base.INITVoteproof) error { return nil },
					func(base.ACCEPTVoteproof) (BlockMapItem, error) { return nil, errors.Errorf("avp") },
					nil
			},
			func(context.Context, string, fixedtree.Tree) ([]blockFileChunk, error) {
				return nil, nil
			},
			func(context.Context, string, fixedtree.Tree) ([]blockFileChunk, error) {
				return nil, nil
			},
			func(context.Context, string, BlockMap) error { return errors.Errorf("map") },
		)
		t.NoError(err)

		t.NoError(pf(t.newProposal(3)))
		t.NoError(ivpf(t.newINITVoteproof()))
		t.NoError(otf(t.newOperationsTree()))
		t.NoError(stf(t.newStatesTree(33)))

		cancel()

		t.ErrorIs(mf(t.newManifest()), context.Canceled)

		bm, err := avpf(t.newACCEPTVoteproof())
		t.ErrorIs(err, context.Canceled)
		t.Nil(bm)
	})

	t.Run("before accept voteproof", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		f := newFuncManagerSave(t.newArgs())

		pf, ivpf, otf, stf, mf, avpf, _, err := f(
			ctx,
			util.UUID().String(),
			func(context.Context, string, base.ProposalSignFact) (BlockMapItem, error) {
				return t.newItem(base.BlockItemProposal), nil
			},
			func(context.Context, string) (
				func(base.INITVoteproof) error,
				func(base.ACCEPTVoteproof) (BlockMapItem, error),
				error,
			) {
				return func(base.INITVoteproof) error { return nil },
					func(base.ACCEPTVoteproof) (BlockMapItem, error) { return nil, errors.Errorf("avp") },
					nil
			},
			func(context.Context, string, fixedtree.Tree) ([]blockFileChunk, error) {
				return nil, nil
			},
			func(context.Context, string, fixedtree.Tree) ([]blockFileChunk, error) {
				return nil, nil
			},
			func(context.Context, string, BlockMap) error { return errors.Errorf("map") },
		)
		t.NoError(err)

		t.NoError(pf(t.newProposal(3)))
		t.NoError(ivpf(t.newINITVoteproof()))
		t.NoError(otf(t.newOperationsTree()))
		t.NoError(stf(t.newStatesTree(33)))
		t.NoError(mf(t.newManifest()))

		cancel()

		bm, err := avpf(t.newACCEPTVoteproof())
		t.ErrorIs(err, context.Canceled)
		t.Nil(bm)
	})
}

func (t *testManagerSave) TestError() {
	ifErrorContains := func(err error, expected string) {
		if err != nil {
			t.ErrorContains(err, expected)
		}
	}

	t.Run("proposal", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		f := newFuncManagerSave(t.newArgs())

		pf, ivpf, otf, stf, mf, avpf, _, err := f(
			ctx,
			util.UUID().String(),
			func(context.Context, string, base.ProposalSignFact) (BlockMapItem, error) {
				return nil, errors.Errorf("proposal")
			},
			func(context.Context, string) (
				func(base.INITVoteproof) error,
				func(base.ACCEPTVoteproof) (BlockMapItem, error),
				error,
			) {
				return func(base.INITVoteproof) error { return errors.Errorf("ivp") },
					func(base.ACCEPTVoteproof) (BlockMapItem, error) { return nil, errors.Errorf("avp") },
					nil
			},
			func(context.Context, string, fixedtree.Tree) ([]blockFileChunk, error) {
				return nil, errors.Errorf("operations tree")
			},
			func(context.Context, string, fixedtree.Tree) ([]blockFileChunk, error) {
				return nil, errors.Errorf("states tree")
			},
			func(context.Context, string, BlockMap) error { return errors.Errorf("map") },
		)
		t.NoError(err)

		ifErrorContains(pf(t.newProposal(3)), "proposal")
		ifErrorContains(ivpf(t.newINITVoteproof()), "proposal")
		ifErrorContains(otf(t.newOperationsTree()), "proposal")
		ifErrorContains(stf(t.newStatesTree(33)), "proposal")
		ifErrorContains(mf(t.newManifest()), "proposal")

		bm, err := avpf(t.newACCEPTVoteproof())
		t.ErrorContains(err, "proposal")
		t.Nil(bm)
	})

	t.Run("init voteproof", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		f := newFuncManagerSave(t.newArgs())

		pf, ivpf, otf, stf, mf, avpf, _, err := f(
			ctx,
			util.UUID().String(),
			func(context.Context, string, base.ProposalSignFact) (BlockMapItem, error) {
				return t.newItem(base.BlockItemProposal), nil
			},
			func(context.Context, string) (
				func(base.INITVoteproof) error,
				func(base.ACCEPTVoteproof) (BlockMapItem, error),
				error,
			) {
				return func(base.INITVoteproof) error { return errors.Errorf("ivp") },
					func(base.ACCEPTVoteproof) (BlockMapItem, error) { return nil, errors.Errorf("avp") },
					nil
			},
			func(context.Context, string, fixedtree.Tree) ([]blockFileChunk, error) {
				return nil, errors.Errorf("operations tree")
			},
			func(context.Context, string, fixedtree.Tree) ([]blockFileChunk, error) {
				return nil, errors.Errorf("states tree")
			},
			func(context.Context, string, BlockMap) error { return errors.Errorf("map") },
		)
		t.NoError(err)

		t.NoError(pf(t.newProposal(3)))

		ifErrorContains(ivpf(t.newINITVoteproof()), "ivp")
		ifErrorContains(otf(t.newOperationsTree()), "ivp")
		ifErrorContains(stf(t.newStatesTree(33)), "ivp")
		ifErrorContains(mf(t.newManifest()), "ivp")

		bm, err := avpf(t.newACCEPTVoteproof())
		t.ErrorContains(err, "ivp")
		t.Nil(bm)
	})

	t.Run("operations tree", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		f := newFuncManagerSave(t.newArgs())

		pf, ivpf, otf, stf, mf, avpf, _, err := f(
			ctx,
			util.UUID().String(),
			func(context.Context, string, base.ProposalSignFact) (BlockMapItem, error) {
				return t.newItem(base.BlockItemProposal), nil
			},
			func(context.Context, string) (
				func(base.INITVoteproof) error,
				func(base.ACCEPTVoteproof) (BlockMapItem, error),
				error,
			) {
				return func(base.INITVoteproof) error { return nil },
					func(base.ACCEPTVoteproof) (BlockMapItem, error) { return nil, errors.Errorf("avp") },
					nil
			},
			func(context.Context, string, fixedtree.Tree) ([]blockFileChunk, error) {
				return nil, errors.Errorf("operations tree")
			},
			func(context.Context, string, fixedtree.Tree) ([]blockFileChunk, error) {
				return nil, errors.Errorf("states tree")
			},
			func(context.Context, string, BlockMap) error { return errors.Errorf("map") },
		)
		t.NoError(err)

		t.NoError(pf(t.newProposal(3)))
		t.NoError(ivpf(t.newINITVoteproof()))

		ifErrorContains(otf(t.newOperationsTree()), "operations tree")
		ifErrorContains(stf(t.newStatesTree(33)), "operations tree")
		ifErrorContains(mf(t.newManifest()), "operations tree")

		bm, err := avpf(t.newACCEPTVoteproof())
		t.ErrorContains(err, "operations tree")
		t.Nil(bm)
	})

	t.Run("states tree", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		f := newFuncManagerSave(t.newArgs())

		pf, ivpf, otf, stf, mf, avpf, _, err := f(
			ctx,
			util.UUID().String(),
			func(context.Context, string, base.ProposalSignFact) (BlockMapItem, error) {
				return t.newItem(base.BlockItemProposal), nil
			},
			func(context.Context, string) (
				func(base.INITVoteproof) error,
				func(base.ACCEPTVoteproof) (BlockMapItem, error),
				error,
			) {
				return func(base.INITVoteproof) error { return nil },
					func(base.ACCEPTVoteproof) (BlockMapItem, error) { return nil, errors.Errorf("avp") },
					nil
			},
			func(context.Context, string, fixedtree.Tree) ([]blockFileChunk, error) {
				return nil, nil
			},
			func(context.Context, string, fixedtree.Tree) ([]blockFileChunk, error) {
				return nil, errors.Errorf("states tree")
			},
			func(context.Context, string, BlockMap) error { return errors.Errorf("map") },
		)
		t.NoError(err)

		t.NoError(pf(t.newProposal(3)))
		t.NoError(ivpf(t.newINITVoteproof()))
		t.NoError(otf(t.newOperationsTree()))

		ifErrorContains(stf(t.newStatesTree(33)), "states tree")
		ifErrorContains(mf(t.newManifest()), "states tree")

		bm, err := avpf(t.newACCEPTVoteproof())
		t.ErrorContains(err, "states tree")
		t.Nil(bm)
	})

	t.Run("manifest", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		f := newFuncManagerSave(t.newArgs())

		pf, ivpf, otf, stf, mf, avpf, _, err := f(
			ctx,
			util.UUID().String(),
			func(context.Context, string, base.ProposalSignFact) (BlockMapItem, error) {
				return t.newItem(base.BlockItemProposal), nil
			},
			func(context.Context, string) (
				func(base.INITVoteproof) error,
				func(base.ACCEPTVoteproof) (BlockMapItem, error),
				error,
			) {
				return func(base.INITVoteproof) error { return nil },
					func(base.ACCEPTVoteproof) (BlockMapItem, error) { return nil, errors.Errorf("avp") },
					nil
			},
			func(context.Context, string, fixedtree.Tree) ([]blockFileChunk, error) {
				return nil, nil
			},
			func(context.Context, string, fixedtree.Tree) ([]blockFileChunk, error) {
				return nil, nil
			},
			func(context.Context, string, BlockMap) error { return errors.Errorf("map") },
		)
		t.NoError(err)

		t.NoError(pf(t.newProposal(3)))
		t.NoError(ivpf(t.newINITVoteproof()))
		t.NoError(otf(t.newOperationsTree()))
		t.NoError(stf(t.newStatesTree(33)))

		ifErrorContains(mf(nil), "nil manifest")

		bm, err := avpf(t.newACCEPTVoteproof())
		t.ErrorContains(err, "nil manifest")
		t.Nil(bm)
	})

	t.Run("accept voteproof", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		f := newFuncManagerSave(t.newArgs())

		pf, ivpf, otf, stf, mf, avpf, _, err := f(
			ctx,
			util.UUID().String(),
			func(context.Context, string, base.ProposalSignFact) (BlockMapItem, error) {
				return t.newItem(base.BlockItemProposal), nil
			},
			func(context.Context, string) (
				func(base.INITVoteproof) error,
				func(base.ACCEPTVoteproof) (BlockMapItem, error),
				error,
			) {
				return func(base.INITVoteproof) error { return nil },
					func(base.ACCEPTVoteproof) (BlockMapItem, error) { return nil, errors.Errorf("avp") },
					nil
			},
			func(context.Context, string, fixedtree.Tree) ([]blockFileChunk, error) {
				return nil, nil
			},
			func(context.Context, string, fixedtree.Tree) ([]blockFileChunk, error) {
				return nil, nil
			},
			func(context.Context, string, BlockMap) error { return errors.Errorf("map") },
		)
		t.NoError(err)

		t.NoError(pf(t.newProposal(3)))
		t.NoError(ivpf(t.newINITVoteproof()))
		t.NoError(otf(t.newOperationsTree()))
		t.NoError(stf(t.newStatesTree(33)))

		t.NoError(mf(t.newManifest()))

		bm, err := avpf(t.newACCEPTVoteproof())
		t.ErrorContains(err, "avp")
		t.Nil(bm)
	})

	t.Run("map", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		f := newFuncManagerSave(t.newArgs())

		pf, ivpf, otf, stf, mf, avpf, _, err := f(
			ctx,
			util.UUID().String(),
			func(context.Context, string, base.ProposalSignFact) (BlockMapItem, error) {
				return t.newItem(base.BlockItemProposal), nil
			},
			func(context.Context, string) (
				func(base.INITVoteproof) error,
				func(base.ACCEPTVoteproof) (BlockMapItem, error),
				error,
			) {
				return func(base.INITVoteproof) error { return nil },
					func(base.ACCEPTVoteproof) (BlockMapItem, error) { return nil, nil },
					nil
			},
			func(context.Context, string, fixedtree.Tree) ([]blockFileChunk, error) {
				return nil, nil
			},
			func(context.Context, string, fixedtree.Tree) ([]blockFileChunk, error) {
				return nil, nil
			},
			func(context.Context, string, BlockMap) error { return errors.Errorf("map") },
		)
		t.NoError(err)

		t.NoError(pf(t.newProposal(3)))
		t.NoError(ivpf(t.newINITVoteproof()))
		t.NoError(otf(t.newOperationsTree()))
		t.NoError(stf(t.newStatesTree(33)))

		t.NoError(mf(t.newManifest()))

		bm, err := avpf(t.newACCEPTVoteproof())
		t.ErrorContains(err, "map")
		t.Nil(bm)
	})
}

func (t *testManagerSave) TestSave() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	f := newFuncManagerSave(t.newArgs())

	proposal := t.newProposal(3)
	ivp := t.newINITVoteproof()
	avp := t.newACCEPTVoteproof()
	opsTree := t.newOperationsTree()
	statesTree := t.newStatesTree(33)
	manifest := t.newManifest()

	proposalItem := t.newItem(base.BlockItemProposal)
	proposalItem.isChunked = false
	voteproofsItem := t.newItem(base.BlockItemVoteproofs)
	voteproofsItem.isChunked = false

	opsChunks := []blockFileChunk{
		t.newBlockFileChunk(),
		t.newBlockFileChunk(),
		t.newBlockFileChunk(),
		t.newBlockFileChunk(),
	}
	statesChunks := []blockFileChunk{
		t.newBlockFileChunk(),
		t.newBlockFileChunk(),
		t.newBlockFileChunk(),
		t.newBlockFileChunk(),
	}

	pf, ivpf, otf, stf, mf, avpf, _, err := f(
		ctx,
		util.UUID().String(),
		func(context.Context, string, base.ProposalSignFact) (BlockMapItem, error) {
			return proposalItem, nil
		},
		func(context.Context, string) (
			func(base.INITVoteproof) error,
			func(base.ACCEPTVoteproof) (BlockMapItem, error),
			error,
		) {
			return func(base.INITVoteproof) error { return nil },
				func(base.ACCEPTVoteproof) (BlockMapItem, error) { return voteproofsItem, nil },
				nil
		},
		func(context.Context, string, fixedtree.Tree) ([]blockFileChunk, error) {
			return opsChunks, nil
		},
		func(context.Context, string, fixedtree.Tree) ([]blockFileChunk, error) {
			return statesChunks, nil
		},
		func(context.Context, string, BlockMap) error { return nil },
	)
	t.NoError(err)

	t.NoError(pf(proposal))
	t.NoError(ivpf(ivp))
	t.NoError(otf(opsTree))
	t.NoError(stf(statesTree))
	t.NoError(mf(manifest))

	bm, err := avpf(avp)
	t.NoError(err)
	t.NotNil(bm)

	t.T().Log("marshal:", util.MustMarshalJSONIndentString(bm))

	t.True(time.Now().Before(bm.ExpiredAt()))

	collected := map[base.BlockItemType]BlockMapItem{}

	bm.Items(func(item BlockMapItem) bool {
		collected[item.Type()] = item

		return true
	})

	t.Equal(4, len(collected))

	EqualBlockMapItem(t.Assert(), proposalItem, collected[base.BlockItemProposal])
	EqualBlockMapItem(t.Assert(), voteproofsItem, collected[base.BlockItemVoteproofs])
	EqualBlockMapItem(t.Assert(), newBlockMapItemFromChunks(base.BlockItemOperationsTree, opsChunks), collected[base.BlockItemOperationsTree])
	EqualBlockMapItem(t.Assert(), newBlockMapItemFromChunks(base.BlockItemStatesTree, statesChunks), collected[base.BlockItemStatesTree])
}

func TestManagerSave(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testManagerSave))
}
