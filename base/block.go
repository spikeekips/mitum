package base

import (
	"context"
	"net/url"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/fixedtree"
)

type Manifest interface {
	util.Hasher
	util.IsValider
	Height() Height
	Previous() util.Hash
	Proposal() util.Hash       // NOTE proposal fact hash
	OperationsTree() util.Hash // NOTE operations tree root hash
	StatesTree() util.Hash     // NOTE states tree root hash
	Suffrage() util.Hash       // NOTE state hash of newly updated SuffrageNodesStateValue
	ProposedAt() time.Time     // NOTE Proposal proposed time
}

type BlockMap interface {
	NodeSign
	Manifest() Manifest
	Item(BlockItemType) (BlockMapItem, bool)
	Items(func(BlockMapItem) bool)
}

type BlockMapItem interface {
	util.IsValider
	Type() BlockItemType
	Checksum() string
}

type BlockItemType string

var (
	BlockItemMap            BlockItemType = "map"
	BlockItemProposal       BlockItemType = "proposal"
	BlockItemOperations     BlockItemType = "operations"
	BlockItemOperationsTree BlockItemType = "operations_tree"
	BlockItemStates         BlockItemType = "states"
	BlockItemStatesTree     BlockItemType = "states_tree"
	BlockItemVoteproofs     BlockItemType = "voteproofs"
)

func (t BlockItemType) IsValid([]byte) error {
	switch t {
	case BlockItemMap,
		BlockItemProposal,
		BlockItemOperations,
		BlockItemOperationsTree,
		BlockItemStates,
		BlockItemStatesTree,
		BlockItemVoteproofs:
		return nil
	default:
		return util.ErrInvalid.Errorf("unknown block map item type, %q", t)
	}
}

func (t BlockItemType) String() string {
	return string(t)
}

func BlockItemFilesName(height Height) string {
	return height.String() + ".json"
}

type BlockItemFile interface {
	util.IsValider
	URI() url.URL
	CompressFormat() string
}

type BlockItemFiles interface {
	util.IsValider
	Item(BlockItemType) (BlockItemFile, bool)
	Items() map[BlockItemType]BlockItemFile
}

func IsValidManifests(m Manifest, previous util.Hash) error {
	if !m.Previous().Equal(previous) {
		return util.ErrInvalid.Errorf("manifests; previous does not match")
	}

	return nil
}

func BatchIsValidMaps(
	ctx context.Context,
	prev BlockMap,
	to Height,
	batchlimit int64,
	blockMapf func(context.Context, Height) (BlockMap, error),
	callback func(BlockMap) error,
) error {
	e := util.ErrInvalid.Errorf("blockmaps in batch")

	prevheight := NilHeight
	if prev != nil {
		prevheight = prev.Manifest().Height()
	}

	var validateLock sync.Mutex
	var maps []BlockMap
	var lastprev BlockMap
	newprev := prev

	if err := util.BatchWork(
		ctx,
		(to - prevheight).Int64(),
		batchlimit,
		func(_ context.Context, last uint64) error {
			lastprev = newprev

			switch r := (last + 1) % uint64(batchlimit); {
			case r == 0:
				maps = make([]BlockMap, batchlimit)
			default:
				maps = make([]BlockMap, r)
			}

			return nil
		},
		func(ctx context.Context, i, last uint64) error {
			height := prevheight + Height(int64(i)) + 1
			lastheight := prevheight + Height(int64(last)) + 1

			m, err := blockMapf(ctx, height)
			if err != nil {
				return err
			}

			if err = func() error {
				validateLock.Lock()
				defer validateLock.Unlock()

				if err = IsValidMaps(m, maps, lastprev); err != nil {
					return err
				}

				if m.Manifest().Height() == lastheight {
					newprev = m
				}

				return nil
			}(); err != nil {
				return err
			}

			return callback(m)
		},
	); err != nil {
		return e.Wrap(err)
	}

	return nil
}

func IsValidMaps(m BlockMap, maps []BlockMap, previous BlockMap) error {
	e := util.ErrInvalid.Errorf("blockmaps")

	prev := NilHeight
	if previous != nil {
		prev = previous.Manifest().Height()
	}

	index := (m.Manifest().Height() - prev - 1).Int64()

	if index < 0 || index >= int64(len(maps)) {
		return e.Errorf("invalid BlockMaps found; wrong index")
	}

	maps[index] = m

	switch {
	case index == 0 && m.Manifest().Height() == GenesisHeight:
	case index == 0 && m.Manifest().Height() != GenesisHeight:
		if err := IsValidManifests(m.Manifest(), previous.Manifest().Hash()); err != nil {
			return e.Wrap(err)
		}
	case maps[index-1] != nil:
		if err := IsValidManifests(m.Manifest(), maps[index-1].Manifest().Hash()); err != nil {
			return e.Wrap(err)
		}
	}

	// revive:disable-next-line:optimize-operands-order
	if index+1 < int64(len(maps)) && maps[index+1] != nil {
		if err := IsValidManifests(maps[index+1].Manifest(), m.Manifest().Hash()); err != nil {
			return e.Wrap(err)
		}
	}

	return nil
}

func IsValidProposalWithManifest(proposal ProposalSignFact, manifest Manifest) error {
	e := util.ErrInvalid.Errorf("proposal with manifest")

	switch {
	case proposal.Point().Height() != manifest.Height():
		return e.Errorf("height does not match")
	case !proposal.Fact().Hash().Equal(manifest.Proposal()):
		return e.Errorf("hash does not match")
	}

	return nil
}

func IsValidOperationsTreeWithManifest(tr fixedtree.Tree, ops []Operation, manifest Manifest) error {
	e := util.ErrInvalid.Errorf("operations and tree with manifest")

	switch n := len(ops); {
	case tr.Len() != n:
		return e.Errorf("number does not match")
	case n < 1:
		return nil
	}

	mops, duplicated := util.IsDuplicatedSliceWithMap(ops, func(i Operation) (bool, string) {
		if i == nil {
			return true, ""
		}

		return true, i.Fact().Hash().String()
	})
	defer clear(mops)

	if duplicated {
		return e.Errorf("duplicated operation found in operations")
	}

	if err := tr.Traverse(func(_ uint64, node fixedtree.Node) (bool, error) {
		switch on, err := util.AssertInterfaceValue[OperationFixedtreeNode](node); {
		case err != nil:
			return false, err
		default:
			if _, found := mops[on.Operation().String()]; !found {
				return false, errors.Errorf("operation in tree not found in operations")
			}

			return true, nil
		}
	}); err != nil {
		return e.Wrap(err)
	}

	if !tr.Root().Equal(manifest.OperationsTree()) {
		return e.Errorf("hash does not match")
	}

	return nil
}

func IsValidStatesTreeWithManifest(tr fixedtree.Tree, sts []State, manifest Manifest) error {
	e := util.ErrInvalid.Errorf("states and tree with manifest")

	switch n := len(sts); {
	case tr.Len() != n:
		return e.Errorf("number does not match")
	case n < 1:
		return nil
	}

	msts, duplicated := util.IsDuplicatedSliceWithMap(sts, func(i State) (bool, string) {
		if i == nil {
			return true, ""
		}

		return true, i.Hash().String()
	})
	defer clear(msts)

	if duplicated {
		return e.Errorf("duplicated state found in states")
	}

	if err := tr.Traverse(func(_ uint64, node fixedtree.Node) (bool, error) {
		switch i, found := msts[node.Key()]; {
		case !found:
			return false, errors.Errorf("state in tree not found in states")
		case i.Height() != manifest.Height():
			return false, errors.Errorf("height does not match")
		}

		return true, nil
	}); err != nil {
		return e.Wrap(err)
	}

	if !tr.Root().Equal(manifest.StatesTree()) {
		return e.Errorf("hash does not match")
	}

	return nil
}

func IsValidVoteproofsWithManifest(vps [2]Voteproof, manifest Manifest) error {
	e := util.ErrInvalid.Errorf("voteproofs with manifest")

	if vps[0] == nil || vps[1] == nil {
		return e.Errorf("empty voteproof")
	}

	var ivp INITVoteproof
	if err := util.SetInterfaceValue(vps[0], &ivp); err != nil {
		return e.Wrap(err)
	}

	var avp ACCEPTVoteproof
	if err := util.SetInterfaceValue(vps[1], &avp); err != nil {
		return e.Wrap(err)
	}

	switch {
	case ivp.Point().Height() != manifest.Height(),
		avp.Point().Height() != manifest.Height():
		return e.Errorf("height does not match")
	case !ivp.Point().Point.Equal(avp.Point().Point):
		return e.Errorf("point does not match")
	}

	return nil
}

func IsValidGenesisOperation(op Operation, networkID NetworkID, signer Publickey) error {
	e := util.ErrInvalid.Errorf("genesis operation")

	if err := op.IsValid(networkID); err != nil {
		return e.Wrap(err)
	}

	signs := op.Signs()

	var found bool

	for i := range signs {
		if signs[i].Signer().Equal(signer) {
			found = true

			break
		}
	}

	if !found {
		return e.Errorf("genesis block creator not signs genesis operation, %q", op.Hash())
	}

	return nil
}

func IsEqualManifest(a, b Manifest) error {
	if a == nil && b == nil {
		return errors.Errorf("nil manifests")
	}

	switch {
	case a.Height() != b.Height():
		return errors.Errorf("different manifest height; %d != %d", a.Height(), b.Height())
	case !a.Hash().Equal(b.Hash()):
		return errors.Errorf("different manifest hash; %q != %q", a.Hash(), b.Hash())
	default:
		return nil
	}
}

func IsEqualBlockMap(a, b BlockMap) error {
	if err := IsEqualManifest(a.Manifest(), b.Manifest()); err != nil {
		return errors.WithMessage(err, "different blockmaps")
	}

	var err error

	a.Items(func(ai BlockMapItem) bool {
		bi, found := b.Item(ai.Type())
		if !found {
			err = errors.Errorf("block item, %q not found", ai.Type())

			return false
		}

		if err = IsEqualBlockMapItem(ai, bi); err != nil {
			return false
		}

		return true
	})

	return err
}

func IsEqualBlockMapItem(a, b BlockMapItem) error {
	switch {
	case a.Type() != b.Type():
		return errors.Errorf("different block item; %q != %q", a.Type(), b.Type())
	case a.Checksum() != b.Checksum():
		return errors.Errorf(
			"different block item checksum, %q; %q != %q", a.Type(), a.Checksum(), b.Checksum())
	default:
		return nil
	}
}

func IsValidBlockItemFilesWithBlockMap(bm BlockMap, bfiles BlockItemFiles) error {
	e := util.ErrInvalid.Errorf("block item files with blockmap")

	if bm == nil {
		return e.Errorf("empty block map")
	}

	if bfiles == nil {
		return e.Errorf("empty block item files")
	}

	var ierr error

	bm.Items(func(item BlockMapItem) bool {
		if _, found := bfiles.Item(item.Type()); !found {
			ierr = errors.Errorf("item not found in block item files, %q", item.Type())

			return false
		}

		return true
	})

	return util.ErrInvalid.Wrap(ierr)
}
