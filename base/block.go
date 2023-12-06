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

func ValidateManifests(m Manifest, previous util.Hash) error {
	if !m.Previous().Equal(previous) {
		return errors.Errorf("previous does not match")
	}

	return nil
}

func BatchValidateMaps(
	ctx context.Context,
	prev BlockMap,
	to Height,
	batchlimit int64,
	blockMapf func(context.Context, Height) (BlockMap, error),
	callback func(BlockMap) error,
) error {
	e := util.StringError("validate BlockMaps in batch")

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
		func(ctx context.Context, last uint64) error {
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

				if err = ValidateMaps(m, maps, lastprev); err != nil {
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

func ValidateMaps(m BlockMap, maps []BlockMap, previous BlockMap) error {
	prev := NilHeight
	if previous != nil {
		prev = previous.Manifest().Height()
	}

	index := (m.Manifest().Height() - prev - 1).Int64()

	e := util.StringError("validate BlockMaps")

	if index < 0 || index >= int64(len(maps)) {
		return e.Errorf("invalid BlockMaps found; wrong index")
	}

	maps[index] = m

	switch {
	case index == 0 && m.Manifest().Height() == GenesisHeight:
	case index == 0 && m.Manifest().Height() != GenesisHeight:
		if err := ValidateManifests(m.Manifest(), previous.Manifest().Hash()); err != nil {
			return e.Wrap(err)
		}
	case maps[index-1] != nil:
		if err := ValidateManifests(m.Manifest(), maps[index-1].Manifest().Hash()); err != nil {
			return e.Wrap(err)
		}
	}

	// revive:disable-next-line:optimize-operands-order
	if index+1 < int64(len(maps)) && maps[index+1] != nil {
		if err := ValidateManifests(maps[index+1].Manifest(), m.Manifest().Hash()); err != nil {
			return e.Wrap(err)
		}
	}

	return nil
}

func ValidateProposalWithManifest(proposal ProposalSignFact, manifest Manifest) error {
	e := util.StringError("invalid proposal by manifest")

	switch {
	case proposal.Point().Height() != manifest.Height():
		return e.Errorf("height does not match")
	case !proposal.Fact().Hash().Equal(manifest.Proposal()):
		return e.Errorf("hash does not match")
	}

	return nil
}

func ValidateOperationsTreeWithManifest(tr fixedtree.Tree, ops []Operation, manifest Manifest) error {
	e := util.StringError("invalid operations and it's tree by manifest")

	switch n := len(ops); {
	case tr.Len() != n:
		return e.Errorf("number does not match")
	case n < 1:
		return nil
	}

	mops, duplicated := util.IsDuplicatedSlice(ops, func(i Operation) (bool, string) {
		if i == nil {
			return true, ""
		}

		return true, i.Fact().Hash().String()
	})
	if duplicated {
		return e.Errorf("duplicated operation found in operations")
	}

	if err := tr.Traverse(func(_ uint64, node fixedtree.Node) (bool, error) {
		on, ok := node.(OperationFixedtreeNode)
		if !ok {
			return false, errors.Errorf("expected OperationFixedtreeNode, but %T", node)
		}

		if _, found := mops[on.Operation().String()]; !found {
			return false, errors.Errorf("operation in tree not found in operations")
		}

		return true, nil
	}); err != nil {
		return e.Wrap(err)
	}

	if !tr.Root().Equal(manifest.OperationsTree()) {
		return e.Errorf("hash does not match")
	}

	return nil
}

func ValidateStatesTreeWithManifest(tr fixedtree.Tree, sts []State, manifest Manifest) error {
	e := util.StringError("invalid states and it's tree by manifest")

	switch n := len(sts); {
	case tr.Len() != n:
		return e.Errorf("number does not match")
	case n < 1:
		return nil
	}

	msts, duplicated := util.IsDuplicatedSlice(sts, func(i State) (bool, string) {
		if i == nil {
			return true, ""
		}

		return true, i.Hash().String()
	})
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

func ValidateVoteproofsWithManifest(vps [2]Voteproof, manifest Manifest) error {
	e := util.StringError("invalid voteproofs by manifest")

	if vps[0] == nil || vps[1] == nil {
		return e.Errorf("empty voteproof")
	}

	var ivp INITVoteproof

	switch i, ok := vps[0].(INITVoteproof); {
	case !ok:
		return e.Errorf("expected INITVoteproof, but %T", vps[0])
	default:
		ivp = i
	}

	var avp ACCEPTVoteproof

	switch i, ok := vps[1].(ACCEPTVoteproof); {
	case !ok:
		return e.Errorf("expected ACCEPTVoteproof, but %T", vps[0])
	default:
		avp = i
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

func ValidateGenesisOperation(op Operation, networkID NetworkID, signer Publickey) error {
	if err := op.IsValid(networkID); err != nil {
		return err
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
		return util.ErrInvalid.Errorf("genesis block creator not signs genesis operation, %q", op.Hash())
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
