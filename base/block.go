package base

import (
	"context"
	"net/url"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/fixedtree"
	"github.com/spikeekips/mitum/util/hint"
)

type Manifest interface {
	util.Hasher
	util.IsValider
	Height() Height
	Previous() util.Hash
	Proposal() util.Hash       // NOTE proposal fact hash
	OperationsTree() util.Hash // NOTE operations tree root hash
	StatesTree() util.Hash     // NOTE states tree root hash
	Suffrage() util.Hash       // NOTE state hash of newly updated SuffrageStateValue
	ProposedAt() time.Time     // NOTE Proposal proposed time
}

type BlockMap interface {
	NodeSigned
	Manifest() Manifest
	Item(BlockMapItemType) (BlockMapItem, bool)
	Items(func(BlockMapItem) bool)
	Writer() hint.Hint
	Encoder() hint.Hint
}

type BlockMapItem interface {
	util.IsValider
	Type() BlockMapItemType
	URL() *url.URL
	Checksum() string
	Num() uint64
}

type BlockMapItemType string

var (
	BlockMapItemTypeProposal       BlockMapItemType = "blockmapitem_proposal"
	BlockMapItemTypeOperations     BlockMapItemType = "blockmapitem_operations"
	BlockMapItemTypeOperationsTree BlockMapItemType = "blockmapitem_operations_tree"
	BlockMapItemTypeStates         BlockMapItemType = "blockmapitem_states"
	BlockMapItemTypeStatesTree     BlockMapItemType = "blockmapitem_states_tree"
	BlockMapItemTypeVoteproofs     BlockMapItemType = "blockmapitem_voteproofs"
)

func (t BlockMapItemType) IsValid([]byte) error {
	switch t {
	case BlockMapItemTypeProposal,
		BlockMapItemTypeOperations,
		BlockMapItemTypeOperationsTree,
		BlockMapItemTypeStates,
		BlockMapItemTypeStatesTree,
		BlockMapItemTypeVoteproofs:
		return nil
	default:
		return util.ErrInvalid.Errorf("unknown block map item type, %q", t)
	}
}

func (t BlockMapItemType) String() string {
	return string(t)
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
	batchlimit uint64,
	blockMapf func(context.Context, Height) (BlockMap, error),
	callback func(BlockMap) error,
) error {
	e := util.StringErrorFunc("failed to validate BlockMaps in batch")

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
		uint64((to - prevheight).Int64()),
		batchlimit,
		func(ctx context.Context, last uint64) error {
			lastprev = newprev

			switch r := (last + 1) % batchlimit; {
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
		return e(err, "")
	}

	return nil
}

func ValidateMaps(m BlockMap, maps []BlockMap, previous BlockMap) error {
	prev := NilHeight
	if previous != nil {
		prev = previous.Manifest().Height()
	}

	index := (m.Manifest().Height() - prev - 1).Int64()

	e := util.StringErrorFunc("failed to validate BlockMaps")

	if index < 0 || index >= int64(len(maps)) {
		return e(nil, "invalid BlockMaps found; wrong index")
	}

	maps[index] = m

	switch {
	case index == 0 && m.Manifest().Height() == GenesisHeight:
	case index == 0 && m.Manifest().Height() != GenesisHeight:
		if err := ValidateManifests(m.Manifest(), previous.Manifest().Hash()); err != nil {
			return e(err, "")
		}
	case maps[index-1] != nil:
		if err := ValidateManifests(m.Manifest(), maps[index-1].Manifest().Hash()); err != nil {
			return e(err, "")
		}
	}

	// revive:disable-next-line:optimize-operands-order
	if index+1 < int64(len(maps)) && maps[index+1] != nil {
		if err := ValidateManifests(maps[index+1].Manifest(), m.Manifest().Hash()); err != nil {
			return e(err, "")
		}
	}

	return nil
}

func ValidateProposalWithManifest(proposal ProposalSignedFact, manifest Manifest) error {
	e := util.StringErrorFunc("invalid proposal by manifest")

	switch {
	case proposal.Point().Height() != manifest.Height():
		return e(nil, "height does not match")
	case !proposal.Fact().Hash().Equal(manifest.Proposal()):
		return e(nil, "hash does not match")
	}

	return nil
}

func ValidateOperationsTreeWithManifest(tr fixedtree.Tree, ops []Operation, manifest Manifest) error {
	e := util.StringErrorFunc("invalid operations and it's tree by manifest")

	switch n := len(ops); {
	case tr.Len() != n:
		return e(nil, "number does not match")
	case n < 1:
		return nil
	}

	mops, duplicated := util.CheckSliceDuplicated(ops, func(_ interface{}, i int) string {
		return ops[i].Fact().Hash().String()
	})
	if duplicated {
		return e(nil, "duplicated operation found in operations")
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
		return e(err, "")
	}

	if !tr.Root().Equal(manifest.OperationsTree()) {
		return e(nil, "hash does not match")
	}

	return nil
}

func ValidateStatesTreeWithManifest(tr fixedtree.Tree, sts []State, manifest Manifest) error {
	e := util.StringErrorFunc("invalid states and it's tree by manifest")

	switch n := len(sts); {
	case tr.Len() != n:
		return e(nil, "number does not match")
	case n < 1:
		return nil
	}

	msts, duplicated := util.CheckSliceDuplicated(sts, func(_ interface{}, i int) string {
		return sts[i].Hash().String()
	})
	if duplicated {
		return e(nil, "duplicated state found in states")
	}

	if err := tr.Traverse(func(_ uint64, node fixedtree.Node) (bool, error) {
		switch i, found := msts[node.Key()]; {
		case !found:
			return false, errors.Errorf("state in tree not found in states")
		case i.(State).Height() != manifest.Height(): //nolint:forcetypeassert //...
			return false, errors.Errorf("height does not match")
		}

		return true, nil
	}); err != nil {
		return e(err, "")
	}

	if !tr.Root().Equal(manifest.StatesTree()) {
		return e(nil, "hash does not match")
	}

	return nil
}

func ValidateVoteproofsWithManifest(vps []Voteproof, manifest Manifest) error {
	e := util.StringErrorFunc("invalid voteproofs by manifest")

	switch {
	case len(vps) != 2: //nolint:gomnd //...
		return e(nil, "not voteproofs")
	case vps[0] == nil, vps[1] == nil:
		return e(nil, "empty voteproof")
	}

	var ivp INITVoteproof

	switch i, ok := vps[0].(INITVoteproof); {
	case !ok:
		return e(nil, "expected INITVoteproof, but %T", vps[0])
	default:
		ivp = i
	}

	var avp ACCEPTVoteproof

	switch i, ok := vps[1].(ACCEPTVoteproof); {
	case !ok:
		return e(nil, "expected ACCEPTVoteproof, but %T", vps[0])
	default:
		avp = i
	}

	switch {
	case ivp.Point().Height() != manifest.Height(),
		avp.Point().Height() != manifest.Height():
		return e(nil, "height does not match")
	case !ivp.Point().Point.Equal(avp.Point().Point):
		return e(nil, "point does not match")
	}

	return nil
}

func ValidateGenesisOperation(op Operation, networkID NetworkID, signer Publickey) error {
	if err := op.IsValid(networkID); err != nil {
		return err
	}

	signed := op.Signed()

	var found bool

	for i := range signed {
		if signed[i].Signer().Equal(signer) {
			found = true

			break
		}
	}

	if !found {
		return util.ErrInvalid.Errorf("genesis block creator not signed genesis operation, %q", op.Hash())
	}

	return nil
}
