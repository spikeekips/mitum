package isaacblock

import (
	"context"
	"crypto/sha256"
	"io"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/fixedtree"
)

type (
	ImportBlocksBlockMapFunc     func(context.Context, base.Height) (base.BlockMap, bool, error)
	ImportBlocksBlockMapItemFunc func(
		context.Context, base.Height, base.BlockMapItemType) (io.ReadCloser, func() error, bool, error)
)

type BlockImporter struct {
	m                        base.BlockMap
	enc                      encoder.Encoder
	bwdb                     isaac.BlockWriteDatabase
	avp                      base.ACCEPTVoteproof
	sufst                    base.State
	localfs                  *LocalFSImporter
	finisheds                *util.ShardedMap[base.BlockMapItemType, bool]
	mergeBlockWriteDatabasef func(context.Context) error
	root                     string
	networkID                base.NetworkID
	statestree               fixedtree.Tree
	batchlimit               uint64
}

func NewBlockImporter(
	root string,
	encs *encoder.Encoders,
	m base.BlockMap,
	bwdb isaac.BlockWriteDatabase,
	mergeBlockWriteDatabasef func(context.Context) error,
	networkID base.NetworkID,
) (*BlockImporter, error) {
	e := util.StringErrorFunc("failed new BlockImporter")

	enc := encs.Find(m.Encoder())
	if enc == nil {
		return nil, e(nil, "unknown encoder, %q", m.Encoder())
	}

	localfs, err := NewLocalFSImporter(root, enc, m)
	if err != nil {
		return nil, e(err, "")
	}

	finisheds, _ := util.NewShardedMap(base.BlockMapItemType(""), false, 6) //nolint:gomnd //...

	im := &BlockImporter{
		root:                     root,
		m:                        m,
		enc:                      enc,
		localfs:                  localfs,
		bwdb:                     bwdb,
		networkID:                networkID,
		finisheds:                finisheds,
		batchlimit:               333, //nolint:gomnd // enough big size
		mergeBlockWriteDatabasef: mergeBlockWriteDatabasef,
	}

	if err := im.WriteMap(m); err != nil {
		return nil, e(err, "")
	}

	return im, nil
}

func (im *BlockImporter) Reader() (isaac.BlockReader, error) {
	return NewLocalFSReaderFromHeight(im.root, im.m.Manifest().Height(), im.enc)
}

func (im *BlockImporter) WriteMap(m base.BlockMap) error {
	e := util.StringErrorFunc("failed to write BlockMap")

	im.m = m

	m.Items(func(item base.BlockMapItem) bool {
		_ = im.finisheds.SetValue(item.Type(), false)

		return true
	})

	// NOTE save map
	if err := im.localfs.WriteMap(m); err != nil {
		return e(err, "failed to write BlockMap")
	}

	if err := im.bwdb.SetBlockMap(m); err != nil {
		return e(err, "")
	}

	return nil
}

func (im *BlockImporter) WriteItem(t base.BlockMapItemType, r io.Reader) error {
	e := util.StringErrorFunc("failed to write item")

	if err := im.importItem(t, r); err != nil {
		return e(err, "failed to import item, %q", t)
	}

	_ = im.finisheds.SetValue(t, true)

	return nil
}

func (im *BlockImporter) Save(context.Context) (func(context.Context) error, error) {
	e := util.StringErrorFunc("failed to save")

	if !im.isfinished() {
		return nil, e(nil, "not yet finished")
	}

	if im.sufst != nil {
		proof, err := im.statestree.Proof(im.sufst.Hash().String())
		if err != nil {
			return nil, e(err, "failed to make proof of suffrage state")
		}

		sufproof := NewSuffrageProof(im.m, im.sufst, proof, im.avp)

		if err := im.bwdb.SetSuffrageProof(sufproof); err != nil {
			return nil, e(err, "")
		}
	}

	if err := im.bwdb.Write(); err != nil {
		return nil, e(err, "")
	}

	if err := im.localfs.Save(); err != nil {
		return nil, e(err, "")
	}

	return func(ctx context.Context) error {
		return im.mergeBlockWriteDatabasef(ctx)
	}, nil
}

func (im *BlockImporter) CancelImport(context.Context) error {
	e := util.StringErrorFunc("failed to cancel")

	if err := im.bwdb.Cancel(); err != nil {
		return e(err, "")
	}

	if err := im.localfs.Cancel(); err != nil {
		return e(err, "")
	}

	im.finisheds.Close()

	return nil
}

func (im *BlockImporter) importItem(t base.BlockMapItemType, r io.Reader) error {
	item, found := im.m.Item(t)
	if !found {
		return nil
	}

	if _, ok := r.(util.ChecksumReader); ok {
		return errors.Errorf("not allowed ChecksumReader")
	}

	var tr io.ReadCloser

	switch w, err := im.localfs.WriteItem(t); {
	case err != nil:
		return err
	default:
		defer func() {
			_ = w.Close()
		}()

		tr = io.NopCloser(io.TeeReader(r, w))
	}

	var cr util.ChecksumReader

	switch {
	case isCompressedBlockMapItemType(t):
		j := util.NewHashChecksumReader(tr, sha256.New())

		gr, err := util.NewGzipReader(j)
		if err != nil {
			return err
		}

		cr = util.NewDummyChecksumReader(gr, j)
	default:
		cr = util.NewHashChecksumReader(tr, sha256.New())
	}

	defer func() {
		_ = cr.Close()
	}()

	var ierr error

	switch t {
	case base.BlockMapItemTypeStatesTree:
		ierr = im.importStatesTree(item, cr)
	case base.BlockMapItemTypeStates:
		ierr = im.importStates(item, cr)
	case base.BlockMapItemTypeOperations:
		ierr = im.importOperations(item, cr)
	case base.BlockMapItemTypeVoteproofs:
		ierr = im.importVoteproofs(item, cr)
	default:
		if _, err := io.ReadAll(cr); err != nil {
			return errors.WithStack(err)
		}
	}

	if ierr != nil {
		return ierr
	}

	if cr.Checksum() != item.Checksum() {
		return errors.Errorf("checksum does not match, expected=%q != %q", item.Checksum(), cr.Checksum())
	}

	return nil
}

func (im *BlockImporter) isfinished() bool {
	var notyet bool
	im.m.Items(func(item base.BlockMapItem) bool {
		switch finished, found := im.finisheds.Value(item.Type()); {
		case !found:
			notyet = true

			return false
		case !finished:
			notyet = true

			return false
		default:
			return true
		}
	})

	return !notyet
}

func (im *BlockImporter) importOperations(item base.BlockMapItem, r io.Reader) error {
	e := util.StringErrorFunc("failed to import operations")

	ops := make([]util.Hash, item.Num())
	if uint64(len(ops)) > im.batchlimit {
		ops = make([]util.Hash, im.batchlimit)
	}

	left := item.Num()

	var index uint64

	validate := func(op base.Operation) error {
		return op.IsValid(im.networkID)
	}
	if im.m.Manifest().Height() == base.GenesisHeight {
		validate = func(op base.Operation) error {
			return base.ValidateGenesisOperation(op, im.networkID, im.m.Signer())
		}
	}

	if err := LoadRawItems(r, im.enc.Decode, func(_ uint64, v interface{}) error {
		op, ok := v.(base.Operation)
		if !ok {
			return errors.Errorf("not Operation, %T", v)
		}

		if err := validate(op); err != nil {
			return err
		}

		ops[index] = op.Hash()

		if index == uint64(len(ops))-1 {
			if err := im.bwdb.SetOperations(ops); err != nil {
				return err
			}

			index = 0

			switch left = left - uint64(len(ops)); {
			case left > im.batchlimit:
				ops = make([]util.Hash, im.batchlimit)
			default:
				ops = make([]util.Hash, left)
			}

			return nil
		}

		index++

		return nil
	}); err != nil {
		return e(err, "")
	}

	return nil
}

func (im *BlockImporter) importStates(item base.BlockMapItem, r io.Reader) error {
	e := util.StringErrorFunc("failed to import states")

	sts := make([]base.State, item.Num())
	if uint64(len(sts)) > im.batchlimit {
		sts = make([]base.State, im.batchlimit)
	}

	left := item.Num()

	var index uint64

	if err := LoadRawItems(r, im.enc.Decode, func(_ uint64, v interface{}) error {
		st, ok := v.(base.State)
		if !ok {
			return errors.Errorf("not State, %T", v)
		}

		if err := st.IsValid(nil); err != nil {
			return err
		}

		if base.IsSuffrageNodesState(st) {
			im.sufst = st
		}

		sts[index] = st

		if index == uint64(len(sts))-1 {
			if err := im.bwdb.SetStates(sts); err != nil {
				return err
			}

			index = 0

			switch left = left - uint64(len(sts)); {
			case left > im.batchlimit:
				sts = make([]base.State, im.batchlimit)
			default:
				sts = make([]base.State, left)
			}

			return nil
		}

		index++

		return nil
	}); err != nil {
		return e(err, "")
	}

	return nil
}

func (im *BlockImporter) importStatesTree(item base.BlockMapItem, r io.Reader) error {
	e := util.StringErrorFunc("failed to import states tree")

	tr, err := LoadTree(im.enc, item, r, func(i interface{}) (fixedtree.Node, error) {
		node, ok := i.(fixedtree.Node)
		if !ok {
			return nil, errors.Errorf("not StateFixedtreeNode, %T", i)
		}

		return node, nil
	})
	if err != nil {
		return e(err, "failed to load StatesTree")
	}

	im.statestree = tr

	return nil
}

func (im *BlockImporter) importVoteproofs(item base.BlockMapItem, r io.Reader) error {
	e := util.StringErrorFunc("failed to import voteproofs")

	vps, err := LoadVoteproofsFromReader(r, item.Num(), im.enc.Decode)
	if err != nil {
		return e(err, "")
	}

	for i := range vps {
		if vps[i] == nil {
			continue
		}

		if err := vps[i].IsValid(im.networkID); err != nil {
			return e(err, "")
		}
	}

	if err := base.ValidateVoteproofsWithManifest(vps, im.m.Manifest()); err != nil {
		return e(err, "")
	}

	im.avp = vps[1].(base.ACCEPTVoteproof) //nolint:forcetypeassert //...

	return nil
}
