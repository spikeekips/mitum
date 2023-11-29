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
	"github.com/spikeekips/mitum/util/hint"
)

type (
	ImportBlocksBlockMapFunc     func(context.Context, base.Height) (base.BlockMap, bool, error)
	ImportBlocksBlockMapItemFunc func(
		context.Context, base.Height, base.BlockItemType, func(io.Reader, bool) error,
	) error
)

type BlockImporter struct {
	m                        base.BlockMap
	enc                      encoder.Encoder
	bwdb                     isaac.BlockWriteDatabase
	sufst                    base.State
	localfs                  *LocalFSImporter
	finisheds                *util.ShardedMap[base.BlockItemType, bool]
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
	e := util.StringError(" BlockImporter")

	enc, found := encs.Find(m.Encoder())
	if !found {
		return nil, e.Errorf("unknown encoder, %q", m.Encoder())
	}

	localfs, err := NewLocalFSImporter(root, encs.JSON(), enc, m)
	if err != nil {
		return nil, e.Wrap(err)
	}

	finisheds, _ := util.NewShardedMap[base.BlockItemType, bool](6, nil) //nolint:gomnd //...

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
		return nil, e.Wrap(err)
	}

	return im, nil
}

func (im *BlockImporter) Reader() (isaac.BlockReader, error) {
	return NewLocalFSReaderFromHeight(im.root, im.m.Manifest().Height(), im.enc)
}

func (im *BlockImporter) WriteMap(m base.BlockMap) error {
	e := util.StringError("write BlockMap")

	im.m = m

	m.Items(func(item base.BlockMapItem) bool {
		_ = im.finisheds.SetValue(item.Type(), false)

		return true
	})

	// NOTE save map
	if err := im.localfs.WriteMap(m); err != nil {
		return e.WithMessage(err, "write BlockMap")
	}

	if err := im.bwdb.SetBlockMap(m); err != nil {
		return e.Wrap(err)
	}

	return nil
}

func (im *BlockImporter) WriteItem(t base.BlockItemType, r *util.CompressedReader) error {
	e := util.StringError("write item")

	if err := im.importItem(t, r); err != nil {
		return e.WithMessage(err, "import item, %q", t)
	}

	_ = im.finisheds.SetValue(t, true)

	return nil
}

func (im *BlockImporter) Save(context.Context) (func(context.Context) error, error) {
	e := util.StringError("save")

	if !im.isfinished() {
		return nil, e.Errorf("not yet finished")
	}

	if im.sufst != nil {
		proof, err := im.statestree.Proof(im.sufst.Hash().String())
		if err != nil {
			return nil, e.WithMessage(err, "make proof of suffrage state")
		}

		sufproof := NewSuffrageProof(im.m, im.sufst, proof)

		if err := im.bwdb.SetSuffrageProof(sufproof); err != nil {
			return nil, e.Wrap(err)
		}
	}

	if err := im.bwdb.Write(); err != nil {
		return nil, e.Wrap(err)
	}

	if err := im.localfs.Save(); err != nil {
		return nil, e.Wrap(err)
	}

	return func(ctx context.Context) error {
		return im.mergeBlockWriteDatabasef(ctx)
	}, nil
}

func (im *BlockImporter) CancelImport(context.Context) error {
	e := util.StringError("cancel")

	if err := im.bwdb.Cancel(); err != nil {
		return e.Wrap(err)
	}

	if err := im.localfs.Cancel(); err != nil {
		return e.Wrap(err)
	}

	im.finisheds.Close()

	return nil
}

func (im *BlockImporter) importItem(t base.BlockItemType, r *util.CompressedReader) error {
	item, found := im.m.Item(t)
	if !found {
		return nil
	}

	var cr util.ChecksumReader

	switch w, err := im.localfs.WriteItem(t); {
	case err != nil:
		return err
	default:
		defer func() {
			_ = w.Close()
		}()

		f, err := r.Tee(w)
		if err != nil {
			return err
		}

		cr = util.NewHashChecksumReader(f, sha256.New())
	}

	if err := func() error {
		switch t {
		case base.BlockItemStatesTree:
			return im.importStatesTree(cr)
		case base.BlockItemStates:
			return im.importStates(cr)
		case base.BlockItemOperations:
			return im.importOperations(cr)
		case base.BlockItemVoteproofs:
			return im.importVoteproofs(cr)
		default:
			return im.importOther(cr)
		}
	}(); err != nil {
		return err
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

func (im *BlockImporter) importOperations(r io.Reader) error {
	e := util.StringError("import operations")

	var left uint64
	var ops []util.Hash
	br := r

	switch i, _, _, count, err := readCountHeader(br); {
	case err != nil:
		return err
	case count < 1:
		return nil
	default:
		left = count
		ops = make([]util.Hash, count)
		br = i
	}

	if uint64(len(ops)) > im.batchlimit {
		ops = make([]util.Hash, im.batchlimit)
	}

	var index uint64

	validate := func(op base.Operation) error {
		return op.IsValid(im.networkID)
	}
	if im.m.Manifest().Height() == base.GenesisHeight {
		validate = func(op base.Operation) error {
			return base.ValidateGenesisOperation(op, im.networkID, im.m.Signer())
		}
	}

	if err := LoadRawItems(br, im.enc.Decode, func(_ uint64, v interface{}) error {
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
		return e.Wrap(err)
	}

	return nil
}

func (im *BlockImporter) importStates(r io.Reader) error {
	e := util.StringError("import states")

	var left uint64
	var sts []base.State
	br := r

	switch i, _, _, count, err := readCountHeader(br); {
	case err != nil:
		return err
	case count < 1:
		return nil
	default:
		left = count
		sts = make([]base.State, count)
		br = i
	}

	if uint64(len(sts)) > im.batchlimit {
		sts = make([]base.State, im.batchlimit)
	}

	var index uint64

	if err := LoadRawItems(br, im.enc.Decode, func(_ uint64, v interface{}) error {
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
		return e.Wrap(err)
	}

	return nil
}

func (im *BlockImporter) importStatesTree(r io.Reader) error {
	e := util.StringError("import states tree")

	br := r

	var count uint64
	var treehint hint.Hint

	switch i, _, _, j, k, err := readTreeHeader(br); {
	case err != nil:
		return err
	case j < 1:
		return nil
	default:
		br = i
		count = j
		treehint = k
	}

	tr, err := LoadTree(im.enc, count, treehint, br, func(i interface{}) (fixedtree.Node, error) {
		node, ok := i.(fixedtree.Node)
		if !ok {
			return nil, errors.Errorf("not StateFixedtreeNode, %T", i)
		}

		return node, nil
	})
	if err != nil {
		return e.WithMessage(err, "load StatesTree")
	}

	im.statestree = tr

	return nil
}

func (im *BlockImporter) importVoteproofs(r io.Reader) error {
	e := util.StringError("import voteproofs")

	br := r

	switch i, _, _, err := readBaseHeader(br); {
	case err != nil:
		return err
	default:
		br = i
	}

	vps, err := LoadVoteproofsFromReader(br, im.enc.Decode)
	if err != nil {
		return e.Wrap(err)
	}

	for i := range vps {
		if vps[i] == nil {
			continue
		}

		if err := vps[i].IsValid(im.networkID); err != nil {
			return e.Wrap(err)
		}
	}

	if err := base.ValidateVoteproofsWithManifest(vps, im.m.Manifest()); err != nil {
		return e.Wrap(err)
	}

	return nil
}

func (*BlockImporter) importOther(r io.Reader) error {
	br := r

	switch i, _, _, err := readBaseHeader(br); {
	case err != nil:
		return err
	default:
		br = i
	}

	if _, err := io.ReadAll(br); err != nil {
		return errors.WithStack(err)
	}

	return nil
}
