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
)

type BlockImporter struct {
	m          base.BlockMap
	enc        encoder.Encoder
	localfs    *LocalFSImporter
	db         isaac.BlockWriteDatabase
	finisheds  *util.LockedMap
	batchlimit uint64
}

func NewBlockImporter(
	root string,
	encs *encoder.Encoders,
	m base.BlockMap,
	db isaac.BlockWriteDatabase,
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

	im := &BlockImporter{
		m:          m,
		enc:        enc,
		localfs:    localfs,
		db:         db,
		finisheds:  util.NewLockedMap(),
		batchlimit: 333, //nolint:gomnd // enough big size
	}

	if err := im.WriteMap(m); err != nil {
		return nil, e(err, "")
	}

	return im, nil
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

	if err := im.db.SetMap(m); err != nil {
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

func (im *BlockImporter) Save(context.Context) error {
	var notyet bool
	im.m.Items(func(item base.BlockMapItem) bool {
		switch i, found := im.finisheds.Value(item.Type()); {
		case !found:
			notyet = true

			return false
		case !i.(bool): //nolint:forcetypeassert //...
			notyet = true

			return false
		default:
			return true
		}
	})

	e := util.StringErrorFunc("failed to save")

	if notyet {
		return e(nil, "not yet finished")
	}

	if err := im.db.Write(); err != nil {
		return e(err, "")
	}

	if err := im.localfs.Save(); err != nil {
		return e(err, "")
	}

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
		return errors.Wrap(err, "")
	default:
		tr = io.NopCloser(io.TeeReader(r, w))
	}

	var cr util.ChecksumReader

	switch {
	case isCompressedBlockMapItemType(t):
		j := util.NewHashChecksumReader(tr, sha256.New())

		gr, err := util.NewGzipReader(j)
		if err != nil {
			return errors.Wrap(err, "")
		}

		cr = util.NewDummyChecksumReader(gr, j)
	default:
		cr = util.NewHashChecksumReader(tr, sha256.New())
	}

	var err error

	switch t {
	case base.BlockMapItemTypeStates:
		err = im.importStates(item, cr)
	case base.BlockMapItemTypeOperations:
		err = im.importOperations(item, cr)
	default:
		_, err = io.ReadAll(cr)
	}

	// BLOCK last voteproofs should be set by VoteproofsPool.SetLastVoteproofs()

	if err != nil {
		return errors.Wrap(err, "")
	}

	if cr.Checksum() != item.Checksum() {
		return errors.Errorf("checksum does not match")
	}

	return nil
}

func (im *BlockImporter) importOperations(item base.BlockMapItem, r io.Reader) error {
	e := util.StringErrorFunc("failed to import operations")

	ops := make([]util.Hash, item.Num())
	if uint64(len(ops)) > im.batchlimit {
		ops = make([]util.Hash, im.batchlimit)
	}

	left := item.Num()

	var index uint64

	if err := LoadRawItems(r, im.enc.Decode, func(_ uint64, v interface{}) error {
		op, ok := v.(base.Operation)
		if !ok {
			return errors.Errorf("not Operation, %T", v)
		}

		ops[index] = op.Hash()

		if index == uint64(len(ops))-1 {
			if err := im.db.SetOperations(ops); err != nil {
				return errors.Wrap(err, "")
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
	e := util.StringErrorFunc("failed to import operations")

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

		sts[index] = st

		if index == uint64(len(sts))-1 {
			if err := im.db.SetStates(sts); err != nil {
				return errors.Wrap(err, "")
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
