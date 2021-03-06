package leveldbstorage

import (
	"context"
	"sync"

	"github.com/syndtr/goleveldb/leveldb"
	"golang.org/x/xerrors"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/base/block"
	"github.com/spikeekips/mitum/storage"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
)

type SyncerStorage struct {
	sync.RWMutex
	*logging.Logging
	main       *Storage
	storage    *Storage
	heightFrom base.Height
	heightTo   base.Height
}

func NewSyncerStorage(main *Storage) *SyncerStorage {
	return &SyncerStorage{
		Logging: logging.NewLogging(func(c logging.Context) logging.Emitter {
			return c.Str("module", "leveldb-syncer-storage")
		}),
		main:       main,
		storage:    NewMemStorage(main.Encoders(), main.Encoder()),
		heightFrom: base.Height(-1),
	}
}

func (st *SyncerStorage) manifestKey(height base.Height) []byte {
	return util.ConcatBytesSlice(
		keyPrefixTmp,
		leveldbManifestHeightKey(height),
	)
}

func (st *SyncerStorage) Manifest(height base.Height) (block.Manifest, bool, error) {
	raw, err := st.storage.DB().Get(st.manifestKey(height), nil)
	if err != nil {
		if xerrors.Is(err, storage.NotFoundError) {
			return nil, false, nil
		}

		return nil, false, wrapError(err)
	}

	m, err := st.storage.loadManifest(raw)
	if err != nil {
		if xerrors.Is(err, storage.NotFoundError) {
			return nil, false, nil
		}
		return nil, false, err
	}

	return m, true, nil
}

func (st *SyncerStorage) Manifests(heights []base.Height) ([]block.Manifest, error) {
	var bs []block.Manifest
	for i := range heights {
		if b, found, err := st.Manifest(heights[i]); !found {
			return nil, storage.NotFoundError.Errorf("manifest not found by height")
		} else if err != nil {
			return nil, err
		} else {
			bs = append(bs, b)
		}
	}

	return bs, nil
}

func (st *SyncerStorage) SetManifests(manifests []block.Manifest) error {
	st.Log().VerboseFunc(func(e *logging.Event) logging.Emitter {
		var heights []base.Height
		for i := range manifests {
			heights = append(heights, manifests[i].Height())
		}

		return e.Interface("heights", heights)
	}).
		Int("manifests", len(manifests)).
		Msg("set manifests")

	batch := &leveldb.Batch{}

	for i := range manifests {
		m := manifests[i]
		if b, err := marshal(st.storage.Encoder(), m); err != nil {
			return err
		} else {
			key := st.manifestKey(m.Height())
			batch.Put(key, b)
		}
	}

	return wrapError(st.storage.DB().Write(batch, nil))
}

func (st *SyncerStorage) HasBlock(height base.Height) (bool, error) {
	return st.storage.db.Has(leveldbBlockHeightKey(height), nil)
}

func (st *SyncerStorage) block(height base.Height) (block.Block, bool, error) {
	return st.storage.blockByHeight(height)
}

func (st *SyncerStorage) SetBlocks(blocks []block.Block) error {
	st.Log().VerboseFunc(func(e *logging.Event) logging.Emitter {
		var heights []base.Height
		for i := range blocks {
			heights = append(heights, blocks[i].Height())
		}

		return e.Interface("heights", heights)
	}).
		Int("blocks", len(blocks)).
		Msg("set blocks")

	for i := range blocks {
		blk := blocks[i]

		st.checkHeight(blk.Height())

		if bs, err := st.storage.OpenBlockStorage(blk); err != nil {
			return err
		} else if err := bs.SetBlock(context.Background(), blk); err != nil {
			return err
		} else if err := bs.Commit(context.Background()); err != nil {
			return err
		}
	}

	return nil
}

func (st *SyncerStorage) Commit() error {
	st.Log().Debug().
		Hinted("from_height", st.heightFrom).
		Hinted("to_height", st.heightTo).
		Msg("trying to commit blocks")

	for i := st.heightFrom.Int64(); i <= st.heightTo.Int64(); i++ {
		if blk, found, err := st.block(base.Height(i)); !found {
			return storage.NotFoundError.Errorf("block not found")
		} else if err != nil {
			return err
		} else if err := st.commitBlock(blk); err != nil {
			st.Log().Error().Err(err).Int64("height", i).Msg("failed to commit block")
			return err
		}

		st.Log().Debug().Int64("height", i).Msg("committed block")
	}

	return nil
}

func (st *SyncerStorage) commitBlock(blk block.Block) error {
	if bs, err := st.main.OpenBlockStorage(blk); err != nil {
		return err
	} else if err := bs.SetBlock(context.Background(), blk); err != nil {
		return err
	} else if err := bs.Commit(context.Background()); err != nil {
		return err
	}

	return nil
}

func (st *SyncerStorage) checkHeight(height base.Height) {
	st.Lock()
	defer st.Unlock()

	switch {
	case st.heightFrom < 0:
		st.heightFrom = height
		st.heightTo = height
	case st.heightFrom > height:
		st.heightFrom = height
	case st.heightTo < height:
		st.heightTo = height
	}
}

func (st *SyncerStorage) Close() error {
	return wrapError(st.storage.DB().Close())
}
