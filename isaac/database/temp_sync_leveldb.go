package isaacdatabase

import (
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	leveldbstorage2 "github.com/spikeekips/mitum/storage/leveldb2"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	leveldbutil "github.com/syndtr/goleveldb/leveldb/util"
)

type LeveldbTempSyncPool struct {
	*baseLeveldb
}

func NewLeveldbTempSyncPool(
	height base.Height,
	st *leveldbstorage2.Storage,
	encs *encoder.Encoders,
	enc encoder.Encoder,
) (*LeveldbTempSyncPool, error) {
	return newLeveldbTempSyncPool(height, st, encs, enc), nil
}

func newLeveldbTempSyncPool(
	height base.Height,
	st *leveldbstorage2.Storage,
	encs *encoder.Encoders,
	enc encoder.Encoder,
) *LeveldbTempSyncPool {
	pst := leveldbstorage2.NewPrefixStorage(st, newPrefixStoragePrefixByHeight(leveldbLabelSyncPool, height))

	return &LeveldbTempSyncPool{
		baseLeveldb: newBaseLeveldb(pst, encs, enc),
	}
}

func (db *LeveldbTempSyncPool) BlockMap(height base.Height) (m base.BlockMap, found bool, _ error) {
	switch b, found, err := db.st.Get(leveldbTempSyncMapKey(height)); {
	case err != nil:
		return nil, false, err
	case !found:
		return nil, false, nil
	default:
		if err := db.readHinter(b, &m); err != nil {
			return nil, false, err
		}

		return m, true, nil
	}
}

func (db *LeveldbTempSyncPool) SetBlockMap(m base.BlockMap) error {
	b, err := db.marshal(m)
	if err != nil {
		return err
	}

	return db.st.Put(leveldbTempSyncMapKey(m.Manifest().Height()), b, nil)
}

func (db *LeveldbTempSyncPool) Cancel() error {
	e := util.StringErrorFunc("failed to cancel temp sync pool")

	r := leveldbutil.BytesPrefix(db.st.Prefix())

	if _, err := leveldbstorage2.BatchRemove(db.st.Storage, r, 333); err != nil { //nolint:gomnd //...
		return e(err, "")
	}

	if err := db.Close(); err != nil {
		return e(err, "")
	}

	return nil
}

func CleanSyncPool(st *leveldbstorage2.Storage) error {
	r := leveldbutil.BytesPrefix(leveldbstorage2.HashPrefix(leveldbLabelSyncPool))

	if _, err := leveldbstorage2.BatchRemove(st, r, 333); err != nil { //nolint:gomnd //...
		return errors.WithMessage(err, "failed to clean syncpool database")
	}

	return nil
}
