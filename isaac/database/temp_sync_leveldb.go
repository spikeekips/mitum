package isaacdatabase

import (
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	leveldbstorage "github.com/spikeekips/mitum/storage/leveldb"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	leveldbutil "github.com/syndtr/goleveldb/leveldb/util"
)

type LeveldbTempSyncPool struct {
	*baseLeveldb
}

func NewLeveldbTempSyncPool(
	height base.Height,
	st *leveldbstorage.Storage,
	encs *encoder.Encoders,
	enc encoder.Encoder,
) (*LeveldbTempSyncPool, error) {
	return newLeveldbTempSyncPool(height, st, encs, enc), nil
}

func newLeveldbTempSyncPool(
	height base.Height,
	st *leveldbstorage.Storage,
	encs *encoder.Encoders,
	enc encoder.Encoder,
) *LeveldbTempSyncPool {
	pst := leveldbstorage.NewPrefixStorage(st, newPrefixStoragePrefixByHeight(leveldbLabelSyncPool, height))

	return &LeveldbTempSyncPool{
		baseLeveldb: newBaseLeveldb(pst, encs, enc),
	}
}

func (db *LeveldbTempSyncPool) BlockMap(height base.Height) (m base.BlockMap, found bool, _ error) {
	pst, err := db.st()
	if err != nil {
		return nil, false, err
	}

	switch b, found, err := pst.Get(leveldbTempSyncMapKey(height)); {
	case err != nil:
		return nil, false, err
	case !found:
		return nil, false, nil
	case len(b) < 1:
		return nil, false, nil
	default:
		if err := db.readHinter(b, &m); err != nil {
			return nil, false, err
		}

		return m, true, nil
	}
}

func (db *LeveldbTempSyncPool) SetBlockMap(m base.BlockMap) error {
	b, _, err := db.marshal(m, nil)
	if err != nil {
		return err
	}

	pst, err := db.st()
	if err != nil {
		return err
	}

	return pst.Put(leveldbTempSyncMapKey(m.Manifest().Height()), b, nil)
}

func (db *LeveldbTempSyncPool) Cancel() error {
	e := util.StringError("cancel temp sync pool")

	pst, err := db.st()
	if err != nil {
		return e.Wrap(err)
	}

	if err := func() error {
		db.Lock()
		defer db.Unlock()

		r := leveldbutil.BytesPrefix(pst.Prefix())

		_, err := leveldbstorage.BatchRemove(pst.Storage, r, 333) //nolint:gomnd //...

		return err
	}(); err != nil {
		return e.Wrap(err)
	}

	if err := db.Close(); err != nil {
		return e.Wrap(err)
	}

	return nil
}

func CleanSyncPool(st *leveldbstorage.Storage) error {
	r := leveldbutil.BytesPrefix(leveldbLabelSyncPool[:])

	if _, err := leveldbstorage.BatchRemove(st, r, 333); err != nil { //nolint:gomnd //...
		return errors.WithMessage(err, "clean syncpool database")
	}

	return nil
}
