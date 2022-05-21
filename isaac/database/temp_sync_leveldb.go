package isaacdatabase

import (
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	leveldbstorage "github.com/spikeekips/mitum/storage/leveldb"
	"github.com/spikeekips/mitum/util/encoder"
)

type LeveldbTempSyncPool struct {
	*baseLeveldb
	st *leveldbstorage.WriteStorage
}

func NewLeveldbTempSyncPool(
	f string,
	encs *encoder.Encoders,
	enc encoder.Encoder,
) (*LeveldbTempSyncPool, error) {
	st, err := leveldbstorage.NewWriteStorage(f)
	if err != nil {
		return nil, errors.Wrap(err, "failed new LeveldbTempSyncPool")
	}

	return newLeveldbTempSyncPool(st, encs, enc), nil
}

func newLeveldbTempSyncPool(
	st *leveldbstorage.WriteStorage,
	encs *encoder.Encoders,
	enc encoder.Encoder,
) *LeveldbTempSyncPool {
	return &LeveldbTempSyncPool{
		baseLeveldb: newBaseLeveldb(st, encs, enc),
		st:          st,
	}
}

func (db *LeveldbTempSyncPool) Map(height base.Height) (base.BlockMap, bool, error) {
	switch b, found, err := db.st.Get(leveldbTempSyncMapKey(height)); {
	case err != nil:
		return nil, false, errors.Wrap(err, "")
	case !found:
		return nil, false, nil
	default:
		m, err := db.decodeBlockMap(b)
		if err != nil {
			return nil, false, errors.Wrap(err, "")
		}

		return m, true, nil
	}
}

func (db *LeveldbTempSyncPool) SetMap(m base.BlockMap) error {
	b, err := db.marshal(m)
	if err != nil {
		return errors.Wrap(err, "")
	}

	err = db.st.Put(leveldbTempSyncMapKey(m.Manifest().Height()), b, nil)

	return errors.Wrap(err, "")
}

func (db *LeveldbTempSyncPool) Close() error {
	if err := db.Remove(); err != nil {
		return errors.Wrap(err, "failed to cancel LeveldbTempSyncPool")
	}

	return nil
}

func (db *LeveldbTempSyncPool) Cancel() error {
	return db.Close()
}
