package isaac

import (
	"testing"

	"github.com/spikeekips/mitum/base"
	leveldbstorage "github.com/spikeekips/mitum/storage/leveldb"
	"github.com/spikeekips/mitum/util"
	"github.com/stretchr/testify/suite"
)

type testLeveldbPermanentDatabase struct {
	testCommonPermanentDatabase
}

func TestLeveldbPermanentDatabase(tt *testing.T) {
	t := new(testLeveldbPermanentDatabase)
	t.newDB = func() PermanentDatabase {
		st := leveldbstorage.NewMemWriteStorage()
		db, err := newLeveldbPermanentDatabase(st, t.Encs, t.Enc)
		t.NoError(err)

		return db
	}

	t.newFromDB = func(db PermanentDatabase) (PermanentDatabase, error) {
		return newLeveldbPermanentDatabase(db.(*LeveldbPermanentDatabase).st, t.Encs, t.Enc)
	}

	t.setState = func(perm PermanentDatabase, st base.State) error {
		db := perm.(*LeveldbPermanentDatabase)

		e := util.StringErrorFunc("failed to set state")

		b, err := db.marshal(st)
		if err != nil {
			return e(err, "")
		}

		if err := db.st.Put(leveldbStateKey(st.Key()), b, nil); err != nil {
			return e(err, "failed to put state")
		}

		if st.Key() == SuffrageStateKey {
			if err := db.st.Put(leveldbSuffrageKey(st.Height()), b, nil); err != nil {
				return e(err, "failed to put suffrage by block height")
			}

			sv := st.Value().(base.SuffrageStateValue)
			if err := db.st.Put(leveldbSuffrageHeightKey(sv.Height()), b, nil); err != nil {
				return e(err, "failed to put suffrage by height")
			}
		}

		return nil
	}

	suite.Run(tt, t)
}
