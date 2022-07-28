package isaacdatabase

import (
	"testing"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	leveldbstorage "github.com/spikeekips/mitum/storage/leveldb"
	"github.com/spikeekips/mitum/util"
	"github.com/stretchr/testify/suite"
)

type testLeveldbPermanent struct {
	testCommonPermanent
}

func TestLeveldbPermanent(tt *testing.T) {
	t := new(testLeveldbPermanent)
	t.newDB = func() isaac.PermanentDatabase {
		st := leveldbstorage.NewMemStorage()
		db, err := newLeveldbPermanent(st, t.Encs, t.Enc)
		t.NoError(err)

		return db
	}

	t.newFromDB = func(db isaac.PermanentDatabase) (isaac.PermanentDatabase, error) {
		return newLeveldbPermanent(db.(*LeveldbPermanent).st.RawStorage(), t.Encs, t.Enc)
	}

	t.setState = func(perm isaac.PermanentDatabase, st base.State) error {
		db := perm.(*LeveldbPermanent)

		e := util.StringErrorFunc("failed to set state")

		b, err := db.marshal(st)
		if err != nil {
			return e(err, "")
		}

		if err := db.st.Put(leveldbStateKey(st.Key()), b, nil); err != nil {
			return e(err, "failed to put state")
		}

		return nil
	}

	suite.Run(tt, t)
}
