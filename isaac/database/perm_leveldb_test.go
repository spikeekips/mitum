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
		db, err := NewLeveldbPermanent(st, t.Encs, t.Enc, 0)
		t.NoError(err)

		return db
	}

	t.newFromDB = func(db isaac.PermanentDatabase) (isaac.PermanentDatabase, error) {
		return NewLeveldbPermanent(db.(*LeveldbPermanent).pst.RawStorage(), t.Encs, t.Enc, 0)
	}

	t.setState = func(perm isaac.PermanentDatabase, st base.State) error {
		db := perm.(*LeveldbPermanent)

		e := util.StringError("failed to set state")

		b, err := EncodeFrameState(db.enc, st)
		if err != nil {
			return e.Wrap(err)
		}

		if err := db.pst.Put(leveldbStateKey(st.Key()), b, nil); err != nil {
			return e.WithMessage(err, "failed to put state")
		}

		return nil
	}

	suite.Run(tt, t)
}
