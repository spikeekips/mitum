package isaac

import (
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
)

type basePermanentDatabase struct {
	mp     *util.Locked // NOTE last blockdatamap
	sufstt *util.Locked // NOTE last suffrage state
}

func newBasePermanentDatabase() *basePermanentDatabase {
	return &basePermanentDatabase{
		mp:     util.EmptyLocked(),
		sufstt: util.EmptyLocked(),
	}
}

func (db *basePermanentDatabase) LastMap() (base.BlockDataMap, bool, error) {
	switch i, isnil := db.mp.Value(); {
	case isnil || i == nil:
		return nil, false, nil
	default:
		return i.(base.BlockDataMap), true, nil
	}
}

func (db *basePermanentDatabase) LastSuffrage() (base.State, bool, error) {
	switch i, _ := db.sufstt.Value(); {
	case i == nil:
		return nil, false, nil
	default:
		return i.(base.State), true, nil
	}
}

func (db *LeveldbPermanentDatabase) canMergeTempDatabase(temp TempDatabase) bool {
	i, _ := db.mp.Value()

	switch {
	case i == nil:
		return true
	case i.(base.BlockDataMap).Manifest().Height() < temp.Height():
		return true
	default:
		return false
	}
}
