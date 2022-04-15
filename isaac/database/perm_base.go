package database

import (
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util"
)

type basePermanent struct {
	mp     *util.Locked // NOTE last blockdatamap
	sufstt *util.Locked // NOTE last suffrage state
}

func newBasePermanent() *basePermanent {
	return &basePermanent{
		mp:     util.EmptyLocked(),
		sufstt: util.EmptyLocked(),
	}
}

func (db *basePermanent) LastMap() (base.BlockDataMap, bool, error) {
	switch i, isnil := db.mp.Value(); {
	case isnil || i == nil:
		return nil, false, nil
	default:
		return i.(base.BlockDataMap), true, nil
	}
}

func (db *basePermanent) LastSuffrage() (base.State, bool, error) {
	switch i, _ := db.sufstt.Value(); {
	case i == nil:
		return nil, false, nil
	default:
		return i.(base.State), true, nil
	}
}

func (db *LeveldbPermanent) canMergeTempDatabase(temp isaac.TempDatabase) bool {
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
