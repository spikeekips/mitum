package isaac

import (
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
)

type basePermanentDatabase struct {
	m      *util.Locked // NOTE last manifest
	sufstt *util.Locked // NOTE last suffrage state
}

func newBasePermanentDatabase() *basePermanentDatabase {
	return &basePermanentDatabase{
		m:      util.EmptyLocked(),
		sufstt: util.EmptyLocked(),
	}
}

func (db *basePermanentDatabase) LastManifest() (base.Manifest, bool, error) {
	switch i, isnil := db.m.Value(); {
	case isnil || i == nil:
		return nil, false, nil
	default:
		return i.(base.Manifest), true, nil
	}
}

func (db *basePermanentDatabase) LastSuffrage() (base.State, bool, error) {
	switch i, isnil := db.sufstt.Value(); {
	case isnil || i == nil:
		return nil, false, nil
	default:
		return i.(base.State), true, nil
	}
}

func (db *LeveldbPermanentDatabase) canMergeTempDatabase(temp TempDatabase) bool {
	if i, _ := db.m.Value(); i != nil && i.(base.Manifest).Height() >= temp.Height() {
		return false
	}

	return true
}
