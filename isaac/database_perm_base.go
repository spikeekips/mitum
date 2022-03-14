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
		m:      util.NewLocked(nil),
		sufstt: util.NewLocked(nil),
	}
}

func (db *basePermanentDatabase) LastManifest() (base.Manifest, bool, error) {
	var m base.Manifest
	switch _ = db.m.Value(&m); {
	case m == nil:
		return nil, false, nil
	default:
		return m, true, nil
	}
}

func (db *basePermanentDatabase) LastSuffrage() (base.State, bool, error) {
	var m base.State
	switch _ = db.sufstt.Value(&m); {
	case m == nil:
		return nil, false, nil
	default:
		return m, true, nil
	}
}

func (db *LeveldbPermanentDatabase) canMergeTempDatabase(temp TempDatabase) bool {
	var m base.Manifest
	if _ = db.m.Value(&m); m != nil && m.Height() >= temp.Height() {
		return false
	}

	return true
}
