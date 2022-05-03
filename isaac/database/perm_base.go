package isaacdatabase

import (
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util"
)

type basePermanent struct {
	mp     *util.Locked // NOTE last blockdatamap
	sufstt *util.Locked // NOTE last suffrage state
	policy *util.Locked // NOTE last NetworkPolicy
}

func newBasePermanent() *basePermanent {
	return &basePermanent{
		mp:     util.EmptyLocked(),
		sufstt: util.EmptyLocked(),
		policy: util.EmptyLocked(),
	}
}

func (db *basePermanent) LastMap() (base.BlockdataMap, bool, error) {
	switch i, _ := db.mp.Value(); {
	case i == nil:
		return nil, false, nil
	default:
		return i.(base.BlockdataMap), true, nil
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

func (db *basePermanent) LastNetworkPolicy() base.NetworkPolicy {
	switch i, _ := db.policy.Value(); {
	case i == nil:
		return nil
	default:
		return i.(base.NetworkPolicy)
	}
}

func (db *LeveldbPermanent) canMergeTempDatabase(temp isaac.TempDatabase) bool {
	switch i, _ := db.mp.Value(); {
	case i == nil:
		return true
	case i.(base.BlockdataMap).Manifest().Height() < temp.Height():
		return true
	default:
		return false
	}
}
