package isaacdatabase

import (
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
)

type basePermanent struct {
	mp     *util.Locked // NOTE last blockmap
	sufst  *util.Locked // NOTE last suffrage state
	policy *util.Locked // NOTE last NetworkPolicy
}

func newBasePermanent() *basePermanent {
	return &basePermanent{
		mp:     util.EmptyLocked(),
		sufst:  util.EmptyLocked(),
		policy: util.EmptyLocked(),
	}
}

func (db *basePermanent) LastMap() (base.BlockMap, bool, error) {
	switch i, _ := db.mp.Value(); {
	case i == nil:
		return nil, false, nil
	default:
		return i.(base.BlockMap), true, nil //nolint:forcetypeassert //...
	}
}

func (db *basePermanent) LastSuffrage() (base.State, bool, error) {
	switch i, _ := db.sufst.Value(); {
	case i == nil:
		return nil, false, nil
	default:
		return i.(base.State), true, nil //nolint:forcetypeassert //...
	}
}

func (db *basePermanent) LastNetworkPolicy() base.NetworkPolicy {
	switch i, _ := db.policy.Value(); {
	case i == nil:
		return nil
	default:
		return i.(base.NetworkPolicy) //nolint:forcetypeassert //...
	}
}

func (db *basePermanent) Clean() error {
	_, _ = db.mp.Set(func(interface{}) (interface{}, error) {
		db.sufst.SetValue(util.NilLockedValue{})
		db.policy.SetValue(util.NilLockedValue{})

		return util.NilLockedValue{}, nil
	})

	return nil
}

func (db *basePermanent) updateLast(mp base.BlockMap, sufst base.State, policy base.NetworkPolicy) (updated bool) {
	_, err := db.mp.Set(func(i interface{}) (interface{}, error) {
		if i != nil {
			old := i.(base.BlockMap) //nolint:forcetypeassert //...

			if mp.Manifest().Height() <= old.Manifest().Height() {
				return nil, errors.Errorf("old")
			}
		}

		if sufst != nil {
			_ = db.sufst.SetValue(sufst)
		}

		if policy != nil {
			_ = db.policy.SetValue(policy)
		}

		return mp, nil
	})

	return err == nil
}
