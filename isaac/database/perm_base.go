package isaacdatabase

import (
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
)

type basePermanent struct {
	mp     *util.Locked // NOTE last blockmap
	policy *util.Locked // NOTE last NetworkPolicy
	proof  *util.Locked // NOTE last SuffrageProof
}

func newBasePermanent() *basePermanent {
	return &basePermanent{
		mp:     util.EmptyLocked(),
		policy: util.EmptyLocked(),
		proof:  util.EmptyLocked(),
	}
}

func (db *basePermanent) LastBlockMap() (base.BlockMap, bool, error) {
	switch i, _ := db.mp.Value(); {
	case i == nil:
		return nil, false, nil
	default:
		return i.(base.BlockMap), true, nil //nolint:forcetypeassert //...
	}
}

func (db *basePermanent) LastSuffrageProof() (base.SuffrageProof, bool, error) {
	switch i, _ := db.proof.Value(); {
	case i == nil:
		return nil, false, nil
	default:
		return i.(base.SuffrageProof), true, nil //nolint:forcetypeassert //...
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
	_, _ = db.mp.Set(func(bool, interface{}) (interface{}, error) {
		db.policy.SetValue(util.NilLockedValue{})
		db.proof.SetValue(util.NilLockedValue{})

		return util.NilLockedValue{}, nil
	})

	return nil
}

func (db *basePermanent) updateLast(
	mp base.BlockMap, proof base.SuffrageProof, policy base.NetworkPolicy,
) (updated bool) {
	_, err := db.mp.Set(func(_ bool, i interface{}) (interface{}, error) {
		if i != nil {
			old := i.(base.BlockMap) //nolint:forcetypeassert //...

			if mp.Manifest().Height() <= old.Manifest().Height() {
				return nil, errors.Errorf("old")
			}
		}

		if proof != nil {
			_ = db.proof.SetValue(proof)
		}

		if policy != nil {
			_ = db.policy.SetValue(policy)
		}

		return mp, nil
	})

	return err == nil
}
