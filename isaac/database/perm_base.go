package isaacdatabase

import (
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
)

type basePermanent struct {
	lenc    *util.Locked[string]             // NOTE encoder of last blockmap
	mp      *util.Locked[[3]interface{}]     // NOTE last blockmap
	policy  *util.Locked[base.NetworkPolicy] // NOTE last NetworkPolicy
	proof   *util.Locked[[3]interface{}]     // NOTE last SuffrageProof
	stcache *util.GCache[string, base.State]
}

func newBasePermanent(stcachesize int) *basePermanent {
	var stcache *util.GCache[string, base.State]
	if stcachesize > 0 {
		stcache = util.NewLFUGCache[string, base.State](stcachesize)
	}

	return &basePermanent{
		lenc:    util.EmptyLocked[string](),
		mp:      util.EmptyLocked[[3]interface{}](),
		policy:  util.EmptyLocked[base.NetworkPolicy](),
		proof:   util.EmptyLocked[[3]interface{}](),
		stcache: stcache,
	}
}

func (db *basePermanent) LastBlockMap() (base.BlockMap, bool, error) {
	switch i, isempty := db.mp.Value(); {
	case isempty:
		return nil, false, nil
	default:
		return i[0].(base.BlockMap), true, nil //nolint:forcetypeassert //...
	}
}

func (db *basePermanent) LastBlockMapBytes() (enchint string, meta, body []byte, found bool, err error) {
	switch i, isempty := db.lenc.Value(); {
	case isempty:
		return enchint, nil, nil, false, nil
	default:
		enchint = i
	}

	switch i, isempty := db.mp.Value(); {
	case isempty:
		return enchint, nil, nil, false, nil
	default:
		return enchint, i[1].([]byte), i[2].([]byte), true, nil //nolint:forcetypeassert //...
	}
}

func (db *basePermanent) LastSuffrageProof() (base.SuffrageProof, bool, error) {
	switch i, isempty := db.proof.Value(); {
	case isempty:
		return nil, false, nil
	default:
		return i[0].(base.SuffrageProof), true, nil //nolint:forcetypeassert //...
	}
}

func (db *basePermanent) LastSuffrageProofBytes() (enchint string, meta, body []byte, found bool, err error) {
	switch i, isempty := db.lenc.Value(); {
	case isempty:
		return enchint, nil, nil, false, nil
	default:
		enchint = i
	}

	switch i, isempty := db.proof.Value(); {
	case isempty:
		return enchint, nil, nil, false, nil
	default:
		return enchint, i[1].([]byte), i[2].([]byte), true, nil //nolint:forcetypeassert //...
	}
}

func (db *basePermanent) LastNetworkPolicy() base.NetworkPolicy {
	switch i, _ := db.policy.Value(); {
	case i == nil:
		return nil
	default:
		return i
	}
}

func (db *basePermanent) Clean() error {
	_ = db.mp.Empty(func([3]interface{}, bool) error {
		db.policy.EmptyValue()
		db.proof.EmptyValue()

		return nil
	})

	return nil
}

func (db *basePermanent) state(key string) (base.State, bool, error) {
	if db.stcache == nil {
		return nil, false, nil
	}

	st, found := db.stcache.Get(key)

	return st, found, nil
}

func (db *basePermanent) setState(st base.State) {
	if db.stcache == nil {
		return
	}

	db.stcache.Set(st.Key(), st, 0)
}

func (db *basePermanent) updateLast(
	lenc string,
	mp base.BlockMap, mpmeta, mpbody []byte,
	proof base.SuffrageProof, proofmeta, proofbody []byte,
	policy base.NetworkPolicy,
) (updated bool) {
	_, err := db.mp.Set(func(i [3]interface{}, isempty bool) ([3]interface{}, error) {
		if !isempty {
			old := i[0].(base.BlockMap) //nolint:forcetypeassert //...

			if mp.Manifest().Height() <= old.Manifest().Height() {
				return [3]interface{}{}, errors.Errorf("old")
			}
		}

		_ = db.lenc.SetValue(lenc)

		if proof != nil {
			_ = db.proof.SetValue([3]interface{}{proof, proofmeta, proofbody})
		}

		if policy != nil {
			_ = db.policy.SetValue(policy)
		}

		return [3]interface{}{mp, mpmeta, mpbody}, nil
	})

	return err == nil
}

func (db *basePermanent) mergeTempStateCache(stcache *util.GCache[string, [2]interface{}]) {
	if stcache == nil {
		return
	}

	stcache.Traverse(func(_ string, i [2]interface{}) bool {
		switch {
		case !i[1].(bool): //nolint:forcetypeassert //...
		default:
			db.setState(i[0].(base.State)) //nolint:forcetypeassert //...
		}

		return true
	})
}
