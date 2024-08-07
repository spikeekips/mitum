package isaacdatabase

import (
	"sync"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
)

type basePermanent struct {
	lenc                  *util.Locked[string]             // NOTE encoder of last blockmap
	mp                    *util.Locked[[3]interface{}]     // NOTE last blockmap
	policy                *util.Locked[base.NetworkPolicy] // NOTE last NetworkPolicy
	proof                 *util.Locked[[3]interface{}]     // NOTE last SuffrageProof
	stcache               util.GCache[string, base.State]
	instateoperationcache util.GCache[string, bool]
	stateFromCacheFunc    func(string) (base.State, bool, error)
	setStateToCacheFunc   func(base.State)
}

func newBasePermanent(cachesize int) *basePermanent {
	statef := func(string) (base.State, bool, error) { return nil, false, nil }
	setStatef := func(base.State) {}

	var stcache util.GCache[string, base.State]
	var instateoperationcache util.GCache[string, bool]

	if cachesize > 0 {
		stcache = util.NewLFUGCache[string, base.State](cachesize)
		instateoperationcache = util.NewLFUGCache[string, bool](cachesize)

		statef, setStatef = newStateCacheFuncs(stcache)
	}

	return &basePermanent{
		lenc:                  util.EmptyLocked[string](),
		mp:                    util.EmptyLocked[[3]interface{}](),
		policy:                util.EmptyLocked[base.NetworkPolicy](),
		proof:                 util.EmptyLocked[[3]interface{}](),
		stcache:               stcache,
		instateoperationcache: instateoperationcache,
		stateFromCacheFunc:    statef,
		setStateToCacheFunc:   setStatef,
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

func (db *basePermanent) stateFromCache(key string) (base.State, bool, error) {
	if db.stcache == nil {
		return nil, false, nil
	}

	return db.stateFromCacheFunc(key)
}

func (db *basePermanent) setStateToCache(st base.State) {
	if db.stcache == nil {
		return
	}

	db.setStateToCacheFunc(st)
}

func (db *basePermanent) removeStateFromCache(stateKey string) {
	if db.stcache == nil {
		return
	}

	db.stcache.Remove(stateKey)
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

func (db *basePermanent) mergeTempCaches(
	stcache util.GCache[string, [2]interface{}],
	instateoperationcache util.LockedMap[string, bool],
) {
	if stcache != nil {
		stcache.Traverse(func(_ string, i [2]interface{}) bool {
			switch {
			case !i[1].(bool): //nolint:forcetypeassert //...
			default:
				db.setStateToCache(i[0].(base.State)) //nolint:forcetypeassert //...
			}

			return true
		})
	}

	if instateoperationcache != nil && db.instateoperationcache != nil {
		instateoperationcache.Traverse(func(key string, found bool) bool {
			if !found {
				return true
			}

			db.instateoperationcache.Set(key, true, 0)

			return true
		})
	}
}

func newStateCacheFuncs(stcache util.GCache[string, base.State]) (
	func(string) (base.State, bool, error),
	func(base.State),
) {
	var lock sync.RWMutex

	return func(stateKey string) (base.State, bool, error) {
			lock.RLock()
			defer lock.RUnlock()

			st, found := stcache.Get(stateKey)

			return st, found, nil
		},
		func(st base.State) {
			lock.Lock()
			defer lock.Unlock()

			switch prev, found := stcache.Get(st.Key()); {
			case found && prev.Height() >= st.Height():
				return
			default:
				stcache.Set(st.Key(), st, 0)
			}
		}
}
