package isaacdatabase

import (
	"sync"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
)

type MemTempSyncPool struct {
	*util.LockedObjectPool
	sync.RWMutex
}

func NewMemTempSyncPool() *MemTempSyncPool {
	return &MemTempSyncPool{
		LockedObjectPool: util.NewLockedObjectPool(),
	}
}

func (db *MemTempSyncPool) BlockMap(height base.Height) (base.BlockMap, bool, error) {
	db.RLock()
	defer db.RUnlock()

	if db.LockedObjectPool == nil {
		return nil, false, nil
	}

	switch i, found := db.LockedObjectPool.Get(height.String()); {
	case !found, i == nil:
		return nil, false, nil
	default:
		m, ok := i.(base.BlockMap)
		if !ok {
			return nil, false, errors.Errorf("expected BlockMap, but %T", i)
		}

		return m, true, nil
	}
}

func (db *MemTempSyncPool) SetBlockMap(m base.BlockMap) error {
	db.RLock()
	defer db.RUnlock()

	if db.LockedObjectPool == nil {
		return nil
	}

	db.LockedObjectPool.Set(m.Manifest().Height().String(), m)

	return nil
}

func (db *MemTempSyncPool) Close() error {
	db.Lock()
	defer db.Unlock()

	db.LockedObjectPool = nil

	return nil
}

func (db *MemTempSyncPool) Cancel() error {
	return db.Close()
}
