package isaacdatabase

import (
	"sync"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
)

type MemTempSyncPool struct {
	pool util.LockedMap[base.Height, base.BlockMap]
	l    sync.RWMutex
}

func NewMemTempSyncPool() *MemTempSyncPool {
	pool, _ := util.NewLockedMap[base.Height, base.BlockMap](1<<13, nil) //nolint:mnd //...

	return &MemTempSyncPool{pool: pool}
}

func (db *MemTempSyncPool) BlockMap(height base.Height) (base.BlockMap, bool, error) {
	db.l.RLock()
	defer db.l.RUnlock()

	if db.pool == nil {
		return nil, false, nil
	}

	switch i, found := db.pool.Value(height); {
	case !found, i == nil:
		return nil, false, nil
	default:
		return i, true, nil
	}
}

func (db *MemTempSyncPool) SetBlockMap(m base.BlockMap) error {
	db.l.RLock()
	defer db.l.RUnlock()

	if db.pool == nil {
		return nil
	}

	_ = db.pool.SetValue(m.Manifest().Height(), m)

	return nil
}

func (db *MemTempSyncPool) Close() error {
	db.l.Lock()
	defer db.l.Unlock()

	if db.pool == nil {
		return nil
	}

	db.pool.Close()
	db.pool = nil

	return nil
}

func (db *MemTempSyncPool) Cancel() error {
	return db.Close()
}
