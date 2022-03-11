package isaac

import (
	"context"
	"math"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
)

var RetryMergeToPermanentDatabaseError = util.NewError("failed to merge to permanent database; retry")

// Database serves block data like manifest, states and operations from
// TempDatabases and PermanentDatabase. It has several TempDatabases and one
// PermanentDatabase.
type Database interface {
	util.Daemon
	Close() error
	Manifest(height base.Height) (base.Manifest, bool, error)
	LastManifest() (base.Manifest, bool, error)
	Suffrage(suffrageHeight base.Height) (base.State, bool, error)
	LastSuffrage() (base.State, bool, error)
	State(key string) (base.State, bool, error)
	ExistsOperation(operationFactHash util.Hash) (bool, error)
	NewBlockWriteDatabase(height base.Height) (BlockWriteDatabase, error)
	MergeBlockWriteDatabase(BlockWriteDatabase) error
}

type PartialDatabase interface {
	State(key string) (base.State, bool, error)
	ExistsOperation(operationFactHash util.Hash) (bool, error)
}

// TempDatabase is the temporary database; it contains only manifest and
// others of one block for storing block data fast.
type TempDatabase interface {
	PartialDatabase
	Close() error
	Remove() error
	Height() base.Height
	SuffrageHeight() base.Height
	Manifest() (base.Manifest, error)
	Suffrage() (base.State, bool, error)
	States(func(base.State) (bool, error)) error
}

type BlockWriteDatabase interface {
	Close() error
	Cancel() error
	Manifest() (base.Manifest, error)
	SetManifest(m base.Manifest) error
	SetStates(sts []base.State) error
	SetOperations(ops []util.Hash) error
	Write() error
	TempDatabase() (TempDatabase, error)
}

// PermanentDatabase stores block data permanently.
type PermanentDatabase interface {
	PartialDatabase
	Close() error
	LastManifest() (base.Manifest, bool, error)
	Manifest(base.Height) (base.Manifest, bool, error)
	LastSuffrage() (base.State, bool, error)
	Suffrage(suffrageHeight base.Height) (base.State, bool, error)
	MergeTempDatabase(TempDatabase) error
}

type DefaultDatabase struct {
	sync.RWMutex
	*logging.Logging
	*util.ContextDaemon
	perm                  PermanentDatabase
	newBlockWriteDatabase func(base.Height) (BlockWriteDatabase, error)
	temps                 []TempDatabase // NOTE higher height will be prior
	removed               []TempDatabase
	mergeInterval         time.Duration
}

func NewDefaultDatabase(
	perm PermanentDatabase,
	newBlockWriteDatabase func(base.Height) (BlockWriteDatabase, error),
) *DefaultDatabase {
	db := &DefaultDatabase{
		Logging: logging.NewLogging(func(lctx zerolog.Context) zerolog.Context {
			return lctx.Str("module", "default-database")
		}),
		perm:                  perm,
		newBlockWriteDatabase: newBlockWriteDatabase,
		mergeInterval:         time.Second * 10,
	}

	db.ContextDaemon = util.NewContextDaemon("default-database", db.start)

	return db
}

func (db *DefaultDatabase) SetLogging(l *logging.Logging) *logging.Logging {
	_ = db.ContextDaemon.SetLogging(l)

	return db.Logging.SetLogging(l)
}

func (db *DefaultDatabase) Close() error {
	e := util.StringErrorFunc("failed to close database")
	for i := range db.temps {
		if err := db.temps[i].Close(); err != nil {
			return e(err, "failed to close temp")
		}
	}

	if err := db.perm.Close(); err != nil {
		return e(err, "failed to close PermanentDatabase")
	}

	return nil
}

func (db *DefaultDatabase) Manifest(height base.Height) (base.Manifest, bool, error) {
	if temp := db.findTemp(height); temp != nil {
		m, err := temp.Manifest()
		if err != nil {
			return nil, false, err
		}

		return m, true, nil
	}

	return db.perm.Manifest(height)
}

func (db *DefaultDatabase) LastManifest() (base.Manifest, bool, error) {
	if temps := db.activeTemps(); len(temps) > 0 {
		m, err := temps[0].Manifest()
		if err != nil {
			return nil, false, err
		}

		return m, true, nil
	}

	return db.perm.LastManifest()
}

func (db *DefaultDatabase) Suffrage(suffrageHeight base.Height) (base.State, bool, error) {
	temps := db.activeTemps()

end:
	for i := range temps {
		var st base.State
		var v base.SuffrageStateValue
		switch j, found, err := temps[i].Suffrage(); {
		case err != nil:
			return nil, false, err
		case found:
			st = j
			v = j.Value().(base.SuffrageStateValue)
		}

		switch {
		case v == nil:
			continue end
		case v.Height() == suffrageHeight:
			return st, true, nil
		case v.Height() < suffrageHeight:
			return nil, false, nil
		}
	}

	return db.perm.Suffrage(suffrageHeight)
}

func (db *DefaultDatabase) LastSuffrage() (base.State, bool, error) {
	temps := db.activeTemps()

	for i := range temps {
		switch i, found, err := temps[i].Suffrage(); {
		case err != nil:
			return nil, false, err
		case found:
			return i, true, nil
		}
	}

	return db.perm.LastSuffrage()
}

func (db *DefaultDatabase) State(key string) (base.State, bool, error) {
	l := util.NewLocked(nil)
	if err := db.dig(func(p PartialDatabase) (bool, error) {
		switch st, found, err := p.State(key); {
		case err != nil:
			return false, err
		case found:
			_ = l.Set(func(old interface{}) (interface{}, error) {
				switch {
				case old == nil:
					return st, nil
				case st.Height() > old.(base.State).Height():
					return st, nil
				default:
					return nil, errors.Errorf("old")
				}
			})
		}

		return true, nil
	}); err != nil {
		return nil, false, errors.Wrap(err, "failed to find State")
	}

	switch {
	case l.Value() == nil:
		return nil, false, nil
	default:
		return l.Value().(base.State), true, nil
	}
}

func (db *DefaultDatabase) ExistsOperation(h util.Hash) (bool, error) {
	l := util.NewLocked(false)
	if err := db.dig(func(p PartialDatabase) (bool, error) {
		switch found, err := p.ExistsOperation(h); {
		case err != nil:
			return false, err
		case found:
			l.SetValue(true)

			return false, nil
		default:
			return true, nil
		}
	}); err != nil {
		return false, errors.Wrap(err, "failed to check ExistsOperation")
	}

	if l.Value().(bool) {
		return true, nil
	}

	return false, nil
}

func (db *DefaultDatabase) NewBlockWriteDatabase(height base.Height) (BlockWriteDatabase, error) {
	return db.newBlockWriteDatabase(height)
}

func (db *DefaultDatabase) MergeBlockWriteDatabase(w BlockWriteDatabase) error {
	db.Lock()
	defer db.Unlock()

	e := util.StringErrorFunc("failed to merge new TempDatabase")
	temp, err := w.TempDatabase()
	if err != nil {
		return e(err, "")
	}

	preheight := base.NilHeight
	switch {
	case len(db.temps) > 0:
		preheight = db.temps[0].Height()
	default:
		switch m, found, err := db.perm.LastManifest(); {
		case err != nil:
			return e(err, "")
		case found:
			preheight = m.Height()
		}
	}

	if preheight > base.NilHeight && temp.Height() != preheight+1 {
		return e(nil, "new TempDatabase has wrong height, %d != %d + 1", temp.Height(), preheight)
	}

	switch {
	case len(db.temps) < 1:
		db.temps = []TempDatabase{temp}
	default:
		temps := make([]TempDatabase, len(db.temps)+1)
		temps[0] = temp
		copy(temps[1:], db.temps)

		db.temps = temps
	}

	return nil
}

func (db *DefaultDatabase) activeTemps() []TempDatabase {
	db.RLock()
	defer db.RUnlock()

	return db.temps
}

func (db *DefaultDatabase) removeTemp(temp TempDatabase) error {
	db.Lock()
	defer db.Unlock()

	db.removed = append(db.removed, temp)

	if len(db.temps) < 1 {
		return nil
	}

	var found int64 = -1
	if l := len(db.temps); l > 0 {
		found = (db.temps[0].Height() - temp.Height()).Int64()
	}

	if found < 0 || found >= int64(len(db.temps)) {
		return util.NotFoundError.Errorf("TempDatabase not found, height=%d", temp.Height())
	}

	if len(db.temps) < 2 {
		db.temps = nil

		return nil
	}

	temps := make([]TempDatabase, len(db.temps)-1)
	var j int
	for i := range db.temps {
		if int64(i) == found {
			continue
		}

		temps[j] = db.temps[i]
		j++
	}

	db.temps = temps

	return nil
}

func (db *DefaultDatabase) findTemp(height base.Height) TempDatabase {
	db.RLock()
	defer db.RUnlock()

	var found int64 = -1
	if l := len(db.temps); l > 0 {
		found = (db.temps[0].Height() - height).Int64()
	}

	if found >= 0 && found < int64(len(db.temps)) {
		return db.temps[found]
	}

	return nil
}

func (db *DefaultDatabase) dig(f func(PartialDatabase) (bool, error)) error {
	temps := db.activeTemps()
	partials := make([]PartialDatabase, len(temps)+1)
	for i := range temps {
		partials[i] = temps[i]
	}

	partials[len(temps)] = db.perm

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	worker := util.NewErrgroupWorker(ctx, math.MaxInt32)
	defer worker.Close()

	for i := range partials {
		p := partials[i]
		if err := worker.NewJob(func(context.Context, uint64) error {
			switch keep, err := f(p); {
			case err != nil:
				return err
			case !keep:
				cancel()
			}

			return nil
		}); err != nil {
			return err
		}
	}
	worker.Done()

	switch err := worker.Wait(); {
	case err == nil:
	case errors.Is(err, context.Canceled):
	default:
		return errors.Wrap(err, "")
	}

	return nil
}

func (db *DefaultDatabase) start(ctx context.Context) error {
	ticker := time.NewTicker(db.mergeInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			switch err := db.mergePermanent(); {
			case err == nil:
			case errors.Is(err, RetryMergeToPermanentDatabaseError):
				db.Log().Debug().Err(err).Msg("failed to merge to permanent database; retry")
			default:
				return errors.Wrap(err, "")
			}

			if err := db.cleanRemoved(); err != nil {
				return errors.Wrap(err, "")
			}
		}
	}
}

func (db *DefaultDatabase) mergePermanent() error {
	e := util.StringErrorFunc("failed to merge to permanent database")

	temps := db.activeTemps()
	if len(temps) < 1 {
		return nil
	}

	temp := temps[len(temps)-1]
	if err := db.perm.MergeTempDatabase(temp); err != nil {
		return e(err, "")
	}

	if err := db.removeTemp(temp); err != nil {
		return e(err, "")
	}

	return nil
}

func (db *DefaultDatabase) cleanRemoved() error {
	db.Lock()
	defer db.Unlock()

	if len(db.removed) < 3 {
		// NOTE last 3 temp databases will be kept for safe concurreny access from DefaultDatabase.
		return nil
	}

	temp := db.removed[0]

	removed := make([]TempDatabase, len(db.removed)-1)
	copy(removed, db.removed[1:])
	db.removed = removed

	if err := temp.Remove(); err != nil {
		return errors.Wrap(err, "failed to clean removed")
	}

	return nil
}
