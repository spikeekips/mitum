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
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/logging"
)

type DefaultDatabase struct {
	sync.RWMutex
	*logging.Logging
	*util.ContextDaemon
	encs                  *encoder.Encoders
	enc                   encoder.Encoder
	perm                  PermanentDatabase
	newBlockWriteDatabase func(base.Height) (BlockWriteDatabase, error)
	temps                 []TempDatabase // NOTE higher height will be prior
	removed               []TempDatabase
	mergeInterval         time.Duration
	// BLOCK integrate TempPoolDatabase
}

func NewDefaultDatabase(
	root string,
	encs *encoder.Encoders,
	enc encoder.Encoder,
	perm PermanentDatabase,
	newBlockWriteDatabase func(base.Height) (BlockWriteDatabase, error),
) (*DefaultDatabase, error) {
	db := &DefaultDatabase{
		Logging: logging.NewLogging(func(lctx zerolog.Context) zerolog.Context {
			return lctx.Str("module", "default-database")
		}),
		encs:                  encs,
		enc:                   enc,
		perm:                  perm,
		newBlockWriteDatabase: newBlockWriteDatabase,
		mergeInterval:         time.Second * 10,
	}

	if err := db.load(root); err != nil {
		return nil, errors.Wrap(err, "")
	}

	db.ContextDaemon = util.NewContextDaemon("default-database", db.start)

	return db, nil
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

func (db *DefaultDatabase) load(root string) error {
	e := util.StringErrorFunc("failed to load temps to DefaultDatabase")

	var last base.Height
	switch m, found, err := db.perm.LastManifest(); {
	case err != nil:
		return e(err, "")
	case found:
		last = m.Height()
	}

	// NOTE find leveldb directory
	temps, err := loadTempDatabases(root, last, db.encs, db.enc, true)
	if err != nil {
		return e(err, "")
	}

	db.temps = temps

	return nil
}

func (db *DefaultDatabase) Manifest(height base.Height) (base.Manifest, bool, error) {
	switch temps := db.activeTemps(); {
	case len(temps) < 1:
	case temps[0].Height() > height:
		return nil, false, nil
	default:
		if temp := db.findTemp(height); temp != nil {
			m, err := temp.Manifest()
			if err != nil {
				return nil, false, err
			}

			return m, true, nil
		}

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

func (db *DefaultDatabase) SuffrageByHeight(suffrageHeight base.Height) (base.State, bool, error) {
	e := util.StringErrorFunc("failed to find suffrage by suffrage height")

	temps := db.activeTemps()

end:
	for i := range temps {
		temp := temps[i]
		switch sh := temp.SuffrageHeight(); {
		case sh == base.NilHeight:
			continue end
		case sh > suffrageHeight:
			continue end
		}

		switch j, found, err := temp.Suffrage(); {
		case err != nil:
			return nil, false, e(err, "")
		case found:
			return j, true, nil
		}
	}

	st, found, err := db.perm.SuffrageByHeight(suffrageHeight)
	if err != nil {
		return nil, false, e(err, "")
	}

	return st, found, nil
}

func (db *DefaultDatabase) Suffrage(height base.Height) (base.State, bool, error) {
	e := util.StringErrorFunc("failed to find suffrage by block height")

	temps := db.activeTemps()
	if len(temps) > 0 && height > temps[0].Height() {
		return nil, false, nil
	}

	temph := db.findTemp(height)
	if temph != nil {
		switch i, found, err := temph.Suffrage(); {
		case err != nil:
			return nil, false, e(err, "")
		case found:
			return i, true, nil
		}
	}

	if temph != nil {
		for i := range temps {
			temp := temps[i]
			if temp.Height() > height {
				continue
			}

			switch j, found, err := temp.Suffrage(); {
			case err != nil:
				return nil, false, err
			case found:
				return j, true, nil
			}
		}
	}

	st, found, err := db.perm.Suffrage(height)
	if err != nil {
		return nil, false, e(err, "")
	}

	return st, found, nil
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

	var st base.State
	_ = l.Value(&st)

	return st, st != nil, nil
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

	var found bool
	_ = l.Value(&found)

	return found, nil
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
			switch err := db.mergePermanent(ctx); {
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

func (db *DefaultDatabase) mergePermanent(ctx context.Context) error {
	e := util.StringErrorFunc("failed to merge to permanent database")

	temps := db.activeTemps()
	if len(temps) < 1 {
		return nil
	}

	temp := temps[len(temps)-1]
	if err := db.perm.MergeTempDatabase(ctx, temp); err != nil {
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
