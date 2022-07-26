package isaacdatabase2

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	leveldbstorage2 "github.com/spikeekips/mitum/storage/leveldb2"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/logging"
	leveldbutil "github.com/syndtr/goleveldb/leveldb/util"
)

type Default struct {
	enc                   encoder.Encoder
	perm                  isaac.PermanentDatabase
	newBlockWriteDatabase func(base.Height) (isaac.BlockWriteDatabase, error)
	*logging.Logging
	*util.ContextDaemon
	encs          *encoder.Encoders
	temps         []isaac.TempDatabase
	removed       []isaac.TempDatabase
	mergeInterval time.Duration
	sync.RWMutex
}

// FIXME cache result

func NewDefault(
	st *leveldbstorage2.Storage,
	encs *encoder.Encoders,
	enc encoder.Encoder,
	perm isaac.PermanentDatabase,
	newBlockWriteDatabase func(base.Height) (isaac.BlockWriteDatabase, error),
) (*Default, error) {
	db := &Default{
		Logging: logging.NewLogging(func(lctx zerolog.Context) zerolog.Context {
			return lctx.Str("module", "default-database")
		}),
		encs:                  encs,
		enc:                   enc,
		perm:                  perm,
		newBlockWriteDatabase: newBlockWriteDatabase,
		mergeInterval:         time.Second * 2, //nolint:gomnd //...
	}

	if err := db.load(st); err != nil {
		return nil, err
	}

	db.ContextDaemon = util.NewContextDaemon(db.start)

	return db, nil
}

func (db *Default) SetLogging(l *logging.Logging) *logging.Logging {
	if i, ok := db.perm.(logging.SetLogging); ok {
		_ = i.SetLogging(l)
	}

	return db.Logging.SetLogging(l)
}

func (db *Default) Close() error {
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

func (db *Default) load(st *leveldbstorage2.Storage) error {
	e := util.StringErrorFunc("failed to load temps to DefaultDatabase")

	var last base.Height

	switch m, found, err := db.perm.LastBlockMap(); {
	case err != nil:
		return e(err, "")
	case found:
		last = m.Manifest().Height()
	}

	// NOTE find leveldb directory
	temps, err := loadTemps(st, last, db.encs, db.enc, true)
	if err != nil {
		return e(err, "")
	}

	db.temps = temps

	return nil
}

func (db *Default) LastSuffrageProof() (base.SuffrageProof, bool, error) {
	temps := db.activeTemps()

	for i := range temps {
		switch proof, found, err := temps[i].SuffrageProof(); {
		case err != nil:
			return nil, false, err
		case found:
			return proof, true, nil
		}
	}

	proof, found, err := db.perm.LastSuffrageProof()
	if err != nil {
		return nil, false, err
	}

	return proof, found, nil
}

func (db *Default) SuffrageProof(suffrageHeight base.Height) (base.SuffrageProof, bool, error) {
	e := util.StringErrorFunc("failed to find SuffrageProof by suffrage height")

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

		switch j, found, err := temp.SuffrageProof(); {
		case err != nil:
			return nil, false, e(err, "")
		case found:
			return j, true, nil
		}
	}

	proof, found, err := db.perm.SuffrageProof(suffrageHeight)
	if err != nil {
		return nil, false, e(err, "")
	}

	return proof, found, nil
}

// FIXME cache result from storages

func (db *Default) SuffrageProofByBlockHeight(height base.Height) (base.SuffrageProof, bool, error) {
	e := util.StringErrorFunc("failed to find suffrage by SuffrageProof by block height")

	if height < base.GenesisHeight {
		return nil, false, errors.Errorf("wrong height; < 0")
	}

	temps := db.activeTemps()
	if len(temps) > 0 && height > temps[0].Height() {
		return nil, false, nil
	}

	temph := db.findTemp(height)
	if temph != nil {
		switch i, found, err := temph.SuffrageProof(); {
		case err != nil:
			return nil, false, e(err, "")
		case found:
			return i, true, nil
		}
	}

	lastheight := height

	if temph != nil {
		for i := range temps {
			temp := temps[i]
			if temp.Height() > lastheight {
				continue
			}

			switch j, found, err := temp.SuffrageProof(); {
			case err != nil:
				return nil, false, e(err, "")
			case found:
				return j, true, nil
			}
		}

		lastheight = temps[len(temps)-1].Height() - 1
	}

	proof, found, err := db.perm.SuffrageProofByBlockHeight(lastheight)
	if err != nil {
		return nil, false, e(err, "")
	}

	return proof, found, nil
}

func (db *Default) LastNetworkPolicy() base.NetworkPolicy {
	temps := db.activeTemps()

	for i := range temps {
		if i := temps[i].NetworkPolicy(); i != nil {
			return i
		}
	}

	return db.perm.LastNetworkPolicy()
}

func (db *Default) State(key string) (base.State, bool, error) {
	e := util.StringErrorFunc("failed to find State")

	l := util.EmptyLocked()

	if err := db.dig(func(p isaac.PartialDatabase) (bool, error) {
		switch st, found, err := p.State(key); {
		case err != nil:
			return false, err
		case found:
			_, _ = l.Set(func(old interface{}) (interface{}, error) {
				switch {
				case old == nil:
					return st, nil
				case st.Height() > old.(base.State).Height(): //nolint:forcetypeassert //...
					return st, nil
				default:
					return nil, errors.Errorf("old")
				}
			})
		}

		return true, nil
	}); err != nil {
		return nil, false, e(err, "")
	}

	if i, _ := l.Value(); i != nil {
		return i.(base.State), true, nil //nolint:forcetypeassert //...
	}

	st, found, err := db.perm.State(key)
	if err != nil {
		return nil, false, e(err, "")
	}

	return st, found, nil
}

func (db *Default) ExistsInStateOperation(h util.Hash) (bool, error) { //nolint:dupl //...
	e := util.StringErrorFunc("failed to check operation")

	l := util.NewLocked(false)

	if err := db.dig(func(p isaac.PartialDatabase) (bool, error) {
		switch found, err := p.ExistsInStateOperation(h); {
		case err != nil:
			return false, err
		case found:
			_ = l.SetValue(true)

			return false, nil
		default:
			return true, nil
		}
	}); err != nil {
		return false, e(err, "")
	}

	if i, _ := l.Value(); i != nil {
		return i.(bool), nil //nolint:forcetypeassert //...
	}

	found, err := db.perm.ExistsInStateOperation(h)
	if err != nil {
		return false, e(err, "")
	}

	return found, nil
}

func (db *Default) ExistsKnownOperation(h util.Hash) (bool, error) { //nolint:dupl //...
	e := util.StringErrorFunc("failed to check operation")

	l := util.NewLocked(false)

	if err := db.dig(func(p isaac.PartialDatabase) (bool, error) {
		switch found, err := p.ExistsKnownOperation(h); {
		case err != nil:
			return false, err
		case found:
			_ = l.SetValue(true)

			return false, nil
		default:
			return true, nil
		}
	}); err != nil {
		return false, e(err, "")
	}

	if i, _ := l.Value(); i != nil {
		return i.(bool), nil //nolint:forcetypeassert //...
	}

	found, err := db.perm.ExistsKnownOperation(h)
	if err != nil {
		return false, e(err, "")
	}

	return found, nil
}

func (db *Default) BlockMap(height base.Height) (base.BlockMap, bool, error) {
	switch temps := db.activeTemps(); {
	case len(temps) < 1:
	case temps[0].Height() > height:
		return nil, false, nil
	default:
		if temp := db.findTemp(height); temp != nil {
			m, err := temp.BlockMap()
			if err != nil {
				return nil, false, err
			}

			return m, true, nil
		}
	}

	m, found, err := db.perm.BlockMap(height)

	return m, found, err
}

func (db *Default) LastBlockMap() (base.BlockMap, bool, error) {
	if temps := db.activeTemps(); len(temps) > 0 {
		m, err := temps[0].BlockMap()
		if err != nil {
			return nil, false, err
		}

		return m, true, nil
	}

	m, found, err := db.perm.LastBlockMap()

	return m, found, err
}

func (db *Default) NewBlockWriteDatabase(height base.Height) (isaac.BlockWriteDatabase, error) {
	return db.newBlockWriteDatabase(height)
}

func (db *Default) MergeBlockWriteDatabase(w isaac.BlockWriteDatabase) error {
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
		switch m, found, err := db.perm.LastBlockMap(); {
		case err != nil:
			return e(err, "")
		case found:
			preheight = m.Manifest().Height()
		}
	}

	if preheight > base.NilHeight && temp.Height() != preheight+1 {
		return e(nil, "new TempDatabase has wrong height, %d != %d + 1", temp.Height(), preheight)
	}

	switch {
	case len(db.temps) < 1:
		db.temps = []isaac.TempDatabase{temp}
	default:
		temps := make([]isaac.TempDatabase, len(db.temps)+1)
		temps[0] = temp
		copy(temps[1:], db.temps)

		db.temps = temps
	}

	return nil
}

func (db *Default) MergeAllPermanent() error {
	e := util.StringErrorFunc("failed to merge all temps to permanent")

	for len(db.activeTemps()) > 0 {
		if err := db.mergePermanent(context.Background()); err != nil {
			return e(err, "")
		}

		if err := db.cleanRemoved(0); err != nil {
			return e(err, "")
		}
	}

	return nil
}

func (db *Default) activeTemps() []isaac.TempDatabase {
	db.RLock()
	defer db.RUnlock()

	return db.temps
}

func (db *Default) removeTemp(temp isaac.TempDatabase) error {
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
		return util.ErrNotFound.Errorf("TempDatabase not found, height=%d", temp.Height())
	}

	if len(db.temps) < 2 { //nolint:gomnd //...
		db.temps = nil

		return nil
	}

	temps := make([]isaac.TempDatabase, len(db.temps)-1)
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

func (db *Default) findTemp(height base.Height) isaac.TempDatabase {
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

func (db *Default) dig(f func(isaac.PartialDatabase) (bool, error)) error {
	temps := db.activeTemps()
	partials := make([]isaac.PartialDatabase, len(temps)+1)

	for i := range temps {
		partials[i] = temps[i]
	}

	partials[len(temps)] = db.perm

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	worker := util.NewErrgroupWorker(ctx, int64(len(partials)))
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
			break
		}
	}

	worker.Done()

	switch err := worker.Wait(); {
	case err == nil:
	case errors.Is(err, context.Canceled):
	default:
		return err
	}

	return nil
}

func (db *Default) start(ctx context.Context) error {
	ticker := time.NewTicker(db.mergeInterval)
	defer ticker.Stop()

end:
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			switch err := db.mergePermanent(ctx); {
			case err == nil:
			default:
				db.Log().Debug().Err(err).Msg("failed to merge to permanent database; will retry")

				continue end
			}

			if err := db.cleanRemoved(3); err != nil { //nolint:gomnd //...
				db.Log().Debug().Err(err).Msg("failed to clean temp databases; will retry")

				continue end
			}
		}
	}
}

func (db *Default) mergePermanent(ctx context.Context) error {
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

	db.Log().Debug().Interface("height", temp.Height()).Msg("temp database merged")

	return nil
}

func (db *Default) cleanRemoved(limit int) error {
	db.Lock()
	defer db.Unlock()

	if len(db.removed) <= limit {
		// NOTE last limit temp databases will be kept for safe concurreny
		// access from DefaultDatabase.
		return nil
	}

	temp := db.removed[0]

	removed := make([]isaac.TempDatabase, len(db.removed)-1)
	copy(removed, db.removed[1:])
	db.removed = removed

	if err := temp.Remove(); err != nil {
		return errors.Wrap(err, "failed to clean removed")
	}

	db.Log().Debug().Interface("height", temp.Height()).Msg("temp database removed")

	return nil
}

func loadTemps( // revive:disable-line:flag-parameter
	st *leveldbstorage2.Storage,
	minHeight base.Height,
	encs *encoder.Encoders,
	enc encoder.Encoder,
	clean bool,
) ([]isaac.TempDatabase, error) {
	e := util.StringErrorFunc("failed to load TempDatabase")

	if minHeight <= base.GenesisHeight {
		return nil, nil
	}

	var temps []isaac.TempDatabase
	var prefixes [][]byte

	last := minHeight

end:
	for {
		var lastprefix []byte

		found := base.NilHeight
		r := &leveldbutil.Range{
			Start: emptyPrefixStoragePrefixByHeight(leveldbLabelBlockWrite, last+1), //nolint:gomnd //...
			Limit: emptyPrefixStoragePrefixByHeight(leveldbLabelBlockWrite, last+2), //nolint:gomnd //...
		}
		if err := st.Iter(
			r,
			func(key, _ []byte) (bool, error) {
				h, err := HeightFromPrefixStoragePrefix(key)
				if err != nil {
					return true, nil
				}

				k, err := prefixStoragePrefixFromKey(key)
				if err != nil {
					return true, nil
				}

				lastprefix = k
				found = h

				return false, nil
			},
			false,
		); err != nil {
			return nil, e(err, "")
		}

		switch {
		case lastprefix == nil:
			break end
		case found < base.GenesisHeight:
			break end
		case found != last+1:
			return nil, e(nil, "missing height found, %d", last-1)
		}

		temp, err := NewTempLeveldbFromPrefix(st, lastprefix, encs, enc)
		if err != nil {
			return nil, e(err, "")
		}

		temps = append(temps, temp)
		prefixes = append(prefixes, lastprefix)

		last = found
	}

	if clean {
		for i := range temps {
			if err := cleanOneHeight(st, temps[i].Height(), prefixes[i]); err != nil {
				return nil, e(err, "")
			}
		}

		if err := removeHigherHeights(st, last+1); err != nil {
			return nil, e(err, "")
		}
	}

	sort.Slice(temps, func(i, j int) bool {
		return temps[i].Height() > temps[j].Height()
	})

	return temps, nil
}
