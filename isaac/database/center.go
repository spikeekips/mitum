package isaacdatabase

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	leveldbstorage "github.com/spikeekips/mitum/storage/leveldb"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/logging"
	leveldbutil "github.com/syndtr/goleveldb/leveldb/util"
)

type Center struct {
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

func NewCenter(
	st *leveldbstorage.Storage,
	encs *encoder.Encoders,
	enc encoder.Encoder,
	perm isaac.PermanentDatabase,
	newBlockWriteDatabase func(base.Height) (isaac.BlockWriteDatabase, error),
) (*Center, error) {
	db := &Center{
		Logging: logging.NewLogging(func(lctx zerolog.Context) zerolog.Context {
			return lctx.Str("module", "center-database")
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

func (db *Center) SetLogging(l *logging.Logging) *logging.Logging {
	if i, ok := db.perm.(logging.SetLogging); ok {
		_ = i.SetLogging(l)
	}

	return db.Logging.SetLogging(l)
}

func (db *Center) Close() error {
	e := util.StringErrorFunc("failed to close database")

	// NOTE don't close temps
	//for i := range db.temps {
	//	if err := db.temps[i].Close(); err != nil {
	//		return e(err, "failed to close temp")
	//	}
	//}

	if err := db.perm.Close(); err != nil {
		return e(err, "failed to close PermanentDatabase")
	}

	return nil
}

func (db *Center) load(st *leveldbstorage.Storage) error {
	e := util.StringErrorFunc("failed to load temps to CenterDatabase")

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

func (db *Center) LastSuffrageProof() (base.SuffrageProof, bool, error) {
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

func (db *Center) LastSuffrageProofBytes() (enchint hint.Hint, meta, body []byte, found bool, err error) {
	temps := db.activeTemps()

	for i := range temps {
		switch enchint, meta, body, found, err := temps[i].SuffrageProofBytes(); {
		case err != nil:
			return enchint, nil, nil, false, err
		case found:
			return enchint, meta, body, found, nil
		}
	}

	return db.perm.LastSuffrageProofBytes()
}

func (db *Center) SuffrageProof(suffrageHeight base.Height) (base.SuffrageProof, bool, error) {
	e := util.StringErrorFunc("failed to find SuffrageProof by suffrage height")

	switch _, proof, found, err := db.suffrageProofInTemps(suffrageHeight); {
	case err != nil:
		return nil, false, e(err, "")
	case found:
		return proof, true, nil
	}

	proof, found, err := db.perm.SuffrageProof(suffrageHeight)
	if err != nil {
		return nil, false, e(err, "")
	}

	return proof, found, nil
}

func (db *Center) SuffrageProofBytes(suffrageHeight base.Height) (
	enchint hint.Hint, meta, body []byte, found bool, err error,
) {
	e := util.StringErrorFunc("failed to find SuffrageProof by suffrage height")

	var temp isaac.TempDatabase

	switch temp, _, found, err = db.suffrageProofInTemps(suffrageHeight); {
	case err != nil:
		return enchint, nil, nil, false, e(err, "")
	case found:
		switch enchint, meta, body, found, err = temp.SuffrageProofBytes(); {
		case err != nil:
			return enchint, nil, nil, false, e(err, "")
		case found:
			return enchint, meta, body, found, nil
		}
	}

	enchint, meta, body, found, err = db.perm.SuffrageProofBytes(suffrageHeight)
	if err != nil {
		return enchint, nil, nil, false, e(err, "")
	}

	return enchint, meta, body, found, nil
}

func (db *Center) SuffrageProofByBlockHeight(height base.Height) (base.SuffrageProof, bool, error) {
	e := util.StringErrorFunc("failed to find suffrage by SuffrageProof by block height")

	if height < base.GenesisHeight {
		return nil, false, errors.Errorf("wrong height; < 0")
	}

	lastheight := height

	if temps := db.activeTemps(); len(temps) > 0 {
		if height > temps[0].Height() {
			return nil, false, nil
		}

		if temph := db.findTemp(height); temph != nil {
			switch i, found, err := temph.SuffrageProof(); {
			case err != nil:
				return nil, false, e(err, "")
			case found:
				return i, true, nil
			}

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
	}

	proof, found, err := db.perm.SuffrageProofByBlockHeight(lastheight)
	if err != nil {
		return nil, false, e(err, "")
	}

	return proof, found, nil
}

func (db *Center) LastNetworkPolicy() base.NetworkPolicy {
	temps := db.activeTemps()

	for i := range temps {
		if i := temps[i].NetworkPolicy(); i != nil {
			return i
		}
	}

	return db.perm.LastNetworkPolicy()
}

func (db *Center) State(key string) (base.State, bool, error) {
	e := util.StringErrorFunc("failed to find State")

	l := util.EmptyLocked()

	if err := db.state(key, func(key string, p isaac.TempDatabase) (bool, error) {
		switch st, found, err := p.State(key); {
		case err != nil, !found:
			return false, err
		default:
			_ = l.SetValue(st)

			return true, nil
		}
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

func (db *Center) StateBytes(key string) (ht hint.Hint, _, _ []byte, _ bool, _ error) {
	e := util.StringErrorFunc("failed to find state bytes")

	l := util.EmptyLocked()

	if err := db.state(key, func(key string, p isaac.TempDatabase) (bool, error) {
		switch enchint, meta, body, found, err := p.StateBytes(key); {
		case err != nil, !found:
			return false, err
		default:
			_ = l.SetValue([3]interface{}{enchint, meta, body})

			return true, nil
		}
	}); err != nil {
		return ht, nil, nil, false, e(err, "")
	}

	if i, _ := l.Value(); i != nil {
		j := i.([3]interface{})                                          //nolint:forcetypeassert //...
		return j[0].(hint.Hint), j[1].([]byte), j[2].([]byte), true, nil //nolint:forcetypeassert //...
	}

	enchint, meta, body, found, err := db.perm.StateBytes(key)
	if err != nil {
		return ht, nil, nil, false, e(err, "")
	}

	return enchint, meta, body, found, nil
}

func (db *Center) state(key string, f func(string, isaac.TempDatabase) (bool, error)) error {
	l := util.EmptyLocked()

	return db.dig(func(p isaac.TempDatabase) (bool, error) {
		if _, err := l.Set(func(_ bool, old interface{}) (interface{}, error) {
			if old != nil && p.Height() <= old.(base.Height) { //nolint:forcetypeassert //...
				return nil, util.ErrLockedSetIgnore.Errorf("old")
			}

			switch found, err := f(key, p); {
			case err != nil:
				return nil, err
			case found:
				return p.Height(), nil
			default:
				return nil, util.ErrLockedSetIgnore.Errorf("not found")
			}
		}); err != nil {
			return false, err
		}

		return true, nil
	})
}

func (db *Center) ExistsInStateOperation(h util.Hash) (bool, error) { //nolint:dupl //...
	e := util.StringErrorFunc("failed to check operation")

	l := util.EmptyLocked()

	if err := db.dig(func(p isaac.TempDatabase) (bool, error) {
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

func (db *Center) ExistsKnownOperation(h util.Hash) (bool, error) { //nolint:dupl //...
	e := util.StringErrorFunc("failed to check operation")

	l := util.EmptyLocked()

	if err := db.dig(func(p isaac.TempDatabase) (bool, error) {
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

func (db *Center) BlockMap(height base.Height) (base.BlockMap, bool, error) {
	switch temps := db.activeTemps(); {
	case len(temps) < 1:
	case temps[0].Height() < height:
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

	return db.perm.BlockMap(height)
}

func (db *Center) BlockMapBytes(height base.Height) (enchint hint.Hint, meta, body []byte, found bool, err error) {
	switch temps := db.activeTemps(); {
	case len(temps) < 1:
	case temps[0].Height() < height:
		return enchint, nil, nil, false, nil
	default:
		if temp := db.findTemp(height); temp != nil {
			enchint, meta, body, err := temp.BlockMapBytes()
			if err != nil {
				return enchint, nil, nil, false, err
			}

			return enchint, meta, body, found, err
		}
	}

	return db.perm.BlockMapBytes(height)
}

func (db *Center) LastBlockMap() (base.BlockMap, bool, error) {
	if temps := db.activeTemps(); len(temps) > 0 {
		m, err := temps[0].BlockMap()
		if err != nil {
			return nil, false, err
		}

		return m, true, nil
	}

	return db.perm.LastBlockMap()
}

func (db *Center) LastBlockMapBytes() (enchint hint.Hint, meta, body []byte, found bool, err error) {
	if temps := db.activeTemps(); len(temps) > 0 {
		enchint, meta, body, err = temps[0].BlockMapBytes()

		return enchint, meta, body, err != nil, err
	}

	return db.perm.LastBlockMapBytes()
}

func (db *Center) NewBlockWriteDatabase(height base.Height) (isaac.BlockWriteDatabase, error) {
	return db.newBlockWriteDatabase(height)
}

func (db *Center) MergeBlockWriteDatabase(w isaac.BlockWriteDatabase) error {
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

func (db *Center) MergeAllPermanent() error {
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

func (db *Center) activeTemps() []isaac.TempDatabase {
	db.RLock()
	defer db.RUnlock()

	temps := make([]isaac.TempDatabase, len(db.temps))
	copy(temps, db.temps)

	return temps
}

func (db *Center) removeTemp(temp isaac.TempDatabase) error {
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

func (db *Center) findTemp(height base.Height) isaac.TempDatabase {
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

func (db *Center) dig(f func(isaac.TempDatabase) (bool, error)) error {
	temps := db.activeTemps()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	worker := util.NewErrgroupWorker(ctx, int64(len(temps)))
	defer worker.Close()

	for i := range temps {
		p := temps[i]

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

func (db *Center) start(ctx context.Context) error {
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

func (db *Center) mergePermanent(ctx context.Context) error {
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

func (db *Center) cleanRemoved(limit int) error {
	db.Lock()
	defer db.Unlock()

	if len(db.removed) <= limit {
		// NOTE last limit temp databases will be kept for safe concurreny
		// access from CenterDatabase.
		return nil
	}

	temp := db.removed[0]

	removed := make([]isaac.TempDatabase, len(db.removed)-1)
	copy(removed, db.removed[1:])
	db.removed = removed

	height := temp.Height()

	if err := temp.Remove(); err != nil {
		return errors.Wrap(err, "failed to clean removed")
	}

	db.Log().Debug().Interface("height", height).Msg("temp database removed")

	return nil
}

func loadTemps( // revive:disable-line:flag-parameter
	st *leveldbstorage.Storage,
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

func (db *Center) suffrageProofInTemps(suffrageHeight base.Height) (
	isaac.TempDatabase, base.SuffrageProof, bool, error,
) {
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
			return nil, nil, false, err
		case found:
			return temp, j, true, nil
		}
	}

	return nil, nil, false, nil
}
