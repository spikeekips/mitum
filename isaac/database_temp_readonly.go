package isaac

import (
	"sync"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/storage"
	leveldbstorage "github.com/spikeekips/mitum/storage/leveldb"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/valuehash"
)

type TempRODatabase struct {
	sync.Mutex
	st     *leveldbstorage.ReadonlyStorage
	encs   *encoder.Encoders
	enc    encoder.Encoder
	m      base.Manifest // NOTE last manifest
	sufstt base.State    // NOTE last suffrage state
}

func newTempRODatabase(f string, encs *encoder.Encoders, enc encoder.Encoder) (*TempRODatabase, error) {
	st, err := leveldbstorage.NewReadonlyStorage(f)
	if err != nil {
		return nil, errors.Wrap(err, "failed new TempRODatabase")
	}

	db := &TempRODatabase{st: st, encs: encs, enc: enc}

	if err := db.loadManifests(); err != nil {
		return nil, errors.Wrap(err, "")
	}

	if err := db.loadSuffrages(); err != nil {
		return nil, errors.Wrap(err, "")
	}

	return db, nil
}

func newTempRODatabaseFromWOStorage(wst *TempWODatabase) (*TempRODatabase, error) {
	st := leveldbstorage.NewReadonlyStorageFromWrite(wst.st)

	e := util.StringErrorFunc("failed new TempRODatabase from TempWODatabase")
	if wst.m == nil {
		return nil, e(nil, "empty manifest in TempWODatabase")
	}

	return &TempRODatabase{
		st:     st,
		encs:   wst.encs,
		enc:    wst.enc,
		m:      wst.m,
		sufstt: wst.sufstt,
	}, nil
}

func (db *TempRODatabase) Close() error {
	db.Lock()
	defer db.Unlock()

	if err := db.st.Close(); err != nil {
		return errors.Wrap(err, "failed to close TempRODatabase")
	}

	return nil
}

func (db *TempRODatabase) Remove() error {
	db.Lock()
	defer db.Unlock()

	if err := db.st.Close(); err != nil {
		return errors.Wrap(err, "failed to close TempWODatabase")
	}

	if err := db.st.Remove(); err != nil {
		return errors.Wrap(err, "failed to remove TempRODatabase")
	}

	return nil
}

func (db *TempRODatabase) LastManifest() (base.Manifest, bool, error) {
	if db.m == nil {
		return nil, false, nil
	}

	return db.m, true, nil
}

func (db *TempRODatabase) Manifest(height base.Height) (base.Manifest, bool, error) {
	if db.m.Height() == height {
		return db.m, true, nil
	}

	return nil, false, nil
}

func (db *TempRODatabase) LastSuffrage() (base.State, bool, error) {
	if db.sufstt == nil {
		return nil, false, nil
	}

	return db.sufstt, true, nil
}

func (db *TempRODatabase) Suffrage(suffrageHeight base.Height) (base.State, bool, error) {
	switch {
	case db.sufstt == nil:
		return nil, false, nil
	case db.sufstt.Height() != suffrageHeight:
		return nil, false, nil
	default:
		return db.sufstt, true, nil
	}
}

func (db *TempRODatabase) State(h util.Hash) (base.State, bool, error) {
	e := util.StringErrorFunc("failed to get state")

	switch b, found, err := db.st.Get(stateKey(h)); {
	case err != nil:
		return nil, false, e(err, "")
	case !found:
		return nil, false, nil
	default:
		i, err := db.loadState(b)
		if err != nil {
			return nil, false, e(err, "")
		}

		return i, true, nil
	}
}

func (db *TempRODatabase) ExistsOperation(h util.Hash) (bool, error) {
	switch found, err := db.st.Exists(operationKey(h)); {
	case err == nil:
		return found, nil
	default:
		return false, errors.Wrap(err, "failed to check exists operation")
	}
}

func (db *TempRODatabase) loadManifests() error {
	e := util.StringErrorFunc("failed to load manifest")

	switch b, found, err := db.st.Get(manifestKey()); {
	case err != nil:
		return e(err, "")
	case !found:
		return e(err, "manifest not found")
	default:
		m, err := db.loadManifest(b)
		if err != nil {
			return e(err, "")
		}

		db.m = m

		return nil
	}
}

func (db *TempRODatabase) loadSuffrages() error {
	e := util.StringErrorFunc("failed to load suffrage state")

	var h util.Hash
	switch b, found, err := db.st.Get(suffrageKey()); {
	case err != nil:
		return e(err, "")
	case !found:
		return nil
	default:
		h = valuehash.NewBytes(b)
	}

	switch b, found, err := db.st.Get(stateKey(h)); {
	case err != nil:
		return e(err, "")
	case !found:
		return e(nil, "suffrage state not found")
	default:
		st, err := db.loadSuffrage(b)
		if err != nil {
			return e(err, "")
		}

		db.sufstt = st

		return nil
	}
}

func (db *TempRODatabase) loadManifest(b []byte) (base.Manifest, error) {
	if b == nil {
		return nil, nil
	}

	e := util.StringErrorFunc("failed to load manifest")

	hinter, err := db.loadHinter(b)
	switch {
	case err != nil:
		return nil, e(err, "")
	case hinter == nil:
		return nil, e(nil, "empty manifest")
	}

	switch i, ok := hinter.(base.Manifest); {
	case !ok:
		return nil, e(nil, "not manifest: %T", hinter)
	default:
		return i, nil
	}
}

func (db *TempRODatabase) loadSuffrage(b []byte) (base.State, error) {
	e := util.StringErrorFunc("failed to load suffrage")

	switch i, err := db.loadState(b); {
	case err != nil:
		return nil, e(err, "failed to load suffrage state")
	case i.Value() == nil:
		return nil, storage.NotFoundError.Errorf("state value not found")
	default:
		if _, ok := i.Value().(base.SuffrageStateValue); !ok {
			return nil, e(nil, "not suffrage state value: %T", i.Value())
		}

		return i, nil
	}
}

func (db *TempRODatabase) loadState(b []byte) (base.State, error) {
	if b == nil {
		return nil, nil
	}

	e := util.StringErrorFunc("failed to load state")

	hinter, err := db.loadHinter(b)
	switch {
	case err != nil:
		return nil, e(err, "")
	case hinter == nil:
		return nil, nil
	}

	switch i, ok := hinter.(base.State); {
	case !ok:
		return nil, e(nil, "not suffrage state: %T", hinter)
	default:
		return i, nil
	}
}

func (db *TempRODatabase) loadHinter(b []byte) (interface{}, error) {
	if b == nil {
		return nil, nil
	}

	var ht hint.Hint
	ht, raw, err := loadHint(b)
	if err != nil {
		return nil, err
	}

	switch i := db.encs.Find(ht); {
	case i == nil:
		return nil, util.NotFoundError.Errorf("encoder not found for %q", ht)
	default:
		return i.(encoder.Encoder).Decode(raw)
	}
}
