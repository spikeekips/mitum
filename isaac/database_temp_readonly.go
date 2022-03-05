package isaac

import (
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/storage"
	leveldbstorage "github.com/spikeekips/mitum/storage/leveldb"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/valuehash"
)

type TempRODatabase struct {
	*baseDatabase
	st     *leveldbstorage.ReadonlyStorage
	m      base.Manifest // NOTE last manifest
	sufstt base.State    // NOTE last suffrage state
}

func NewTempRODatabase(f string, encs *encoder.Encoders, enc encoder.Encoder) (*TempRODatabase, error) {
	e := util.StringErrorFunc("failed to open TempRODatabase")

	st, err := leveldbstorage.NewReadonlyStorage(f)
	if err != nil {
		return nil, e(err, "")
	}

	db, err := newTempRODatabase(st, encs, enc)
	if err != nil {
		return nil, e(err, "")
	}

	return db, nil
}

func newTempRODatabase(
	st *leveldbstorage.ReadonlyStorage,
	encs *encoder.Encoders,
	enc encoder.Encoder,
) (*TempRODatabase, error) {
	db := &TempRODatabase{
		baseDatabase: newBaseDatabase(st, encs, enc),
		st:           st,
	}

	if err := db.loadManifest(); err != nil {
		return nil, err
	}

	if err := db.loadSuffrage(); err != nil {
		return nil, err
	}

	return db, nil
}

func newTempRODatabaseFromWOStorage(wst *TempWODatabase) (*TempRODatabase, error) {
	e := util.StringErrorFunc("failed new TempRODatabase from TempWODatabase")
	st, err := leveldbstorage.NewReadonlyStorageFromWrite(wst.st)
	if err != nil {
		return nil, e(err, "")
	}

	if wst.m == nil {
		return nil, e(nil, "empty manifest in TempWODatabase")
	}

	return &TempRODatabase{
		baseDatabase: newBaseDatabase(st, wst.encs, wst.enc),
		st:           st,
		m:            wst.m,
		sufstt:       wst.sufstt,
	}, nil
}

func (db *TempRODatabase) Height() (manifestHeight base.Height, suffrageHeight base.Height) {
	sh := base.NilHeight
	if db.m == nil {
		return base.NilHeight, sh
	}

	if db.sufstt != nil {
		sh = db.sufstt.Value().(base.SuffrageStateValue).Height()
	}

	return db.m.Height(), sh
}

func (db *TempRODatabase) Manifest() (base.Manifest, error) {
	if db.m == nil {
		return nil, storage.NotFoundError.Errorf("manifest not found")
	}

	return db.m, nil
}

func (db *TempRODatabase) Suffrage() (base.State, bool, error) {
	if db.sufstt == nil {
		return nil, false, nil
	}

	return db.sufstt, true, nil
}

func (db *TempRODatabase) State(h util.Hash) (base.State, bool, error) {
	e := util.StringErrorFunc("failed to get state")

	switch b, found, err := db.st.Get(stateDBKey(h)); {
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
	switch found, err := db.st.Exists(operationDBKey(h)); {
	case err == nil:
		return found, nil
	default:
		return false, errors.Wrap(err, "failed to check exists operation")
	}
}

func (db *TempRODatabase) loadManifest() error {
	e := util.StringErrorFunc("failed to load manifest")

	switch b, found, err := db.st.Get(manifestDBKey()); {
	case err != nil:
		return e(err, "")
	case !found:
		return e(err, "manifest not found")
	default:
		m, err := db.decodeManifest(b)
		if err != nil {
			return e(err, "")
		}

		db.m = m

		return nil
	}
}

func (db *TempRODatabase) loadSuffrage() error {
	e := util.StringErrorFunc("failed to load suffrage state")

	var h util.Hash
	switch b, found, err := db.st.Get(suffrageDBKey()); {
	case err != nil:
		return e(err, "")
	case !found:
		return nil
	default:
		h = valuehash.NewBytes(b)
	}

	switch b, found, err := db.st.Get(stateDBKey(h)); {
	case err != nil:
		return e(err, "")
	case !found:
		return e(nil, "suffrage state not found")
	default:
		st, err := db.decodeSuffrage(b)
		if err != nil {
			return e(err, "")
		}

		db.sufstt = st

		return nil
	}
}

func (db *TempRODatabase) decodeManifest(b []byte) (base.Manifest, error) {
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

func (db *TempRODatabase) decodeSuffrage(b []byte) (base.State, error) {
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
