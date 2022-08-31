package isaacdatabase

import (
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/storage"
	leveldbstorage "github.com/spikeekips/mitum/storage/leveldb"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/hint"
)

type TempLeveldb struct {
	*baseLeveldb
	mp     base.BlockMap // NOTE last blockmap
	sufst  base.State    // NOTE last suffrage state
	policy base.NetworkPolicy
	proof  base.SuffrageProof
}

func NewTempLeveldbFromPrefix(
	st *leveldbstorage.Storage,
	prefix []byte,
	encs *encoder.Encoders,
	enc encoder.Encoder,
) (*TempLeveldb, error) {
	pst := leveldbstorage.NewPrefixStorage(st, prefix)

	db := &TempLeveldb{
		baseLeveldb: newBaseLeveldb(pst, encs, enc),
	}

	if err := db.loadLastBlockMap(); err != nil {
		return nil, err
	}

	if err := db.loadSuffrageState(); err != nil {
		return nil, err
	}

	if err := db.loadSuffrageProof(); err != nil {
		return nil, err
	}

	if err := db.loadNetworkPolicy(); err != nil {
		return nil, err
	}

	return db, nil
}

func newTempLeveldbFromBlockWriteStorage(wst *LeveldbBlockWrite) (*TempLeveldb, error) {
	e := util.StringErrorFunc("failed new TempLeveldbDatabase from TempLeveldbDatabase")

	var mp base.BlockMap

	switch i, err := wst.BlockMap(); {
	case err != nil:
		return nil, e(err, "")
	default:
		mp = i
	}

	sufst := wst.SuffrageState()
	policy := wst.NetworkPolicy()

	var proof base.SuffrageProof
	if i, _ := wst.proof.Value(); i != nil {
		proof = i.(base.SuffrageProof) //nolint:forcetypeassert //...
	}

	return &TempLeveldb{
		baseLeveldb: wst.baseLeveldb,
		mp:          mp,
		sufst:       sufst,
		policy:      policy,
		proof:       proof,
	}, nil
}

func (db *TempLeveldb) Close() error {
	if err := db.baseLeveldb.Close(); err != nil {
		return err
	}

	db.Lock()
	defer db.Unlock()

	db.clean()

	return nil
}

func (db *TempLeveldb) Remove() error {
	db.Lock()
	defer db.Unlock()

	if db.mp == nil {
		return nil
	}

	if err := removeLowerHeights(db.st.Storage, db.mp.Manifest().Height()+1); err != nil {
		return err
	}

	db.clean()

	return nil
}

func (db *TempLeveldb) clean() {
	db.mp = nil
	db.sufst = nil
	db.policy = nil
	db.proof = nil
}

func (db *TempLeveldb) Height() base.Height {
	if db.mp == nil {
		return base.NilHeight
	}

	return db.mp.Manifest().Height()
}

func (db *TempLeveldb) SuffrageHeight() base.Height {
	if db.sufst == nil {
		return base.NilHeight
	}

	return db.sufst.Value().(base.SuffrageNodesStateValue).Height() //nolint:forcetypeassert //...
}

func (db *TempLeveldb) BlockMap() (base.BlockMap, error) {
	if db.mp == nil {
		return nil, storage.NotFoundError.Errorf("blockmap not found")
	}

	return db.mp, nil
}

func (db *TempLeveldb) SuffrageProof() (base.SuffrageProof, bool, error) {
	if db.proof == nil {
		return nil, false, nil
	}

	return db.proof, true, nil
}

func (db *TempLeveldb) NetworkPolicy() base.NetworkPolicy {
	return db.policy
}

func (db *TempLeveldb) State(key string) (st base.State, found bool, err error) {
	found, err = db.getRecord(leveldbStateKey(key), db.st.Get, &st)

	return st, found, err
}

func (db *TempLeveldb) StateBytes(key string) (enchint hint.Hint, meta, body []byte, found bool, err error) {
	return db.getRecordBytes(leveldbStateKey(key), db.st.Get)
}

func (db *TempLeveldb) ExistsInStateOperation(h util.Hash) (bool, error) {
	return db.existsInStateOperation(h)
}

func (db *TempLeveldb) ExistsKnownOperation(h util.Hash) (bool, error) {
	return db.existsKnownOperation(h)
}

func (db *TempLeveldb) loadLastBlockMap() error {
	switch m, err := db.baseLeveldb.loadLastBlockMap(); {
	case err != nil:
		return err
	case m == nil:
		return util.ErrNotFound.Errorf("last BlockMap not found")
	default:
		db.mp = m

		return nil
	}
}

func (db *TempLeveldb) loadSuffrageState() error {
	e := util.StringErrorFunc("failed to load suffrage state")

	switch b, found, err := db.st.Get(leveldbStateKey(isaac.SuffrageStateKey)); {
	case err != nil:
		return e(err, "")
	case !found:
		return nil
	default:
		st, err := db.decodeSuffrage(b)
		if err != nil {
			return e(err, "")
		}

		db.sufst = st

		return nil
	}
}

func (db *TempLeveldb) loadSuffrageProof() error {
	e := util.StringErrorFunc("failed to load SuffrageProof")

	switch b, found, err := db.st.Get(leveldbKeySuffrageProof); {
	case err != nil:
		return e(err, "")
	case !found:
		return nil
	default:
		var proof base.SuffrageProof

		if _, err := db.readHinter(b, &proof); err != nil {
			return e(err, "")
		}

		db.proof = proof

		return nil
	}
}

func (db *TempLeveldb) loadNetworkPolicy() error {
	switch policy, found, err := db.baseLeveldb.loadNetworkPolicy(); {
	case err != nil:
		return err
	case !found:
		return nil
	default:
		db.policy = policy

		return nil
	}
}
