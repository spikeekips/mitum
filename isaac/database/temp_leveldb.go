package isaacdatabase

import (
	"bytes"
	"sync"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/storage"
	leveldbstorage "github.com/spikeekips/mitum/storage/leveldb"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	leveldbutil "github.com/syndtr/goleveldb/leveldb/util"
)

type TempLeveldb struct {
	*baseLeveldb
	mp                    base.BlockMap // NOTE last blockmap
	sufst                 base.State    // NOTE last suffrage state
	policy                base.NetworkPolicy
	proof                 base.SuffrageProof
	stcache               util.GCache[string, [2]interface{}]
	instateoperationcache util.LockedMap[string, bool]
	proofmeta             []byte
	proofbody             []byte
	mpmeta                []byte // NOTE last blockmap bytes
	mpbody                []byte // NOTE last blockmap bytes
	l                     sync.Mutex
}

func NewTempLeveldbFromPrefix(
	st *leveldbstorage.Storage,
	prefix []byte,
	encs *encoder.Encoders,
	enc encoder.Encoder,
) (*TempLeveldb, error) {
	pst := leveldbstorage.NewPrefixStorage(st, prefix)

	db := &TempLeveldb{
		baseLeveldb:           newBaseLeveldb(pst, encs, enc),
		instateoperationcache: util.NewSingleLockedMap[string, bool](),
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

	if err := db.loadInStateOperations(); err != nil {
		return nil, err
	}

	return db, nil
}

func newTempLeveldbFromBlockWriteStorage(wst *LeveldbBlockWrite) (*TempLeveldb, error) {
	var mp base.BlockMap
	var mpmeta, mpb []byte

	switch i, meta, j := wst.blockmaps(); {
	case i == nil:
		return nil, errors.Errorf("empty blockmap")
	default:
		mp = i
		mpmeta = meta
		mpb = j
	}

	var proof base.SuffrageProof
	var proofmeta, proofbody []byte

	if i, meta, j := wst.proofs(); i != nil {
		proof = i
		proofmeta = meta
		proofbody = j
	}

	sufst := wst.SuffrageState()
	policy := wst.NetworkPolicy()

	db := &TempLeveldb{
		baseLeveldb: wst.baseLeveldb,
		mp:          mp,
		mpmeta:      mpmeta,
		mpbody:      mpb,
		sufst:       sufst,
		policy:      policy,
		proof:       proof,
		proofmeta:   proofmeta,
		proofbody:   proofbody,
		stcache:     wst.stcache,
	}

	if wst.instateoperationcache.Len() > 0 {
		db.instateoperationcache = wst.instateoperationcache
	}

	return db, nil
}

func (db *TempLeveldb) Close() error {
	db.l.Lock()
	defer db.l.Unlock()

	if err := db.baseLeveldb.Close(); err != nil {
		return err
	}

	db.clean()

	return nil
}

func (db *TempLeveldb) Remove() error {
	db.l.Lock()
	defer db.l.Unlock()

	if db.mp == nil {
		return nil
	}

	if err := db.baseLeveldb.Remove(); err != nil {
		return err
	}

	db.clean()

	return nil
}

func (db *TempLeveldb) Merge() error {
	pst, err := db.st()
	if err != nil {
		return err
	}

	if err := pst.Put(leveldbTempMergedKey(db.Height()), nil, nil); err != nil {
		return err
	}

	r := &leveldbutil.Range{
		Start: emptyPrefixStoragePrefixByHeight(leveldbLabelBlockWrite, db.Height()),   //nolint:mnd //...
		Limit: emptyPrefixStoragePrefixByHeight(leveldbLabelBlockWrite, db.Height()+1), //nolint:mnd //...
	}

	var lastprefix []byte
	var useless [][]byte

	if err := pst.Iter(
		r,
		func(key, _ []byte) (bool, error) {
			switch k, err := prefixStoragePrefixFromKey(key); {
			case err != nil:
			case bytes.Equal(k, db.Prefix()):
			case bytes.Equal(k, lastprefix):
			default:
				lastprefix = k

				useless = append(useless, k)
			}

			return true, nil
		},
		false,
	); err != nil {
		return err
	}

	if len(useless) < 1 {
		return nil
	}

	for i := range useless {
		if err := leveldbstorage.RemoveByPrefix(pst.Storage, useless[i]); err != nil {
			return err
		}
	}

	return nil
}

func (db *TempLeveldb) clean() {
	db.mp = nil
	db.mpmeta = nil
	db.mpbody = nil
	db.sufst = nil
	db.policy = nil
	db.proof = nil
	db.proofmeta = nil
	db.proofbody = nil

	if db.stcache != nil {
		db.stcache.Purge()
	}

	if db.instateoperationcache != nil {
		db.instateoperationcache.Close()
	}
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

func (db *TempLeveldb) LastBlockMap() (base.BlockMap, bool, error) {
	if db.mp == nil {
		return nil, false, storage.ErrNotFound.Errorf("blockmap not found")
	}

	return db.mp, true, nil
}

func (db *TempLeveldb) BlockMapBytes() (enchint string, meta, body []byte, _ error) {
	return db.enc.Hint().String(), db.mpmeta, db.mpbody, nil //nolint:forcetypeassert //...
}

func (db *TempLeveldb) SuffrageProof() (base.SuffrageProof, bool, error) {
	if db.proof == nil {
		return nil, false, nil
	}

	return db.proof, true, nil
}

func (db *TempLeveldb) LastSuffrageProofBytes() (enchint string, meta, body []byte, found bool, err error) {
	if db.proof == nil {
		return enchint, nil, nil, false, nil
	}

	return db.enc.Hint().String(), db.proofmeta, db.proofbody, true, nil
}

func (db *TempLeveldb) NetworkPolicy() base.NetworkPolicy {
	return db.policy
}

func (db *TempLeveldb) State(key string) (st base.State, found bool, err error) {
	switch incache, st, found := db.stateFromCache(key); {
	case !incache:
	default:
		return st, found, nil
	}

	switch st, found, err := db.state(key); {
	case err != nil:
		return nil, false, err
	default:
		db.setStateCache(key, st, found)

		return st, found, nil
	}
}

func (db *TempLeveldb) StateBytes(key string) (enchint string, meta, body []byte, found bool, err error) {
	pst, err := db.st()
	if err != nil {
		return enchint, nil, nil, false, err
	}

	switch b, found, err := pst.Get(leveldbStateKey(key)); {
	case err != nil, !found:
		return enchint, nil, nil, found, err
	default:
		enchint, meta, body, err := ReadOneHeaderFrame(b)

		return enchint, meta, body, true, err
	}
}

func (db *TempLeveldb) ExistsInStateOperation(h util.Hash) (bool, error) {
	if db.instateoperationcache == nil {
		return false, nil
	}

	found, _ := db.instateoperationcache.Value(h.String())

	return found, nil
}

func (db *TempLeveldb) ExistsKnownOperation(h util.Hash) (bool, error) {
	return db.existsKnownOperation(h)
}

func (db *TempLeveldb) isMerged() (bool, error) {
	pst, err := db.st()
	if err != nil {
		return false, err
	}

	return pst.Exists(leveldbTempMergedKey(db.Height()))
}

func (db *TempLeveldb) loadLastBlockMap() error {
	switch m, enchint, meta, body, err := db.baseLeveldb.loadLastBlockMap(); {
	case err != nil:
		return err
	case m == nil:
		return util.ErrNotFound.Errorf("last BlockMap not found")
	default:
		_, enc, found, err := db.encs.FindByString(enchint)
		if err != nil {
			return err
		}

		if !found {
			return errors.Errorf("encoder not found, %q", enchint)
		}

		db.enc = enc
		db.mp = m
		db.mpmeta = meta
		db.mpbody = body

		return nil
	}
}

func (db *TempLeveldb) loadSuffrageState() error {
	e := util.StringError("load suffrage state")

	switch st, found, err := db.State(isaac.SuffrageStateKey); {
	case err != nil:
		return e.Wrap(err)
	case !found:
		db.setStateCache(isaac.SuffrageStateKey, nil, false)

		return nil
	default:
		if !base.IsSuffrageNodesState(st) {
			return e.Errorf("not suffrage state")
		}

		db.sufst = st

		db.setStateCache(st.Key(), st, true)

		return nil
	}
}

func (db *TempLeveldb) loadSuffrageProof() error {
	e := util.StringError("load SuffrageProof")

	pst, err := db.st()
	if err != nil {
		return e.Wrap(err)
	}

	if err := pst.Iter(
		leveldbutil.BytesPrefix(leveldbKeySuffrageProof[:]),
		func(_ []byte, b []byte) (bool, error) {
			enchint, meta, body, err := ReadOneHeaderFrame(b)
			if err != nil {
				return false, err
			}

			var proof base.SuffrageProof

			if err := DecodeFrame(db.encs, enchint, body, &proof); err != nil {
				return false, err
			}

			db.proof = proof
			db.proofmeta = meta
			db.proofbody = body

			return false, nil
		},
		false,
	); err != nil {
		return e.Wrap(err)
	}

	return nil
}

func (db *TempLeveldb) loadNetworkPolicy() error {
	switch st, policy, found, err := db.baseLeveldb.loadNetworkPolicy(); {
	case err != nil:
		return err
	case !found:
		db.setStateCache(isaac.NetworkPolicyStateKey, nil, false)

		return nil
	default:
		db.policy = policy

		db.setStateCache(st.Key(), st, true)

		return nil
	}
}

func (db *TempLeveldb) loadInStateOperations() error {
	pst, err := db.st()
	if err != nil {
		return err
	}

	if err := pst.Iter(
		leveldbutil.BytesPrefix(leveldbKeyPrefixInStateOperation[:]),
		func(_ []byte, b []byte) (bool, error) {
			_ = db.instateoperationcache.SetValue(string(b), true)

			return true, nil
		},
		false,
	); err != nil {
		return err
	}

	if db.instateoperationcache.Len() < 1 {
		db.instateoperationcache = nil
	}

	return nil
}

func (db *TempLeveldb) state(key string) (st base.State, found bool, err error) {
	pst, err := db.st()
	if err != nil {
		return nil, false, err
	}

	switch b, found, err := pst.Get(leveldbStateKey(key)); {
	case err != nil, !found:
		return nil, found, err
	default:
		if err := ReadDecodeFrame(db.encs, b, &st); err != nil {
			return nil, true, err
		}

		return st, true, nil
	}
}

func (db *TempLeveldb) stateFromCache(key string) (bool, base.State, bool) {
	if db.stcache == nil {
		return false, nil, false
	}

	switch i, found := db.stcache.Get(key); {
	case !found:
		return false, nil, false
	case !i[1].(bool): //nolint:forcetypeassert //...
		return true, nil, false
	default:
		return true, i[0].(base.State), true //nolint:forcetypeassert //...
	}
}

func (db *TempLeveldb) setStateCache(key string, st base.State, found bool) {
	if db.stcache == nil {
		return
	}

	db.stcache.Set(key, [2]interface{}{st, found}, 0)
}

func (db *TempLeveldb) iterStateKeys(f func(string) (bool, error)) error {
	pst, err := db.st()
	if err != nil {
		return err
	}

	return pst.Iter(
		leveldbutil.BytesPrefix(leveldbKeyPrefixState[:]),
		func(key, _ []byte) (bool, error) {
			switch stateKey, err := stateKeyFromKey(key); {
			case err != nil:
				return false, err
			default:
				return f(stateKey)
			}
		},
		true,
	)
}
