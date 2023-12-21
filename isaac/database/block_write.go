package isaacdatabase

import (
	"bytes"
	"context"
	"math"
	"sync"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/storage"
	leveldbstorage "github.com/spikeekips/mitum/storage/leveldb"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/syndtr/goleveldb/leveldb"
	leveldbutil "github.com/syndtr/goleveldb/leveldb/util"
)

type LeveldbBlockWrite struct {
	*baseLeveldb
	mp                    *util.Locked[[3]interface{}]
	sufst                 *util.Locked[base.State]
	policy                *util.Locked[base.State]
	proof                 *util.Locked[[3]interface{}]
	laststates            *util.ShardedMap[string, base.Height]
	stcache               *util.GCache[string, [2]interface{}]
	instateoperationcache util.LockedMap[string, bool]
	height                base.Height
	sync.Mutex
}

func NewLeveldbBlockWrite(
	height base.Height,
	st *leveldbstorage.Storage,
	encs *encoder.Encoders,
	enc encoder.Encoder,
	stcachesize int,
) *LeveldbBlockWrite {
	pst := leveldbstorage.NewPrefixStorage(st, newPrefixStoragePrefixByHeight(leveldbLabelBlockWrite, height))

	laststates, _ := util.NewShardedMap[string, base.Height](math.MaxInt8, nil)

	var stcache *util.GCache[string, [2]interface{}]
	if stcachesize > 0 {
		stcache = util.NewLFUGCache[string, [2]interface{}](stcachesize)
	}

	return &LeveldbBlockWrite{
		baseLeveldb:           newBaseLeveldb(pst, encs, enc),
		height:                height,
		mp:                    util.EmptyLocked[[3]interface{}](),
		sufst:                 util.EmptyLocked[base.State](),
		policy:                util.EmptyLocked[base.State](),
		proof:                 util.EmptyLocked[[3]interface{}](),
		laststates:            laststates,
		stcache:               stcache,
		instateoperationcache: util.NewSingleLockedMap[string, bool](),
	}
}

func (db *LeveldbBlockWrite) Close() error {
	db.Lock()
	defer db.Unlock()

	if db.mp == nil {
		return nil
	}

	db.clean()

	return nil
}

func (db *LeveldbBlockWrite) Write() error {
	db.Lock()
	defer db.Unlock()

	db.laststates.Close()

	return nil
}

func (db *LeveldbBlockWrite) clean() {
	db.mp.EmptyValue()
	db.sufst.EmptyValue()
	db.policy.EmptyValue()
	db.proof.EmptyValue()
	db.mp = nil
	db.sufst = nil
	db.policy = nil
	db.proof = nil

	if db.laststates != nil {
		db.laststates.Close()
	}
}

func (db *LeveldbBlockWrite) Cancel() error {
	db.Lock()
	defer db.Unlock()

	if db.mp == nil {
		return nil
	}

	db.clean()

	if db.stcache != nil {
		db.stcache.Purge()
	}

	db.instateoperationcache.Close()

	return nil
}

func (db *LeveldbBlockWrite) SetStates(sts []base.State) error {
	if len(sts) < 1 {
		return nil
	}

	e := util.StringError("set states in TempLeveldbDatabase")

	pst, err := db.st()
	if err != nil {
		return e.Wrap(err)
	}

	worker, err := util.NewErrgroupWorker(context.Background(), int64(len(sts)))
	if err != nil {
		return e.Wrap(err)
	}

	defer worker.Close()

	for i := range sts {
		st := sts[i]

		if err := worker.NewJob(func(context.Context, uint64) error {
			if err := db.setState(st); err != nil {
				return err
			}

			ops := st.Operations()

			for j := range ops {
				op := ops[j]

				if err := pst.Put(leveldbInStateOperationKey(op), []byte(op.String()), nil); err != nil {
					return err
				}

				db.instateoperationcache.SetValue(op.String(), true)
			}

			return nil
		}); err != nil {
			return e.Wrap(err)
		}
	}

	worker.Done()

	if err := worker.Wait(); err != nil {
		return e.Wrap(err)
	}

	return nil
}

func (db *LeveldbBlockWrite) SetOperations(ops []util.Hash) error {
	if len(ops) < 1 {
		return nil
	}

	pst, err := db.st()
	if err != nil {
		return err
	}

	worker, err := util.NewErrgroupWorker(context.Background(), int64(len(ops)))
	if err != nil {
		return err
	}

	defer worker.Close()

	e := util.StringError("set operation")

	for i := range ops {
		op := ops[i]
		if op == nil {
			return e.Errorf("empty operation hash")
		}

		if err := worker.NewJob(func(context.Context, uint64) error {
			if err := pst.Put(leveldbKnownOperationKey(op), op.Bytes(), nil); err != nil {
				return e.Wrap(err)
			}

			return nil
		}); err != nil {
			return e.Wrap(err)
		}
	}

	worker.Done()

	if err := worker.Wait(); err != nil {
		return e.Wrap(err)
	}

	return nil
}

func (db *LeveldbBlockWrite) BlockMap() (base.BlockMap, error) {
	switch i, _, _ := db.blockmaps(); {
	case i == nil:
		return nil, storage.ErrNotFound.Errorf("empty blockmap")
	default:
		return i, nil
	}
}

func (db *LeveldbBlockWrite) BlockMapBytes() (enchint string, meta, body []byte, err error) {
	switch _, meta, i := db.blockmaps(); {
	case i == nil:
		return enchint, nil, nil, storage.ErrNotFound.Errorf("empty blockmap")
	default:
		return db.enc.Hint().String(), meta, i, nil //nolint:forcetypeassert //...
	}
}

func (db *LeveldbBlockWrite) SetBlockMap(m base.BlockMap) error {
	if m.Manifest().Height() != db.height {
		return errors.Errorf("wrong height of BlockMap")
	}

	pst, err := db.st()
	if err != nil {
		return err
	}

	if _, err := db.mp.Set(func(i [3]interface{}, isempty bool) (v [3]interface{}, _ error) {
		if !isempty {
			if m.Manifest().Height() <= i[0].(base.BlockMap).Manifest().Height() { //nolint:forcetypeassert //...
				return v, util.ErrLockedSetIgnore
			}
		}

		meta := m.Manifest().Hash().Bytes()

		switch marshaled, b, err := EncodeOneHeaderFrame(db.enc, meta, m); {
		case err != nil:
			return v, err
		default:
			if err := pst.Put(leveldbBlockMapKey(m.Manifest().Height()), b, nil); err != nil {
				return v, err
			}

			return [3]interface{}{m, meta, marshaled}, nil
		}
	}); err != nil {
		return errors.Wrap(err, "set blockmap")
	}

	return nil
}

func (db *LeveldbBlockWrite) SuffrageState() base.State {
	i, _ := db.sufst.Value()

	return i
}

func (db *LeveldbBlockWrite) NetworkPolicy() base.NetworkPolicy {
	i, _ := db.policy.Value()
	if i == nil {
		return nil
	}

	return i.Value().(base.NetworkPolicyStateValue).Policy() //nolint:forcetypeassert //...
}

func (db *LeveldbBlockWrite) SetSuffrageProof(proof base.SuffrageProof) error {
	pst, err := db.st()
	if err != nil {
		return err
	}

	if _, err := db.proof.Set(func(i [3]interface{}, isempty bool) (v [3]interface{}, _ error) {
		switch {
		case isempty:
		case proof.SuffrageHeight() <= i[0].(base.SuffrageProof).SuffrageHeight(): //nolint:forcetypeassert //...
			return v, util.ErrLockedSetIgnore
		}

		var meta []byte
		if proof.Map().Manifest().Suffrage() != nil {
			meta = proof.Map().Manifest().Suffrage().Bytes()
		}

		switch marshaled, b, err := EncodeOneHeaderFrame(db.enc, meta, proof); {
		case err != nil:
			return v, err
		default:
			if err := pst.Put(leveldbSuffrageProofKey(proof.SuffrageHeight()), b, nil); err != nil {
				return v, err
			}

			if err := pst.Put(
				leveldbSuffrageProofByBlockHeightKey(proof.Map().Manifest().Height()),
				b, nil,
			); err != nil {
				return v, err
			}

			return [3]interface{}{proof, meta, marshaled}, nil
		}
	}); err != nil {
		return errors.Wrap(err, "set SuffrageProof")
	}

	return nil
}

func (db *LeveldbBlockWrite) TempDatabase() (isaac.TempDatabase, error) {
	e := util.StringError("make TempDatabase from BlockWriteDatabase")

	temp, err := func() (isaac.TempDatabase, error) {
		db.Lock()
		defer db.Unlock()

		if _, err := db.BlockMap(); err != nil {
			return nil, err
		}

		temp, err := newTempLeveldbFromBlockWriteStorage(db)
		if err != nil {
			return nil, errors.WithMessage(err, "new TempLeveldbDatabase from TempLeveldbDatabase")
		}

		return temp, nil
	}()
	if err != nil {
		return nil, e.Wrap(err)
	}

	if err := db.Close(); err != nil {
		return nil, e.Wrap(err)
	}

	return temp, nil
}

func (db *LeveldbBlockWrite) blockmaps() (base.BlockMap, []byte, []byte) {
	switch i, isempty := db.mp.Value(); {
	case isempty:
		return nil, nil, nil
	default:
		return i[0].(base.BlockMap), i[1].([]byte), i[2].([]byte) //nolint:forcetypeassert //...
	}
}

func (db *LeveldbBlockWrite) proofs() (base.SuffrageProof, []byte, []byte) {
	switch i, isempty := db.proof.Value(); {
	case isempty:
		return nil, nil, nil
	default:
		return i[0].(base.SuffrageProof), i[1].([]byte), i[2].([]byte) //nolint:forcetypeassert //...
	}
}

func (db *LeveldbBlockWrite) setState(st base.State) error {
	e := util.StringError("set state")

	if !db.isLastStates(st) {
		return nil
	}

	pst, err := db.st()
	if err != nil {
		return e.Wrap(err)
	}

	b, err := EncodeFrameState(db.enc, st)
	if err != nil {
		return err
	}

	switch {
	case base.IsSuffrageNodesState(st) && st.Key() == isaac.SuffrageStateKey:
		db.updateLockedStates(st, db.sufst)
	case base.IsNetworkPolicyState(st) && st.Key() == isaac.NetworkPolicyStateKey:
		db.updateLockedStates(st, db.policy)
	}

	if err := pst.Put(leveldbStateKey(st.Key()), b, nil); err != nil {
		return e.Wrap(err)
	}

	if db.stcache != nil {
		db.stcache.Set(st.Key(), [2]interface{}{st, true}, 0)
	}

	return nil
}

func (db *LeveldbBlockWrite) isLastStates(st base.State) bool {
	var islast bool
	_, _, _ = db.laststates.Set(st.Key(), func(i base.Height, found bool) (base.Height, error) {
		if found && st.Height() <= i {
			return base.NilHeight, errors.Errorf("old")
		}

		islast = true

		return st.Height(), nil
	})

	return islast
}

func (*LeveldbBlockWrite) updateLockedStates(st base.State, locked *util.Locked[base.State]) {
	_, _ = locked.Set(func(i base.State, _ bool) (base.State, error) {
		if i != nil && st.Height() <= i.Height() {
			return i, nil
		}

		return st, nil
	})
}

var (
	prefixStoragePrefixByHeightLength int = 10 + util.ULIDLen // len(label) + len(int64 bytes) + len(ULID.String())
	emptyULID                             = bytes.Repeat([]byte{0x00}, util.ULIDLen)
)

func newPrefixStoragePrefixByHeight(label leveldbstorage.KeyPrefix, height base.Height) []byte {
	return leveldbstorage.NewPrefixKey(label, height.Bytes(), []byte(util.ULID().String()))
}

func emptyPrefixStoragePrefixByHeight(label leveldbstorage.KeyPrefix, height base.Height) []byte {
	return leveldbstorage.NewPrefixKey(label, height.Bytes(), emptyULID)
}

func prefixStoragePrefixFromKey(b []byte) ([]byte, error) {
	if len(b) < prefixStoragePrefixByHeightLength {
		return nil, errors.Errorf("wrong key of prefix storage prefix")
	}

	return b[:prefixStoragePrefixByHeightLength], nil
}

// removeHigherHeights removes all the BlockWrite labeled data higher than
// height; including height
func removeHigherHeights(st *leveldbstorage.Storage, height base.Height) error {
	batch := &leveldb.Batch{}
	defer batch.Reset()

	r := leveldbutil.BytesPrefix(leveldbLabelBlockWrite[:])
	r.Start = emptyPrefixStoragePrefixByHeight(leveldbLabelBlockWrite, height)

	if _, err := leveldbstorage.BatchRemove(st, r, 333); err != nil { //nolint:gomnd //...
		return errors.WithMessage(err, "remove higher heights")
	}

	return nil
}
