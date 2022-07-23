package isaacdatabase

import (
	"context"
	"math"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/storage"
	leveldbstorage "github.com/spikeekips/mitum/storage/leveldb"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
)

type LeveldbBlockWrite struct {
	*baseLeveldb
	st         *leveldbstorage.WriteStorage
	mp         *util.Locked
	sufst      *util.Locked
	policy     *util.Locked
	proof      *util.Locked
	laststates *util.ShardedMap
	height     base.Height
}

func NewLeveldbBlockWrite(
	height base.Height,
	f string,
	encs *encoder.Encoders,
	enc encoder.Encoder,
) (*LeveldbBlockWrite, error) {
	st, err := leveldbstorage.NewWriteStorage(f)
	if err != nil {
		return nil, errors.Wrap(err, "failed new TempLeveldbDatabase")
	}

	return newLeveldbBlockWrite(st, height, encs, enc), nil
}

func newLeveldbBlockWrite(
	st *leveldbstorage.WriteStorage,
	height base.Height,
	encs *encoder.Encoders,
	enc encoder.Encoder,
) *LeveldbBlockWrite {
	return &LeveldbBlockWrite{
		baseLeveldb: newBaseLeveldb(st, encs, enc),
		st:          st,
		height:      height,
		mp:          util.EmptyLocked(),
		sufst:       util.EmptyLocked(),
		policy:      util.EmptyLocked(),
		proof:       util.EmptyLocked(),
		laststates:  util.NewShardedMap(math.MaxInt8),
	}
}

func (db *LeveldbBlockWrite) Close() error {
	if err := db.baseLeveldb.Close(); err != nil {
		return err
	}

	db.Lock()
	defer db.Unlock()

	db.mp = nil
	db.sufst = nil
	db.policy = nil
	db.proof = nil
	db.laststates = nil

	return nil
}

func (db *LeveldbBlockWrite) Cancel() error {
	db.laststates.Close()

	if err := db.Remove(); err != nil {
		return errors.Wrap(err, "failed to cancel TempLeveldbDatabase")
	}

	return nil
}

func (db *LeveldbBlockWrite) SetStates(sts []base.State) error {
	if len(sts) < 1 {
		return nil
	}

	e := util.StringErrorFunc("failed to set states in TempLeveldbDatabase")

	worker := util.NewErrgroupWorker(context.Background(), int64(len(sts)))
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

				if err := db.st.Put(leveldbInStateOperationKey(op), op.Bytes(), nil); err != nil {
					return err
				}
			}

			return nil
		}); err != nil {
			return e(err, "")
		}
	}

	worker.Done()

	if err := worker.Wait(); err != nil {
		return e(err, "")
	}

	return nil
}

func (db *LeveldbBlockWrite) SetOperations(ops []util.Hash) error {
	if len(ops) < 1 {
		return nil
	}

	worker := util.NewErrgroupWorker(context.Background(), int64(len(ops)))
	defer worker.Close()

	e := util.StringErrorFunc("failed to set operation")

	for i := range ops {
		op := ops[i]
		if op == nil {
			return e(nil, "empty operation hash")
		}

		if err := worker.NewJob(func(context.Context, uint64) error {
			if err := db.st.Put(leveldbKnownOperationKey(op), op.Bytes(), nil); err != nil {
				return e(err, "")
			}

			return nil
		}); err != nil {
			return e(err, "")
		}
	}

	worker.Done()

	if err := worker.Wait(); err != nil {
		return e(err, "")
	}

	return nil
}

func (db *LeveldbBlockWrite) BlockMap() (base.BlockMap, error) {
	switch i, _ := db.mp.Value(); {
	case i == nil:
		return nil, storage.NotFoundError.Errorf("empty blockmap")
	default:
		return i.(base.BlockMap), nil //nolint:forcetypeassert //...
	}
}

func (db *LeveldbBlockWrite) SetBlockMap(m base.BlockMap) error {
	if _, err := db.mp.Set(func(i interface{}) (interface{}, error) {
		b, err := db.marshal(m)
		if err != nil {
			return nil, err
		}

		if err := db.st.Put(leveldbBlockMapKey(m.Manifest().Height()), b, nil); err != nil {
			return nil, err
		}

		if i != nil && m.Manifest().Height() <= i.(base.BlockMap).Manifest().Height() { //nolint:forcetypeassert //...
			return i, nil
		}

		return m, nil
	}); err != nil {
		return errors.Wrap(err, "failed to set blockmap")
	}

	return nil
}

func (db *LeveldbBlockWrite) SuffrageState() base.State {
	i, _ := db.sufst.Value()
	if i == nil {
		return nil
	}

	return i.(base.State) //nolint:forcetypeassert //...
}

func (db *LeveldbBlockWrite) NetworkPolicy() base.NetworkPolicy {
	i, _ := db.policy.Value()
	if i == nil {
		return nil
	}

	return i.(base.State).Value().(base.NetworkPolicyStateValue).Policy() //nolint:forcetypeassert //...
}

func (db *LeveldbBlockWrite) SetSuffrageProof(proof base.SuffrageProof) error {
	if _, err := db.proof.Set(func(i interface{}) (interface{}, error) {
		b, err := db.marshal(proof)
		if err != nil {
			return nil, err
		}

		if err := db.st.Put(leveldbSuffrageProofKey(proof.SuffrageHeight()), b, nil); err != nil {
			return nil, err
		}

		if err := db.st.Put(leveldbSuffrageProofByBlockHeightKey(proof.Map().Manifest().Height()), b, nil); err != nil {
			return nil, err
		}

		if i != nil && proof.SuffrageHeight() <=
			i.(base.SuffrageProof).SuffrageHeight() { //nolint:forcetypeassert //...
			return i, nil
		}

		return proof, nil
	}); err != nil {
		return errors.Wrap(err, "failed to set SuffrageProof")
	}

	return nil
}

func (db *LeveldbBlockWrite) Write() error {
	db.Lock()
	defer db.Unlock()

	db.laststates.Close()

	if err := db.st.Write(); err != nil {
		return errors.Wrap(err, "failed to write to TempLeveldbDatabase")
	}

	return nil
}

func (db *LeveldbBlockWrite) TempDatabase() (isaac.TempDatabase, error) {
	db.Lock()
	defer db.Unlock()

	db.laststates.Close()

	e := util.StringErrorFunc("failed to make TempDatabase from BlockWriteDatabase")

	if _, err := db.BlockMap(); err != nil {
		return nil, e(err, "")
	}

	return newTempLeveldbFromBlockWriteStorage(db)
}

func (db *LeveldbBlockWrite) setState(st base.State) error {
	e := util.StringErrorFunc("failed to set state")

	if !db.isLastStates(st) {
		return nil
	}

	b, err := db.marshal(st)
	if err != nil {
		return errors.Wrap(err, "failed to set state")
	}

	switch {
	case base.IsSuffrageState(st) && st.Key() == isaac.SuffrageStateKey:
		db.updateLockedStates(st, db.sufst)
	case base.IsNetworkPolicyState(st) && st.Key() == isaac.NetworkPolicyStateKey:
		db.updateLockedStates(st, db.policy)
	}

	if err := db.st.Put(leveldbStateKey(st.Key()), b, nil); err != nil {
		return e(err, "")
	}

	return nil
}

func (db *LeveldbBlockWrite) isLastStates(st base.State) bool {
	var islast bool
	_, _ = db.laststates.Set(st.Key(), func(i interface{}) (interface{}, error) {
		if !util.IsNilLockedValue(i) && st.Height() <= i.(base.Height) { //nolint:forcetypeassert //...
			return nil, errors.Errorf("old")
		}

		islast = true

		return st.Height(), nil
	})

	return islast
}

func (*LeveldbBlockWrite) updateLockedStates(st base.State, locked *util.Locked) {
	_, _ = locked.Set(func(i interface{}) (interface{}, error) {
		if i != nil && st.Height() <= i.(base.State).Height() { //nolint:forcetypeassert //...
			return i, nil
		}

		return st, nil
	})
}
