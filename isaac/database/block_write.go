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
	st     *leveldbstorage.WriteStorage
	mp     *util.Locked
	sufst  *util.Locked
	policy *util.Locked
	proof  *util.Locked
	height base.Height
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
	}
}

func (db *LeveldbBlockWrite) Cancel() error {
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

	workch := make(chan util.ContextWorkerCallback)

	go func() {
		defer close(workch)

		for i := range sts {
			st := sts[i]

			workch <- func(context.Context, uint64) error {
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
			}
		}
	}()

	if err := util.RunErrgroupWorkerByChan(context.Background(), workch); err != nil {
		return e(err, "")
	}

	return nil
}

func (db *LeveldbBlockWrite) SetOperations(ops []util.Hash) error {
	if len(ops) < 1 {
		return nil
	}

	worker := util.NewErrgroupWorker(context.Background(), math.MaxInt32)
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
		if i != nil {
			return i, nil
		}

		b, err := db.marshal(m)
		if err != nil {
			return nil, err
		}

		return m, db.st.Put(leveldbKeyPrefixBlockMap, b, nil)
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

	return i.(base.NetworkPolicy) //nolint:forcetypeassert //...
}

func (db *LeveldbBlockWrite) SetSuffrageProof(proof base.SuffrageProof) error {
	if _, err := db.proof.Set(func(i interface{}) (interface{}, error) {
		if i != nil {
			return i, nil
		}

		b, err := db.marshal(proof)
		if err != nil {
			return nil, err
		}

		return proof, db.st.Put(leveldbKeySuffrageProof, b, nil)
	}); err != nil {
		return errors.Wrap(err, "failed to set SuffrageProof")
	}

	return nil
}

func (db *LeveldbBlockWrite) Write() error {
	db.Lock()
	defer db.Unlock()

	if err := db.st.Write(); err != nil {
		return errors.Wrap(err, "failed to write to TempLeveldbDatabase")
	}

	return nil
}

func (db *LeveldbBlockWrite) TempDatabase() (isaac.TempDatabase, error) {
	db.Lock()
	defer db.Unlock()

	e := util.StringErrorFunc("failed to make TempDatabase from BlockWriteDatabase")

	switch m, err := db.BlockMap(); {
	case err != nil:
		return nil, e(err, "")
	case m.Manifest().Height() != db.height:
		return nil, e(nil, "wrong blockmap")
	}

	return newTempLeveldbFromBlockWriteStorage(db)
}

func (db *LeveldbBlockWrite) setState(st base.State) error {
	e := util.StringErrorFunc("failed to set state")

	if st.Height() != db.height {
		return e(nil, "wrong state height")
	}

	b, err := db.marshal(st)
	if err != nil {
		return errors.Wrap(err, "failed to set state")
	}

	switch {
	case base.IsSuffrageState(st) && st.Key() == isaac.SuffrageStateKey:
		_ = db.sufst.SetValue(st)
	case base.IsNetworkPolicyState(st) && st.Key() == isaac.NetworkPolicyStateKey:
		_ = db.policy.SetValue(st.Value().(base.NetworkPolicyStateValue).Policy()) //nolint:forcetypeassert //...
	}

	if err := db.st.Put(leveldbStateKey(st.Key()), b, nil); err != nil {
		return e(err, "")
	}

	return nil
}
