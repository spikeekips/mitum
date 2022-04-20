package database

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
	height base.Height
	mp     *util.Locked // NOTE blockdatamap
	sufstt *util.Locked // NOTE suffrage state
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
		sufstt:      util.EmptyLocked(),
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

	worker := util.NewErrgroupWorker(context.Background(), math.MaxInt32)
	defer worker.Close()

	var suffragestate base.State
	go func() {
		defer worker.Done()

	end:
		for i := range sts {
			st := sts[i]

			if err := base.IsSuffrageState(st); err == nil && st.Key() == isaac.SuffrageStateKey {
				suffragestate = st
			}

			err := worker.NewJob(func(context.Context, uint64) error {
				if err := db.setState(st); err != nil {
					return e(err, "")
				}

				return nil
			})
			if err != nil {
				break
			}

			ops := st.Operations()
			for j := range ops {
				op := ops[j]
				err := worker.NewJob(func(context.Context, uint64) error {
					if err := db.st.Put(leveldbInStateOperationKey(op), op.Bytes(), nil); err != nil {
						return e(err, "")
					}

					return nil
				})
				if err != nil {
					break end
				}
			}
		}
	}()

	if err := worker.Wait(); err != nil {
		return e(err, "")
	}

	if suffragestate != nil {
		if _, err := db.sufstt.Set(func(i interface{}) (interface{}, error) {
			if err := db.st.Put(leveldbKeyPrefixSuffrage, []byte(suffragestate.Key()), nil); err != nil {
				return nil, errors.Wrap(err, "failed to put suffrage state")
			}

			return suffragestate, nil
		}); err != nil {
			return e(err, "failed to put suffrage state")
		}
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
	go func() {
		defer worker.Done()

		for i := range ops {
			op := ops[i]
			err := worker.NewJob(func(context.Context, uint64) error {
				if err := db.st.Put(leveldbKnownOperationKey(op), op.Bytes(), nil); err != nil {
					return e(err, "")
				}

				return nil
			})
			if err != nil {
				break
			}
		}
	}()

	if err := worker.Wait(); err != nil {
		return e(err, "")
	}

	return nil
}

func (db *LeveldbBlockWrite) Map() (base.BlockDataMap, error) {
	switch i, isnil := db.mp.Value(); {
	case isnil || i == nil:
		return nil, storage.NotFoundError.Errorf("empty blockdatamap")
	default:
		return i.(base.BlockDataMap), nil
	}
}

func (db *LeveldbBlockWrite) SetMap(m base.BlockDataMap) error {
	if _, err := db.mp.Set(func(interface{}) (interface{}, error) {
		b, err := db.marshal(m)
		if err != nil {
			return nil, err
		}

		return m, db.st.Put(leveldbKeyPrefixBlockDataMap, b, nil)
	}); err != nil {
		return errors.Wrap(err, "failed to set blockdatamap")
	}

	return nil
}

func (db *LeveldbBlockWrite) SuffrageState() base.State {
	i, isnil := db.sufstt.Value()
	if isnil {
		return nil
	}

	return i.(base.State)
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

	switch m, err := db.Map(); {
	case err != nil:
		return nil, e(err, "")
	case m.Manifest().Height() != db.height:
		return nil, e(nil, "wrong blockdatamap")
	}

	return newTempLeveldbFromBlockWriteStorage(db)
}

func (db *LeveldbBlockWrite) setState(st base.State) error {
	if st.Height() != db.height {
		return errors.Errorf("wrong state height")
	}

	b, err := db.marshal(st)
	if err != nil {
		return errors.Wrap(err, "failed to set state")
	}

	if err := db.st.Put(leveldbStateKey(st.Key()), b, nil); err != nil {
		return errors.Errorf("failed to put state")
	}

	return nil
}
