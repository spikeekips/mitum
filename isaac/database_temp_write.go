package isaac

import (
	"context"
	"math"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/storage"
	leveldbstorage "github.com/spikeekips/mitum/storage/leveldb"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
)

type TempWODatabase struct {
	*baseDatabase
	st     *leveldbstorage.WriteStorage // BLOCK just use write storage, not BatchStorage
	height base.Height
	m      *util.Locked // NOTE manifest
	sufstt *util.Locked // NOTE suffrage state
}

func NewTempWODatabase(
	height base.Height,
	f string,
	encs *encoder.Encoders,
	enc encoder.Encoder,
) (*TempWODatabase, error) {
	st, err := leveldbstorage.NewWriteStorage(f)
	if err != nil {
		return nil, errors.Wrap(err, "failed new TempWODatabase")
	}

	return newTempWODatabase(st, height, encs, enc), nil
}

func newTempWODatabase(
	st *leveldbstorage.WriteStorage,
	height base.Height,
	encs *encoder.Encoders,
	enc encoder.Encoder,
) *TempWODatabase {
	return &TempWODatabase{
		baseDatabase: newBaseDatabase(st, encs, enc),
		st:           st,
		height:       height,
		m:            util.NewLocked(nil),
		sufstt:       util.NewLocked(nil),
	}
}

func (db *TempWODatabase) Cancel() error {
	if err := db.Remove(); err != nil {
		return errors.Wrap(err, "failed to cancel TempWODatabase")
	}

	return nil
}

func (db *TempWODatabase) Manifest() (base.Manifest, error) {
	var m base.Manifest
	switch _ = db.m.Value(&m); {
	case m == nil:
		return nil, storage.NotFoundError.Errorf("empty manifest")
	default:
		return m, nil
	}
}

func (db *TempWODatabase) SetManifest(m base.Manifest) error {
	if err := db.m.Set(func(i interface{}) (interface{}, error) {
		if m.Height() != db.height {
			return nil, errors.Errorf("wrong manifest height")
		}

		b, err := db.marshal(m)
		if err != nil {
			return nil, errors.Errorf("failed to marshal manifest")
		}

		return m, db.st.Put(manifestDBKey(), b, nil)
	}); err != nil {
		return errors.Wrap(err, "failed to set manifest")
	}

	return nil
}

func (db *TempWODatabase) SetStates(sts []base.State) error {
	if len(sts) < 1 {
		return nil
	}

	e := util.StringErrorFunc("failed to set states in TempWODatabase")

	worker := util.NewErrgroupWorker(context.Background(), math.MaxInt16)
	defer worker.Close()

	var suffragestate base.State
	go func() {
		defer worker.Done()

		for i := range sts {
			st := sts[i]

			_, issuffragestatevalue := st.Value().(base.SuffrageStateValue)
			if st.Key() == base.SuffrageStateKey && issuffragestatevalue {
				suffragestate = st
			}

			err := worker.NewJob(func(context.Context, uint64) error {
				if st.Height() != db.height {
					return e(nil, "wrong state height")
				}

				switch {
				case st.Key() == base.SuffrageStateKey && !issuffragestatevalue:
					return e(nil, "invalid suffrage state; not SuffrageStateValue, %T", st.Value())
				case st.Key() != base.SuffrageStateKey && issuffragestatevalue:
					return e(nil, "invalid state value; value is SuffrageStateValue, but state key is not suffrage state")
				}

				b, err := db.marshal(st)
				if err != nil {
					return errors.Wrap(err, "failed to set state")
				}

				if err := db.st.Put(stateDBKey(st.Key()), b, nil); err != nil {
					return e(err, "failed to put state")
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

	if suffragestate != nil {
		if err := db.sufstt.Set(func(i interface{}) (interface{}, error) {
			if err := db.st.Put(suffrageDBKey(), []byte(suffragestate.Key()), nil); err != nil {
				return nil, errors.Wrap(err, "failed to put suffrage state")
			}

			return suffragestate, nil
		}); err != nil {
			return e(err, "failed to put suffrage state")
		}
	}

	return nil
}

func (db *TempWODatabase) SetOperations(ops []util.Hash) error {
	if len(ops) < 1 {
		return nil
	}

	worker := util.NewErrgroupWorker(context.Background(), math.MaxInt16)
	defer worker.Close()

	e := util.StringErrorFunc("failed to set operation")
	go func() {
		defer worker.Done()

		for i := range ops {
			op := ops[i]
			err := worker.NewJob(func(context.Context, uint64) error {
				if err := db.st.Put(operationDBKey(op), nil, nil); err != nil {
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

func (db *TempWODatabase) Write() error {
	db.Lock()
	defer db.Unlock()

	if err := db.st.Write(); err != nil {
		return errors.Wrap(err, "failed to write to TempWODatabase")
	}

	return nil
}

func (db *TempWODatabase) TempDatabase() (TempDatabase, error) {
	db.Lock()
	defer db.Unlock()

	e := util.StringErrorFunc("failed to make TempDatabase from BlockWriteDatabase")

	switch m, err := db.Manifest(); {
	case err != nil:
		return nil, e(err, "")
	case m.Height() != db.height:
		return nil, e(nil, "wrong manifest")
	}

	return newTempRODatabaseFromWOStorage(db)
}
