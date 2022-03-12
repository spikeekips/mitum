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
	st     *leveldbstorage.BatchStorage // BLOCK just use write storage, not BatchStorage
	height base.Height
	m      base.Manifest // NOTE manifest
	sufstt base.State    // NOTE suffrage state
	done   bool
}

func NewTempWODatabase(
	height base.Height,
	f string,
	encs *encoder.Encoders,
	enc encoder.Encoder,
) (*TempWODatabase, error) {
	st, err := leveldbstorage.NewBatchStorage(f)
	if err != nil {
		return nil, errors.Wrap(err, "failed new TempWODatabase")
	}

	return newTempWODatabase(st, height, encs, enc), nil
}

func newTempWODatabase(
	st *leveldbstorage.BatchStorage,
	height base.Height,
	encs *encoder.Encoders,
	enc encoder.Encoder,
) *TempWODatabase {
	return &TempWODatabase{
		baseDatabase: newBaseDatabase(st, encs, enc),
		st:           st,
		height:       height,
	}
}

func (db *TempWODatabase) Cancel() error {
	if err := db.Remove(); err != nil {
		return errors.Wrap(err, "failed to cancel TempWODatabase")
	}

	return nil
}

func (db *TempWODatabase) Manifest() (base.Manifest, error) {
	if db.m == nil {
		return nil, storage.NotFoundError.Errorf("empty manifest")
	}

	return db.m, nil
}

func (db *TempWODatabase) SetManifest(m base.Manifest) error {
	db.Lock()
	defer db.Unlock()

	if err := db.setManifest(m); err != nil {
		db.m = nil

		return err
	}

	db.m = m

	return nil
}

func (db *TempWODatabase) setManifest(m base.Manifest) error {
	e := util.StringErrorFunc("failed to set manifest")
	if m.Height() != db.height {
		return e(nil, "wrong manifest height")
	}

	b, err := db.marshal(m)
	if err != nil {
		return e(err, "failed to marshal manifest")
	}

	db.st.Put(manifestDBKey(), b)
	db.done = false

	return nil
}

func (db *TempWODatabase) SetStates(sts []base.State) error {
	db.Lock()
	defer db.Unlock()

	if len(sts) < 1 {
		return nil
	}

	e := util.StringErrorFunc("failed to set states in TempWODatabase")

	worker := util.NewErrgroupWorker(context.Background(), math.MaxInt16)
	defer worker.Close()

	var suffragestate base.State
	go func() {
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

				db.st.Put(stateDBKey(st.Key()), b)

				return nil
			})
			if err != nil {
				break
			}
		}

		worker.Done()
	}()

	if err := worker.Wait(); err != nil {
		return e(err, "")
	}

	if suffragestate != nil {
		db.st.Put(suffrageDBKey(), []byte(suffragestate.Key()))
		db.sufstt = suffragestate
	}

	db.done = false

	return nil
}

func (db *TempWODatabase) SetOperations(ops []util.Hash) error {
	if len(ops) < 1 {
		return nil
	}

	for i := range ops {
		db.st.Put(operationDBKey(ops[i]), nil)
	}

	db.done = false

	return nil
}

func (db *TempWODatabase) Write() error {
	db.Lock()
	defer db.Unlock()

	if err := db.st.Write(); err != nil {
		return errors.Wrap(err, "failed to write to TempWODatabase")
	}

	db.done = true

	return nil
}

func (db *TempWODatabase) TempDatabase() (TempDatabase, error) {
	db.Lock()
	defer db.Unlock()

	switch {
	case !db.done:
		return nil, errors.Errorf("failed to make TempDatabase from BlockWriteDatabase; not yet done")
	case db.m == nil:
		return nil, errors.Errorf("failed to make TempDatabase from BlockWriteDatabase; empty manifest")
	case db.m.Height() != db.height:
		return nil, errors.Errorf("failed to make TempDatabase from BlockWriteDatabase; wrong manifest")
	}

	return newTempRODatabaseFromWOStorage(db)
}
