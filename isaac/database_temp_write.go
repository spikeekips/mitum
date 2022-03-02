package isaac

import (
	"context"
	"math"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	leveldbstorage "github.com/spikeekips/mitum/storage/leveldb"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
)

type TempWODatabase struct {
	*baseDatabase
	st     *leveldbstorage.BatchStorage
	height base.Height
	m      base.Manifest // NOTE manifest
	sufstt base.State    // NOTE suffrage state
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

			if _, ok := st.Value().(base.SuffrageStateValue); ok {
				suffragestate = st
			}

			err := worker.NewJob(func(context.Context, uint64) error {
				if st.Height() != db.height {
					return e(nil, "wrong state height")
				}

				b, err := db.marshal(st)
				if err != nil {
					return errors.Wrap(err, "failed to set state")
				}

				db.st.Put(stateDBKey(st.Hash()), b)

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

	db.st.Put(suffrageDBKey(), suffragestate.Hash().Bytes())
	db.sufstt = suffragestate

	return nil
}

func (db *TempWODatabase) SetOperations(ops []util.Hash) error {
	if len(ops) < 1 {
		return nil
	}

	for i := range ops {
		db.st.Put(operationDBKey(ops[i]), nil)
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

func (db *TempWODatabase) ToRO() (*TempRODatabase, error) {
	db.Lock()
	defer db.Unlock()

	return newTempRODatabaseFromWOStorage(db)
}
